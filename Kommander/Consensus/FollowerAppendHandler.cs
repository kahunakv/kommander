using Kommander.Data;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The follower side of AppendEntries: term and membership validation, leader adoption, the Log
/// Matching check, and enqueueing the batch to the WAL.
///
/// <para><b>The rejection carries this node's term, not an echo of the sender's.</b> It is the only
/// channel through which a deposed leader can learn a higher term exists — pre-vote correctly stops
/// the bumped node from winning an election against a healthy quorum, so without this the leader
/// ships batches forever while this replica's frontier stays frozen (the Jepsen per-partition term
/// wedge).</para>
///
/// <para><b>The Log Matching classification order is load-bearing.</b> Term match is tested BEFORE
/// the hole test, because a shared <c>-1</c> term is a snapshot boundary whose term is unknown after
/// compaction, not a hole. Testing the hole first misreads that boundary and truncates the batch the
/// leader just shipped, which it re-ships and the follower re-truncates forever — a livelock that
/// strands the follower exactly one entry below the boundary. The stale-read guard before the
/// truncate exists for the same reason: the truncate is irreversible, so a queued-but-unread append
/// must never look like a hole.</para>
///
/// <para><b>The hole repair never deletes below the advertised commit frontier.</b> The blanket
/// truncation is licensed by one premise: a row above an unfilled gap cannot have earned quorum
/// credit here, so this node's advertised frontier stays below the gap. The committed-frontier
/// fence tests that premise instead of assuming it — a frontier that reaches the anchor means the
/// node advertises a prefix it does not hold, and the repair falls back to a backfill anchored at
/// the contiguous presence frontier.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class FollowerAppendHandler
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ProposalRegistry proposals;
    private readonly RaftPartitionLogThrottle logThrottle;
    private readonly IRaftOperationReplySink replySink;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Adopts the sender as this term's leader and takes the durable step-down. Injected because the
    /// transition spans election, proposal and replication state; the same delegate serves the
    /// snapshot-install path so the two can never drift apart.
    /// </summary>
    private readonly Func<string, long, Task> adoptLeaderAsync;

    public FollowerAppendHandler(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ProposalRegistry proposals,
        RaftPartitionLogThrottle logThrottle,
        IRaftOperationReplySink replySink,
        ILogger<IRaft> logger,
        Func<string, long, Task> adoptLeaderAsync)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.proposals = proposals;
        this.logThrottle = logThrottle;
        this.replySink = replySink;
        this.logger = logger;
        this.adoptLeaderAsync = adoptLeaderAsync;
    }

    private void CompleteReply(ulong? correlationId, RaftResponse response)
    {
        if (correlationId is not null)
            replySink.TryComplete(correlationId.Value, response);
    }

    public async Task AppendLogsCoreAsync(
        string endpoint,
        long leaderTerm,
        HLCTimestamp timestamp,
        List<RaftLog>? logs,
        long prevLogIndex = 0,
        long prevLogTerm = 0,
        ulong? replyCorrelationId = null,
        bool quiesce = false
    )
    {
        if (coreState.CurrentTerm > leaderTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from a leader {Endpoint} with old ReceivedTerm={Term} CurrentTerm={CurrentTerm}. Ignoring...", host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, leaderTerm, coreState.CurrentTerm);

            // The rejection carries THIS node's coreState.CurrentTerm (not an echo of the sender's stale term):
            // it is the only channel through which a deposed leader can learn a higher term exists.
            // A node whose term was bumped by a failed election rejects every AppendLogs here — before
            // the WAL is ever touched — so if this reply echoed the stale term, the sender would keep
            // shipping batches forever ("send=True" every round) while this node's committed frontier
            // stays frozen, and pre-vote (correctly) prevents the higher term from propagating by
            // election against a healthy leader. Raft §5.1 closes the loop on the receiving side:
            // CompleteAppendLogsAsync steps down and adopts any response term above its own.
            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs, 
                new(endpoint), 
                new CompleteAppendLogsRequest(host.PartitionId, coreState.CurrentTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LeaderInOldTerm, -1)
            ));
            
            return;
        }

        // Membership fence: a valid AppendEntries authoritatively identifies a term's leader, but only a
        // committed roster member can legitimately be one. Without this, any endpoint reachable through
        // the transport (e.g. registered in the transport map before joining membership) could be adopted
        // as leader merely by sending logs with a fresh term, churning an established partition's
        // leadership. Skipped for the already-accepted leader so a roster snapshot that briefly lags a
        // role change cannot make a follower reject its real leader.
        if (host.Leader != endpoint && !host.IsMember(endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring AppendLogs from non-member {Endpoint} Term={Term}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, leaderTerm);

            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs,
                new(endpoint),
                new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogsFromAnotherLeader, -1)
            ));

            return;
        }

        // One-leader-per-term fence (Raft §5.2). While this node holds Leader state for leaderTerm,
        // its election quorum proves no other node can legitimately lead the same term, so an
        // equal-term AppendLogs from another endpoint is a protocol violation — not a leadership
        // announcement — and adopting the sender would abdicate real leadership and orphan the
        // term's in-flight replication. The observed source is a zombie broadcast: a deposed peer
        // whose queued WAL commit completion fanned out AppendLogs stamped with a term it adopted
        // but never won (the Caraxes run-J split-brain: the resumed node adopted the new term from
        // the elected leader's own traffic, then re-broadcast under it, and the elected leader
        // stepped down inside its own term). The fence deliberately covers ONLY the Leader state:
        // a follower's host.Leader legitimately lags one term behind (the vote path adopts a higher
        // term while keeping old leader knowledge until the winner's first AppendLogs), so for a
        // follower a same-term sender that conflicts with host.Leader is usually the real new
        // leader announcing itself and must still be adopted below.
        if (coreState.NodeState == RaftNodeState.Leader && coreState.CurrentTerm == leaderTerm)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Rejecting AppendLogs from {Endpoint} claiming leadership of our own term {Term} — one leader per term; not adopting.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, leaderTerm);

            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs,
                new(endpoint),
                new CompleteAppendLogsRequest(host.PartitionId, coreState.CurrentTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogsFromAnotherLeader, -1)
            ));

            return;
        }

        // leaderTerm >= coreState.CurrentTerm is guaranteed here (the coreState.CurrentTerm > leaderTerm case returned
        // above). A valid AppendEntries authoritatively identifies the single leader of leaderTerm,
        // so adopt it regardless of whom we voted for this term. Granting a vote to a candidate does
        // not make it the leader: under a vote split a different candidate can win the term with
        // another quorum, so gating leader acceptance on our vote record (expectedLeaders) would
        // make this follower reject the real leader forever and wedge the partition. expectedLeaders
        // must constrain voting only, never leader acceptance.
        if (host.Leader != endpoint || coreState.CurrentTerm != leaderTerm || coreState.NodeState != RaftNodeState.Follower)
        {
            // Identical to the snapshot-install adoption, and deliberately the same call: both are
            // leader RPCs that authoritatively identify the term's leader, so they must take exactly
            // the same durable step-down. Keeping two copies in step by hand is how one of them drifts.
            await adoptLeaderAsync(endpoint, leaderTerm).ConfigureAwait(false);
        }

        coreState.LastHeartbeat = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);
        // B3: a received AppendLogs (heartbeat or real batch) is the primary "we heard from the leader"
        // signal. Anchor the monotonic shadow to local now so the follower election gate measures the
        // silence interval on the local clock — this is the exact site whose HLC subtraction used to
        // freeze the timeout for the length of a leader's clock skew.
        coreState.LastHeartbeatTicks = host.GetMonotonicTimestamp();
        // A quiesce-flagged message tells us to stop expecting heartbeats and gate elections
        // on SWIM liveness instead.  Any non-quiesce AppendLogs (real logs or normal heartbeat)
        // wakes us back up by clearing the flag.
        coreState.SetQuiesced(quiesce);

        // Log Matching Property check: the follower must hold an entry at prevLogIndex whose
        // term equals prevLogTerm before it can safely append the incoming batch.
        //
        // Mismatch classification:
        //   * localTermAtPrev < 0 — hole: no entry exists at prevLogIndex. Holes arise because the
        //     live-propose path ships prevLogIndex=0 and skips contiguity, so an out-of-order batch
        //     can leave a gap below prevLogIndex on the follower. The repair truncates the orphaned
        //     tail above the gap so the leader heals it in one forward backfill pass instead of
        //     walking nextIndex down one slot at a time. This is safe by construction: a hole at
        //     prevLogIndex proves the committed prefix ends below it, so the truncated tail is
        //     necessarily uncommitted. The committed-frontier fence checks that construction holds
        //     before the delete and falls back to a backfill repair when it does not.
        //   * localTermAtPrev >= 0 && localTermAtPrev != prevLogTerm — genuine term divergence: an
        //     entry exists but belongs to a different term. The existing backtrack path is used
        //     unchanged; the leader decrements nextIndex and retries with an earlier anchor.
        //
        // GetAnyTermAtAsync is used (not GetRangeAsync) so that a Proposed-but-uncommitted entry at
        // prevLogIndex is matched correctly; GetRangeAsync filters uncommitted entries.
        if (prevLogIndex > 0 && logs is not null && logs.Count > 0)
        {
            long localMaxLog = await wal.GetMaxLogAsync().ConfigureAwait(false);

            if (prevLogIndex > localMaxLog)
            {
                // Follower is simply behind the leader's append point (prevLogIndex is ahead of our
                // tail). Backfill backtracks nextIndex and catches it up — benign and noisy under
                // high write concurrency, so this stays at Debug. Genuine divergence (a term mismatch
                // at an existing entry) is the Warning below.
                logger.LogDebugLogMatchingFollowerBehind(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, prevLogIndex, localMaxLog);

                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }

            long localTermAtPrev = await wal.GetAnyTermAtAsync(prevLogIndex).ConfigureAwait(false);

            // Classify the anchor by TERM MATCH first, before the hole test. A shared value — including
            // -1 == -1 — is a match and falls through to append. The -1 == -1 case is a snapshot boundary: the
            // follower's entry at prevLogIndex is a CommittedCheckpoint whose term is unknown after compaction
            // (LastIncludedTerm can be -1), and the leader anchors on its own equally-compacted boundary, so the
            // snapshot-covered prefix already agrees. Testing the hole (localTermAtPrev < 0) BEFORE the match
            // would misread that -1 boundary term as a hole and truncate the just-shipped anchored backfill,
            // which the leader re-ships and the follower re-truncates forever — a live-lock that strands the
            // follower exactly one entry below the boundary (and its consumer's applied prefix with it).
            if (localTermAtPrev != prevLogTerm)
            {
                if (localTermAtPrev < 0)
                {
                    // Stale-read guard: the backend reads race the WAL write queue, so an entry whose
                    // physical append is still queued reads as absent (-1) — a FALSE hole. The presence
                    // frontier advances at enqueue time and is contiguous, so it covering prevLogIndex
                    // proves an entry exists there; truncating on that stale read would enqueue the
                    // delete BEHIND the pending append and discard a possibly-committed entry. Report
                    // "behind" instead and let the leader retry against the durable log. Unreachable
                    // under strict per-partition write FIFO (the prevLogIndex > localMaxLog pre-check
                    // fires first) — kept as defense in depth because the truncate is irreversible.
                    long presentIndexAtAnchor = wal.GetPresentIndex();
                    if (presentIndexAtAnchor >= prevLogIndex)
                    {
                        if (logger.IsEnabled(LogLevel.Debug))
                            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] False-hole read at prevLogIndex={PrevLogIndex} (presence frontier {PresentIndex} covers it; append still queued) — deferring to backfill retry instead of truncating.",
                                host.LocalEndpoint, host.PartitionId, coreState.NodeState, prevLogIndex, presentIndexAtAnchor);
                        host.EnqueueResponse(endpoint, new(
                            RaftResponderRequestType.CompleteAppendLogs,
                            new(endpoint),
                            new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                        ));
                        return;
                    }

                    // Committed-frontier fence (defense in depth; the truncate below is irreversible).
                    // The safety argument for the blanket truncation rests on ONE premise: the rows above
                    // the gap never earned quorum credit here, so this node's own advertised commit
                    // frontier stays below prevLogIndex. Two mechanisms hold that premise up — the commit
                    // frontier only ever advances contiguously (AdvanceCommitFrontier buffers an over-gap
                    // id instead of jumping it), and the over-gap ack gate withholds the Success ack that
                    // would let a row above a gap count toward propose quorum. A frontier that reaches
                    // prevLogIndex means one of them failed: the node advertises a resolved prefix
                    // covering an id it does not hold, a leader may have counted a row that is about to be
                    // deleted, and the premise no longer licenses the delete. Refuse, alarm, and report
                    // the contiguous presence frontier so the leader's anchored backfill repairs the hole
                    // from BELOW instead — non-destructive, and it converges: once the backfill closes the
                    // gap the anchor matches and this branch is not reached again.
                    long advertisedCommitFrontier = wal.GetCommitIndex();
                    if (advertisedCommitFrontier >= prevLogIndex)
                    {
                        long repairAnchor = presentIndexAtAnchor >= 0 ? presentIndexAtAnchor : localMaxLog;
                        logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Refusing log-hole repair from {Endpoint} at prevLogIndex={PrevLogIndex}: the advertised commit frontier {Frontier} covers the hole, so the truncation could delete a Committed row this node already acked. Reporting LogMismatch anchored at {Anchor} for a backfill repair instead.",
                            host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, prevLogIndex, advertisedCommitFrontier, repairAnchor);
                        host.EnqueueResponse(endpoint, new(
                            RaftResponderRequestType.CompleteAppendLogs,
                            new(endpoint),
                            new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, repairAnchor)
                        ));
                        return;
                    }

                    // Hole: no entry exists at prevLogIndex even though prevLogIndex <= localMaxLog, so the
                    // follower's log has an internal gap. This proves the follower's truly committed prefix ends
                    // below prevLogIndex: the leader commits contiguously, so no entry above an unfilled gap can
                    // have been quorum-committed — any entry sitting above the gap is an orphan delivered out of
                    // order by the unanchored live-propose broadcast. Truncating that orphaned tail (everything
                    // after prevLogIndex-1) can therefore never discard committed data — the fence above proves
                    // the advertised frontier agrees. Reporting the post-truncation max lets the leader heal
                    // the gap in one forward backfill pass instead of walking nextIndex down one slot at a time.
                    long newMax = await wal.TruncateLogsAfterAsync(prevLogIndex - 1).ConfigureAwait(false);
                    logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Log-hole repair from {Endpoint}: prevLogIndex={PrevLogIndex} truncated to newMax={NewMax}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, prevLogIndex, newMax);
                    host.EnqueueResponse(endpoint, new(
                        RaftResponderRequestType.CompleteAppendLogs,
                        new(endpoint),
                        new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, newMax)
                    ));
                    return;
                }

                // Genuine term divergence: entry exists at prevLogIndex but belongs to a
                // different term. Leader backtracks nextIndex and retries with an earlier anchor.
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Log Matching rejection from {Endpoint}: prevLogIndex={PrevLogIndex} localTerm={LocalTerm} != prevLogTerm={PrevLogTerm}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, prevLogIndex, localTermAtPrev, prevLogTerm);
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }

            // Anchor-contiguity check (AppendEntries semantics: entries[] immediately follows
            // prevLogIndex). A matching anchor proves the shared prefix through prevLogIndex —
            // nothing more. A batch whose first entry sits ABOVE prevLogIndex+1 would be written
            // over a gap the anchor never vouched for; accepting it strands this follower's commit
            // frontier below the gap while its log grows (the Jepsen one-stuck-entry wedge) with
            // no signal anywhere. Only the backfill path sends anchored batches and it sends them
            // contiguous by construction, so this firing means the sender's read skipped entries
            // it believes committed but holds uncommitted — reject loudly and let it repair.
            // The unanchored live-propose broadcast (prevLogIndex == 0) is exempt by the enclosing
            // guard: out-of-order lone-high deliveries are its documented, frontier-buffered shape.
            long firstIncomingId = long.MaxValue;
            foreach (RaftLog incoming in logs)
            {
                if (incoming.Id < firstIncomingId)
                    firstIncomingId = incoming.Id;
            }

            if (firstIncomingId != prevLogIndex + 1)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Non-contiguous anchored batch from {Endpoint}: prevLogIndex={PrevLogIndex} but first entry is {FirstId} — rejecting.",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, prevLogIndex, firstIncomingId);
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }
        }

        if (logs is not null && logs.Count > 0)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugReceivedLogs(
                    host.LocalEndpoint,
                    host.PartitionId,
                    coreState.NodeState,
                    endpoint,
                    leaderTerm,
                    timestamp,
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );

            WALWriteOperation? operation;

            try
            {
                operation = wal.EnqueueProposeOrCommit(logs, timestamp, endpoint, leaderTerm);
            }
            catch (WAL.IO.BackpressureExceededException ex)
            {
                // The WAL queue for this partition is full, so these entries were not
                // accepted. Answer the leader instead of letting the exception escape to
                // the executor's catch-all: an unanswered append is indistinguishable from
                // a dropped message, so the leader re-sends on its next tick with no idea
                // the follower is saturated — and each escaped exception also costs a
                // logged stack trace, which is I/O this node is already short of.
                //
                // Reporting the local max lets the leader anchor its next attempt without
                // walking nextIndex backwards, exactly as the Log Matching rejections above
                // do. nextIndex is deliberately not advanced by this status on the leader,
                // so the retry rides the normal heartbeat/backfill cadence rather than
                // spinning against a queue that never gets a chance to drain.
                long saturatedMax = await wal.GetMaxLogAsync().ConfigureAwait(false);

                logThrottle.LogWalSaturated(endpoint, ex.CurrentDepth, saturatedMax);

                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.FollowerWalSaturated, saturatedMax)
                ));
                return;
            }

            if (operation is not null)
            {
                Scheduling.RaftPendingWalOperation pendingAppend = proposals.RentPending();
                pendingAppend.ReplyCorrelationId = replyCorrelationId;
                pendingAppend.Logs = logs;
                pendingAppend.Endpoint = endpoint;
                pendingAppend.Timestamp = timestamp;
                proposals.TrackPending(operation.OperationId, pendingAppend);
                return;
            }

            // Duplicate batch: the WAL planned nothing because every entry is already present (or
            // already resolved) locally. Do NOT stay silent — the proposal-retry path re-sends a
            // batch whose original ack may have been lost in a fault window, and without a fresh
            // ack the leader can never credit this peer and the proposal can never reach quorum.
            // Durability gate: re-ack only when the batch's max id is already durable in the
            // backend (GetMaxLogAsync reads durable rows, and the per-partition write FIFO means
            // everything below it landed too). If the original append is still queued, its own
            // completion will carry the ack — re-acking early would claim a durability the disk
            // does not yet have, and the single-fsync ticket releases on propose-quorum-DURABLE.
            long alreadyHeldMax = -1;
            foreach (RaftLog held in logs)
            {
                if (held.Id > alreadyHeldMax)
                    alreadyHeldMax = held.Id;
            }

            long durableMax = await wal.GetMaxLogAsync().ConfigureAwait(false);
            long presentIndexAtReack = wal.GetPresentIndex();
            if (durableMax >= alreadyHeldMax && (presentIndexAtReack < 0 || presentIndexAtReack >= alreadyHeldMax))
            {
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.Success,
                        wal.GetCommitIndex())
                ));
            }
            else if (durableMax >= alreadyHeldMax)
            {
                // Held above a gap: durable but not contiguously grounded. Same quorum-integrity
                // gate as the completion ack in CompleteFollowerAppend — a Success re-ack here
                // would count toward quorum for entries this node cannot defend in an election
                // (freshness advertises only the contiguous presence frontier). Report LogMismatch
                // anchored at the presence frontier so the leader backfills the gap; the retry
                // after that repair re-acks Success through the branch above.
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, presentIndexAtReack)
                ));
            }

            CompleteReply(replyCorrelationId, RaftResponseStatic.NoneResponse);
            return;
        }
        
        // A heartbeat ack carries the follower's TRUE committed frontier, in both commit modes.
        // This is the "leader's leaderCommit on reconnect" feedback channel: a follower whose
        // commit frontier regressed on restart (commit markers lost in a crash — lazy markers on
        // the single-fsync path, the asynchronous commit broadcast on the two-fsync path — then
        // reconstructed conservatively) advertises the lower value so the leader can re-supply
        // the still-committed tail. An earlier version reported -1 here with WalSingleFsyncCommit
        // off, which left the regression permanently invisible on an idle partition: the damaged
        // follower kept acking, stayed in quorum and stayed election-eligible while silently
        // missing acknowledged entries.
        long reportedCommittedIndex = wal.GetCommitIndex();

        host.EnqueueResponse(endpoint, new(
            RaftResponderRequestType.CompleteAppendLogs,
            new(endpoint),
            new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.Success, reportedCommittedIndex)
        ));

        CompleteReply(replyCorrelationId, RaftResponseStatic.NoneResponse);
    }
}
