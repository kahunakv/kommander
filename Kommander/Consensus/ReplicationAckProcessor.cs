using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The leader's handling of one follower's AppendEntries acknowledgement: term fencing, progress
/// advance or backtrack, quorum accounting, and the immediate follow-on backfill.
///
/// <para><b>The committedIndex field is overloaded and that is the trap this type exists around.</b>
/// On a Success ack it carries the follower's gap-aware committed frontier; on a rejection it
/// carries the follower's raw max log id, which sits arbitrarily far ABOVE the frontier whenever the
/// log has an uncommitted or non-contiguous tail — exactly the state of a follower that needs
/// backfilling. Folding a rejection's value into the reported-frontier map pins an over-estimate no
/// later truthful report can correct, and the peer is then never backfilled however far behind it
/// is. Only Success acks may reach <c>SetCommitFrontier</c>, and they do so last-writer-wins so a
/// crash-restarted peer can lower it and become visible as behind again.</para>
///
/// <para><b>A higher response term deposes this leader</b> (Raft §5.1) and is handled before the
/// term fence, which would otherwise discard the ack as "not my term" and learn nothing. This is the
/// only repair channel for a per-partition term wedge.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class ReplicationAckProcessor
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ReplicationTracker tracker;
    private readonly ProposalRegistry proposals;
    private readonly ReadIndexCoordinator readIndex;
    private readonly BackfillSender sender;
    private readonly ElectionCoordinator election;
    private readonly RaftPartitionLogThrottle logThrottle;
    private readonly ILogger<IRaft> logger;

    /// <summary>Fails and drops in-flight proposals when a higher term forces a step-down.</summary>
    private readonly Action failAllActiveProposalWaiters;

    public ReplicationAckProcessor(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ReplicationTracker tracker,
        ProposalRegistry proposals,
        ReadIndexCoordinator readIndex,
        BackfillSender sender,
        ElectionCoordinator election,
        RaftPartitionLogThrottle logThrottle,
        ILogger<IRaft> logger,
        Action failAllActiveProposalWaiters)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.tracker = tracker;
        this.proposals = proposals;
        this.readIndex = readIndex;
        this.sender = sender;
        this.election = election;
        this.logThrottle = logThrottle;
        this.logger = logger;
        this.failAllActiveProposalWaiters = failAllActiveProposalWaiters;
    }

    public async ValueTask CompleteAppendLogsAsync(string endpoint, HLCTimestamp timestamp, RaftOperationStatus status, long committedIndex, long responseTerm = -1)
    {
        // ── Raft §5.1: a response stamped with a HIGHER term deposes us ─────────────────────────
        // Terms only enter a node through elections, so a higher response term proves a newer term
        // exists — this leader (or candidate) is stale and must step down and adopt it BEFORE the
        // fence below, which would discard the ack as "not my term" and learn nothing.
        //
        // This is the only repair channel for a per-partition term wedge (the Jepsen frozen-frontier
        // stall): a follower whose term was bumped by a failed election rejects every AppendLogs with
        // LeaderInOldTerm carrying its higher term, while pre-vote (correctly) keeps it from winning
        // an election against our still-healthy quorum — so without this step-down the leader ships
        // backfill forever, the follower rejects it forever, and that partition's replica on the
        // bumped node never commits another entry. Stepping down lets the next election converge the
        // term (either we re-win at a higher term and re-ship, or the bumped node's log wins).
        // Mirrors the higher-voteTerm adoption in VoteAsync: bookkeeping is gated on state, the term
        // adoption is not, and the adopted term is persisted with no vote so a crash cannot regress it.
        // Membership-fenced like inbound AppendLogs: only a committed roster member can depose a
        // leader, so an endpoint outside membership cannot churn leadership with a fabricated term.
        if (responseTerm > coreState.CurrentTerm && host.IsMember(endpoint))
        {
            bool stepDown = coreState.NodeState != RaftNodeState.Follower;

            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Stepping down on higher-term append ack from {Endpoint}: responseTerm={ResponseTerm} coreState.CurrentTerm={CurrentTerm} Status={Status}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, responseTerm, coreState.CurrentTerm, status);

            if (stepDown)
            {
                coreState.NodeState = RaftNodeState.Follower;
                host.Leader = "";
                tracker.ClearAll();
                coreState.LocalCommittedIndex = -1;
                failAllActiveProposalWaiters();
            }

            coreState.CurrentTerm = responseTerm;
            election.ResetPreVoteRound();

            if (stepDown)
                await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

            await wal.PersistHardStateAsync(coreState.CurrentTerm, null).ConfigureAwait(false);
            return;
        }

        // ── Leader + term fence ──────────────────────────────────────────────────
        // Reject a stale ACK BEFORE any mutation (HLC receive, node activity, commit/backfill cursors,
        // matchIndex/nextIndex, startCommitIndexes). Without this, a delayed old-term ACK could make an
        // outdated follower look caught-up — e.g. appear eligible for a leadership transfer — or perturb
        // a later term's catch-up. responseTerm < 0 preserves the previous behaviour for callers that do
        // not stamp a term.
        if (responseTerm >= 0 && (coreState.NodeState != RaftNodeState.Leader || responseTerm != coreState.CurrentTerm))
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Ignoring stale CompleteAppendLogs from {Endpoint}: responseTerm={ResponseTerm} coreState.CurrentTerm={CurrentTerm}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, responseTerm, coreState.CurrentTerm);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "append_ack_term_mismatch"));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);

        if (endpoint != host.LocalEndpoint)
            host.UpdateLastNodeActivity(endpoint, host.PartitionId, currentTime);
        
        // LogMismatch: the follower's log diverges at the prevLogIndex we sent.
        // committedIndex carries the follower's local max log at the time of rejection.
        // Backtrack formula: max(1, min(nextIndex[peer]-1, committedIndex+1)).
        // Taking min ensures we step back at least one position even when the follower's
        // max equals the anchor we just tried, preventing a livelock on repeated rejection
        // at the same anchor point.
        if (status == RaftOperationStatus.LogMismatch)
        {
            long currentNext  = tracker.GetNextIndexOrDefault(endpoint, committedIndex + 2);
            long backtracked  = Math.Max(1, Math.Min(currentNext - 1, committedIndex + 1));
            tracker.SetNextIndex(endpoint, backtracked);

            logger.LogDebugBacktrackingNextIndex(
                host.LocalEndpoint,
                host.PartitionId,
                coreState.NodeState,
                endpoint,
                currentNext,
                backtracked,
                committedIndex
            );

            return;
        }

        if (committedIndex > 0)
        {
            tracker.AdvanceStartCommitIndex(endpoint, committedIndex);

            logger.LogTraceSuccessfullyCompletedLogs(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, timestamp, committedIndex, (currentTime - timestamp).TotalMilliseconds);
        }

        if (status != RaftOperationStatus.Success)
        {
            // A saturated follower is the one rejection that must change the leader's behaviour
            // rather than just its logs. Every other status here is a condition the next batch
            // might resolve; this one is the follower saying it has no room, so re-sending
            // immediately is what keeps it from ever having room. Pause entry-carrying backfill
            // to this peer for a window and let its queue drain.
            if (status == RaftOperationStatus.FollowerWalSaturated)
                tracker.PauseBackfill(endpoint, host.Configuration.FollowerSaturationBackoff);

            logThrottle.LogFailedAppendAck(status, endpoint, timestamp, committedIndex);

            return;
        }

        // Record the follower's self-reported commit frontier — SUCCESS acks only. The
        // committedIndex field is overloaded: on Success it carries the follower's gap-aware
        // committed frontier (or -1: "nothing committed" on the single-fsync path, "no report"
        // from a legacy-path heartbeat ack), while rejection acks reuse the same field for the
        // follower's raw max log id (the LogMismatch backtrack anchor, the saturation report).
        // A raw max log sits arbitrarily far ABOVE the committed frontier whenever the log has
        // an uncommitted or non-contiguous tail — precisely the state of a follower whose
        // frontier stalled behind a lost commit marker while the unanchored live-propose
        // broadcast keeps growing its log. Folding a rejection's value into this map therefore
        // pinned an over-estimate that no later truthful (lower) report could correct, so
        // SendHeartbeat computed followerGap ≈ 0 and never backfilled the peer: its commit
        // frontier stalled forever while its log grew (the Jepsen stranded-replica findings).
        //
        // The update is last-writer-wins, not monotonic, for the same reason. The follower is
        // the only authority on its own frontier, and a genuine regression (crash-restart that
        // lost lazy commit markers) must be able to LOWER the record so the gap becomes visible
        // to the heartbeat backfill gate again. A reordered stale ack can transiently lower it
        // too — that costs one redundant, idempotent backfill batch and self-corrects on the
        // peer's next ack, whereas refusing lower reports cost a permanently stranded replica.
        // -1 is recorded only as an initial seed: a fresh follower must still enter the map so
        // SendHeartbeat's TryGetValue lag check sees it at all, but a legacy heartbeat ack's
        // "no report" must not erase a real frontier already recorded from an append ack.
        if (committedIndex >= 0 || !tracker.HasCommitFrontier(endpoint))
            tracker.SetCommitFrontier(endpoint, committedIndex);

        // Same-term success acks double as leadership proof: they feed the read-index confirmation
        // round and the check-quorum recency window. Only term-stamped acks count — an unstamped
        // (-1) ack passed the term fence above by default and could belong to an earlier stint of
        // this node's leadership.
        if (responseTerm >= 0 && endpoint != host.LocalEndpoint && coreState.NodeState == RaftNodeState.Leader)
        {
            readIndex.RecordVoterAck(endpoint, host.GetMonotonicTimestamp());
            await readIndex.RegisterAckAsync(endpoint).ConfigureAwait(false);
        }

        // Everything below this guard derives replication progress from committedIndex, so it
        // requires an actual report. A Success ack with committedIndex < 0 carries NO frontier
        // information — the legacy (two-fsync) heartbeat ack always reports -1, and the embedded
        // Kahuna consumer runs that path by default. Feeding the -1 through the progress math
        // fabricated a catch-up target out of nothing: right after an election win the optimistic
        // seed holds matchIndex = 0, a -1 report left newMatchIndex at 0, nextIndex became
        // 0 + 1 = 1, and the eager fast path below shipped a backfill anchored at 1 for a peer
        // whose real frontier was millions of entries higher. On a compacted WAL that anchor can
        // never be served, so every leadership change on a busy partition produced a refused
        // batch and then a full snapshot transfer to an in-sync voter (the Caraxes "anchored at
        // 1" soak finding) — and before the refusals escalated at the choke point, a permanently
        // wedged cluster. A no-report ack must leave matchIndex, nextIndex, the fast-path
        // triggers and the regression detection untouched; the -1 frontier seed above is still
        // recorded so quiescence counts the peer as contacted, and the proposal quorum
        // accounting below still runs.
        if (committedIndex >= 0)
        {
            // Success: advance matchIndex and nextIndex for this peer so the backfill loop
            // knows the follower has caught up to at least committedIndex. matchIndex stays monotonic
            // (a stale in-flight ack must not drag a peer's recorded progress backwards), so the prior
            // value is captured first — it is the only evidence of a genuine frontier regression, which
            // the fast-path re-supply below keys on.
            // newMatchIndex mirrors matchIndex[endpoint] locally — this method previously re-read the
            // dictionary up to four more times below (a string hash + compare each) for a value fully
            // determined right here on the highest-frequency inbound message a leader handles.
            bool hadMatchIndex = tracker.TryGetMatchIndex(endpoint, out long priorMatchIndex);
            long newMatchIndex = priorMatchIndex;
            if (!hadMatchIndex || committedIndex > priorMatchIndex)
            {
                newMatchIndex = committedIndex;
                tracker.SetMatchIndex(endpoint, committedIndex);
            }
            tracker.SetNextIndex(endpoint, newMatchIndex + 1);

            // Immediately ship the next bounded batch only while an active catch-up is in progress,
            // so a multi-batch backfill converges without stalling a full heartbeat per batch. This
            // must honour the same BackfillThreshold gate as the heartbeat path: a follower lagging by
            // ≤ threshold is intentionally not actively backfilled (small lag rides on normal
            // replication), and eagerly catching it up here would, e.g., make a barely-behind node look
            // fresh enough to receive a leadership transfer it should not.
            if (coreState.NodeState == RaftNodeState.Leader
                && host.Configuration.BackfillEnabled
                && coreState.LocalCommittedIndex - newMatchIndex > host.Configuration.BackfillThreshold)
            {
                RaftNode? behindNode = RaftPeers.FindByEndpoint(host.Nodes, endpoint);
                if (behindNode is not null)
                    await sender.TrySendBackfillBatchAsync(behindNode, committedIndex, host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId)).ConfigureAwait(false);
            }

            // Fast-path far-behind re-supply: a follower whose reported frontier trails the leader by more
            // than BackfillThreshold is streamed forward here on its own ack (anchored normally via
            // nextIndex), so a multi-batch catch-up converges without stalling a heartbeat per batch. This
            // mirrors the eager catch-up above but keys on the reported committed frontier rather than
            // matchIndex; confined to the fast path (flag off ⇒ a heartbeat reports -1 ⇒ never fires).
            if (host.Configuration.WalSingleFsyncCommit
                && coreState.NodeState == RaftNodeState.Leader
                && host.Configuration.BackfillEnabled
                && committedIndex >= 0
                && coreState.LocalCommittedIndex - committedIndex > host.Configuration.BackfillThreshold)
            {
                RaftNode? behindNode2 = RaftPeers.FindByEndpoint(host.Nodes, endpoint);
                if (behindNode2 is not null)
                    await sender.TrySendBackfillBatchAsync(behindNode2, committedIndex, host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId)).ConfigureAwait(false);
            }

            // Commit-frontier REGRESSION detection (crash-restart signature) — detection only; the repair
            // is done by SendHeartbeat, not here. A follower that restarted after losing its lazy commit
            // markers reports a frontier BELOW what the leader's monotonic matchIndex already recorded.
            //
            // This must NOT re-ship inline. An earlier version did, and it livelocked the cluster under load:
            // the anchored re-supply issues a WAL read plus an AppendLogs on the hot per-ack path, and when
            // it fires on a peer that is NOT genuinely crash-restarted — a reordered ack during ordinary
            // catch-up can transiently satisfy the "below matchIndex" test — the anchored batch fights the
            // in-flight forward catch-up (log-hole truncate → re-replicate) and starves the executor enough
            // to stall elections. So detection is split from action:
            //   * Here (cheap, per ack): if the ack looks like a genuine regression, RECORD the reported
            //     frontier. If instead the ack shows the peer at/above its recorded match (normal progress),
            //     CLEAR any pending note — a transient reordering self-heals before the next heartbeat and
            //     never triggers a re-supply.
            //   * In SendHeartbeat (paced, once per interval): act on any note still standing.
            // The "was caught up" clause (priorMatchIndex within BackfillThreshold of coreState.LocalCommittedIndex)
            // excludes a still-climbing joining/far-behind follower, whose low acks are catch-up, not
            // regression — those are handled by the threshold paths above.
            if (host.Configuration.WalSingleFsyncCommit && coreState.NodeState == RaftNodeState.Leader && committedIndex >= 0)
            {
                bool frontierRegressed = hadMatchIndex
                    && committedIndex < priorMatchIndex
                    && priorMatchIndex >= coreState.LocalCommittedIndex - host.Configuration.BackfillThreshold;

                if (frontierRegressed)
                    tracker.RecordRegressedFrontier(endpoint, committedIndex);
                else if (committedIndex >= newMatchIndex)
                    tracker.ClearRegressedFrontier(endpoint);
            }
        }

        if (!proposals.TryGet(timestamp, out RaftProposalQuorum? proposal))
            return;

        if (proposal.State != RaftProposalState.Incomplete)
            return;

        proposal.MarkNodeCompleted(endpoint);

        if (!proposal.HasQuorum())
        {
            logger.LogInfoProposalPartiallyCompletedAt(host.LocalEndpoint, host.PartitionId, coreState.NodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);
            return;
        }

        logger.LogInfoProposalCompletedAt(host.LocalEndpoint, host.PartitionId, coreState.NodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);

        proposal.SetState(RaftProposalState.Completed);

        // Observability (off in production): report the acknowledgements that carried this proposal to commit
        // quorum — the local leader (a voter, implicitly durable) plus every registered voter that acked.
        // Learner acks never appear here (learners are not registered in the quorum). A live quorum-discipline
        // checker uses these to verify each commit had a voter majority and no learner was counted.
        if (host.CommitAckObservationEnabled)
        {
            long committedId = proposal.LastLogIndex;
            int votersTotal = host.Nodes.Count(n => host.IsVoter(n.Endpoint)) + 1; // +1 for the local leader
            List<RaftCommitAckObservation> acks =
            [
                new(host.PartitionId, committedId, coreState.CurrentTerm, host.LocalEndpoint, host.IsVoter(host.LocalEndpoint), votersTotal),
            ];
            foreach (string acker in proposal.CompletedEndpoints())
                acks.Add(new RaftCommitAckObservation(host.PartitionId, committedId, coreState.CurrentTerm, acker, host.IsVoter(acker), votersTotal));
            host.ObserveCommitAcks(acks);
        }

        if (!proposal.AutoCommit)
        {
            logger.LogInfoProposalNoAutoCommit(host.LocalEndpoint, host.PartitionId, coreState.NodeState, timestamp);
            // Manual two-phase: the public ReplicateLogs(autoCommit:false) caller awaits the
            // propose phase, which succeeds here on propose-quorum-durable (the explicit commit
            // comes later via CommitLogs, whose result returns through the reply-correlation path,
            // not this waiter). CheckTicketCompletion historically reported {AutoCommit:false,
            // Completed} as Committed, so complete the waiter the same way — otherwise the caller
            // blocks until the 10 s timeout. CompleteLeaderCommit/Rollback fire TrySetResult again
            // later; both are idempotent no-ops once this has run.
            proposal.CompleteWaiter(RaftProposalTicketState.Committed, proposal.LastLogIndex);
            return;
        }

        // Single-fsync fast path: release the client ticket on propose-quorum-durable,
        // ahead of the commit fsync below. No-op unless WalSingleFsyncCommit is on.
        proposals.TryReleaseTicketOnQuorumDurable(proposal);

        WALWriteOperation operation = wal.EnqueueCommit(proposal.Logs);
        Scheduling.RaftPendingWalOperation pendingAutoCommit = proposals.RentPending();
        pendingAutoCommit.Proposal = proposal;
        pendingAutoCommit.TicketId = timestamp;
        proposals.TrackPending(operation.OperationId, pendingAutoCommit);
    }
}
