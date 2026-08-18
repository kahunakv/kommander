using System.Diagnostics;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The second half of every write: routing a durable WAL completion back to whichever operation
/// enqueued it — a leader propose, commit or rollback, or a follower append.
///
/// <para><b>Why writes are split in two.</b> The partition executor is single-threaded, so the
/// enqueue side cannot wait for durability without stalling the very acks the write needs to reach
/// quorum. The pending-operation record carries the reply correlation across the gap, and this type
/// is where the ticket is finally resolved and committed entries are delivered.</para>
///
/// <para><b>Fences, not assertions.</b> Completions can arrive for a superseded term, the wrong
/// partition, an inverted range, or an operation that no longer exists — all normal after a
/// step-down. Each is discarded rather than trusted; a stale completion that slipped through would
/// advance the commit frontier on behalf of a term this node no longer leads.</para>
///
/// <para><b>The follower ack reports the gap-aware commit frontier, never the raw batch max.</b>
/// The unanchored live-propose path can leave a lone high entry over a hole, and advertising that
/// id would make the leader read the follower as caught up and stop repairing the missing prefix —
/// a permanently stranded replica. Pinned by
/// <c>TestFollowerAckReportsCommitFrontier</c>.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class WalCompletionRouter
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ProposalRegistry proposals;
    private readonly LogApplicator applier;
    private readonly BackfillSender sender;
    private readonly IRaftOperationReplySink replySink;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Abandons an unpublished promotion when its barrier cannot complete. Injected because the
    /// revert is a leadership transition owned by the core, not a completion concern.
    /// </summary>
    private readonly Func<string, Task> revertUnpublishedPromotionAsync;

    public WalCompletionRouter(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ProposalRegistry proposals,
        LogApplicator applier,
        BackfillSender sender,
        IRaftOperationReplySink replySink,
        ILogger<IRaft> logger,
        Func<string, Task> revertUnpublishedPromotionAsync)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.proposals = proposals;
        this.applier = applier;
        this.sender = sender;
        this.replySink = replySink;
        this.logger = logger;
        this.revertUnpublishedPromotionAsync = revertUnpublishedPromotionAsync;
    }

    private void CompleteReply(ulong? correlationId, RaftResponse response)
    {
        if (correlationId is not null)
            replySink.TryComplete(correlationId.Value, response);
    }

    public async Task CompleteWalOperationAsync(RaftWalCompletion? completion)
    {
        if (completion is null)
            return;

        // ── Partition fence ────────────────────────────────────────────────────
        // A completion for a different partition must never drive our state machine.
        // This can happen during the transition period if a completion is mis-routed.
        if (completion.PartitionId != host.PartitionId)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion for partition {CompletionPartition} delivered to partition {HostPartition}; discarding stale completion.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState,
                completion.PartitionId, host.PartitionId);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "partition_mismatch"));
            return;
        }

        // ── Term fence ─────────────────────────────────────────────────────────
        // A completion submitted when the node was in an earlier term must not
        // advance state after a leadership or followership change.  Term -1 means
        // "not set" (legacy / test paths) and bypasses the fence.
        if (completion.Term >= 0 && completion.Term != coreState.CurrentTerm)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion for term {CompletionTerm} delivered in term {CurrentTerm}; discarding stale completion (op {OperationId}).",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState,
                completion.Term, coreState.CurrentTerm, completion.OperationId);
            if (proposals.TryTakePending(completion.OperationId, out Scheduling.RaftPendingWalOperation? stalePending))
                proposals.ReturnPending(stalePending);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "term_mismatch"));
            return;
        }

        // ── Log-range validation ───────────────────────────────────────────────
        if (completion.MinLogIndex >= 0 && completion.MaxLogIndex >= 0 && completion.MinLogIndex > completion.MaxLogIndex)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} has inverted log range [{Min},{Max}]; discarding.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState,
                completion.OperationId, completion.MinLogIndex, completion.MaxLogIndex);
            return;
        }

        // ── Pending-operation fence ────────────────────────────────────────────
        // Use the envelope OperationId (authoritative) as the lookup key.
        // All operation types that carry per-operation data in pending (leader and
        // follower paths) require the pending entry: a completion for an operation
        // that was never registered — or was already processed — must not drive
        // further state transitions; that would create orphaned proposals and
        // mis-routed client replies.  Only Compaction is fire-and-forget.
        bool found = proposals.TryTakePending(completion.OperationId, out RaftPendingWalOperation? pending);

        if (!found && completion.OperationType is
            WALWriteOperationType.LeaderPropose or
            WALWriteOperationType.LeaderCommit or
            WALWriteOperationType.LeaderRollback or
            WALWriteOperationType.FollowerAppend)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} ({Type}) is not in pendingWalOperations; discarding unknown/superseded completion.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState,
                completion.OperationId, completion.OperationType);
            return;
        }

        // ── Min-log cross-check against pending entry ──────────────────────────
        if (pending?.Logs is { Count: > 0 } pendingLogs && completion.MinLogIndex >= 0)
        {
            // Indexed loop, not Enumerable.Min: this runs on every WAL completion (propose,
            // commit, rollback, follower append) and the LINQ path boxes the list enumerator.
            long actualMin = pendingLogs[0].Id;
            for (int i = 1; i < pendingLogs.Count; i++)
            {
                if (pendingLogs[i].Id < actualMin)
                    actualMin = pendingLogs[i].Id;
            }

            if (actualMin != completion.MinLogIndex)
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} min-log-index mismatch: envelope {EnvelopeMin} vs actual {ActualMin}; discarding.",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState,
                    completion.OperationId, completion.MinLogIndex, actualMin);
                return;
            }
        }

        switch (completion.OperationType)
        {
            case WALWriteOperationType.LeaderPropose:
                await CompleteLeaderPropose(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.LeaderCommit:
                await CompleteLeaderCommit(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.LeaderRollback:
                await CompleteLeaderRollback(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.FollowerAppend:
                await CompleteFollowerAppend(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.Compaction:
            default:
                CompleteReply(pending?.ReplyCorrelationId, RaftResponseStatic.NoneResponse);
                break;
        }

        // Return the drained metadata object to the pool. Only reached on the main completion path
        // (rare error early-returns above simply let their entry be collected); the entry was already
        // removed from the dictionary at the fence above, and each op completes once, so there is no
        // double-return. The Complete* handlers have finished reading `pending` by here.
        if (found && pending is not null)
            proposals.ReturnPending(pending);
    }

    /// <summary>
    /// Completes a leader propose WAL write by broadcasting the proposed entries to all peers.
    /// <para>
    /// The live-replication broadcast deliberately carries no Log Matching anchors. A follower that
    /// is transiently behind (e.g. a node still catching up during a concurrent join) would reject
    /// an anchored live proposal with <see cref="RaftOperationStatus.LogMismatch"/>, but the
    /// live-proposal quorum path has no recovery for a rejected proposal — it simply never reaches
    /// quorum and times out (<c>ProposalTimeout</c>), and under load this livelocks. Log Matching is
    /// therefore enforced only on the backfill path, which has <c>nextIndex</c> backtracking to
    /// recover; the leader never ships a non-contiguous live batch, so contiguity holds by
    /// construction on this path.
    /// </para>
    /// </summary>
    private async Task CompleteLeaderPropose(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;
        List<RaftLog> logs = pending?.Logs ?? [];
        bool autoCommit = pending?.AutoCommit ?? false;

        if (completion.Status != RaftOperationStatus.Success)
        {
            // The promotion-barrier no-op failed to even persist locally: the barrier can never
            // commit, so the unpublished leadership must be abandoned rather than held open.
            if (coreState.LeadershipBarrierTicket != HLCTimestamp.Zero && ticketId == coreState.LeadershipBarrierTicket && coreState.NodeState == RaftNodeState.Leader)
                await revertUnpublishedPromotionAsync($"barrier propose failed ({completion.Status})").ConfigureAwait(false);

            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, ticketId));
            return;
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(logs, autoCommit, ticketId);

        // Register the local leader as a voter participant and mark it completed immediately.
        // Must be done via AddExpectedNodeCompletion so MarkNodeCompleted (which now only
        // updates existing keys) correctly counts the self-vote in the quorum denominator.
        proposalQuorum.AddExpectedNodeCompletion(host.LocalEndpoint);
        proposalQuorum.MarkNodeCompleted(host.LocalEndpoint);

        AppendLogsGrpcLogCache? grpcLogCache = logs.Count > 0 ? new() : null;

        // Recorded while the loop already visits every peer, so the single-voter check below
        // does not re-scan host.Nodes with a closure-allocating LINQ Any on every propose.
        bool hasVoterPeer = false;

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            // Learners receive log entries for catch-up but must not count toward quorum.
            // Only add voters to the quorum set; AppendLogToNode is called for all nodes.
            if (host.IsVoter(node.Endpoint))
            {
                hasVoterPeer = true;
                proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            }
            sender.AppendLogToNode(node, ticketId, logs, grpcLogCache: grpcLogCache);
        }

        if (!proposals.TryAdd(ticketId, proposalQuorum))
        {
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Errored, HLCTimestamp.Zero));
            return;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId, string.Join(',', logs.Select(x => x.Id.ToString())));

        // Single-voter leader (no voter peers): the self-completion above already satisfies
        // quorum, but no voter ack will arrive to drive CompleteAppendLogsAsync. Drive the
        // Completed → (auto)commit transition here. Guarded to voter-only peers so learner-only
        // peers (which never ack for quorum) don't silently prevent single-voter commit.
        if (!hasVoterPeer)
        {
            proposalQuorum.SetState(RaftProposalState.Completed);

            if (autoCommit)
            {
                // A single-voter leader is its own quorum, so the propose fsync that just
                // completed already made the entry quorum-durable: the fast path applies here too.
                proposals.TryReleaseTicketOnQuorumDurable(proposalQuorum);

                WALWriteOperation commitOperation = wal.EnqueueCommit(proposalQuorum.Logs);
                Scheduling.RaftPendingWalOperation pendingFollowUpCommit = proposals.RentPending();
                pendingFollowUpCommit.Proposal = proposalQuorum;
                pendingFollowUpCommit.TicketId = ticketId;
                proposals.TrackPending(commitOperation.OperationId, pendingFollowUpCommit);
            }
        }

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, ticketId));
    }

    /// <summary>
    /// Commits the proposal locally and broadcasts the committed entries to all peers.
    /// <para>
    /// The commit broadcast deliberately carries no Log Matching anchors: it re-ships ids that
    /// were already validated and accepted by each follower during the (anchored) propose, so a
    /// per-commit anchor adds no safety. It would, however, force a WAL term read
    /// (<c>GetAnyTermAtAsync</c>) on this hot completion path before fan-out, which stalls commit
    /// propagation under load and caused proposal timeouts. Divergence is detected and repaired on
    /// the propose and backfill paths, which remain anchored.
    /// </para>
    /// <para>
    /// Also applies every committed consumer entry to the local consumer state machine via
    /// <see cref="ApplyLogToConsumerAsync"/>. Followers receive these via
    /// <see cref="CompleteFollowerAppend"/>; the leader must apply them through the same path so
    /// its consumer projection stays consistent. This covers entries inherited from a prior term
    /// that have no local proposal waiter on this node and would otherwise be silently absent
    /// from the leader's consumer state.
    /// </para>
    /// <para>
    /// Applies are strictly in log order, exactly like the follower path. Pipelined proposals
    /// reach quorum in network order, so this completion can arrive while an earlier proposal is
    /// still in flight below it; the batch is then deferred (<see cref="DeferLeaderApplies"/>)
    /// rather than applied, because applying it would advance the cursor over the in-flight entry
    /// and permanently suppress its later delivery. The blocker's own completion flushes deferred
    /// batches in order via <see cref="FlushDeferredLeaderAppliesAsync"/>.
    /// </para>
    /// </summary>
    private async Task CompleteLeaderCommit(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        // Inherited-tail re-commit (see EnqueueInheritedRecommitMarkers): no proposal ticket, no
        // client waiter, no commit broadcast — the markers are lazy durability for entries already
        // committed and applied. Success needs nothing further; failure only means the on-disk
        // range stays Proposed (and therefore unbackfillable) until a later drain retries, which
        // is worth a log line but must not run the ordinary null-proposal failure handling below
        // (that path can revert an armed promotion).
        if (pending is { IsInheritedRecommit: true })
        {
            if (completion.Status != RaftOperationStatus.Success)
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Durable re-commit of inherited entries failed ({Status}) — the range stays Proposed on disk and cannot be backfilled until a later drain retries.",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, completion.Status);
            return;
        }

        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

        if (completion.Status != RaftOperationStatus.Success || proposal is null)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit proposal {Timestamp}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId);

            // A failed commit of the promotion-barrier no-op means this leader can never prove its
            // consumer projection complete: revert instead of holding unpublished leadership open.
            if (coreState.LeadershipBarrierTicket != HLCTimestamp.Zero && ticketId == coreState.LeadershipBarrierTicket && coreState.NodeState == RaftNodeState.Leader)
                await revertUnpublishedPromotionAsync($"barrier commit failed ({completion.Status})").ConfigureAwait(false);

            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, 0));
            return;
        }

        proposal.SetState(RaftProposalState.Committed);
        // Unblock event-driven waiters on the public write path. If TryReleaseTicketOnQuorumDurable
        // already fired on the fast path (WalSingleFsyncCommit + autoCommit), TrySetResult is a no-op.
        proposal.CompleteWaiter(RaftProposalTicketState.Committed, completion.MaxLogIndex);
        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        if (completion.MaxLogIndex > coreState.LocalCommittedIndex)
            coreState.LocalCommittedIndex = completion.MaxLogIndex;

        AppendLogsGrpcLogCache? grpcLogCache = proposal.Logs.Count > 0 ? new() : null;

        // Send committed entries to ALL peers (voters + learners). proposal.Nodes only tracks
        // quorum voters; learners were excluded from quorum but still need log delivery so their
        // WAL stays in sync. host.Nodes already excludes self, so no self-skip is needed here.
        foreach (RaftNode node in host.Nodes)
            sender.AppendLogToNode(node, ticketId, proposal.Logs, grpcLogCache: grpcLogCache);

        // Apply any inherited Proposed entries (from a prior term) that sit between the
        // last-applied cursor and this commit batch. These are entries proposed by the
        // previous leader, held as Proposed in our WAL, and now committed by quorum.
        // They have no local proposal waiter and were never delivered via CompleteFollowerAppend,
        // so the leader's consumer would silently miss them without this drain.
        long inheritedEnd = completion.MinLogIndex - 1;
        InheritedDrainStatus drainStatus = InheritedDrainStatus.Covered;
        if (inheritedEnd >= 0 && inheritedEnd > coreState.LastAppliedIndex)
        {
            // Reads race the WAL write queue on the leader too: with pipelined proposals, an
            // earlier entry (or its commit marker) can still sit in the write scheduler when this
            // completion's drain reads the backend, and the absent id is indistinguishable from a
            // real hole. Retry until the writes land and the drain covers the range, bounded by
            // the barrier timeout — stepping down on a transient read gap would churn leadership
            // under ordinary pipelined load. The loop exits as soon as the range is covered (or
            // the drain hits a current-term in-flight proposal, which retrying cannot resolve),
            // so the common case adds no latency.
            // Same bounds as the promotion drain: a sole voter only needs its own write queue to
            // drain, so its bound is short; with voter peers the full barrier timeout is worth
            // spending before stepping down.
            bool drainHasVoterPeers = sender.HasVoterPeer();
            TimeSpan inheritedDrainBound = drainHasVoterPeers ? host.Configuration.LeadershipBarrierTimeout : TimeSpan.FromMilliseconds(250);
            long drainStartTicks = Stopwatch.GetTimestamp();

            while ((drainStatus =
                       await applier.DrainInheritedAppliesAsync(coreState.LastAppliedIndex + 1, inheritedEnd).ConfigureAwait(false)) == InheritedDrainStatus.Hole)
            {
                if (Stopwatch.GetElapsedTime(drainStartTicks) > inheritedDrainBound)
                {
                    // Same sole-voter escape as the promotion drain: with no voter peer to defer
                    // to, stepping down just re-elects this node into the same gap forever. The
                    // gap is unrecoverable either way — keep serving and say so loudly.
                    if (!drainHasVoterPeers)
                    {
                        logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain incomplete with no voter peers to defer to — proceeding as sole voter; entries in the gap are unrecoverable.",
                            host.LocalEndpoint, host.PartitionId, coreState.NodeState);

                        // Deliver everything this survivor DOES hold past the gap, so only the
                        // genuinely absent entries are lost rather than the whole suffix. The
                        // skip-gaps drain still stops at a current-term in-flight proposal
                        // (self-quorum resolves those), so it reports Covered or BlockedByInFlight.
                        drainStatus = await applier.DrainInheritedAppliesAsync(coreState.LastAppliedIndex + 1, inheritedEnd, skipGaps: true).ConfigureAwait(false);
                    }

                    break;
                }

                await Task.Delay(2).ConfigureAwait(false);
            }
        }

        // Apply committed consumer entries to the local state machine. Mirrors the apply loop
        // in CompleteFollowerAppend so the leader's consumer projection stays in sync — including
        // its in-order discipline:
        //   * Covered — the cursor is contiguous up to this batch: apply it, then flush any
        //     out-of-order batches that deferred behind an entry this batch just resolved.
        //   * BlockedByInFlight — an earlier pipelined proposal is still awaiting quorum below
        //     this batch. Applying now would advance the cursor over that entry and its own
        //     completion would then be suppressed by the exactly-once guard, silently skipping a
        //     committed, client-acknowledged write on the leader alone (the Jepsen hole). Defer
        //     this batch; the blocker's completion flushes it in order.
        //   * Hole — delivering would advance the cursor over withheld entries and orphan them
        //     permanently: skip; this leader steps down below and the next leader (or a later
        //     drain) delivers everything in order.
        switch (drainStatus)
        {
            case InheritedDrainStatus.Covered:
                foreach (RaftLog log in proposal.Logs)
                    await applier.ApplyLogToConsumerAsync(log).ConfigureAwait(false);

                await applier.FlushDeferredLeaderAppliesAsync().ConfigureAwait(false);
                break;

            case InheritedDrainStatus.BlockedByInFlight:
                applier.DeferLeaderApplies(completion.MinLogIndex, proposal.Logs);
                break;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugCommittedLogs(
                host.LocalEndpoint,
                host.PartitionId,
                coreState.NodeState,
                ticketId,
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString())),
                (currentTime - proposal.StartTimestamp).TotalMilliseconds
            );

        wal.NotifyCommitted();

        // Promotion barrier: this commit's inherited drain (above) has applied every prior-term
        // entry below the barrier no-op, so the consumer projection is now provably complete —
        // publish leadership. Fenced on state and term so a stale barrier completion from a
        // superseded promotion can never publish. An incomplete inherited drain (a WAL hole in the
        // inherited range) disproves projection completeness: publishing anyway would serve an
        // arbitrary missing committed range for the whole tenure (a leader is never backfilled),
        // so revert and let a node with a contiguous log win the next term.
        if (coreState.LeadershipBarrierTicket != HLCTimestamp.Zero && ticketId == coreState.LeadershipBarrierTicket)
        {
            coreState.LeadershipBarrierTicket = HLCTimestamp.Zero;

            if (coreState.NodeState == RaftNodeState.Leader && coreState.CurrentTerm == coreState.LeadershipBarrierTerm)
            {
                // BlockedByInFlight cannot legitimately happen here (the barrier is the first
                // proposal of the term, so nothing current-term sits below it), but if it ever
                // does the projection is just as unproven as with a hole: revert either way.
                if (drainStatus != InheritedDrainStatus.Covered)
                {
                    await revertUnpublishedPromotionAsync("inherited-entry drain could not cover the pre-barrier range").ConfigureAwait(false);
                }
                else
                {
                    host.Leader = host.LocalEndpoint;

                    if (logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier committed at {Ticket}; leadership published",
                            host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId);

                    await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
                }
            }
        }
        else if (drainStatus == InheritedDrainStatus.Hole && coreState.NodeState == RaftNodeState.Leader)
        {
            // A hole below an ORDINARY commit is equally disqualifying: this leader's consumer
            // projection cannot cover the committed range and it is never backfilled, so every
            // grant it serves is minted from incomplete state. Ignoring the incomplete drain here
            // (while the batch apply advanced the cursor) is what silently orphaned the whole
            // inherited range. Step down; the entries this commit made durable are quorum-safe and
            // the next leader delivers them in order. BlockedByInFlight deliberately does NOT step
            // down: it is routine pipelining, and its batch was deferred above, not orphaned.
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain incomplete on a leader commit — stepping down.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState);
            await revertUnpublishedPromotionAsync("inherited-entry drain incomplete on a leader commit").ConfigureAwait(false);
        }

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, completion.MaxLogIndex));
    }

    /// <summary>
    /// Rolls back the proposal locally and broadcasts the rolled-back entries to all peers.
    /// Like <see cref="CompleteLeaderCommit"/>, this delivery carries no Log Matching anchors:
    /// it targets ids the follower already saw during the anchored propose, and adding a WAL term
    /// read on this completion path would stall propagation. LMP remains enforced on propose/backfill.
    /// </summary>
    private async Task CompleteLeaderRollback(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

        // A rolled-back promotion barrier (however it got here — the barrier is auto-commit and
        // internal, but a rollback request by ticket id is possible) can never publish leadership.
        if (coreState.LeadershipBarrierTicket != HLCTimestamp.Zero && ticketId == coreState.LeadershipBarrierTicket && coreState.NodeState == RaftNodeState.Leader)
            await revertUnpublishedPromotionAsync("barrier proposal rolled back").ConfigureAwait(false);

        if (completion.Status != RaftOperationStatus.Success || proposal is null)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't rollback proposal {Timestamp}", host.LocalEndpoint, host.PartitionId, coreState.NodeState, ticketId);
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, 0));
            return;
        }

        proposal.SetState(RaftProposalState.RolledBack);
        // Signal failure to any event-driven waiter so the public write path is unblocked
        // immediately rather than waiting for the proposal to expire from activeProposals.
        proposal.CompleteWaiter(RaftProposalTicketState.NotFound, -1);

        AppendLogsGrpcLogCache? grpcLogCache = proposal.Logs.Count > 0 ? new() : null;

        // Same as CompleteLeaderCommit: deliver rollback to all peers, not just quorum voters.
        foreach (RaftNode node in host.Nodes)
            sender.AppendLogToNode(node, ticketId, proposal.Logs, grpcLogCache: grpcLogCache);

        // Resolve the rolled-back range for apply ordering. Rolled-back ids are advance-only for
        // the applied cursor (ApplyLogToConsumerAsync never delivers non-Committed types), and a
        // pipelined batch that committed out of order above this range may be parked in
        // deferredLeaderApplies waiting for it — without this, that batch would only flush when a
        // later commit's inherited drain happened to read the rollback markers back from the WAL.
        // Uses the same drain gate as CompleteLeaderCommit so the pre-first-id sentinel and
        // compacted prefixes are classified by the snapshot floor, not by naive contiguity, but
        // with a single attempt and no step-down: a rollback is not a serving decision, so on
        // Hole (a write still queued behind the read) we simply leave the range for a later
        // commit's retrying drain instead of stalling this completion.
        // Term-fenced by CompleteWalOperationAsync, so a stale tenure's rollback never runs this.
        if (coreState.NodeState == RaftNodeState.Leader && proposal.Logs.Count > 0 && completion.MinLogIndex >= 0)
        {
            long rolledBackInheritedEnd = completion.MinLogIndex - 1;
            InheritedDrainStatus rollbackDrainStatus = InheritedDrainStatus.Covered;
            if (rolledBackInheritedEnd >= 0 && rolledBackInheritedEnd > coreState.LastAppliedIndex)
                rollbackDrainStatus = await applier.DrainInheritedAppliesAsync(coreState.LastAppliedIndex + 1, rolledBackInheritedEnd).ConfigureAwait(false);

            switch (rollbackDrainStatus)
            {
                case InheritedDrainStatus.Covered:
                    foreach (RaftLog log in proposal.Logs)
                        await applier.ApplyLogToConsumerAsync(log).ConfigureAwait(false);

                    await applier.FlushDeferredLeaderAppliesAsync().ConfigureAwait(false);
                    break;

                case InheritedDrainStatus.BlockedByInFlight:
                    applier.DeferLeaderApplies(completion.MinLogIndex, proposal.Logs);
                    break;
            }
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugRolledbackLogs(
                host.LocalEndpoint,
                host.PartitionId,
                coreState.NodeState,
                ticketId,
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString()))
            );

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, completion.MaxLogIndex));
    }

    /// <summary>
    /// Finalises a follower append after the WAL write completes. Dispatches each committed
    /// log entry to the appropriate callback: entries on P0 with <c>LogType == "_RaftSystem"</c>
    /// go to <c>InvokeSystemReplicationReceived</c> (system coordinator); all other entries —
    /// including non-system types on P0 — go to <c>InvokeReplicationReceived</c> (consumer).
    /// This type-based routing is what allows P0 to host consumer data alongside coordinator
    /// entries without any WAL format change.
    /// </summary>
    private async Task CompleteFollowerAppend(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        string endpoint = pending!.Endpoint ?? "";
        long leaderTerm = completion.Term;
        HLCTimestamp timestamp = pending.Timestamp;
        // Report the WAL's gap-aware commit frontier, NOT the raw batch max (completion.MaxLogIndex).
        // The unanchored live-propose path (prevLogIndex==0) can write a lone high entry over a gap on
        // a behind follower without an LMP check, leaving a hole. GetMaxLog (Keys.Max) would then
        // advertise that high id as the follower's progress, the leader's backfill gate would see the
        // follower as caught up (coreState.LocalCommittedIndex - reported == 0), and the missing prefix would
        // never be repaired — a stable non-contiguous log. GetCommitIndex stops at the hole, so the
        // leader keeps matchIndex/nextIndex behind it and backfills the prefix forward until the log
        // is contiguous. This drives only backfill/nextIndex bookkeeping (not quorum commit, which is
        // the propose-ticket path), and mirrors the gap-aware heartbeat-ack report at the fast path.
        long committedIndex = completion.Status == RaftOperationStatus.Success ? wal.GetCommitIndex() : -1;

        if (completion.Status == RaftOperationStatus.Success)
        {
            // Exactly-once, IN-ORDER apply, bounded by the WAL's gap-aware committed frontier
            // (committedIndex = GetCommitIndex). Contract: deliver every committed id exactly once, in order,
            // never over a hole. The unanchored live-propose broadcast ships prevLogIndex=0, so a behind
            // follower can persist a high committed entry before its prefix exists; the commit frontier buffers
            // that id over the gap (AdvanceCommitFrontier), so committedIndex stops below the hole and it is
            // withheld until backfill fills the prefix.
            //
            // Fast path (no WAL read): deliver the contiguous committed prefix straight from this batch. This
            // is the steady-state case — entries arrive in order and this delivers them without a scheduler
            // round-trip. Stops at the first id that is not exactly frontier+1, is beyond the committed
            // frontier, or is not yet committed (Proposed) — anything the batch cannot deliver in order.
            foreach (RaftLog log in pending.Logs ?? [])
            {
                if (log.Id != coreState.LastAppliedIndex + 1 || log.Id > committedIndex)
                    break;
                if (log.Type == RaftLogType.Committed)
                {
                    // Promotion-barrier no-ops are consensus-internal: skip delivery, advance cursor.
                    if (log.LogType != RaftSystemConfig.LeadershipBarrierLogType)
                    {
                        if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                        {
                            if (!await host.InvokeSystemReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                                host.InvokeReplicationError(host.PartitionId, log);
                        }
                        else if (!await host.InvokeReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                            host.InvokeReplicationError(host.PartitionId, log);
                    }
                }
                else if (log.Type != RaftLogType.CommittedCheckpoint)
                    break;                          // Proposed/other non-committed entry: not deliverable yet.
                coreState.LastAppliedIndex = log.Id;          // advance over delivered entries and skipped checkpoints
            }

            // Slow path (rare): the committed frontier is still ahead of the applied cursor — a hole just
            // filled, so entries buffered by earlier out-of-order batches (no longer in this batch) became
            // deliverable. Drain them from the WAL in order. A no-op when the fast path already caught up.
            if (committedIndex > coreState.LastAppliedIndex)
                await applier.DrainCommittedAppliesAsync(committedIndex).ConfigureAwait(false);

            wal.NotifyCommitted();
        }

        if (!string.IsNullOrEmpty(endpoint))
        {
            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs,
                new(endpoint),
                new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, completion.Status, committedIndex)
            ));
        }

        CompleteReply(pending.ReplyCorrelationId, RaftResponseStatic.NoneResponse);
    }
}
