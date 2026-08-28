using Kommander.Data;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// Delivery of committed log entries to the application consumer, and the ordering rules that make
/// that delivery exactly-once and gap-free.
///
/// <para><b>The invariant this type exists to hold.</b> The applied cursor may only advance over
/// entries that were actually delivered. Two situations threaten that. A WAL hole or a still-Proposed
/// entry inside a committed range means the next id is not deliverable yet — the drain STOPS rather
/// than advancing past it, deliberately trading a stall for a silent permanent hole. And with
/// pipelined proposals, quorum acks complete in network order, not log order, so a later batch can
/// commit while an earlier one is still in flight; delivering it immediately would advance the
/// cursor over the in-flight entry and the exactly-once guard would then suppress that entry's own
/// delivery forever. Such batches are parked and flushed in id order as the blocking proposals
/// resolve.</para>
///
/// <para>Both rules are fixed Jepsen findings (a Log Matching violation and a leader-only applied
/// hole). Do not "optimize" a stall into an advance.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class LogApplicator
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ProposalRegistry proposals;
    private readonly ReadIndexCoordinator readIndex;
    private readonly ILogger<IRaft> logger;

    public LogApplicator(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ProposalRegistry proposals,
        ReadIndexCoordinator readIndex,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.proposals = proposals;
        this.readIndex = readIndex;
        this.logger = logger;
    }

    /// <summary>
    /// Delivers every committed WAL entry from <c>LastAppliedIndex + 1</c> through
    /// <paramref name="upToIndex"/> (inclusive) to the consumer via
    /// <see cref="ApplyLogToConsumerAsync"/>.  Reads the WAL in bounded batches to
    /// avoid loading the full tail into memory.
    ///
    /// <para>Reads ALL entry types so that resolved-but-not-committed entries (rolled back)
    /// advance the cursor instead of reading as gaps — a committed-only read made every
    /// rolled-back id in the range look absent and withheld the drain forever behind it.
    /// Only <c>Committed</c> entries are delivered (<see cref="ApplyLogToConsumerAsync"/>
    /// filters); checkpoints and rolled-back entries just advance the cursor.</para>
    ///
    /// <para>Returns <see langword="false"/> when the drain could not reach
    /// <paramref name="upToIndex"/>: an id absent above the snapshot floor, an unresolved
    /// (<c>Proposed</c>) entry inside the resolved range (its commit marker not yet visible), or
    /// a missing tail. On the follower path this is routine — reads race the write queue and the
    /// leader's re-ship/backfill retries the drain — but at promotion (after the WAL write queue
    /// is fenced) it means the projection genuinely cannot cover the frontier and the caller must
    /// not serve. A no-op returning <see langword="true"/> when <see cref="RaftPartitionCoreState.LastAppliedIndex"/>
    /// already covers <paramref name="upToIndex"/>.</para>
    /// </summary>
    public async Task<bool> DrainCommittedAppliesAsync(long upToIndex, bool skipGaps = false)
    {
        if (upToIndex < 0 || coreState.LastAppliedIndex >= upToIndex)
            return true;

        const int BatchSize = 512;
        long from = coreState.LastAppliedIndex + 1;

        while (from <= upToIndex)
        {
            List<RaftLog> batch = await wal.GetRangeAllTypesAsync(from, BatchSize).ConfigureAwait(false);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                // A row past the target does NOT prove the range below it is covered: with the
                // expected next id still queued in the WAL write scheduler (invisible to this
                // backend read), the first visible row can sit above the target — returning
                // "covered" here skipped the classification below and reported success over an
                // undelivered range. Stop and let the exit checks classify the remainder (gap →
                // withhold and retry; covered → true).
                if (log.Id > upToIndex)
                {
                    from = upToIndex + 1;           // terminate the outer loop; exit checks decide
                    break;
                }
                if (log.Id <= coreState.LastAppliedIndex)
                    continue;                       // already applied (defensive; the read starts at 'from')
                if (log.Id != coreState.LastAppliedIndex + 1 && !skipGaps)
                {
                    // The expected next id (coreState.LastAppliedIndex+1) is absent from the range. Classify by
                    // the snapshot floor rather than by "does an entry exist" — the in-memory commit frontier
                    // (upToIndex) can transiently lead the durable WAL (a write still queued in the WAL
                    // scheduler, or an entry that hole-repair truncated after the frontier overshot it), so an
                    // absent id is ambiguous on its own:
                    //   * expected ABOVE the floor → a real gap (unapplied write lag OR a truncated hole). Both
                    //     are re-shipped by the leader's backfill, so WITHHOLD and let a later drain deliver the
                    //     id and the tail in order. Delivering past it would skip it permanently.
                    //   * expected AT/BELOW the floor, or the -1/0 pre-restore sentinel (below the first log id):
                    //     the id was compacted by a snapshot or never existed. Not a gap — ACCEPT this entry as
                    //     the next contiguous delivery (the cursor advances to it below).
                    // With skipGaps (a sole voter proceeding past an unrecoverable gap), every present
                    // entry is delivered regardless of holes.
                    long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
                    if (coreState.LastAppliedIndex + 1 > 0 && coreState.LastAppliedIndex + 1 > floor)
                        return false;
                }

                // An unresolved entry inside the resolved range: its commit (or rollback) marker has
                // not landed in the backend yet. Withhold — delivering past it would skip it, and
                // advancing the cursor over it would mark it applied without delivery. With skipGaps
                // the marker is unrecoverable (no peer will re-commit it for a sole voter mid-tenure);
                // the entry stays undelivered but the cursor moves on.
                if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                {
                    if (!skipGaps)
                        return false;

                    if (log.Id > coreState.LastAppliedIndex)
                        coreState.LastAppliedIndex = log.Id;
                    continue;
                }

                await ApplyLogToConsumerAsync(log).ConfigureAwait(false);
            }

            long next = coreState.LastAppliedIndex + 1;
            if (next <= from)   // guard: coreState.LastAppliedIndex did not advance past 'from' — would loop forever
                break;
            from = next;
        }

        // The loop can exit with the range uncovered (an empty batch: the tail of the range is
        // absent). A missing tail above the floor is a gap exactly like an interior hole.
        if (coreState.LastAppliedIndex < upToIndex && !skipGaps)
        {
            long expected = coreState.LastAppliedIndex + 1;
            long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
            if (expected > 0 && expected > floor)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Delivers a single committed WAL entry to the consumer state machine and
    /// advances <see cref="RaftPartitionCoreState.LastAppliedIndex"/>.
    ///
    /// <para>Skips entries whose <see cref="RaftLog.Type"/> is not
    /// <see cref="RaftLogType.Committed"/> (e.g. <c>CommittedCheckpoint</c>), but
    /// still advances the cursor so they are not re-read on subsequent drain calls.</para>
    /// </summary>
    public async Task ApplyLogToConsumerAsync(RaftLog log)
    {
        // Deliver each committed index to the consumer at most once. The cursor still advances below for
        // any id past the frontier (including CommittedCheckpoint entries, which are not delivered), but a
        // re-delivery of an already-applied index — which the follower path can see because the leader
        // re-sends committed entries (commit broadcast + backfill/idle re-ship) — must not reach the
        // consumer twice. See CompleteFollowerAppend for the primary site this guards.
        // Promotion-barrier no-ops are consensus-internal: never delivered, cursor still advances.
        if (log.Type == RaftLogType.Committed && log.Id > coreState.LastAppliedIndex
            && log.LogType != RaftSystemConfig.LeadershipBarrierLogType)
        {
            try
            {
                bool ok;
                if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                    ok = await host.InvokeSystemReplicationReceived(host.PartitionId, log).ConfigureAwait(false);
                else
                    ok = await host.InvokeReplicationReceived(host.PartitionId, log).ConfigureAwait(false);

                if (!ok)
                    host.InvokeReplicationError(host.PartitionId, log);
            }
            catch (Exception ex)
            {
                // A throwing consumer bypasses the false-return InvokeReplicationError path;
                // catch here to ensure the error is always reported and the drain continues.
                logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Consumer threw during apply of log {LogId}: {Message}\n{Stacktrace}",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, log.Id, ex.Message, ex.StackTrace);
                host.InvokeReplicationError(host.PartitionId, log);
            }
        }

        if (log.Id > coreState.LastAppliedIndex)
            coreState.LastAppliedIndex = log.Id;

        readIndex.CompleteApplyWaiters();
    }

    /// <summary>
    /// Applies inherited Proposed entries from a prior term in the gap
    /// [<paramref name="from"/>, <paramref name="upToIndex"/>] to the consumer state
    /// machine.  Called at the head of <see cref="CompleteLeaderCommit"/> to deliver
    /// entries that are committed by quorum (the new leader won election with this log)
    /// but have no local proposal waiter and were never touched by
    /// <see cref="CompleteFollowerAppend"/>.
    ///
    /// <para>Only entries from a strictly older term (<see cref="RaftLog.Term"/> &lt;
    /// <c>CurrentTerm</c>) are delivered; current-term Proposed entries are in-flight
    /// writes that have not yet reached quorum and must not be applied prematurely.</para>
    ///
    /// <para>Reads the WAL via <see cref="IRaftWalFacade.GetRangeAllTypesAsync"/> so that
    /// Proposed entries (whose lazy-commit markers may be absent after a crash on the
    /// single-fsync fast path) are visible.</para>
    ///
    /// <para><b>Delivery contract:</b> consumers only ever observe entries with
    /// <see cref="RaftLogType.Committed"/>. An inherited prior-term entry whose WAL record still
    /// reads Proposed is delivered as a normalized <b>copy</b> with the type rewritten to
    /// Committed: delivering the raw WAL instance leaks internal commit bookkeeping and makes the
    /// applied metadata differ from nodes that received the same entry through the commit
    /// broadcast. The drain never stamps the WAL instance itself — the durable re-commit enqueue
    /// (<see cref="EnqueueInheritedRecommitMarkers"/> → <c>EnqueueCommit</c>) is the single
    /// authority that stamps final types.</para>
    ///
    /// <para><b>Gap contract:</b> returns <see cref="InheritedDrainStatus.Hole"/> when an id in the
    /// range is absent above the snapshot floor — a WAL hole. Advancing over it (the old behavior)
    /// would silently skip entries that may be committed elsewhere and mark them applied forever,
    /// leaving the consumer projection permanently incomplete on this node. The caller must not
    /// treat a <c>Hole</c> drain as proof of projection completeness (the barrier completion
    /// reverts the promotion). Ids at/below the floor were compacted and are accepted, exactly as
    /// in <see cref="DrainCommittedAppliesAsync"/>.</para>
    ///
    /// <para><b>In-flight contract:</b> returns <see cref="InheritedDrainStatus.BlockedByInFlight"/>
    /// (without advancing the cursor) when it reaches a current-term <c>Proposed</c> entry. That is
    /// not an inherited orphan but a pipelined proposal still awaiting quorum, and its own
    /// commit/rollback completion delivers it. Advancing the cursor over it here would make that
    /// later delivery hit the exactly-once guard in <see cref="ApplyLogToConsumerAsync"/> and skip
    /// the entry permanently — the leader-only applied-sequence hole found by Jepsen. This applies
    /// even with <paramref name="skipGaps"/>: a sole voter's in-flight proposals still resolve via
    /// self-quorum, so they must not be advanced over either.</para>
    /// </summary>
    public async Task<InheritedDrainStatus> DrainInheritedAppliesAsync(long from, long upToIndex, bool skipGaps = false)
    {
        const int BatchSize = 512;
        long expected = from;

        // Prior-term entries this drain advances over while they are still Proposed on disk. The
        // drain treats them as committed (delivers them to the consumer; the caller serves reads
        // from that state), so their WAL records must be committed DURABLY as well — collected here
        // and re-committed at every exit. Leaving them Proposed is not merely a restart hazard: the
        // backfill read (GetRangeAsync) filters uncommitted entries, so a leader whose inherited
        // range is Proposed on disk silently ships followers an anchored batch that SKIPS that
        // range — the batch lands above the followers' gap, no frontier ever advances, and the
        // partition wedges with no error anywhere (the Jepsen one-stuck-entry shape).
        List<RaftLog>? recommit = null;

        while (from <= upToIndex)
        {
            List<RaftLog> batch = await wal.GetRangeAllTypesAsync(from, BatchSize).ConfigureAwait(false);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                // A row past the range does NOT prove the range was covered: with the expected
                // next id still queued in the WAL write scheduler (invisible to this backend
                // read), the first visible row can sit above upToIndex — returning Covered here
                // let the barrier completion apply its batch and jump the cursor over an entry
                // that was never delivered and never re-committed. Nothing ever revisits it: the
                // consumer projection silently misses a committed entry for the whole tenure, and
                // the (gap-aware) commit frontier pins below it while applied marches on — the
                // CommitMonotonicity violations of CI run 33195170707. Fall through to the exit
                // checks, which classify the unvisited remainder (hole → the caller's retry loop
                // re-reads after the queued write lands; covered → Covered).
                if (log.Id > upToIndex)
                {
                    from = upToIndex + 1;           // terminate the outer loop; exit checks decide
                    break;
                }

                if (log.Id > expected && !skipGaps)
                {
                    long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
                    if (expected > 0 && expected > floor)
                    {
                        // Throttled: the promotion paths retry this drain every 2ms for up to the
                        // barrier timeout, and an unthrottled line per attempt wrote ~4.4k
                        // identical lines per episode in the Jepsen stores.
                        if (ShouldLogDrainHole())
                            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain found a WAL hole: expected {Expected}, next present {Present} (floor {Floor}) (suppressedSinceLastLine={Suppressed}).",
                                host.LocalEndpoint, host.PartitionId, coreState.NodeState, expected, log.Id, floor, TakeSuppressedDrainHoleLogs());
                        EnqueueInheritedRecommitMarkers(recommit);
                        return InheritedDrainStatus.Hole;
                    }
                    // Compacted below the floor: accept this entry as the next contiguous delivery.
                }

                // A current-term unresolved entry is a pipelined proposal still in flight, not an
                // inherited orphan: stop without advancing the cursor over it (see the in-flight
                // contract in the summary). The caller defers its batch until this entry resolves.
                if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint && log.Term >= coreState.CurrentTerm)
                {
                    EnqueueInheritedRecommitMarkers(recommit);
                    return InheritedDrainStatus.BlockedByInFlight;
                }

                expected = log.Id + 1;

                // Advancing over a prior-term Proposed entry commits it (Raft §5.4.2: the
                // current-term commit above it proves the prefix); record it for the durable
                // re-commit. Includes prior-term barrier no-ops — never delivered, but the durable
                // frontier must still pass them. The in-memory frontier advances HERE, at the
                // proof point: waiting for the batched re-commit enqueue at drain exit leaves the
                // applied cursor ahead of the advertised commit frontier for the whole delivery
                // window (a confirmed commit-below-applied inversion to the chaos oracle under
                // load), and permanently if that enqueue hits backpressure.
                if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                {
                    (recommit ??= []).Add(log);
                    wal.MarkInheritedCommitted(log.Id);
                }
                else
                {
                    // Already durably resolved on disk (Committed / RolledBack / checkpoint) — but
                    // possibly NOT yet absorbed by the in-memory frontier: a follower stint can
                    // miss a resolution's bookkeeping (its re-ship heals a follower, but a leader
                    // is never backfilled). This drain advances the applied cursor over the row,
                    // so the frontier must absorb the same resolution or it pins below the cursor
                    // for the whole tenure while every new commit buffers above the phantom gap
                    // (the mid-tenure CommitMonotonicity shape of CI run 33195170707). Gap-buffered
                    // and idempotent: ids at/below the frontier are ignored.
                    wal.MarkInheritedCommitted(log.Id);
                }

                // Apply committed entries and inherited Proposed entries (prior term only).
                // Skip current-term Proposed entries — they are in-flight proposals.
                // Promotion-barrier no-ops (including a prior term's, from a promotion that died
                // before committing its barrier) are consensus-internal and never delivered.
                bool deliver = (log.Type == RaftLogType.Committed ||
                               (log.Type == RaftLogType.Proposed && log.Term < coreState.CurrentTerm))
                               && log.LogType != RaftSystemConfig.LeadershipBarrierLogType;

                // Exactly-once: only deliver entries past the applied frontier (the cursor advances below).
                if (deliver && log.Id > coreState.LastAppliedIndex)
                {
                    // Consumers must only ever observe Committed entries. An inherited prior-term
                    // entry is committed by this drain (§5.4.2), but its WAL record still reads
                    // Proposed until the durable re-commit lands; delivering the WAL instance as-is
                    // leaks that internal state — nodes that receive the same entry through the
                    // commit broadcast observe Committed, so per-node applied metadata diverges
                    // (the Scenario09 applied-prefix divergence). Deliver a normalized copy; the
                    // WAL instance is stamped only by the durable re-commit enqueue, not here.
                    RaftLog delivery = log.Type == RaftLogType.Proposed
                        ? new RaftLog { Id = log.Id, Type = RaftLogType.Committed, Term = log.Term, Time = log.Time, LogType = log.LogType, LogData = log.LogData }
                        : log;

                    try
                    {
                        bool ok;
                        if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                            ok = await host.InvokeSystemReplicationReceived(host.PartitionId, delivery).ConfigureAwait(false);
                        else
                            ok = await host.InvokeReplicationReceived(host.PartitionId, delivery).ConfigureAwait(false);

                        if (!ok)
                            host.InvokeReplicationError(host.PartitionId, delivery);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Consumer threw during inherited-entry apply of log {LogId}: {Message}\n{Stacktrace}",
                            host.LocalEndpoint, host.PartitionId, coreState.NodeState, log.Id, ex.Message, ex.StackTrace);
                        host.InvokeReplicationError(host.PartitionId, delivery);
                    }
                }

                if (log.Id > coreState.LastAppliedIndex)
                    coreState.LastAppliedIndex = log.Id;
            }

            long next = coreState.LastAppliedIndex + 1;
            if (next <= from)   // guard: no progress (e.g. all entries were checkpoints or wrong term)
                break;
            from = next;
        }

        EnqueueInheritedRecommitMarkers(recommit);

        // The loop can also exit without reaching upToIndex (an empty batch: the whole tail of the
        // range is absent). A missing tail above the floor is a hole exactly like an interior gap.
        if (expected <= upToIndex && !skipGaps)
        {
            long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
            if (expected > 0 && expected > floor)
            {
                // Same 1s throttle as the interior-gap line above (the 2ms retry loops).
                if (ShouldLogDrainHole())
                    logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain missing the range tail: expected through {UpToIndex}, present through {Expected} (floor {Floor}) (suppressedSinceLastLine={Suppressed}).",
                        host.LocalEndpoint, host.PartitionId, coreState.NodeState, upToIndex, expected - 1, floor, TakeSuppressedDrainHoleLogs());
                return InheritedDrainStatus.Hole;
            }
        }

        readIndex.CompleteApplyWaiters();
        return InheritedDrainStatus.Covered;
    }

    /// <summary>
    /// Durably commits inherited prior-term entries the drain advanced over: writes their commit
    /// markers via <see cref="IRaftWalFacade.EnqueueCommit"/> so the on-disk log converges with the
    /// in-memory decision that they are committed. Without this, the entries stay Proposed on disk:
    /// a leader crash re-loses the applied projection they back, and — worse — the backfill read
    /// filters them out, so followers missing the range are shipped anchored batches that silently
    /// skip it and the partition wedges (see the note at the collection site). Lazy like all commit
    /// markers on the single-fsync path: the enqueue is not awaited for durability, and a
    /// backpressure rejection only defers the repair to the next drain — delivery already happened,
    /// so failing the drain over it would be strictly worse.
    ///
    /// OWNERSHIP: the list is handed to the WAL operation, which holds the REFERENCE until a
    /// worker thread serializes it later — the caller must neither clear nor reuse it after this
    /// call. An earlier version called <c>inherited.Clear()</c> here "for safety": the async write
    /// then drained an empty payload, wrote nothing, and completed Success — the re-commit logged
    /// as done while the rows stayed <c>Proposed</c> on disk, which is exactly the silent
    /// leader-side wedge observed in Jepsen run 31766873204 (n3/p2, rows 180..181, 4.7k backfill
    /// refusals). Every drain exit returns immediately after calling this, so no double-enqueue
    /// is possible without the clear.
    /// </summary>
    /// <summary>Monotonic tick of the last drain-hole error line, for the 1s log throttle.</summary>
    private long lastDrainHoleLogTicks;

    /// <summary>Drain-hole error lines suppressed since the last one written.</summary>
    private long suppressedDrainHoleLogs;

    /// <summary>
    /// 1-per-second throttle for the drain-hole error lines: the promotion paths retry the
    /// inherited drain every 2ms for up to the barrier timeout, and logging every attempt wrote
    /// thousands of identical lines per episode. Runs on the partition's serialized executor path,
    /// so plain fields suffice.
    /// </summary>
    private bool ShouldLogDrainHole()
    {
        long now = global::System.Diagnostics.Stopwatch.GetTimestamp();

        if (lastDrainHoleLogTicks != 0 && (now - lastDrainHoleLogTicks) < global::System.Diagnostics.Stopwatch.Frequency)
        {
            suppressedDrainHoleLogs++;
            return false;
        }

        lastDrainHoleLogTicks = now;
        return true;
    }

    /// <summary>Returns and resets the suppressed-line count for inclusion in the next line.</summary>
    private long TakeSuppressedDrainHoleLogs()
    {
        long suppressed = suppressedDrainHoleLogs;
        suppressedDrainHoleLogs = 0;
        return suppressed;
    }

    private void EnqueueInheritedRecommitMarkers(List<RaftLog>? inherited)
    {
        if (inherited is null || inherited.Count == 0)
            return;

        try
        {
            WALWriteOperation operation = wal.EnqueueCommit(inherited);

            RaftPendingWalOperation pending = proposals.RentPending();
            pending.IsInheritedRecommit = true;
            proposals.TrackPending(operation.OperationId, pending);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Durably re-committing {Count} inherited prior-term entries ({First}..{Last})",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, inherited.Count, inherited[0].Id, inherited[^1].Id);
        }
        catch (Exception ex)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Could not enqueue durable re-commit of {Count} inherited entries ({Message}) — the on-disk range stays Proposed and unbackfillable until the next promotion retries.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, inherited.Count, ex.Message);
        }
    }


    /// <summary>
    /// Parks a leader batch (committed or rolled-back entries) whose completion arrived while an
    /// earlier current-term proposal was still unresolved below it. Flushed in id order by
    /// <see cref="FlushDeferredLeaderAppliesAsync"/> once the applied cursor reaches the batch.
    /// Copies the list: proposals and their pending-operation envelopes are pooled, so retaining
    /// <c>proposal.Logs</c> itself would alias a buffer that gets recycled. The <see cref="RaftLog"/>
    /// instances are safe to retain (not pooled), and <see cref="RaftWriteAhead.EnqueueCommit"/> /
    /// <c>EnqueueRollback</c> already stamped their final types, so a later flush delivers (or
    /// advances over) them correctly.
    /// </summary>
    public void DeferLeaderApplies(long minLogIndex, List<RaftLog> logs)
    {
        if (deferredLeaderAppliesTerm != coreState.CurrentTerm)
        {
            deferredLeaderApplies.Clear();
            deferredLeaderAppliesTerm = coreState.CurrentTerm;
        }

        deferredLeaderApplies[minLogIndex] = new List<RaftLog>(logs);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Deferring apply of batch starting at {MinLogIndex}: an earlier proposal below it is still in flight (applied cursor {LastApplied}).",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, minLogIndex, coreState.LastAppliedIndex);
    }

    /// <summary>
    /// Delivers deferred out-of-order leader batches that have become contiguous with the applied
    /// cursor, in id order. Called wherever the leader path advances the cursor (commit and
    /// rollback completions): the batch just applied may have been the in-flight blocker that
    /// earlier out-of-order completions deferred behind. Per-log exactly-once is preserved by the
    /// cursor guard inside <see cref="ApplyLogToConsumerAsync"/>. Clears the buffer wholesale when
    /// the term has moved on — the WAL-based drains own delivery after a step-down, and a stale
    /// rolled-back range could since have been re-proposed at the same ids.
    /// </summary>
    public async ValueTask FlushDeferredLeaderAppliesAsync()
    {
        if (deferredLeaderApplies.Count == 0)
            return;

        if (deferredLeaderAppliesTerm != coreState.CurrentTerm)
        {
            deferredLeaderApplies.Clear();
            return;
        }

        while (deferredLeaderApplies.Count > 0)
        {
            KeyValuePair<long, List<RaftLog>> next = deferredLeaderApplies.First();
            if (next.Key > coreState.LastAppliedIndex + 1)
                break;

            deferredLeaderApplies.Remove(next.Key);

            foreach (RaftLog log in next.Value)
                await ApplyLogToConsumerAsync(log).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Leader batches whose WAL completion arrived while an earlier current-term proposal was
    /// still unresolved below them, keyed by the batch's lowest log id. With pipelined proposals,
    /// quorum acks complete in network order, not log order: a later proposal can commit while an
    /// earlier one is still in flight. Delivering the later batch immediately would advance
    /// <see cref="RaftPartitionCoreState.LastAppliedIndex"/> over the in-flight entry, and the exactly-once guard in
    /// <see cref="ApplyLogToConsumerAsync"/> would then suppress that entry's own delivery forever —
    /// a permanent hole in the leader's applied sequence (the Jepsen Log Matching violation).
    /// Batches parked here are flushed in id order by <see cref="FlushDeferredLeaderAppliesAsync"/>
    /// as the blocking proposals resolve (commit or rollback).
    /// </summary>
    private readonly SortedDictionary<long, List<RaftLog>> deferredLeaderApplies = [];

    /// <summary>
    /// Term the entries in <see cref="deferredLeaderApplies"/> were deferred in. A term change
    /// invalidates the buffer: after a step-down the WAL-based drains (follower append or the next
    /// promotion) own in-order delivery, and a rolled-back id from the stale tenure could be
    /// re-proposed with a different payload, so flushing stale advance-only ranges would skip real
    /// entries. Checked lazily on every defer/flush rather than at each of the many
    /// leader→follower transition sites.
    /// </summary>
    private long deferredLeaderAppliesTerm = -1;
}
