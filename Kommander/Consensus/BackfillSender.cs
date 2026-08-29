using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// The leader's outbound AppendEntries path: enqueueing a batch to one peer, and reading the
/// bounded committed range that brings a lagging peer forward.
///
/// <para><b>Why the saturation check lives here.</b> This type is the single choke point every
/// entry-carrying batch passes through — the heartbeat round, the ack fast-path re-supply, and the
/// forced heartbeat after every leadership publication all funnel into it. The observed saturation
/// storm was driven by election churn rather than by a timer, so a throttle attached to the
/// heartbeat interval would not have caught it.</para>
///
/// <para><b>The anchor-contiguity refusal is load-bearing.</b> An anchored batch asserts, through
/// its <c>(prevLogIndex, prevLogTerm)</c> pair, that its entries immediately follow the anchor.
/// When the WAL read comes back starting above the anchor, no committed entry sits there, and
/// shipping it anyway lands the batch over the follower's gap, advances nothing, and repeats
/// forever with no error surfacing anywhere — the observed Jepsen wedge. Refusing and routing to a
/// snapshot is the correct response; do not "fix" the refusal by relaxing the anchor.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class BackfillSender
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ReplicationTracker tracker;
    private readonly NonContiguousBackfillTracker backfillTracker;
    private readonly SnapshotSender snapshotSender;
    private readonly RaftPartitionLogThrottle logThrottle;
    private readonly ILogger<IRaft> logger;

    public BackfillSender(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ReplicationTracker tracker,
        NonContiguousBackfillTracker backfillTracker,
        SnapshotSender snapshotSender,
        RaftPartitionLogThrottle logThrottle,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.tracker = tracker;
        this.backfillTracker = backfillTracker;
        this.snapshotSender = snapshotSender;
        this.logThrottle = logThrottle;
        this.logger = logger;
    }

    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// <paramref name="prevLogIndex"/> and <paramref name="prevLogTerm"/> are the Log Matching
    /// anchors: the id and term of the entry immediately preceding the first entry in
    /// <paramref name="logs"/>.  Both default to 0 (no anchor check) for heartbeats and live
    /// proposals where the leader knows the follower is in sync; they are set for backfill batches
    /// so a divergent follower can reject with <see cref="RaftOperationStatus.LogMismatch"/> and
    /// enable leader-side backtracking via <c>nextIndex</c>.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <param name="prevLogIndex">Id of the entry immediately before the first entry in <paramref name="logs"/>; 0 skips the check.</param>
    /// <param name="prevLogTerm">Term of the entry at <paramref name="prevLogIndex"/>; 0 when index is 0.</param>
    public void AppendLogToNode(
        RaftNode node,
        HLCTimestamp timestamp,
        List<RaftLog>? logs,
        long prevLogIndex = 0,
        long prevLogTerm = 0,
        bool quiesce = false,
        AppendLogsGrpcLogCache? grpcLogCache = null)
    {
        AppendLogsRequest request;

        if (logs is null || logs.Count == 0)
            request = new(host.PartitionId, coreState.CurrentTerm, timestamp, host.LocalEndpoint) { Quiesce = quiesce };
        else
        {
            request = new(host.PartitionId, coreState.CurrentTerm, timestamp, host.LocalEndpoint, logs, prevLogIndex, prevLogTerm)
            {
                Quiesce = quiesce,
                GrpcLogCache = grpcLogCache,
            };

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}/{PartitionId}/{State}] Enqueued entries for {Endpoint} {Timestamp} PrevLogIndex={PrevLogIndex} Logs={Logs}",
                    host.LocalEndpoint,
                    host.PartitionId,
                    coreState.NodeState,
                    node.Endpoint,
                    timestamp,
                    prevLogIndex,
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );
        }

        host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.AppendLogs, node, request));
    }

    /// <summary>
    /// Reads a bounded committed range for <paramref name="node"/> from the WAL and ships it via
    /// <see cref="AppendLogToNode"/> with the correct Log Matching anchors.
    /// Returns <see cref="BackfillSendResult.Sent"/> when at least one entry was shipped; otherwise
    /// the specific reason nothing was sent. The reaction by cause happens HERE, not at a call
    /// site: <see cref="BackfillSendResult.CompactionFloor"/> and the deliberate
    /// <see cref="BackfillSendResult.NonContiguous"/> refusal escalate to a snapshot transfer via
    /// <see cref="EscalateRefusalToSnapshotAsync"/> before they return, while
    /// <see cref="BackfillSendResult.SaturationPaused"/> simply waits — a saturated follower is not
    /// below the floor, and shipping it a full snapshot only adds load. Callers may still branch on
    /// the result for their own flow, but no caller is responsible for the rescue.
    /// </summary>
    /// <param name="node">The peer to send the batch to.</param>
    /// <param name="followerMaxLog">The highest committed index the leader believes the follower holds;
    /// used as the fallback start when <c>nextIndex</c> has not been backtracked below it.</param>
    /// <param name="timestamp">HLC timestamp to stamp the outbound request.</param>
    /// <param name="anchorToFollowerFrontier">
    /// Ignores <c>nextIndex</c> and starts the batch at <paramref name="followerMaxLog"/> + 1.
    /// Required by the fast-path re-supply of a <b>regressed</b> follower: <c>nextIndex</c> is
    /// derived from the monotonic <c>matchIndex</c> and so still points above the frontier the
    /// follower just reported, which is precisely the range that must be re-shipped.
    /// </param>
    /// <param name="round">
    /// Optional per-round memo (see <see cref="BackfillRoundBatches"/>). When supplied, a range already
    /// read in this heartbeat round is reused instead of being re-read and re-decoded from the WAL for
    /// each follower anchored at the same index — the common shape of a multi-follower catch-up. Pass
    /// <see langword="null"/> from one-off call sites, where there is nothing to share with.
    /// </param>
    public async Task<BackfillSendResult> TrySendBackfillBatchAsync(
        RaftNode node,
        long followerMaxLog,
        HLCTimestamp timestamp,
        bool anchorToFollowerFrontier = false,
        BackfillRoundBatches? round = null)
    {
        // Saturation backoff. This peer refused a batch because its WAL queue was full, and it
        // needs an interval in which to drain before another one arrives. Checked here rather
        // than at the call sites because this is the single choke point every entry-carrying
        // batch passes through — the heartbeat round, the ack fast-path re-supply, and the
        // forced heartbeat that follows every leadership publication all funnel into it. That
        // last one matters: the observed storm was driven by election churn, not by a timer, so
        // a throttle attached to the heartbeat interval would not have caught it.
        if (tracker.IsBackfillPaused(node.Endpoint))
            return BackfillSendResult.SaturationPaused;

        // Outbound-queue saturation. Unlike the ack-driven pause above, this gate needs no reply
        // from the peer: a follower that stopped draining entirely (SIGSTOP pause, dead network)
        // never acks, so the ack-driven pause never engages, and every heartbeat used to read a
        // fresh full batch from the WAL and stack it onto the peer's outbound queue behind all the
        // previous ones. The queue byte-cap would drop the batch anyway; checking here skips the
        // WAL read and the batch materialization altogether. SaturationPaused deliberately does
        // not escalate to a snapshot: the peer is not below the compaction floor, it is just not
        // consuming, and a snapshot transfer would only add to the unsent backlog.
        if (host.IsOutboundQueueSaturated(node.Endpoint))
            return BackfillSendResult.SaturationPaused;

        // No-progress pacing. A follower whose reported commit frontier is stuck — a lost lazy
        // commit marker below the leader's monotonic matchIndex — acknowledges every duplicate
        // batch with Success and the same frontier, and on the single-fsync path each such ack
        // funnels right back into this method: unpaced, the pair ping-pongs at network speed
        // forever, reading a WAL range per iteration on the shared read scheduler with zero
        // progress, zero writes, and zero log lines. The probe self-heals on any frontier
        // advance; while a fruitless streak stands, further batches wait out an exponential
        // pause (heartbeat-interval base, capped) BEFORE the WAL read is issued.
        //
        // The anchored repair paths are EXEMPT. anchorToFollowerFrontier marks the heartbeat's
        // regressed-frontier and mismatch-anchored repairs: both are take-once notes acted on at
        // most once per heartbeat interval, so they cannot self-excite — and both exist precisely
        // to converge a peer whose frontier is not advancing. Pausing them consumed the note and
        // shipped nothing, deferring the repair to whenever the peer's NEXT rejection re-recorded
        // it AND the pause window had expired; under a restart-heavy fault profile that starved
        // meta-partition repair for whole fault cycles (the Jepsen 1.3.4 regression). The streak
        // still records these ships, so a peer that stays stuck even against anchored repair still
        // warns and still paces the unanchored paths.
        long reportedFrontier = tracker.GetCommitFrontierOrDefault(node.Endpoint, -1);
        ReplicationTracker.BackfillProgress progress = tracker.ObserveBackfillProgress(node.Endpoint, reportedFrontier);

        if (!anchorToFollowerFrontier && progress.FruitlessShips > 0)
        {
            TimeSpan pause = NoProgressPause(progress.FruitlessShips);
            if (pause > TimeSpan.Zero
                && RaftMonotonic.Elapsed(progress.LastShipTicks, host.GetMonotonicTimestamp()) < pause)
            {
                KommanderMetrics.RecordBackfillNoProgressPause(host.PartitionId);
                logThrottle.LogBackfillNoProgressPaused(node.Endpoint, progress.FruitlessShips, pause, reportedFrontier);
                return BackfillSendResult.NoProgressPaused;
            }
        }

        // Anchor fallback. nextIndex derives from the monotonic matchIndex, which a transiently
        // overshooting frontier report can pin ABOVE the entry the follower actually needs; every
        // batch anchored there is a duplicate the follower acknowledges without progress. After
        // the configured number of fruitless ships, anchor at the reported frontier instead: that
        // re-ships the first entry the follower has not committed — including its commit marker,
        // the piece a marker-loss wedge is missing. Anchoring low only costs redundant idempotent
        // entries; anchoring high costs convergence.
        bool reanchorAtReportedFrontier = !anchorToFollowerFrontier
            && host.Configuration.BackfillNoProgressAnchorFallbackShips > 0
            && reportedFrontier >= 0
            && progress.FruitlessShips >= host.Configuration.BackfillNoProgressAnchorFallbackShips;

        long from;
        if (anchorToFollowerFrontier)
            from = followerMaxLog + 1;
        else if (reanchorAtReportedFrontier)
        {
            from = reportedFrontier + 1;

            // Observability: without this line a partition can re-anchor for a whole run with no
            // evidence anywhere below the 4-ship Warning — the gap that kept the Jepsen 1.3.4
            // analysis at "hypothesis". Bounded: a re-anchored attempt on this path is itself
            // paced by the no-progress pause, so the volume cannot exceed the ship rate.
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}/{PartitionId}/{State}] Backfill anchor fallback for {Endpoint}: fruitlessShips={FruitlessShips}, anchoring at reported frontier {Frontier} instead of nextIndex",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState,
                    node.Endpoint, progress.FruitlessShips, reportedFrontier);
        }
        else if (tracker.TryGetNextIndex(node.Endpoint, out long ni) && ni <= coreState.LocalCommittedIndex)
            from = ni;
        else
            from = followerMaxLog + 1;

        long prevIdx = from - 1;

        if (round is not null && round.TryGet(from, out BackfillRoundBatches.Batch? cached))
        {
            if (cached!.Logs.Count == 0)
            {
                // A memoized refusal is still a refusal for THIS follower: it must escalate to the
                // snapshot fallback exactly like the follower that triggered the original read.
                if (cached.EmptyResult != BackfillSendResult.SaturationPaused)
                    await EscalateRefusalToSnapshotAsync(node).ConfigureAwait(false);
                return cached.EmptyResult;
            }

            logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, coreState.NodeState, cached.Logs.Count, node.Endpoint, from, prevIdx, coreState.LocalCommittedIndex);

            backfillTracker.ClearIfCovered(node.Endpoint, from, "a contiguous batch was shipped at or below the episode anchor");
            AppendLogToNode(node, timestamp, cached.Logs, prevIdx, cached.PrevTerm, grpcLogCache: cached.GrpcLogCache);
            RecordShipped(node, reportedFrontier, from);
            return BackfillSendResult.Sent;
        }

        // Read the log AS STORED (all row types), not just committed rows. Classic AppendEntries
        // ships any entry from the leader's log and signals commitment separately; here the row
        // types themselves are the commit signal, and shipping a Proposed or RolledBack row simply
        // reproduces the leader's bookkeeping on the follower (the planner has an arm for every
        // type). Filtering to committed rows made the leader's uncommitted inherited tail
        // unshippable, which deadlocked promotions under the over-gap ack gate: the barrier could
        // not commit until the followers' gaps were filled, and the gap rows could not ship until
        // the barrier's commit re-committed them. It also mis-routed the "Proposed run at the
        // anchor" case into the snapshot fallback. Quorum integrity is unaffected: backfill acks
        // carry the backfill timestamp, never a proposal ticket, so a shipped Proposed row still
        // gains quorum credit only through the proposal-retry re-ack (gated on contiguous
        // presence).
        List<RaftLog> backfill = await wal.GetRangeAllTypesAsync(
            from,
            host.Configuration.MaxBackfillEntriesPerRound,
            host.Configuration.MaxBackfillBytesPerRound).ConfigureAwait(false);

        if (backfill.Count == 0)
        {
            // Memoize the empty result as well: every follower anchored here would otherwise repeat the
            // same read before falling through to the snapshot path.
            round?.Add(from, backfill, 0);
            await EscalateRefusalToSnapshotAsync(node).ConfigureAwait(false);
            return BackfillSendResult.CompactionFloor;
        }

        // Anchor-contiguity guard: an anchored batch asserts, via (prevIdx, prevTerm), that its
        // entries IMMEDIATELY follow the anchor. The read came back with a first id ABOVE `from`,
        // so no committed entry exists at the anchor — shipping it anchored at from-1 would land it
        // over the follower's gap, advance nothing, and repeat forever with no error anywhere (the
        // observed Jepsen wedge). With the all-types read above, a Proposed run at the anchor now
        // ships instead of tripping this guard; the remaining cause is a row genuinely absent at
        // `from` — a compaction floor above the anchor (the peer is below the floor and only a
        // snapshot can seed it), or a truncated hole. The reported message carries the anchor, the
        // first id, and the last checkpoint so a reader can tell. Refuse to ship: NonContiguous
        // deliberately routes the heartbeat path to its snapshot fallback.
        //
        // The condition must stay visible, but "never suppress" was previously implemented as "warn
        // on every heartbeat forever", which buries the signal where the repair never lands.
        // ReportNonContiguousBackfillAsync scopes it to one Warning per episode and keeps the live
        // condition queryable through GetBackfillStatuses — the SnapshotSender discipline, applied
        // to the refusal that routes into it.
        if (backfill[0].Id != from)
        {
            await backfillTracker.ReportAsync(node.Endpoint, from, backfill[0].Id).ConfigureAwait(false);
            round?.Add(from, [], 0, BackfillSendResult.NonContiguous);
            await EscalateRefusalToSnapshotAsync(node).ConfigureAwait(false);
            return BackfillSendResult.NonContiguous;
        }

        long prevTerm = prevIdx > 0 ? await wal.GetAnyTermAtAsync(prevIdx).ConfigureAwait(false) : 0;

        BackfillRoundBatches.Batch? shared = round?.Add(from, backfill, prevTerm);

        logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, coreState.NodeState, backfill.Count, node.Endpoint, from, prevIdx, coreState.LocalCommittedIndex);

        backfillTracker.ClearIfCovered(node.Endpoint, from, "a contiguous batch was shipped at or below the episode anchor");
        AppendLogToNode(node, timestamp, backfill, prevIdx, prevTerm, grpcLogCache: shared?.GrpcLogCache);
        RecordShipped(node, reportedFrontier, from);
        return BackfillSendResult.Sent;
    }

    /// <summary>
    /// Fruitless-ship count at which a no-progress episode logs its one Warning (and counts one
    /// episode metric). By this point the anchor fallback has already re-anchored at the reported
    /// frontier and the batch still produced no advance, so the follower cannot be converged by
    /// log shipping alone — an operator-relevant condition, not a transient.
    /// </summary>
    private const int NoProgressWarnShips = 4;

    /// <summary>
    /// The pause owed after <paramref name="fruitlessShips"/> consecutive ships without frontier
    /// progress: heartbeat-interval base, doubling per fruitless ship, capped by configuration.
    /// A zero heartbeat interval disables the pacing (test configurations).
    /// </summary>
    private TimeSpan NoProgressPause(int fruitlessShips)
    {
        TimeSpan basePause = host.Configuration.HeartbeatInterval;
        if (basePause <= TimeSpan.Zero)
            return TimeSpan.Zero;

        double pauseMs = basePause.TotalMilliseconds * Math.Pow(2, Math.Min(fruitlessShips - 1, 20));

        TimeSpan cap = host.Configuration.BackfillNoProgressPauseCap;
        if (cap > TimeSpan.Zero && pauseMs > cap.TotalMilliseconds)
            pauseMs = cap.TotalMilliseconds;

        return TimeSpan.FromMilliseconds(pauseMs);
    }

    /// <summary>
    /// Records a shipped batch against the peer's convergence probe and, when a fruitless streak
    /// crosses <see cref="NoProgressWarnShips"/>, logs the episode's single Warning and counts it.
    /// </summary>
    private void RecordShipped(RaftNode node, long reportedFrontier, long anchor)
    {
        int fruitlessShips = tracker.RecordBackfillShip(node.Endpoint, reportedFrontier);

        if (fruitlessShips < NoProgressWarnShips || !tracker.TryMarkBackfillNoProgressWarned(node.Endpoint))
            return;

        KommanderMetrics.RecordBackfillNoProgressEpisode(host.PartitionId);
        logger.LogWarning(
            "[{LocalEndpoint}/{PartitionId}/{State}] Backfill to {Endpoint} shipped {Ships} consecutive batches without its reported commit frontier advancing past {Frontier}; batches are now paced and anchored at the frontier (last anchor {Anchor})",
            host.LocalEndpoint, host.PartitionId, coreState.NodeState,
            node.Endpoint, fruitlessShips, reportedFrontier, anchor);
    }

    /// <summary>
    /// Escalates one refused backfill attempt to a snapshot transfer (or records that no snapshot
    /// can be produced).
    ///
    /// <para><b>Why this lives at the choke point and not at a call site.</b> The escalation used to
    /// run only inside <c>HeartbeatDriver.SendHeartbeat</c>. The ack fast-path re-supply calls this
    /// sender too and discarded its result, so a peer whose refusals all arrived through acks was
    /// never offered a snapshot: the Caraxes soak wedged a healthy 3-voter cluster permanently that
    /// way — 24,253 refusals, zero snapshot attempts. Escalating here makes
    /// a refusal without an escalation impossible, from every present and future caller.</para>
    ///
    /// <para><b>Cost and pacing.</b> <see cref="SnapshotSender.CanAttempt"/> is checked first, so a
    /// refusal during an in-flight transfer or inside a failure backoff window costs two dictionary
    /// lookups and no WAL read. Only a compaction floor above zero escalates: a refusal caused by an
    /// uncommitted run on an uncompacted WAL has nothing to snapshot from and is repaired by the
    /// inherited-tail re-commit instead.</para>
    /// </summary>
    private async Task EscalateRefusalToSnapshotAsync(RaftNode node)
    {
        if (coreState.NodeState != RaftNodeState.Leader)
            return;

        if (!snapshotSender.CanAttempt(node.Endpoint))
            return;

        long lastCheckpoint = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
        if (lastCheckpoint <= 0)
            return;

        bool p0System = host.PartitionId == RaftSystemConfig.SystemPartition && host.SystemStateTransfer is not null;
        if (host.PartitionStateTransfer is not null || host.StateMachineTransfer is not null || p0System)
        {
            // The in-flight guard inside TrySend prevents duplicate transfers; the postToExecutor
            // callback advances the follower's recorded frontier once it confirms installation.
            // LastIncludedTerm is the term at the checkpoint index (may be -1 when compacted away,
            // in which case the receiver falls back to its own matching rules).
            long lastIncludedTerm = await wal.GetAnyTermAtAsync(lastCheckpoint).ConfigureAwait(false);
            snapshotSender.TrySend(node, lastCheckpoint, coreState.CurrentTerm, lastIncludedTerm);
        }
        else
        {
            // The follower needs a snapshot and none can be produced. Record the condition (one
            // Warning per episode, queryable via GetSnapshotStatuses) so an operator can see it
            // and register a transfer — a silent skip here is how a peer gets stranded with no
            // evidence anywhere.
            snapshotSender.ReportUnproducible(node);
        }
    }

    /// <summary>
    /// Called when a follower has acknowledged (or rejected) an AppendLogs request.
    /// On <see cref="RaftOperationStatus.Success"/> advances <c>matchIndex</c> and
    /// <c>nextIndex</c> for the peer and immediately ships the next bounded backfill
    /// batch if the follower is still behind, so convergence does not wait a full heartbeat
    /// interval per batch.
    /// On <see cref="RaftOperationStatus.LogMismatch"/> backtracks <c>nextIndex</c> using
    /// <c>max(1, min(nextIndex-1, followerMax+1))</c>, which always steps back
    /// at least one position even when the follower's max equals the anchor we sent.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="timestamp"></param>
    /// <param name="status"></param>
    /// <param name="committedIndex"></param>
    /// <param name="responseTerm">
    /// The term the acknowledging follower stamped on its reply. A follower ACK is only meaningful to
    /// the node that is currently the leader of that term; a delayed ACK from an earlier term must not
    /// repopulate progress/backfill/startCommitIndexes state after a step-down or term change. A value
    /// &lt; 0 means "not set" (legacy / in-process / test callers) and bypasses the fence, mirroring
    /// <see cref="CompleteWalOperationAsync"/>.
    /// </param>
    /// <summary>
    /// True when at least one peer is a voter. Replaces LINQ <c>Any</c> with a plain loop on
    /// paths that run per propose/commit — the capturing lambda allocated a closure per call.
    /// </summary>
    public bool HasVoterPeer()
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;
        for (int i = 0; i < nodes.Count; i++)
        {
            if (host.IsVoter(nodes[i].Endpoint))
                return true;
        }

        return false;
    }
}
