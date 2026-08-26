
using System.Diagnostics.Metrics;
using Kommander.Scheduling;
using Kommander.WAL.IO;

namespace Kommander.Diagnostics;

/// <summary>
/// Central <see cref="Meter"/> for Kommander.
///
/// <para>All instruments are static and shared for the lifetime of the process.
/// They have zero allocation overhead when no listener (<see cref="MeterListener"/>,
/// OpenTelemetry SDK, dotnet-counters, etc.) is attached.</para>
///
/// <para>The meter name is <c>"Kommander"</c>. Consumers should subscribe to that
/// name to receive all library metrics.</para>
/// </summary>
public static class KommanderMetrics
{
    /// <summary>The name used when creating the <see cref="Meter"/>.</summary>
    public const string MeterName = "Kommander";

    internal static readonly Meter Meter =
        new(MeterName, typeof(KommanderMetrics).Assembly.GetName().Version?.ToString() ?? "1.0");

    // ── Partition executor ────────────────────────────────────────────────────

    /// <summary>Total operations successfully processed by all partition executors.</summary>
    internal static readonly Counter<long> ExecutorOperationsTotal =
        Meter.CreateCounter<long>(
            "raft.executor.operations_total",
            description: "Total operations processed by partition executors, by class.");

    /// <summary>Total client proposals rejected by the admission gate (queue full).</summary>
    internal static readonly Counter<long> ExecutorRejectionsTotal =
        Meter.CreateCounter<long>(
            "raft.executor.rejections_total",
            description: "Total client proposals rejected because the per-partition queue was full.");

    /// <summary>
    /// Dispatch latency for individual operations, in milliseconds, tagged by
    /// <c>partition_id</c> and <c>operation_class</c> (Control/Replication/Client/Maintenance).
    /// </summary>
    internal static readonly Histogram<double> ExecutorOperationDurationMs =
        Meter.CreateHistogram<double>(
            "raft.executor.operation_duration_ms",
            unit: "ms",
            description: "Per-operation dispatch latency in the partition executor, by operation class.");

    // ── WAL scheduler ─────────────────────────────────────────────────────────

    /// <summary>Total WAL write batches dispatched to the storage adapter.</summary>
    internal static readonly Counter<long> WalBatchesTotal =
        Meter.CreateCounter<long>(
            "raft.wal.batches_total",
            description: "Total WAL write batches dispatched to the storage adapter.");

    /// <summary>Total individual WAL write operations completed.</summary>
    internal static readonly Counter<long> WalOperationsTotal =
        Meter.CreateCounter<long>(
            "raft.wal.operations_total",
            description: "Total individual WAL write operations completed.");

    /// <summary>
    /// Distribution of WAL write batch sizes (number of operations per flush).
    /// Use this to validate scheduler batching efficiency under load.
    /// </summary>
    internal static readonly Histogram<int> WalBatchSize =
        Meter.CreateHistogram<int>(
            "raft.wal.batch_size",
            description: "Distribution of WAL write batch sizes (operations per storage flush).");

    /// <summary>
    /// Entries between the application-durability floor and the last checkpoint, sampled once per
    /// compaction pass when an <see cref="IApplicationDurabilityProvider"/> is configured. A value
    /// bounded by the application's flush cadence is healthy; sustained growth means the
    /// application flusher is falling behind (or stalled) and the WAL is being retained for it.
    /// </summary>
    internal static readonly Histogram<long> WalDurabilityFloorLag =
        Meter.CreateHistogram<long>(
            "raft.wal.durability_floor_lag",
            description: "Entries between the application-durability floor and the last checkpoint, per compaction pass.");

    /// <summary>
    /// Compaction passes that removed nothing because every remaining removable entry sits above
    /// the application-durability floor. A sustained rate indicates a stalled application flusher
    /// (the WAL grows without bound until the floor advances).
    /// </summary>
    internal static readonly Counter<long> WalCompactionBlockedByDurabilityFloorTotal =
        Meter.CreateCounter<long>(
            "raft.wal.compaction_blocked_by_durability_floor_total",
            description: "Compaction passes fully blocked by the application-durability floor.");

    /// <summary>
    /// Every compaction pass that was invoked, tagged by <c>partition_id</c> and <c>outcome</c>:
    /// <c>no_checkpoint</c> (nothing has checkpointed, so there is no floor to compact to),
    /// <c>floor_not_positive</c> (a composed retention floor left nothing removable),
    /// <c>effective</c> (the pass reached <c>CompactLogsOlderThan</c>), or <c>failed</c>.
    ///
    /// <para>The sum across outcomes is "passes invoked", so invoked-vs-effective is readable from
    /// telemetry alone. This exists because a WAL that never shrinks used to produce no signal at
    /// all: the passes fired and returned before the first log statement, making "the threshold was
    /// never reached" indistinguishable from "every pass found no checkpoint" on a dashboard.</para>
    /// </summary>
    internal static readonly Counter<long> WalCompactionPassesTotal =
        Meter.CreateCounter<long>(
            "raft.wal.compaction_passes_total",
            description: "Compaction passes invoked, by outcome (no_checkpoint/floor_not_positive/effective/failed).");

    /// <summary>Outcome tag values for <see cref="WalCompactionPassesTotal"/>.</summary>
    internal static class CompactionOutcome
    {
        internal const string NoCheckpoint = "no_checkpoint";
        internal const string FloorNotPositive = "floor_not_positive";
        internal const string Effective = "effective";
        internal const string Failed = "failed";
    }

    /// <summary>Counts one invoked compaction pass, tagged by partition and outcome.</summary>
    internal static void RecordCompactionPass(int partitionId, string outcome) =>
        WalCompactionPassesTotal.Add(1,
            new KeyValuePair<string, object?>("partition_id", partitionId),
            new KeyValuePair<string, object?>("outcome", outcome));

    /// <summary>Records the durability-floor lag for one compaction pass, tagged by partition.</summary>
    internal static void RecordDurabilityFloorLag(int partitionId, long lag) =>
        WalDurabilityFloorLag.Record(lag, new KeyValuePair<string, object?>("partition_id", partitionId));

    /// <summary>Counts one compaction pass fully blocked by the durability floor, tagged by partition.</summary>
    internal static void RecordCompactionBlockedByDurabilityFloor(int partitionId) =>
        WalCompactionBlockedByDurabilityFloorTotal.Add(1, new KeyValuePair<string, object?>("partition_id", partitionId));

    /// <summary>
    /// Backfill batches skipped (WAL range read and all) because recent batches to the peer
    /// shipped without its reported commit frontier advancing and the no-progress pause had not
    /// elapsed. A sustained rate on one partition means a follower is acknowledging batches it
    /// cannot commit — the leader is pacing what would otherwise be a network-speed WAL read loop.
    /// </summary>
    internal static readonly Counter<long> BackfillNoProgressPausesTotal =
        Meter.CreateCounter<long>(
            "raft.backfill.no_progress_pauses_total",
            description: "Backfill batches skipped because the peer's commit frontier is not advancing and the pause has not elapsed.");

    /// <summary>
    /// No-progress backfill episodes that crossed the warning threshold: a follower kept
    /// acknowledging batches without its commit frontier advancing, long enough that the leader
    /// re-anchored and backed off to the pause cap. Should stay 0 on a healthy cluster; each
    /// increment has a matching leader Warning naming the peer, the anchor, and the frontier.
    /// </summary>
    internal static readonly Counter<long> BackfillNoProgressEpisodesTotal =
        Meter.CreateCounter<long>(
            "raft.backfill.no_progress_episodes_total",
            description: "Backfill no-progress episodes that crossed the warning threshold.");

    /// <summary>Counts one paced-skip of a backfill batch, tagged by partition.</summary>
    internal static void RecordBackfillNoProgressPause(int partitionId) =>
        BackfillNoProgressPausesTotal.Add(1, new KeyValuePair<string, object?>("partition_id", partitionId));

    /// <summary>Counts one warned no-progress backfill episode, tagged by partition.</summary>
    internal static void RecordBackfillNoProgressEpisode(int partitionId) =>
        BackfillNoProgressEpisodesTotal.Add(1, new KeyValuePair<string, object?>("partition_id", partitionId));

    /// <summary>
    /// Restores whose WAL read was narrowed by the soft checkpoint: the application-durability
    /// floor sat above the last hard checkpoint, so replay started at the floor instead. This is
    /// the signal that cold-restart replay is bounded by the application's flush lag rather than
    /// by the retained log length.
    /// </summary>
    internal static readonly Counter<long> WalRestoreNarrowedBySoftFloorTotal =
        Meter.CreateCounter<long>(
            "raft.wal.restore_narrowed_by_soft_floor_total",
            description: "Partition restores whose replay was narrowed to the application-durability floor (soft checkpoint).");

    /// <summary>Counts one restore narrowed by the soft checkpoint, tagged by partition.</summary>
    internal static void RecordRestoreNarrowedBySoftFloor(int partitionId) =>
        WalRestoreNarrowedBySoftFloorTotal.Add(1, new KeyValuePair<string, object?>("partition_id", partitionId));

    /// <summary>
    /// Entries a compaction pass retained below the checkpoint for a live, acking follower (the
    /// live-replica lag budget). A persistently large value for one partition means a follower is
    /// chronically behind but still being served by backfill instead of snapshots.
    /// </summary>
    internal static readonly Histogram<long> WalCompactionLiveReplicaHold =
        Meter.CreateHistogram<long>(
            "raft.wal.compaction_live_replica_hold_entries",
            description: "Entries retained below the checkpoint for a live lagging follower, per pass.");

    /// <summary>Records the entries held below the checkpoint for a live replica in one pass.</summary>
    internal static void RecordCompactionHeldByLiveReplica(int partitionId, long heldEntries) =>
        WalCompactionLiveReplicaHold.Record(heldEntries, new KeyValuePair<string, object?>("partition_id", partitionId));

    // ── Snapshot transfer ─────────────────────────────────────────────────────

    /// <summary>
    /// Failed leader→follower snapshot transfer attempts, tagged by <c>partition_id</c> and
    /// <c>cause</c> (export failure, chunk rejection, no transfer registered, …). A sustained rate
    /// for one partition means a below-floor follower cannot be seeded — pair with
    /// <see cref="IRaft.GetSnapshotStatuses"/> for the stuck endpoint and last error.
    /// </summary>
    internal static readonly Counter<long> SnapshotTransferFailuresTotal =
        Meter.CreateCounter<long>(
            "raft.snapshot.transfer_failures_total",
            description: "Failed leader-to-follower snapshot transfer attempts, by cause.");

    /// <summary>Counts one failed snapshot transfer attempt, tagged by partition and cause.</summary>
    internal static void RecordSnapshotTransferFailure(int partitionId, string cause) =>
        SnapshotTransferFailuresTotal.Add(1,
            new KeyValuePair<string, object?>("partition_id", partitionId),
            new KeyValuePair<string, object?>("cause", cause));

    /// <summary>
    /// Snapshot-rescue convergence breaker trips: N consecutive installs each reported success and
    /// the follower still returned below the compaction floor, so the leader stopped escalating.
    /// This never shows in <see cref="SnapshotTransferFailuresTotal"/> — nothing in such a loop
    /// fails — which is exactly why it needs its own signal.
    /// </summary>
    internal static readonly Counter<long> SnapshotRescueBreakerTrippedTotal =
        Meter.CreateCounter<long>(
            "raft.snapshot.rescue_breaker_tripped_total",
            description: "Snapshot rescue loops stopped by the convergence breaker, by partition.");

    /// <summary>Counts one convergence-breaker trip, tagged by partition.</summary>
    internal static void RecordSnapshotRescueBreakerTripped(int partitionId) =>
        SnapshotRescueBreakerTrippedTotal.Add(1, new KeyValuePair<string, object?>("partition_id", partitionId));

    // ── State machine ─────────────────────────────────────────────────────────

    /// <summary>
    /// Total WAL completions discarded because they were stale (wrong partition,
    /// wrong term, or mismatched operation id).  A sustained non-zero rate indicates
    /// leadership churn or mis-routed completions.
    /// </summary>
    internal static readonly Counter<long> StaleCompletionsTotal =
        Meter.CreateCounter<long>(
            "raft.stale_completions_total",
            description: "WAL completions discarded as stale (wrong partition, term, or operation id).");

    /// <summary>Total times a partition transitioned to the Candidate state to start an election.</summary>
    internal static readonly Counter<long> ElectionsStartedTotal =
        Meter.CreateCounter<long>(
            "raft.elections_started_total",
            description: "Total election attempts (transitions to Candidate state), by partition.");

    /// <summary>Total heartbeats sent by leader partitions.</summary>
    internal static readonly Counter<long> HeartbeatsSentTotal =
        Meter.CreateCounter<long>(
            "raft.heartbeats_sent_total",
            description: "Total heartbeat messages sent by leader partitions.");

    /// <summary>
    /// Milliseconds between consecutive heartbeats sent by a leader.
    /// Sustained values well above <c>HeartbeatInterval</c> indicate leader
    /// scheduling pressure or CPU starvation.
    /// </summary>
    internal static readonly Histogram<double> HeartbeatDelayMs =
        Meter.CreateHistogram<double>(
            "raft.heartbeat_delay_ms",
            unit: "ms",
            description: "Interval between consecutive heartbeats sent by a leader partition, in milliseconds.");

    /// <summary>
    /// Milliseconds elapsed since the last received heartbeat at the moment an
    /// election is triggered.  High values indicate prolonged leader absence.
    /// </summary>
    internal static readonly Histogram<double> ElectionDelayMs =
        Meter.CreateHistogram<double>(
            "raft.election_delay_ms",
            unit: "ms",
            description: "Time since last heartbeat when an election was triggered, in milliseconds.");

    // ── Leader balancer ───────────────────────────────────────────────────────

    /// <summary>
    /// Total leadership-transfer moves by outcome: <c>planned</c>, <c>drain</c>,
    /// <c>succeeded</c>, <c>timed_out</c>. A <c>drain</c> move is one the degraded-node detector
    /// forced off a slow node, as opposed to an ordinary <c>planned</c> rebalance.  A sustained high <c>timed_out</c> rate indicates suggestions
    /// are not reaching their recipient or the recipient is dropping them.
    /// </summary>
    internal static readonly Counter<long> BalancerMovesTotal =
        Meter.CreateCounter<long>(
            "raft.balancer.moves_total",
            description: "Total leadership transfer moves by outcome (planned/succeeded/timed_out).");

    /// <summary>
    /// Total balancer passes skipped because the global view was incomplete (fewer
    /// fresh reports than live nodes or a report older than the TTL).
    /// </summary>
    internal static readonly Counter<long> BalancerSkippedPassesTotal =
        Meter.CreateCounter<long>(
            "raft.balancer.skipped_passes_total",
            description: "Balancer passes skipped because the global view was incomplete.");

    // Observable gauge state — updated by the coordinator at the end of each pass (P0 only).
    // Stored as long bit-fields so Interlocked can update them safely across threads.
    // The OTel callback reads without a lock; a torn read of a gauge sample is acceptable.
    private static long _balancerCountImbalanceBits;
    private static long _balancerLoadImbalanceBits;

    internal static double BalancerCountImbalance
    {
        get => global::System.BitConverter.Int64BitsToDouble(
                   Interlocked.Read(ref _balancerCountImbalanceBits));
        set => Interlocked.Exchange(ref _balancerCountImbalanceBits,
                   global::System.BitConverter.DoubleToInt64Bits(value));
    }

    internal static double BalancerLoadImbalance
    {
        get => global::System.BitConverter.Int64BitsToDouble(
                   Interlocked.Read(ref _balancerLoadImbalanceBits));
        set => Interlocked.Exchange(ref _balancerLoadImbalanceBits,
                   global::System.BitConverter.DoubleToInt64Bits(value));
    }

    private static long _balancerSlowNodes;

    /// <summary>
    /// Number of nodes currently classified as slow by the degraded-node detector (P0 leader only;
    /// 0 when the balancer is off, avoidance is off, or this node is not the P0 leader). A value
    /// that oscillates between passes means the hysteresis thresholds are too tight for the
    /// deployment's latency variance.
    /// </summary>
    internal static long BalancerSlowNodes
    {
        get => Interlocked.Read(ref _balancerSlowNodes);
        set => Interlocked.Exchange(ref _balancerSlowNodes, value);
    }

    // ── Observable gauges (dynamic per-partition) ─────────────────────────────

    // Weak references allow GC to collect stopped instances without leaking.
    private static readonly object _executorLock = new();
    private static readonly List<WeakReference<RaftPartitionExecutor>> _registeredExecutors = [];

    private static readonly object _schedulerLock = new();
    private static readonly List<WeakReference<FairWalScheduler>> _registeredSchedulers = [];

    static KommanderMetrics()
    {
        Meter.CreateObservableGauge(
            "raft.executor.client_queue_depth",
            MeasureClientQueueDepths,
            description: "Current number of client proposals pending in each partition executor's queue.");

        Meter.CreateObservableGauge(
            "raft.wal.queue_depth",
            MeasureWalQueueDepths,
            description: "Current number of pending-or-in-flight WAL operations per partition in the scheduler.");

        Meter.CreateObservableGauge(
            "raft.balancer.count_imbalance",
            static () => BalancerCountImbalance,
            description: "Max node leadership count minus target (P0 leader only; 0 when balancer is off or node is not P0).");

        Meter.CreateObservableGauge(
            "raft.balancer.load_imbalance",
            static () => BalancerLoadImbalance,
            description: "Fractional load imbalance: (maxLoad / meanLoad) - 1 (P0 leader only; 0 when not applicable).");

        Meter.CreateObservableGauge(
            "raft.balancer.slow_nodes",
            static () => BalancerSlowNodes,
            description: "Nodes currently classified slow by commit-wait (P0 leader only; 0 when not applicable).");
    }

    /// <summary>
    /// Registers an executor so its client-queue depth is included in the
    /// <c>raft.executor.client_queue_depth</c> observable gauge.
    /// Called automatically by <see cref="RaftPartitionExecutor"/> on construction.
    /// </summary>
    internal static void RegisterExecutor(RaftPartitionExecutor executor)
    {
        lock (_executorLock)
            _registeredExecutors.Add(new WeakReference<RaftPartitionExecutor>(executor));
    }

    /// <summary>
    /// Registers a WAL scheduler so its per-partition queue depths are included in
    /// the <c>raft.wal.queue_depth</c> observable gauge.
    /// Called automatically by <see cref="FairWalScheduler.Start"/>.
    /// </summary>
    internal static void RegisterScheduler(FairWalScheduler scheduler)
    {
        lock (_schedulerLock)
            _registeredSchedulers.Add(new WeakReference<FairWalScheduler>(scheduler));
    }

    private static IEnumerable<Measurement<int>> MeasureClientQueueDepths()
    {
        List<Measurement<int>> result;
        lock (_executorLock)
        {
            result = new List<Measurement<int>>(_registeredExecutors.Count);
            List<WeakReference<RaftPartitionExecutor>> dead = [];

            foreach (WeakReference<RaftPartitionExecutor> wr in _registeredExecutors)
            {
                if (wr.TryGetTarget(out RaftPartitionExecutor? ex))
                    // Reuse the executor's precomputed partition_id tag — no per-scrape int boxing.
                    result.Add(new Measurement<int>(ex.ClientQueueDepth, ex.PartitionIdTag));
                else
                    dead.Add(wr);
            }

            foreach (WeakReference<RaftPartitionExecutor> d in dead)
                _registeredExecutors.Remove(d);
        }
        return result;
    }

    private static IEnumerable<Measurement<int>> MeasureWalQueueDepths()
    {
        List<Measurement<int>> result;
        lock (_schedulerLock)
        {
            result = new List<Measurement<int>>();
            List<WeakReference<FairWalScheduler>> dead = [];

            foreach (WeakReference<FairWalScheduler> wr in _registeredSchedulers)
            {
                if (wr.TryGetTarget(out FairWalScheduler? scheduler))
                {
                    foreach ((int partitionId, int depth) in scheduler.SnapshotPartitionDepths())
                        result.Add(new Measurement<int>(depth,
                            new KeyValuePair<string, object?>("partition_id", partitionId)));
                }
                else
                    dead.Add(wr);
            }

            foreach (WeakReference<FairWalScheduler> d in dead)
                _registeredSchedulers.Remove(d);
        }
        return result;
    }
}
