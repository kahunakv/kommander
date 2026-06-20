
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
    /// Total leadership-transfer moves by outcome: <c>planned</c>, <c>succeeded</c>,
    /// <c>timed_out</c>.  A sustained high <c>timed_out</c> rate indicates suggestions
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
                    result.Add(new Measurement<int>(ex.ClientQueueDepth,
                        new KeyValuePair<string, object?>("partition_id", ex.PartitionId)));
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
