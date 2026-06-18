using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL.IO;

/// <summary>
/// Fair, partition-aware WAL write scheduler with cross-partition group commit.
///
/// <para>Design goals:</para>
/// <list type="bullet">
///   <item>FIFO write order within each partition — no operation is ever applied out
///       of submission order for the same partition.</item>
///   <item>Fair scheduling across partitions — a hot partition cannot starve others.
///       Each partition occupies at most one slot in the global ready-queue at a
///       time; partitions alternate in round-robin fashion as they produce work.</item>
///   <item><b>Cross-partition group commit:</b> when multiple partitions are ready
///       simultaneously, a single worker drains up to
///       <see cref="DefaultMaxGroupBatchPartitions"/> of them and issues ONE
///       <c>walAdapter.Write</c> call spanning all of them.  For RocksDB (which
///       shares a single WAL across all column families) this means one fsync
///       regardless of how many partitions are included — the dominant cost saving
///       in many-partition deployments.</item>
///   <item>Compatible writes are batched within each drain cycle to amortize
///       <c>walAdapter.Write</c> overhead.</item>
///   <item>Bounded per-partition queues with back-pressure: callers receive a
///       <see cref="BackpressureExceededException"/> when the per-partition depth
///       limit is reached rather than silently blocking indefinitely.</item>
///   <item>Graceful drain on <see cref="Stop"/> — all operations accepted before
///       <see cref="Stop"/> is called are guaranteed to complete (or error) before
///       worker threads exit.</item>
/// </list>
///
/// <para><b>Concurrency model:</b> up to <c>workerCount</c> workers can hold
/// write batches simultaneously.  The single-writer-per-partition invariant is
/// maintained by the <c>InFlight</c> flag: a partition included in one worker's
/// group batch cannot simultaneously appear in another worker's batch because its
/// ID is consumed from <c>_readyPartitions</c> (a drain-once queue) before
/// <c>InFlight</c> is set.</para>
/// </summary>
public sealed class FairWalScheduler : IRaftWalScheduler, IDisposable
{
    // ── Constants ──────────────────────────────────────────────────────────

    /// <summary>Default max operations drained from a partition in a single batch.</summary>
    public const int DefaultMaxBatchSize = 256;

    /// <summary>Default per-partition pending-queue depth limit.</summary>
    public const int DefaultMaxQueueDepth = 4096;

    /// <summary>
    /// Default maximum number of partitions coalesced into a single WAL write call.
    /// When multiple partitions are ready simultaneously a worker drains up to this
    /// many of them and issues one <c>walAdapter.Write</c> (one fsync for RocksDB).
    /// </summary>
    public const int DefaultMaxGroupBatchPartitions = 64;

    // ── Configuration ──────────────────────────────────────────────────────

    private readonly IWAL walAdapter;
    private readonly ILogger<IRaft> logger;
    private readonly int maxQueueDepthPerPartition;
    private readonly int maxGlobalQueueDepth;
    private readonly int maxBatchSize;
    private readonly int maxGroupBatchPartitions;

    // Total pending-or-in-flight operations across all partitions.
    // Checked against maxGlobalQueueDepth (when > 0) in Enqueue;
    // decremented in ProcessGroupBatch after each batch write completes.
    private int _globalQueueDepth;

    // ── Per-partition state ────────────────────────────────────────────────

    private sealed class PartitionState
    {
        /// <summary>Pending operations in submission order.</summary>
        public readonly Queue<WALWriteOperation> Ops = new();

        /// <summary>Guards <see cref="Ops"/>, <see cref="Scheduled"/>, <see cref="InFlight"/>, and <see cref="Depth"/>.</summary>
        public readonly object Lock = new();

        /// <summary>
        /// True when this partition's id is already in <see cref="FairWalScheduler._readyPartitions"/>.
        /// At most one copy of the id is ever present in the ready-queue for a given partition.
        /// </summary>
        public bool Scheduled;

        /// <summary>
        /// True while a worker thread is executing a WAL write that includes this partition.
        /// A concurrent <see cref="Enqueue"/> that sees <c>InFlight=true</c> must NOT
        /// add the partition to <c>_readyPartitions</c>; the post-I/O lock section
        /// will re-schedule once the write finishes.
        /// </summary>
        public bool InFlight;

        /// <summary>Current number of pending-or-in-flight operations.</summary>
        public int Depth;
    }

    private readonly ConcurrentDictionary<int, PartitionState> _partitions = new();

    // ── Global ready-queue ─────────────────────────────────────────────────

    // Partition IDs that have at least one pending operation waiting to be drained.
    // Bounded to 2× the max possible concurrent partitions to apply light
    // back-pressure at the signalling layer as well.
    private readonly BlockingCollection<int> _readyPartitions;

    // ── Worker threads ─────────────────────────────────────────────────────

    private readonly Thread[] _workers;
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _stopping;
    private bool _started;

    // ── Observability ──────────────────────────────────────────────────────

    private long _totalBatchesWritten;
    private long _totalOperationsCompleted;

    /// <summary>
    /// Total number of <c>walAdapter.Write</c> calls dispatched to the storage adapter.
    /// Each group-commit batch counts as one call regardless of how many partitions it spans,
    /// so this is the count most directly correlated with fsync pressure on RocksDB.
    /// </summary>
    public long TotalBatchesWritten => Interlocked.Read(ref _totalBatchesWritten);

    /// <summary>Total number of individual operations completed.</summary>
    public long TotalOperationsCompleted => Interlocked.Read(ref _totalOperationsCompleted);

    // ── Construction ──────────────────────────────────────────────────────

    /// <param name="walAdapter">Synchronous storage adapter invoked on worker threads.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="workerCount">
    /// Number of worker threads.  Allows up to this many partitions to flush
    /// concurrently.  Defaults to <see cref="Environment.ProcessorCount"/>.
    /// </param>
    /// <param name="maxQueueDepthPerPartition">
    /// Per-partition soft back-pressure limit.  Defaults to
    /// <see cref="DefaultMaxQueueDepth"/>.
    /// </param>
    /// <param name="maxBatchSize">
    /// Maximum operations drained from a partition in a single storage flush.
    /// Defaults to <see cref="DefaultMaxBatchSize"/>.
    /// </param>
    /// <param name="maxGlobalQueueDepth">
    /// Global cap on total pending-or-in-flight operations across all partitions.
    /// 0 or negative disables the global cap (only per-partition limits apply).
    /// </param>
    /// <param name="maxGroupBatchPartitions">
    /// Maximum number of partitions coalesced into a single <c>walAdapter.Write</c>
    /// call (one fsync for RocksDB).  Defaults to
    /// <see cref="DefaultMaxGroupBatchPartitions"/>.
    /// </param>
    public FairWalScheduler(
        IWAL walAdapter,
        ILogger<IRaft> logger,
        int workerCount = 0,
        int maxQueueDepthPerPartition = DefaultMaxQueueDepth,
        int maxBatchSize = DefaultMaxBatchSize,
        int maxGlobalQueueDepth = 0,
        int maxGroupBatchPartitions = DefaultMaxGroupBatchPartitions)
    {
        this.walAdapter = walAdapter;
        this.logger = logger;
        this.maxQueueDepthPerPartition = maxQueueDepthPerPartition;
        this.maxGlobalQueueDepth = maxGlobalQueueDepth > 0 ? maxGlobalQueueDepth : 0;
        this.maxBatchSize = maxBatchSize <= 0 ? DefaultMaxBatchSize : maxBatchSize;
        this.maxGroupBatchPartitions = maxGroupBatchPartitions <= 0 ? DefaultMaxGroupBatchPartitions : maxGroupBatchPartitions;

        if (workerCount <= 0)
            workerCount = Math.Max(1, Environment.ProcessorCount);

        _readyPartitions = new BlockingCollection<int>(boundedCapacity: workerCount * 64);
        _workers = new Thread[workerCount];
    }

    // ── IRaftWalScheduler ──────────────────────────────────────────────────

    /// <inheritdoc/>
    /// <exception cref="BackpressureExceededException">
    /// Thrown when the per-partition pending queue is at or above
    /// <see cref="maxQueueDepthPerPartition"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the scheduler has been stopped or not yet started.
    /// </exception>
    public void Enqueue(WALWriteOperation operation)
    {
        if (_stopping)
            throw new InvalidOperationException("FairWalScheduler: scheduler is stopping; no new operations accepted.");

        if (!_started)
            throw new InvalidOperationException("FairWalScheduler: call Start() before Enqueue().");

        int partitionId = operation.Logs.PartitionId;
        PartitionState state = _partitions.GetOrAdd(partitionId, _ => new PartitionState());

        lock (state.Lock)
        {
            if (state.Depth >= maxQueueDepthPerPartition)
                throw new BackpressureExceededException(partitionId, state.Depth);

            if (maxGlobalQueueDepth > 0)
            {
                int globalDepth = Interlocked.Increment(ref _globalQueueDepth);
                if (globalDepth > maxGlobalQueueDepth)
                {
                    Interlocked.Decrement(ref _globalQueueDepth);
                    throw new BackpressureExceededException(partitionId, globalDepth);
                }
            }

            state.Ops.Enqueue(operation);
            state.Depth++;

            // Schedule the partition in the global ready-queue only if it is not
            // already scheduled AND no worker is currently executing I/O for it.
            // If InFlight=true a worker is mid-write; that worker's post-I/O lock
            // section will re-schedule once the write finishes.
            if (!state.Scheduled && !state.InFlight)
            {
                state.Scheduled = true;
                // TryAdd with a short timeout so we don't block the caller.
                // If the ready-queue is full (workers are overloaded), we still
                // accept the op but wake a worker via a forced add.
                if (!_readyPartitions.TryAdd(partitionId))
                    _readyPartitions.Add(partitionId, _cts.Token);
            }
        }
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>Starts the worker threads.  Must be called exactly once before <see cref="Enqueue"/>.</summary>
    public void Start()
    {
        if (_started)
            return;

        _started = true;
        KommanderMetrics.RegisterScheduler(this);

        for (int i = 0; i < _workers.Length; i++)
        {
            int workerId = i;
            _workers[i] = new Thread(() => WorkerLoop(workerId))
            {
                IsBackground = true,
                Name = $"FairWalScheduler-{workerId}",
            };
            _workers[i].Start();
        }
    }

    /// <summary>
    /// Returns a best-effort snapshot of per-partition pending-or-in-flight depths.
    /// Used by <see cref="KommanderMetrics"/> to populate the
    /// <c>raft.wal.queue_depth</c> observable gauge.  No lock is held during
    /// iteration; values are approximate (suitable for metrics).
    /// </summary>
    internal IEnumerable<(int partitionId, int depth)> SnapshotPartitionDepths()
    {
        foreach (KeyValuePair<int, PartitionState> kv in _partitions)
            yield return (kv.Key, kv.Value.Depth);
    }

    /// <summary>
    /// Stops the scheduler.
    ///
    /// <para>When called, no new operations are accepted.  All operations that were
    /// already <see cref="Enqueue"/>d before this call will be processed and their
    /// <c>OnComplete</c> callbacks fired before worker threads exit.  Callers should
    /// wait for all expected completions before assuming the system is quiescent.</para>
    /// </summary>
    public void Stop()
    {
        if (!_started)
            return; // Start() was never called; nothing to stop.

        _stopping = true;

        // Give workers a signal to drain remaining items and then exit.
        // We do this by cancelling after a best-effort drain pass.
        // Workers complete in-flight batches and then re-check the queue.
        // Once all queues are empty and cancellation is signalled, they exit.
        _cts.Cancel();

        foreach (Thread worker in _workers)
            worker.Join();
    }

    // ── Worker ─────────────────────────────────────────────────────────────

    private void WorkerLoop(int workerId)
    {
        // Per-worker reusable scratch space — cleared at the start of each iteration.
        List<int> partitionGroup = new(maxGroupBatchPartitions);
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches = new(maxGroupBatchPartitions);
        CancellationToken token = _cts.Token;

        while (true)
        {
            // Block until at least one partition is ready.
            int partitionId;
            try
            {
                partitionId = _readyPartitions.Take(token);
            }
            catch (OperationCanceledException)
            {
                // Drain remaining items before exiting.
                DrainRemaining(partitionGroup, groupBatches);
                break;
            }
            catch (ObjectDisposedException)
            {
                // Collection was disposed during shutdown; exit immediately.
                break;
            }

            // Gather additional ready partitions without blocking — this is the
            // group-commit coalescing step.  Up to maxGroupBatchPartitions−1 extra
            // partitions join this worker's batch, all sharing the same Write call.
            partitionGroup.Clear();
            partitionGroup.Add(partitionId);
            while (partitionGroup.Count < maxGroupBatchPartitions && _readyPartitions.TryTake(out int extraId))
                partitionGroup.Add(extraId);

            ProcessGroupBatch(partitionGroup, groupBatches);
        }
    }

    /// <summary>
    /// Core group-commit routine.
    ///
    /// <para>Phase 1 — lock each partition, drain up to <see cref="maxBatchSize"/>
    /// ops, and mark <c>InFlight=true</c>.  The partition's slot in
    /// <c>_readyPartitions</c> has already been consumed (the caller dequeued it),
    /// so no other worker can race to the same partition.</para>
    ///
    /// <para>Phase 2 — issue a single <c>walAdapter.Write</c> spanning all drained
    /// partitions.  For RocksDB this is one <c>db.Write</c> / one fsync regardless
    /// of partition count.</para>
    ///
    /// <para>Phase 3 — per-partition post-write: run <c>FollowerAppend</c>
    /// truncation, fire <c>OnComplete</c> callbacks, decrement depth, clear
    /// <c>InFlight</c>, and re-schedule if new ops arrived during the write.</para>
    /// </summary>
    private void ProcessGroupBatch(
        List<int> partitionIds,
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches)
    {
        groupBatches.Clear();

        // ── Phase 1: drain ops per partition, mark InFlight ────────────────
        foreach (int pid in partitionIds)
        {
            if (!_partitions.TryGetValue(pid, out PartitionState? state))
                continue;

            List<WALWriteOperation> pidBatch = new(maxBatchSize);

            lock (state.Lock)
            {
                while (pidBatch.Count < maxBatchSize && state.Ops.TryDequeue(out WALWriteOperation? op))
                    pidBatch.Add(op);

                // Clear Scheduled (the id has been consumed from _readyPartitions)
                // and raise InFlight so Enqueue cannot re-add this partition while
                // the combined Write is in progress.
                state.Scheduled = false;
                state.InFlight  = true;
            }

            if (pidBatch.Count > 0)
            {
                groupBatches.Add((pid, pidBatch));
            }
            else
            {
                // Nothing to write; clear InFlight immediately.
                lock (state.Lock)
                    state.InFlight = false;
            }
        }

        if (groupBatches.Count == 0)
            return;

        // ── Phase 2: single cross-partition WAL write ──────────────────────
        RaftOperationStatus status;

        try
        {
            // Build the combined log-groups list that spans all partitions in this batch.
            int totalOps = 0;
            foreach ((_, List<WALWriteOperation> ops) in groupBatches)
                totalOps += ops.Count;
            List<(int, List<RaftLog>)> logGroups = new(totalOps);
            foreach ((_, List<WALWriteOperation> ops) in groupBatches)
                foreach (WALWriteOperation op in ops)
                    logGroups.Add(op.Logs);

            status = walAdapter.Write(logGroups);
            Interlocked.Increment(ref _totalBatchesWritten);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[FairWalScheduler] Group WAL write error ({PartitionCount} partitions): {Message}",
                groupBatches.Count, ex.Message);
            status = RaftOperationStatus.Errored;
        }

        KommanderMetrics.WalBatchesTotal.Add(1);

        // ── Phase 3: per-partition post-write cleanup ──────────────────────
        foreach ((int pid, List<WALWriteOperation> pidBatch) in groupBatches)
        {
            KommanderMetrics.WalBatchSize.Record(pidBatch.Count);

            if (status == RaftOperationStatus.Success)
            {
                foreach (WALWriteOperation op in pidBatch)
                {
                    if (op.Type == WALWriteOperationType.FollowerAppend && op.LogIndex > 0)
                        walAdapter.TruncateLogsAfter(pid, op.LogIndex);
                }
            }

            foreach (WALWriteOperation op in pidBatch)
            {
                try
                {
                    op.OnComplete(BuildCompletion(op, status));
                    Interlocked.Increment(ref _totalOperationsCompleted);
                    KommanderMetrics.WalOperationsTotal.Add(1);
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        "[FairWalScheduler] OnComplete callback for partition {PartitionId} op {OperationId} threw: {Message}",
                        pid, op.OperationId, ex.Message);
                }
            }

            if (maxGlobalQueueDepth > 0)
                Interlocked.Add(ref _globalQueueDepth, -pidBatch.Count);

            if (!_partitions.TryGetValue(pid, out PartitionState? state))
                continue;

            lock (state.Lock)
            {
                state.Depth   -= pidBatch.Count;
                state.InFlight = false;

                if (state.Ops.Count > 0 && !state.Scheduled)
                {
                    // Only mark Scheduled=true when the partition ID is actually
                    // inserted into the ready-queue.  If the bounded queue is full,
                    // TryAdd returns false; leaving Scheduled=false lets the next
                    // Enqueue call re-schedule the partition rather than stranding it.
                    if (_readyPartitions.TryAdd(pid))
                        state.Scheduled = true;
                }
            }
        }
    }

    /// <summary>
    /// After the CTS fires, drain any partitions that were already in
    /// the ready-queue but not yet processed.
    /// </summary>
    private void DrainRemaining(
        List<int> partitionGroup,
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches)
    {
        // Drain everything still in the ready-queue as one group-commit batch.
        partitionGroup.Clear();
        while (_readyPartitions.TryTake(out int pid))
            partitionGroup.Add(pid);

        if (partitionGroup.Count > 0)
            ProcessGroupBatch(partitionGroup, groupBatches);

        // Also sweep all partition states for any items that were enqueued
        // but whose partitionId was not yet in the ready-queue (should not
        // happen in normal operation, but guards against race at shutdown).
        // IMPORTANT: skip partitions that are currently in-flight (InFlight=true).
        // The worker executing that batch will clear InFlight in its post-I/O
        // lock section and re-add the partition to _readyPartitions if items
        // remain; this worker's own DrainRemaining first phase (TryTake loop)
        // will then pick it up, preserving the single-writer-per-partition
        // invariant even during shutdown.
        foreach (KeyValuePair<int, PartitionState> kv in _partitions)
        {
            PartitionState state = kv.Value;
            bool shouldProcess;
            lock (state.Lock)
                shouldProcess = state.Ops.Count > 0 && !state.InFlight;

            if (shouldProcess)
            {
                partitionGroup.Clear();
                partitionGroup.Add(kv.Key);
                ProcessGroupBatch(partitionGroup, groupBatches);
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static RaftWalCompletion BuildCompletion(WALWriteOperation op, RaftOperationStatus status)
    {
        List<RaftLog> logs = op.Logs.Logs;
        long minIndex = logs.Count > 0 ? logs.Min(l => l.Id) : -1;

        return new RaftWalCompletion(
            PartitionId: op.Logs.PartitionId,
            OperationId: op.OperationId,
            Term: op.Term,
            MinLogIndex: minIndex,
            MaxLogIndex: op.LogIndex,
            OperationType: op.Type,
            Status: status
        );
    }

    // ── IDisposable ────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        // Join all workers before disposing managed resources so that no worker
        // thread can touch _readyPartitions after it is disposed.
        if (!_stopping)
            Stop();
        _cts.Dispose();
        _readyPartitions.Dispose();
    }
}

/// <summary>
/// Thrown by <see cref="FairWalScheduler.Enqueue"/> when a partition's pending-write
/// queue has reached the configured depth limit.
/// </summary>
public sealed class BackpressureExceededException : Exception
{
    public int PartitionId { get; }
    public int CurrentDepth { get; }

    public BackpressureExceededException(int partitionId, int currentDepth)
        : base($"FairWalScheduler: partition {partitionId} queue depth {currentDepth} exceeded limit.")
    {
        PartitionId = partitionId;
        CurrentDepth = currentDepth;
    }
}
