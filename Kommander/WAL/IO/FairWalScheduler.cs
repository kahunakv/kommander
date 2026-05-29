using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL.IO;

/// <summary>
/// Fair, partition-aware WAL write scheduler.
///
/// <para>Design goals (per the raft-refactor-plan):</para>
/// <list type="bullet">
///   <item>FIFO write order within each partition — no operation is ever applied out
///       of submission order for the same partition.</item>
///   <item>Fair scheduling across partitions — a hot partition cannot starve others.
///       Each partition occupies at most one slot in the global ready-queue at a
///       time; partitions alternate in round-robin fashion as they produce work.</item>
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
/// <para><b>Concurrency model:</b> up to <c>workerCount</c> partitions can have
/// their write batches processed simultaneously.  Within a single partition only
/// one batch is ever in-flight at a time.</para>
/// </summary>
public sealed class FairWalScheduler : IRaftWalScheduler, IDisposable
{
    // ── Constants ──────────────────────────────────────────────────────────

    /// <summary>Maximum number of operations drained from a partition in a single batch.</summary>
    private const int MaxBatchSize = 256;

    /// <summary>Default per-partition pending-queue depth limit.</summary>
    public const int DefaultMaxQueueDepth = 4096;

    // ── Configuration ──────────────────────────────────────────────────────

    private readonly IWAL walAdapter;
    private readonly ILogger<IRaft> logger;
    private readonly int maxQueueDepthPerPartition;

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
        /// True while a worker thread is executing a WAL write for this partition.
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

    /// <summary>Total number of WAL write batches dispatched to the storage adapter.</summary>
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
    public FairWalScheduler(
        IWAL walAdapter,
        ILogger<IRaft> logger,
        int workerCount = 0,
        int maxQueueDepthPerPartition = DefaultMaxQueueDepth)
    {
        this.walAdapter = walAdapter;
        this.logger = logger;
        this.maxQueueDepthPerPartition = maxQueueDepthPerPartition;

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
        List<WALWriteOperation> batch = new(MaxBatchSize);
        CancellationToken token = _cts.Token;

        while (true)
        {
            // Try to take the next ready partition.
            int partitionId;
            try
            {
                partitionId = _readyPartitions.Take(token);
            }
            catch (OperationCanceledException)
            {
                // Drain remaining items before exiting.
                DrainRemaining(batch);
                break;
            }
            catch (ObjectDisposedException)
            {
                // Collection was disposed during shutdown; exit immediately.
                break;
            }

            ProcessPartition(partitionId, batch);
        }
    }

    private void ProcessPartition(int partitionId, List<WALWriteOperation> batch)
    {
        if (!_partitions.TryGetValue(partitionId, out PartitionState? state))
            return;

        batch.Clear();

        lock (state.Lock)
        {
            // Drain up to MaxBatchSize operations.
            // Do NOT decrement Depth here: we decrement after ExecuteBatch completes
            // so that Depth tracks queued-plus-in-flight operations.  Decrementing
            // at dequeue time would let Depth fall to 0 before the WAL write returns,
            // allowing a flood of new enqueues during a slow write and making the
            // backpressure limit unreliable.
            while (batch.Count < MaxBatchSize && state.Ops.TryDequeue(out WALWriteOperation? op))
                batch.Add(op);

            // Clear the scheduled flag and raise the in-flight flag BEFORE
            // releasing the lock and starting I/O.
            // * Scheduled=false: the partition's ID has been removed from
            //   _readyPartitions (we just dequeued it); clearing it allows the
            //   post-I/O code to re-add if needed.
            // * InFlight=true: signals Enqueue that a write is in progress for
            //   this partition, so it must NOT add to _readyPartitions.  Without
            //   this flag a concurrent Enqueue would see Scheduled=false, add the
            //   partition, and a second worker could start on the same partition
            //   while the first write is still in flight.
            state.Scheduled = false;
            state.InFlight  = true;
        }

        if (batch.Count == 0)
        {
            lock (state.Lock)
                state.InFlight = false;
            return;
        }

        // Execute the WAL write outside the partition lock so other partitions
        // are not blocked by this I/O.  Only one worker can be here for a given
        // partition at a time because the partition is not in _readyPartitions
        // while Scheduled=false.
        ExecuteBatch(partitionId, batch);

        // After the write completes, decrement Depth for exactly the items we
        // just processed, clear InFlight, then re-schedule if new items arrived.
        lock (state.Lock)
        {
            state.Depth   -= batch.Count;
            state.InFlight = false;

            if (state.Ops.Count > 0 && !state.Scheduled)
            {
                // Only mark Scheduled=true when the partition ID is actually
                // inserted into the ready-queue.  If the bounded queue is full,
                // TryAdd returns false; leaving Scheduled=false lets the next
                // Enqueue call re-schedule the partition rather than stranding it.
                if (_readyPartitions.TryAdd(partitionId))
                    state.Scheduled = true;
            }
        }
    }

    private void ExecuteBatch(int partitionId, List<WALWriteOperation> batch)
    {
        RaftOperationStatus status;

        try
        {
            List<(int, List<RaftLog>)> logGroups = new(batch.Count);
            foreach (WALWriteOperation op in batch)
                logGroups.Add(op.Logs);

            status = walAdapter.Write(logGroups);
            Interlocked.Increment(ref _totalBatchesWritten);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[FairWalScheduler] Partition {PartitionId} WAL write error: {Message}",
                partitionId, ex.Message);
            status = RaftOperationStatus.Errored;
        }

        foreach (WALWriteOperation op in batch)
        {
            try
            {
                op.OnComplete(BuildCompletion(op, status));
                Interlocked.Increment(ref _totalOperationsCompleted);
            }
            catch (Exception ex)
            {
                logger.LogError(
                    "[FairWalScheduler] OnComplete callback for partition {PartitionId} op {OperationId} threw: {Message}",
                    partitionId, op.OperationId, ex.Message);
            }
        }
    }

    /// <summary>
    /// After the CTS fires, drain any partitions that were already in
    /// the ready-queue but not yet processed.
    /// </summary>
    private void DrainRemaining(List<WALWriteOperation> batch)
    {
        while (_readyPartitions.TryTake(out int partitionId))
            ProcessPartition(partitionId, batch);

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
                ProcessPartition(kv.Key, batch);
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
            Status: status,
            Operation: op
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
