using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL.IO;

/// <summary>
/// Fair, partition-aware scheduler for synchronous WAL read operations.
///
/// <para><b>Goals:</b></para>
/// <list type="bullet">
///   <item>FIFO read order within each partition — a read submitted after
///       a write-completion callback always observes the written data.</item>
///   <item>Fair scheduling across partitions — a read-heavy partition cannot
///       starve other partitions' reads or writes.</item>
///   <item>Bounded per-partition queues with back-pressure: callers receive a
///       <see cref="ReadBackpressureExceededException"/> rather than queuing
///       unboundedly.</item>
///   <item>Graceful drain on <see cref="Stop"/> — every operation accepted before
///       the call completes (or faults) before workers exit.</item>
/// </list>
///
/// <para><b>Usage pattern in <c>RaftWriteAhead</c>:</b></para>
/// <para>
/// The state machine holds a pending-write flag per partition.  A read that
/// semantically depends on a prior write is not submitted until the
/// <c>OnComplete</c> callback fires for that write.  Because reads for the
/// same partition are executed in FIFO order on a dedicated worker thread,
/// such a read is guaranteed to observe the committed data.
/// </para>
/// </summary>
public sealed class FairReadScheduler : IRaftReadScheduler, IDisposable
{
    // ── Constants ──────────────────────────────────────────────────────────

    /// <summary>Maximum number of operations drained from a partition per scheduling cycle.</summary>
    private const int MaxBatchSize = 64;

    /// <summary>Default per-partition queue depth limit.</summary>
    public const int DefaultMaxQueueDepth = 4096;

    // ── Configuration ──────────────────────────────────────────────────────

    private readonly ILogger<IRaft> logger;
    private readonly int maxQueueDepthPerPartition;

    // ── Per-partition state ────────────────────────────────────────────────

    private sealed class PartitionState
    {
        /// <summary>Pending read actions in submission order.</summary>
        public readonly Queue<Action> Ops = new();

        /// <summary>Guards <see cref="Ops"/>, <see cref="Scheduled"/>, <see cref="InFlight"/>, and <see cref="Depth"/>.</summary>
        public readonly object Lock = new();

        /// <summary>
        /// True when this partition's id is already in
        /// <see cref="FairReadScheduler._readyPartitions"/>.
        /// </summary>
        public bool Scheduled;

        /// <summary>
        /// True while a worker thread is executing reads for this partition.
        /// A concurrent <see cref="EnqueueTask{T}"/> that sees <c>InFlight=true</c>
        /// must NOT add the partition to <c>_readyPartitions</c>; the post-read
        /// lock section will re-schedule once the read loop finishes.
        /// </summary>
        public bool InFlight;

        /// <summary>Current number of pending-or-in-flight operations.</summary>
        public int Depth;
    }

    private readonly ConcurrentDictionary<int, PartitionState> _partitions = new();

    // ── Global ready-queue ─────────────────────────────────────────────────

    private readonly BlockingCollection<int> _readyPartitions;

    // ── Worker threads ─────────────────────────────────────────────────────

    private readonly Thread[] _workers;
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _stopping;
    private bool _started;

    // ── Observability ──────────────────────────────────────────────────────

    private long _totalReadsCompleted;

    /// <summary>Total number of read operations completed by this scheduler.</summary>
    public long TotalReadsCompleted => Interlocked.Read(ref _totalReadsCompleted);

    // ── Construction ──────────────────────────────────────────────────────

    /// <param name="logger">Logger.</param>
    /// <param name="workerCount">
    /// Number of dedicated read worker threads.
    /// Defaults to <see cref="Environment.ProcessorCount"/>.
    /// </param>
    /// <param name="maxQueueDepthPerPartition">
    /// Per-partition soft back-pressure limit.
    /// Defaults to <see cref="DefaultMaxQueueDepth"/>.
    /// </param>
    public FairReadScheduler(
        ILogger<IRaft> logger,
        int workerCount = 0,
        int maxQueueDepthPerPartition = DefaultMaxQueueDepth)
    {
        this.logger = logger;
        this.maxQueueDepthPerPartition = maxQueueDepthPerPartition;

        if (workerCount <= 0)
            workerCount = Math.Max(1, Environment.ProcessorCount);

        _readyPartitions = new BlockingCollection<int>(boundedCapacity: workerCount * 64);
        _workers = new Thread[workerCount];
    }

    // ── IRaftReadScheduler ─────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<T> EnqueueTask<T>(int partitionId, Func<T> operation)
    {
        if (_stopping)
            throw new InvalidOperationException("FairReadScheduler: scheduler is stopping; no new operations accepted.");

        if (!_started)
            throw new InvalidOperationException("FairReadScheduler: call Start() before EnqueueTask().");

        PartitionState state = _partitions.GetOrAdd(partitionId, _ => new PartitionState());

        TaskCompletionSource<T> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Capture TCS and delegate in an Action so the queue is typed as Queue<Action>.
        Action work = () =>
        {
            try
            {
                tcs.TrySetResult(operation());
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        };

        lock (state.Lock)
        {
            if (state.Depth >= maxQueueDepthPerPartition)
                throw new ReadBackpressureExceededException(partitionId, state.Depth);

            state.Ops.Enqueue(work);
            state.Depth++;

            if (!state.Scheduled && !state.InFlight)
            {
                state.Scheduled = true;
                if (!_readyPartitions.TryAdd(partitionId))
                    _readyPartitions.Add(partitionId, _cts.Token);
            }
        }

        return tcs.Task;
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>Starts the worker threads. Must be called exactly once before <see cref="EnqueueTask{T}"/>.</summary>
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
                Name = $"FairReadScheduler-{workerId}",
            };
            _workers[i].Start();
        }
    }

    /// <summary>
    /// Stops the scheduler.
    ///
    /// <para>All operations accepted before this call will be executed (or faulted)
    /// and their <see cref="Task{T}"/> results delivered before workers exit.</para>
    /// </summary>
    public void Stop()
    {
        if (!_started)
            return; // Start() was never called; nothing to stop.

        _stopping = true;
        _cts.Cancel();

        foreach (Thread worker in _workers)
            worker.Join();
    }

    // ── Worker ─────────────────────────────────────────────────────────────

    private void WorkerLoop(int workerId)
    {
        List<Action> batch = new(MaxBatchSize);
        CancellationToken token = _cts.Token;

        while (true)
        {
            int partitionId;
            try
            {
                partitionId = _readyPartitions.Take(token);
            }
            catch (OperationCanceledException)
            {
                DrainRemaining(batch);
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            ProcessPartition(partitionId, batch);
        }
    }

    private void ProcessPartition(int partitionId, List<Action> batch)
    {
        if (!_partitions.TryGetValue(partitionId, out PartitionState? state))
            return;

        batch.Clear();

        lock (state.Lock)
        {
            // Do NOT decrement Depth here: we decrement after the read loop
            // completes so that Depth tracks queued-plus-in-flight operations.
            // Decrementing at dequeue time would let Depth fall to 0 before the
            // reads finish, making the backpressure limit unreliable under load.
            while (batch.Count < MaxBatchSize && state.Ops.TryDequeue(out Action? work))
                batch.Add(work);

            // Clear the scheduled flag and raise the in-flight flag BEFORE
            // releasing the lock and running reads.
            // * Scheduled=false: the partition's ID has been removed from
            //   _readyPartitions (we just dequeued it).
            // * InFlight=true: prevents EnqueueTask from adding the partition back
            //   to _readyPartitions while reads are in progress.  Without this a
            //   concurrent enqueue would see Scheduled=false, add the partition, and
            //   a second worker could start on the same partition concurrently.
            state.Scheduled = false;
            state.InFlight  = true;
        }

        if (batch.Count == 0)
        {
            lock (state.Lock)
                state.InFlight = false;
            return;
        }

        foreach (Action work in batch)
        {
            try
            {
                work(); // Sets TCS result or exception internally.
                Interlocked.Increment(ref _totalReadsCompleted);
            }
            catch (Exception ex)
            {
                // Defensive: the Action lambda should never throw (it catches internally),
                // but log just in case.
                logger.LogError(
                    "[FairReadScheduler] Unhandled exception for partition {PartitionId}: {Message}",
                    partitionId, ex.Message);
            }
        }

        // After reads complete, decrement Depth for exactly the items processed,
        // clear InFlight, then re-schedule if new items arrived while executing.
        lock (state.Lock)
        {
            state.Depth   -= batch.Count;
            state.InFlight = false;

            if (state.Ops.Count > 0 && !state.Scheduled)
            {
                // Only mark Scheduled=true when the partition ID is actually
                // inserted into the ready-queue.  If the bounded queue is full,
                // TryAdd returns false; leaving Scheduled=false lets the next
                // EnqueueTask call re-schedule the partition rather than stranding it.
                if (_readyPartitions.TryAdd(partitionId))
                    state.Scheduled = true;
            }
        }
    }

    private void DrainRemaining(List<Action> batch)
    {
        while (_readyPartitions.TryTake(out int partitionId))
            ProcessPartition(partitionId, batch);

        // Sweep all partition states for items not yet in the ready-queue
        // (guards against a rare race at shutdown).
        // IMPORTANT: skip partitions that are currently in-flight (InFlight=true).
        // The worker executing that batch will clear InFlight in its post-read
        // lock section and re-add the partition to _readyPartitions if items
        // remain; this worker's own DrainRemaining first phase (TryTake loop)
        // will then pick it up, preserving the single-worker-per-partition
        // invariant even during shutdown.
        foreach (KeyValuePair<int, PartitionState> kv in _partitions)
        {
            bool shouldProcess;
            lock (kv.Value.Lock)
                shouldProcess = kv.Value.Ops.Count > 0 && !kv.Value.InFlight;

            if (shouldProcess)
                ProcessPartition(kv.Key, batch);
        }
    }

    // ── IDisposable ────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (!_stopping)
            Stop();
        _cts.Dispose();
        _readyPartitions.Dispose();
    }
}

/// <summary>
/// Thrown by <see cref="FairReadScheduler.EnqueueTask{T}"/> when a partition's
/// pending-read queue has reached the configured depth limit.
/// </summary>
public sealed class ReadBackpressureExceededException : Exception
{
    public int PartitionId { get; }
    public int CurrentDepth { get; }

    public ReadBackpressureExceededException(int partitionId, int currentDepth)
        : base($"FairReadScheduler: partition {partitionId} read queue depth {currentDepth} exceeded limit.")
    {
        PartitionId = partitionId;
        CurrentDepth = currentDepth;
    }
}
