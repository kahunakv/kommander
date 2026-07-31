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

        // The ready queue is intentionally UNBOUNDED. Each partition is represented at
        // most once (deduplicated by PartitionState.Scheduled), so the number of live
        // entries is bounded by the count of distinct partitions — not by request rate.
        // A previous bounded capacity (workerCount * 64) capped *distinct partitions*
        // rather than memory, which stranded rescheduled work once the partition count
        // exceeded it (the post-batch TryAdd could fail with no future enqueue to retry).
        // Bounded admission is still enforced per partition via maxQueueDepthPerPartition.
        _readyPartitions = new BlockingCollection<int>();
        _workers = new Thread[workerCount];
    }

    // ── IRaftReadScheduler ─────────────────────────────────────────────────

    /// <inheritdoc/>
    public Task<T> EnqueueTask<T>(int partitionId, Func<T> operation)
    {
        if (_stopping)
            throw new InvalidOperationException("FairReadScheduler: scheduler is stopping; no new operations accepted.");

        // Deliberately NO "call Start() first" guard. An operation enqueued before
        // Start() is parked in its per-partition queue (and the partition id in
        // _readyPartitions); the worker threads created by Start() then drain it in
        // submission order. This closes the startup race where a consumer issues a read
        // before RaftManager.JoinCluster has called Start(): the read is served rather
        // than rejected with a retry. Only _stopping rejects new work — see Stop(),
        // which drains any parked operations even when Start() was never called.
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

            TryScheduleLocked(partitionId, state);
        }

        return tcs.Task;
    }

    /// <summary>
    /// Atomically claims a partition into the ready queue if it has pending work that no
    /// worker owns and it is not already scheduled. MUST be called while holding
    /// <paramref name="state"/>.<see cref="PartitionState.Lock"/>: the same critical section
    /// that observes <c>Ops</c>/<c>Scheduled</c>/<c>InFlight</c> also establishes ownership
    /// (sets <c>Scheduled</c>) before releasing the lock, so two threads cannot both decide
    /// to schedule — or drain — the same partition. Because the ready queue is unbounded,
    /// the <see cref="BlockingCollection{T}.TryAdd(T)"/> always succeeds and never blocks,
    /// so this is safe to invoke under the lock.
    /// </summary>
    /// <returns><c>true</c> if this call newly scheduled the partition.</returns>
    private bool TryScheduleLocked(int partitionId, PartitionState state)
    {
        if (state.Scheduled || state.InFlight || state.Ops.Count == 0)
            return false;

        // Unbounded queue: TryAdd only fails if the collection is marked complete-for-adding,
        // which this scheduler never does. Set Scheduled only when the id is actually present.
        if (!_readyPartitions.TryAdd(partitionId))
            return false;

        state.Scheduled = true;
        return true;
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>
    /// Starts the worker threads. Idempotent (a second call is a no-op).
    /// <para>
    /// Callers may <see cref="EnqueueTask{T}"/> before Start(): such operations are parked
    /// in their per-partition queues and served, in submission order, once the workers spin
    /// up. This avoids a startup race with consumers that read before the owning
    /// <c>RaftManager</c> has called Start().
    /// </para>
    /// </summary>
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
        _stopping = true;

        if (!_started)
        {
            // Start() was never called, but callers may have parked reads via EnqueueTask
            // (which no longer requires Start()). Drain them synchronously on this thread so
            // their awaiters complete instead of hanging forever. This is safe precisely
            // because no worker threads exist: every partition has InFlight=false, so the
            // DrainRemaining sweep processes all parked operations in submission order with
            // no risk of concurrent execution on the same partition.
            DrainRemaining(new List<Action>(MaxBatchSize));
            return;
        }

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
        // The ready queue is unbounded, so TryScheduleLocked never fails to reinsert
        // a non-empty partition — the reschedule can no longer be stranded by a full
        // queue waiting for a future enqueue that may never arrive.
        lock (state.Lock)
        {
            state.Depth   -= batch.Count;
            state.InFlight = false;

            TryScheduleLocked(partitionId, state);
        }
    }

    private void DrainRemaining(List<Action> batch)
    {
        // Shutdown drain. Multiple canceled workers may run this concurrently. The ONLY
        // thing that executes a partition is an atomic TryTake of its id from the ready
        // queue, so each partition is owned by exactly one worker at a time even here.
        // This closes the previous two-drainer TOCTOU where the sweep checked
        // "Ops.Count > 0 && !InFlight" under the lock, released it, and then called
        // ProcessPartition in a separate critical section — letting two drainers split
        // one partition's queue and execute the halves concurrently.
        //
        // Admission is already closed (Stop() sets _stopping before _cts.Cancel()), so the
        // system drains monotonically. Loop until global quiescence: no partition has
        // queued work, none is InFlight, and the ready queue is empty. A partition another
        // worker is still executing (InFlight) is left alone; that worker re-schedules it
        // via TryScheduleLocked on completion and one of the drain loops then claims it.
        while (true)
        {
            // Claim every eligible partition into the ready queue (atomic under each lock).
            foreach (KeyValuePair<int, PartitionState> kv in _partitions)
            {
                lock (kv.Value.Lock)
                    TryScheduleLocked(kv.Key, kv.Value);
            }

            // Drain whatever is claimable right now.
            while (_readyPartitions.TryTake(out int partitionId))
                ProcessPartition(partitionId, batch);

            // Quiescence check: fully drained AND nobody still executing a batch?
            bool pendingWork = false, inFlight = false;
            foreach (KeyValuePair<int, PartitionState> kv in _partitions)
            {
                lock (kv.Value.Lock)
                {
                    if (kv.Value.Ops.Count > 0) pendingWork = true;
                    if (kv.Value.InFlight) inFlight = true;
                }

                if (pendingWork && inFlight)
                    break;
            }

            if (!pendingWork && !inFlight)
                break;

            // Another worker is still finishing a batch that will re-queue its partition.
            // Yield rather than spin so that worker can make progress and re-schedule.
            Thread.Yield();
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
