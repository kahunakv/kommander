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
    // Opt-in relaxation of the single-flight-per-partition invariant (see the ctor doc). When true,
    // several workers may execute one partition's operations concurrently; FIFO *claiming* order is
    // preserved (ops are still dequeued in submission order) but completion order is not. Safe ONLY
    // for schedulers whose operations are mutually independent — pure point/range reads with no
    // read-after-write ordering handled at this layer (e.g. Kahuna's KV persistence reads, where
    // dirty in-memory entries are never served from disk, so disk is only read when authoritative).
    private readonly bool concurrentPerPartition;

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

        /// <summary>Guards <see cref="Ops"/>, <see cref="Scheduled"/>, <see cref="InFlightCount"/>, and <see cref="Depth"/>.</summary>
        public readonly object Lock = new();

        /// <summary>
        /// True when this partition's id is already in
        /// <see cref="FairReadScheduler._readyPartitions"/>.
        /// </summary>
        public bool Scheduled;

        /// <summary>
        /// Number of worker threads currently executing this partition's reads. In ordered mode this
        /// is 0 or 1 and acts as the single-flight gate: an <see cref="EnqueueTask{T}"/> that sees a
        /// non-zero count must NOT add the partition to <c>_readyPartitions</c>; the post-read lock
        /// section re-schedules when the read loop finishes. In concurrent mode it is bounded only by
        /// the worker count and exists for the shutdown quiescence check.
        /// </summary>
        public int InFlightCount;

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

    /// <summary>
    /// Synchronizes admission (<see cref="EnqueueTask{T}"/>) against shutdown
    /// (<see cref="Stop"/>). Admission takes the READ lock; shutdown takes the WRITE lock
    /// to publish <see cref="_stopping"/>. This closes the race where an enqueue passes the
    /// <c>_stopping</c> check, is pre-empted, and then publishes work into a partition queue
    /// *after* <see cref="Stop"/> has already drained and joined every worker — which would
    /// leave the returned task completing never. Because the write lock waits for all
    /// in-flight admissions to release their read lock, any enqueue that observed
    /// <c>_stopping == false</c> has finished publishing before drain begins; any enqueue
    /// that starts after <c>_stopping</c> is set observes it and is rejected.
    /// </summary>
    private readonly ReaderWriterLockSlim _lifecycle = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// Non-zero once a single worker (or the caller of <see cref="Stop"/> when no workers
    /// were started) has claimed the role of shutdown drain coordinator.
    /// </summary>
    private int _draining;

    /// <summary>
    /// Signaled by a worker each time it finishes a batch while <see cref="_stopping"/> is
    /// set. Lets the single drain coordinator block (rather than spin) while it waits for
    /// another worker to finish an in-flight batch and re-schedule its remainder.
    /// </summary>
    private readonly ManualResetEventSlim _drainWake = new(false);

    /// <summary>
    /// Signaled once the single owning <see cref="Stop"/> call has fully drained and joined
    /// all workers. <see cref="_stopping"/> means "stop requested" (admission is closed);
    /// this event means "stop completed" (no worker is running, all resources are safe to
    /// dispose). A concurrent second <see cref="Stop"/>/<see cref="Dispose"/> caller must
    /// wait on this before returning — otherwise it could dispose backend resources while
    /// the owner's drain is still executing I/O on a worker thread.
    /// </summary>
    private readonly ManualResetEventSlim _stopCompleted = new(false);

    // ── Observability ──────────────────────────────────────────────────────

    private long _totalReadsCompleted;

    /// <summary>Total number of read operations completed by this scheduler.</summary>
    public long TotalReadsCompleted => Interlocked.Read(ref _totalReadsCompleted);

    /// <summary>
    /// Test-only seam. Invoked inside <see cref="EnqueueTask{T}"/> immediately after the
    /// admission passes the <see cref="_stopping"/> check and while it still holds the
    /// admission read lock, but before the work is published to the partition queue. Lets a
    /// test deterministically hold an admission "in flight" and prove that <see cref="Stop"/>
    /// blocks on the lifecycle write lock until the admission completes — the interleaving
    /// that would otherwise strand the returned task. Null (and free) on the normal path.
    /// </summary>
    internal Action? OnAfterAdmissionCheck;

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
    /// <param name="concurrentPerPartition">
    /// When true, drops the at-most-one-worker-per-partition invariant: multiple workers may execute
    /// one partition's operations concurrently (submission order still decides dequeue order, but not
    /// completion order). Motivated by single-partition standalone deployments, where the invariant
    /// collapsed ALL backend reads onto one thread at a time — measured as the dominant statement-latency
    /// source under cold caches. Leave false (the default) for any scheduler whose operations require
    /// mutual ordering (WAL reads interleaved with truncations/compaction deletes).
    /// </param>
    public FairReadScheduler(
        ILogger<IRaft> logger,
        int workerCount = 0,
        int maxQueueDepthPerPartition = DefaultMaxQueueDepth,
        bool concurrentPerPartition = false)
    {
        this.logger = logger;
        this.maxQueueDepthPerPartition = maxQueueDepthPerPartition;
        this.concurrentPerPartition = concurrentPerPartition;

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
        // Admission runs under the READ lock so that Stop() (WRITE lock) cannot begin
        // draining until every in-flight admission has finished publishing its work. The
        // _stopping check MUST be inside the lock: checking it outside would reintroduce the
        // window where an enqueue passes the check, Stop() publishes _stopping + drains +
        // joins, and only then does the enqueue publish work no worker will ever run.
        _lifecycle.EnterReadLock();
        try
        {
            if (_stopping)
                throw new InvalidOperationException("FairReadScheduler: scheduler is stopping; no new operations accepted.");

            OnAfterAdmissionCheck?.Invoke();

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
        finally
        {
            _lifecycle.ExitReadLock();
        }
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
        if (state.Scheduled || (!concurrentPerPartition && state.InFlightCount > 0) || state.Ops.Count == 0)
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
    ///
    /// <para>Idempotent and safe to call concurrently: exactly one caller (the first to
    /// observe <see cref="_stopping"/> transition false→true) owns the drain-and-join; every
    /// other caller BLOCKS until that owner has completed. This matters because callers use a
    /// completed <c>Stop()</c> as the signal that no worker is still touching backend storage
    /// before disposing it — a second caller must not return (and let the backend be
    /// disposed) while the owner's drain is still executing I/O on a worker thread.</para>
    /// </summary>
    public void Stop()
    {
        // Publish _stopping ("stop requested") under the WRITE lock. This blocks until every
        // in-flight EnqueueTask (READ lock) has finished publishing its work, and guarantees
        // any admission starting after this point observes _stopping and is rejected. Without
        // this barrier a pre-empted enqueue could append work after the drain below has
        // already run, stranding the returned task forever.
        bool owner;
        _lifecycle.EnterWriteLock();
        try
        {
            owner = !_stopping;
            _stopping = true;
        }
        finally
        {
            _lifecycle.ExitWriteLock();
        }

        if (!owner)
        {
            // Another caller owns the shutdown. Wait until it has fully drained and joined
            // every worker before returning, so resource disposal cannot race active I/O.
            _stopCompleted.Wait();
            return;
        }

        try
        {
            if (!_started)
            {
                // Start() was never called, but callers may have parked reads via EnqueueTask
                // (which no longer requires Start()). Drain them synchronously on this thread
                // so their awaiters complete instead of hanging forever. This is safe
                // precisely because no worker threads exist: every partition has
                // InFlight=false, so the drain processes all parked operations in submission
                // order with no risk of concurrent execution on the same partition.
                DrainRemaining(new List<Action>(MaxBatchSize));
                return;
            }

            _cts.Cancel();

            foreach (Thread worker in _workers)
                worker.Join();
        }
        finally
        {
            // Publish "stop completed" — releases any concurrent waiters above. Set even on
            // an exceptional drain so waiters never hang.
            _stopCompleted.Set();
        }
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
            state.InFlightCount++;

            // Concurrent mode: if operations remain beyond this batch, put the partition straight
            // back in the ready queue BEFORE executing, so another worker can drain the remainder
            // in parallel. Ordered mode must not do this — the invariant is exactly one executor.
            if (concurrentPerPartition)
                TryScheduleLocked(partitionId, state);
        }

        if (batch.Count == 0)
        {
            lock (state.Lock)
                state.InFlightCount--;
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
            state.Depth -= batch.Count;
            state.InFlightCount--;

            TryScheduleLocked(partitionId, state);
        }

        // If we're shutting down, wake the single drain coordinator: it may be blocked
        // waiting for exactly this batch to finish (and re-queue its remainder) so it can
        // claim the partition. Only relevant during Stop(); no cost on the normal path.
        if (_stopping)
            _drainWake.Set();
    }

    private void DrainRemaining(List<Action> batch)
    {
        // Exactly ONE canceled worker coordinates the shutdown drain. Others return
        // immediately: by the time a worker reaches DrainRemaining its own in-progress batch
        // has already completed and re-queued any remainder (ProcessPartition tail), so the
        // coordinator will pick it up. A single coordinator prevents every canceled worker
        // from scanning all partitions and spinning — which would burn CPU across all
        // scheduler threads whenever one slow read holds up shutdown.
        if (Interlocked.CompareExchange(ref _draining, 1, 0) != 0)
            return;

        // The ONLY thing that executes a partition is an atomic TryTake of its id from the
        // ready queue, so each partition is owned by one worker at a time. This closes the
        // previous two-drainer TOCTOU where the sweep checked "Ops.Count > 0 && !InFlight"
        // under the lock, released it, and then called ProcessPartition in a separate
        // critical section — letting two drainers split one partition's queue.
        //
        // Admission is already closed (Stop() publishes _stopping under the write lock before
        // _cts.Cancel()), so the system drains monotonically. Loop until global quiescence:
        // no partition has queued work, none is InFlight, and the ready queue is empty.
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
                    if (kv.Value.InFlightCount > 0) inFlight = true;
                }

                if (pendingWork && inFlight)
                    break;
            }

            if (!pendingWork && !inFlight)
                break;

            if (inFlight)
            {
                // Another worker is still executing a batch it will re-queue on completion.
                // Block (bounded) on its completion signal instead of spinning. The timeout
                // is a safety net against a lost wakeup (the worker may Set the event between
                // our scan and this Wait); worst case is a short, bounded shutdown delay, not
                // a hang or a busy loop.
                _drainWake.Reset();
                _drainWake.Wait(TimeSpan.FromMilliseconds(25));
            }
            // Else: work is pending but claimable (not held by any worker); loop straight back
            // to claim and drain it — each pass makes progress, so this is not a busy spin.
        }
    }

    // ── IDisposable ────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        // Always call Stop() — never skip on _stopping. Stop() is idempotent and, crucially,
        // BLOCKS until the owning shutdown has fully drained and joined. Skipping it when a
        // concurrent Stop() had merely *requested* shutdown (set _stopping) would let Dispose
        // tear down _cts/_readyPartitions while a worker is still draining I/O.
        Stop();

        _cts.Dispose();
        _readyPartitions.Dispose();
        _drainWake.Dispose();
        _stopCompleted.Dispose();
        _lifecycle.Dispose();
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
