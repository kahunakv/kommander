using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Diagnostics;
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
///   <item>Opt-in read coalescing — point reads submitted via
///       <see cref="EnqueueBatchableTask{TArg,T}(int,TArg,IReadBatchExecutor{TArg,T})"/> against one
///       <see cref="IReadBatchExecutor{TArg,T}"/> instance are executed as a single backend
///       call per drained run (e.g. one RocksDB <c>MultiGet</c> for a burst of point reads),
///       without changing admission, fairness, or ordering of plain operations.</item>
///   <item>Cheap hand-offs for microsecond reads: an operation hinted
///       <see cref="ReadExecutionHint.InlineWhenIdle"/> runs on its caller when its partition is idle;
///       parked workers sleep without a spin and are woken one at a time; the callback overloads
///       complete on the worker with no task and no pool hop.</item>
/// </list>
///
/// <para><b>Why the hand-off path is built this way.</b> A profile of CamusDB on a Kahuna memory
/// backend found a 1 µs read costing ~55 µs of process CPU through this scheduler. The old ready queue
/// was a <c>BlockingCollection</c>, whose <c>SemaphoreSlim</c> spins before it parks (and on arm64 macOS
/// every later spin iteration is a <c>Thread.Yield</c> syscall), and every completion queued its awaiter
/// to the thread pool, whose workers spin too. Each hand-off woke a thread that burnt CPU before it
/// parked again. The ready queue is now a <see cref="ConcurrentQueue{T}"/> plus a stack of parked
/// workers, each on its own event with a spin count of zero; see <see cref="Park"/> for the lost-wake
/// argument.</para>
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

    /// <summary>
    /// True when this scheduler owns no threads and runs each operation on the calling thread.
    /// See the constructor's <c>inlineExecution</c> parameter for why a deterministic run needs it.
    /// </summary>
    private readonly bool inlineExecution;

    // ── Constants ──────────────────────────────────────────────────────────

    /// <summary>Maximum number of operations drained from a partition per scheduling cycle.</summary>
    private const int MaxBatchSize = 64;

    /// <summary>Default per-partition queue depth limit.</summary>
    public const int DefaultMaxQueueDepth = 4096;

    // ── Configuration ──────────────────────────────────────────────────────

    private readonly ILogger<IRaft> logger;
    private readonly int maxQueueDepthPerPartition;

    /// <summary>Default upper bound of the adaptive spin window; see <see cref="maxSpinTicks"/>.</summary>
    public static readonly TimeSpan DefaultMaxSpinBeforePark = TimeSpan.FromMicroseconds(20);

    /// <summary>
    /// Upper bound, in <see cref="Stopwatch"/> ticks, of how long ONE idle worker (the spinner) polls the
    /// ready queue before it parks. Zero disables spinning: every idle worker parks at once.
    ///
    /// <para><b>Why a bounded, adaptive spin and not none.</b> Parking at once removed the old
    /// <c>SemaphoreSlim</c> spin (every idle worker, every time) but made every hand-off a kernel wake,
    /// which regressed a high-rate stream (8 callers × 1 µs reads: ~10 → ~16 µs CPU per read). A spin pays
    /// only when the next item arrives inside the window, so the window adapts (<see cref="_spinWindowTicks"/>)
    /// and at most one worker spins at a time (<see cref="_spinning"/>). A slow stream (one read per
    /// 300 µs) decays the window to zero after a few misses and parks at once; a fast one keeps one worker
    /// polling and its producers skip the wake syscall.</para>
    /// </summary>
    private readonly long maxSpinTicks;

    /// <summary>
    /// Current spin window, 0..<see cref="maxSpinTicks"/>. A spin that finds no work halves it (to zero
    /// below a sixteenth of the bound); a park that a producer ends sooner than the bound doubles it (from
    /// zero to a quarter of the bound), because a spin of that length would have caught the work. Racy
    /// plain updates: it is a heuristic, never a correctness input.
    /// </summary>
    private long _spinWindowTicks;

    /// <summary>
    /// 1 while a worker is the spinner. A producer that sees 1 skips the wake: the spinner will find the
    /// id. Correctness does not depend on the skip being right — see <see cref="WorkerLoop"/> for how a
    /// skipped wake is handed on, and <see cref="Park"/> for why a spinner that gives up cannot miss work.
    /// </summary>
#if !KOMMANDER_THREAD_FREE
    private int _spinning;
#endif

    /// <summary>
    /// True while a worker is the spinner. Always false in the thread-free build, which has no workers.
    /// </summary>
    private bool IsSpinnerActive =>
#if KOMMANDER_THREAD_FREE
        false;
#else
        Volatile.Read(ref _spinning) != 0;
#endif

    // ── Per-partition state ────────────────────────────────────────────────

    private sealed class PartitionState
    {
        /// <summary>
        /// Pending read operations in submission order. Each item is either an
        /// <see cref="IReadWorkItem"/> or a <see cref="BatchableOp"/>; the drain loop type-tests,
        /// so no per-item wrapper allocation exists.
        /// </summary>
        public readonly Queue<object> Ops = new();

        /// <summary>Guards <see cref="Ops"/>, <see cref="Scheduled"/>, <see cref="InFlightCount"/>, and <see cref="Depth"/>.</summary>
        public readonly object Lock = new();

        /// <summary>
        /// True when this partition's id is already in <see cref="FairReadScheduler._readyQueue"/>.
        /// Invariant: <c>Ops.Count == 0</c> implies <c>Scheduled == false</c> (only a non-empty
        /// partition is scheduled, and only <see cref="ProcessPartition"/> dequeues, clearing the flag in
        /// the same critical section). The inline admission relies on it.
        /// </summary>
        public bool Scheduled;

        /// <summary>
        /// Number of threads currently executing this partition's reads: workers, plus callers running
        /// an <see cref="ReadExecutionHint.InlineWhenIdle"/> operation. In ordered mode this is 0 or 1
        /// and acts as the single-flight gate: an admission that sees a non-zero count must NOT add the
        /// partition to the ready queue; the post-read lock section re-schedules when the run finishes.
        /// In concurrent mode it is bounded only by the thread count and exists for the shutdown
        /// quiescence check.
        /// </summary>
        public int InFlightCount;

        /// <summary>Current number of pending-or-in-flight operations, inline ones included.</summary>
        public int Depth;
    }

    /// <summary>
    /// Non-generic view of a queued batchable operation so the drain loop can group runs of
    /// same-executor ops without knowing their closed generic type. Grouping key is the executor
    /// <b>instance</b>; because one executor instance only ever produces ops of one closed
    /// generic <see cref="BatchableOp{TArg,T}"/> (whatever their completion kind), every member of a
    /// group is safely castable inside <see cref="RunGroup"/>.
    /// </summary>
    private abstract class BatchableOp
    {
        /// <summary>Admission timestamp for the hand-off histogram; 0 when no listener was attached.</summary>
        internal long EnqueuedAt;

        /// <summary>The executor instance; reference identity defines batch-compatibility.</summary>
        internal abstract object ExecutorKey { get; }

        /// <summary>
        /// Executes <paramref name="count"/> ops from <paramref name="batch"/> starting at
        /// <paramref name="start"/> (all sharing this op's executor) as one backend call, and
        /// completes or faults each op. Never lets an executor exception escape: a
        /// throwing <see cref="IReadBatchExecutor{TArg,T}.ExecuteBatch"/> faults every op in
        /// the group instead, matching the N-individual-failed-reads semantics.
        /// </summary>
        internal abstract void RunGroup(List<object> batch, int start, int count, FairReadScheduler owner);

        /// <summary>Executes this op alone, as a batch of one (the inline path).</summary>
        internal abstract void RunAlone(FairReadScheduler owner);
    }

    /// <summary>
    /// Marker for the state-carried queue items so the drain loop can dispatch them without knowing
    /// their generic closed type. An interface (not a base class) because the task-returning item
    /// IS its <see cref="TaskCompletionSource{T}"/> — merging the two halves into one allocation is
    /// the point of the type.
    /// </summary>
    private interface IReadWorkItem
    {
        /// <summary>Admission timestamp for the hand-off histogram; 0 when no listener was attached.</summary>
        long EnqueuedAt { get; set; }

        /// <summary>Runs the operation and delivers its outcome; never throws.</summary>
        void Run(FairReadScheduler owner);
    }

    /// <summary>
    /// One queued read for the state-carried task path. Subclasses
    /// <see cref="TaskCompletionSource{T}"/> so the queue item, the completion source, and the
    /// (state, delegate) pair are ONE object — with a static caller delegate this makes a read
    /// cost two allocations (item + its <see cref="Task{T}"/>). Continuations always run
    /// asynchronously: an arbitrary awaiter must never run on a scheduler worker.
    /// </summary>
    private sealed class ReadWorkItem<TState, T> : TaskCompletionSource<T>, IReadWorkItem
    {
        private readonly TState state;
        private readonly Func<TState, T> operation;

        internal ReadWorkItem(TState state, Func<TState, T> operation)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            this.state = state;
            this.operation = operation;
        }

        public long EnqueuedAt { get; set; }

        public void Run(FairReadScheduler owner)
        {
            try
            {
                TrySetResult(operation(state));
            }
            catch (Exception ex)
            {
                TrySetException(ex);
            }
        }
    }

    /// <summary>
    /// One queued read for the completion-callback path: the only allocation per read. The thread
    /// that runs the operation runs the callback right after it, so nothing is queued to the pool.
    /// </summary>
    private sealed class CallbackReadWorkItem<TState, T> : IReadWorkItem
    {
        private readonly TState state;
        private readonly Func<TState, T> operation;
        private readonly Action<TState, T?, Exception?> onCompleted;

        internal CallbackReadWorkItem(TState state, Func<TState, T> operation, Action<TState, T?, Exception?> onCompleted)
        {
            this.state = state;
            this.operation = operation;
            this.onCompleted = onCompleted;
        }

        public long EnqueuedAt { get; set; }

        public void Run(FairReadScheduler owner)
        {
            T? result = default;
            Exception? error = null;

            try
            {
                result = operation(state);
            }
            catch (Exception ex)
            {
                error = ex;
            }

            owner.InvokeCallback(onCompleted, state, result, error);
        }
    }

    /// <summary>
    /// Batchable operation for one executor's (TArg, T). The completion kind (task or callback) is a
    /// subclass, so ops of either kind against one executor still coalesce into one call.
    /// </summary>
    private abstract class BatchableOp<TArg, T> : BatchableOp
    {
        private readonly IReadBatchExecutor<TArg, T> executor;
        private readonly TArg arg;

        protected BatchableOp(IReadBatchExecutor<TArg, T> executor, TArg arg)
        {
            this.executor = executor;
            this.arg = arg;
        }

        internal override object ExecutorKey => executor;

        /// <summary>Delivers this op's slot of a successful batch. Must not throw.</summary>
        protected abstract void Complete(T result, FairReadScheduler owner);

        /// <summary>Delivers the exception that faulted the batch. Must not throw.</summary>
        protected abstract void Fault(Exception error, FairReadScheduler owner);

        internal override void RunGroup(List<object> batch, int start, int count, FairReadScheduler owner)
        {
            TArg[] args = new TArg[count];

            for (int i = 0; i < count; i++)
                args[i] = ((BatchableOp<TArg, T>)batch[start + i]).arg;

            T[]? results = Execute(args, out Exception? error);

            for (int i = 0; i < count; i++)
            {
                BatchableOp<TArg, T> op = (BatchableOp<TArg, T>)batch[start + i];

                if (error is not null)
                    op.Fault(error, owner);
                else
                    op.Complete(results![i], owner);
            }
        }

        internal override void RunAlone(FairReadScheduler owner)
        {
            T[]? results = Execute([arg], out Exception? error);

            if (error is not null)
                Fault(error, owner);
            else
                Complete(results![0], owner);
        }

        private T[]? Execute(TArg[] args, out Exception? error)
        {
            try
            {
                T[] results = executor.ExecuteBatch(args);

                if (results.Length != args.Length)
                    throw new InvalidOperationException($"IReadBatchExecutor returned {results.Length} results for {args.Length} arguments.");

                error = null;
                return results;
            }
            catch (Exception ex)
            {
                error = ex;
                return null;
            }
        }
    }

    private sealed class TaskBatchableOp<TArg, T> : BatchableOp<TArg, T>
    {
        internal readonly TaskCompletionSource<T> Tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskBatchableOp(IReadBatchExecutor<TArg, T> executor, TArg arg) : base(executor, arg) { }

        protected override void Complete(T result, FairReadScheduler owner) => Tcs.TrySetResult(result);

        protected override void Fault(Exception error, FairReadScheduler owner) => Tcs.TrySetException(error);
    }

    private sealed class CallbackBatchableOp<TArg, TState, T> : BatchableOp<TArg, T>
    {
        private readonly TState state;
        private readonly Action<TState, T?, Exception?> onCompleted;

        internal CallbackBatchableOp(IReadBatchExecutor<TArg, T> executor, TArg arg, TState state, Action<TState, T?, Exception?> onCompleted)
            : base(executor, arg)
        {
            this.state = state;
            this.onCompleted = onCompleted;
        }

        protected override void Complete(T result, FairReadScheduler owner) =>
            owner.InvokeCallback(onCompleted, state, result, null);

        protected override void Fault(Exception error, FairReadScheduler owner) =>
            owner.InvokeCallback(onCompleted, state, default, error);
    }

    private readonly ConcurrentDictionary<int, PartitionState> _partitions = new();

    // ── Global ready-queue and parked workers ──────────────────────────────

    /// <summary>
    /// Partition ids with claimable work, FIFO over partitions. Intentionally UNBOUNDED: each
    /// partition is represented at most once (deduplicated by <see cref="PartitionState.Scheduled"/>),
    /// so the live entries are bounded by the count of distinct partitions, not by request rate. A
    /// previous bounded capacity capped distinct partitions rather than memory and stranded rescheduled
    /// work. Bounded admission is enforced per partition via <see cref="maxQueueDepthPerPartition"/>.
    /// </summary>
    private readonly ConcurrentQueue<int> _readyQueue = new();

    /// <summary>
    /// One per worker thread. A slot is on the idle stack exactly while its worker is parked or about
    /// to park, and only the thread that pops it sets its event, so a set is never lost or doubled.
    /// </summary>
    private sealed class WorkerSlot : IDisposable
    {
        internal readonly int Index;

        /// <summary>Spin count zero: <c>Wait</c> goes straight to the kernel. No spin on wake.</summary>
        internal readonly ManualResetEventSlim Event = new(false, spinCount: 0);

        /// <summary>Index of the slot below this one on the idle stack, or -1.</summary>
        internal int Next = -1;

        internal WorkerSlot(int index) => Index = index;

        public void Dispose() => Event.Dispose();
    }

    private readonly WorkerSlot[] _slots;

    /// <summary>
    /// Head of the idle-worker stack (a Treiber stack over <see cref="_slots"/>). Low 32 bits: top slot
    /// index + 1 (0 = empty). High 32 bits: a version bumped by every push and pop, so a pop that read
    /// a stale <see cref="WorkerSlot.Next"/> (the slot was popped and pushed again meanwhile — the ABA
    /// case, which reused slots make real) fails its compare-exchange instead of corrupting the stack.
    /// </summary>
    private long _idleHead;

    // ── Worker threads ─────────────────────────────────────────────────────

    private readonly Thread[] _workers;

#if !KOMMANDER_THREAD_FREE
    /// <summary>
    /// Set to 1 by <see cref="Stop"/> (with a full fence) to tell workers to drain and exit. Read by
    /// a parking worker after it pushed itself idle; see <see cref="Park"/>. The thread-free build has
    /// no workers to cancel.
    /// </summary>
    private int _cancelRequested;
#endif

    private volatile bool _stopping;
    private bool _started;

    /// <summary>
    /// Synchronizes admission against shutdown (<see cref="Stop"/>). Admission takes the READ lock;
    /// shutdown takes the WRITE lock to publish <see cref="_stopping"/>. This closes the race where an
    /// enqueue passes the <c>_stopping</c> check, is pre-empted, and then publishes work into a
    /// partition queue *after* <see cref="Stop"/> has already drained and joined every worker — which
    /// would leave the returned task completing never. Because the write lock waits for all
    /// in-flight admissions to release their read lock, any enqueue that observed
    /// <c>_stopping == false</c> has finished publishing (or counted its inline run in flight) before
    /// drain begins; any enqueue that starts after <c>_stopping</c> is set observes it and is rejected.
    /// </summary>
    private readonly ReaderWriterLockSlim _lifecycle = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// Non-zero once a single worker (or the caller of <see cref="Stop"/> when no workers
    /// were started) has claimed the role of shutdown drain coordinator.
    /// </summary>
    private int _draining;

    /// <summary>
    /// Signaled by a worker or an inline caller each time it finishes a run while <see cref="_stopping"/>
    /// is set. Lets the single drain coordinator block (rather than spin) while it waits for an
    /// in-flight run to finish and re-schedule its remainder.
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

    /// <summary>
    /// Non-zero while this thread is executing a scheduler operation or completion callback (on a
    /// worker, a drain, or an inline run), for any instance. An <see cref="ReadExecutionHint.InlineWhenIdle"/>
    /// admission made then queues instead, so inline runs never nest: a callback that issues the next
    /// read cannot recurse without bound, and an operation that (against its contract) re-enters a
    /// scheduler cannot run a second operation underneath itself.
    /// </summary>
    [ThreadStatic]
    private static int t_executionDepth;

    // ── Observability ──────────────────────────────────────────────────────

    private long _inlineReadsCompleted;
    private long _workerReadsCompleted;
    private long _workerWakeups;
    private long _emptyWorkerWakeups;
    private long _backpressureRejections;
    private int _queueDepthHighWater;

    private readonly KeyValuePair<string, object?> _schedulerTag;
    private readonly KeyValuePair<string, object?> _pathInlineTag = new("path", "inline");
    private readonly KeyValuePair<string, object?> _pathWorkerTag = new("path", "worker");

    /// <summary>
    /// Instance name, used as the <c>scheduler</c> tag on every meter this instance publishes
    /// (<c>wal</c> for the one <c>RaftManager</c> builds). A process can host several instances;
    /// every counter below is per instance.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Read operations this instance executed, inline and worker together. An operation is counted
    /// when its run starts, before its task completes or its callback runs, so an awaiter that sees
    /// its read complete always sees it counted.
    /// </summary>
    public long TotalReadsCompleted => InlineReadsCompleted + WorkerReadsCompleted;

    /// <summary>Operations that ran on their caller (<see cref="ReadExecutionHint.InlineWhenIdle"/> admitted inline).</summary>
    public long InlineReadsCompleted => Interlocked.Read(ref _inlineReadsCompleted);

    /// <summary>Operations a worker (or a shutdown/inline-mode drain) ran.</summary>
    public long WorkerReadsCompleted => Interlocked.Read(ref _workerReadsCompleted);

    /// <summary>Times a parked worker was woken by another thread.</summary>
    public long WorkerWakeups => Interlocked.Read(ref _workerWakeups);

    /// <summary>Wake-ups after which the worker found no work: another worker took it first.</summary>
    public long EmptyWorkerWakeups => Interlocked.Read(ref _emptyWorkerWakeups);

    /// <summary>Admissions refused with <see cref="ReadBackpressureExceededException"/>.</summary>
    public long BackpressureRejections => Interlocked.Read(ref _backpressureRejections);

    /// <summary>Highest pending-or-in-flight depth any one partition reached since construction.</summary>
    public int QueueDepthHighWater => Volatile.Read(ref _queueDepthHighWater);

    /// <summary>The <c>scheduler</c> tag, precomputed so a gauge scrape boxes nothing.</summary>
    internal KeyValuePair<string, object?> SchedulerTag => _schedulerTag;

    /// <summary>Test seam: the current adaptive spin window, in <see cref="Stopwatch"/> ticks.</summary>
    internal long SpinWindowTicks => Volatile.Read(ref _spinWindowTicks);

    /// <summary>
    /// Test seam: workers on the idle stack (parked, or pushed and about to park). Walks the stack without
    /// a lock, so it is exact only while no worker or producer is moving.
    /// </summary>
    internal int IdleWorkerCount
    {
        get
        {
            int count = 0;
            int index = (int)(uint)Volatile.Read(ref _idleHead) - 1;

            while (index >= 0 && count <= _slots.Length)
            {
                count++;
                index = Volatile.Read(ref _slots[index].Next);
            }

            return count;
        }
    }

    /// <summary>
    /// Test-only seam. Invoked inside the admission path immediately after it passes the
    /// <see cref="_stopping"/> check and while it still holds the admission read lock, but before
    /// the work is published to the partition queue. Lets a test deterministically hold an admission
    /// "in flight" and prove that <see cref="Stop"/> blocks on the lifecycle write lock until the
    /// admission completes — the interleaving that would otherwise strand the returned task. Null
    /// (and free) on the normal path.
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
    /// <param name="inlineExecution">
    /// When true the scheduler owns no threads: <see cref="Start"/> creates none, and every
    /// enqueued operation runs on the calling thread before <c>EnqueueTask</c> returns, so the
    /// returned task is always already completed.
    /// <para>
    /// This exists for deterministic simulation. A read is the one write-ahead-log operation the
    /// consensus path <b>awaits</b> (writes report through a completion callback instead), and the
    /// partition executor blocks on that await. A simulation pumped by one thread would therefore
    /// deadlock: the pump would wait for a read that only a read worker could run, and no read
    /// worker exists. Returning an already-completed task keeps every such await synchronous.
    /// </para>
    /// <para>
    /// Submission order still decides execution order, and the same drain path runs, so batching
    /// and per-partition ordering behave as in threaded mode. What is given up is cross-partition
    /// fairness and read parallelism, neither of which a deterministic run wants. Never set this
    /// in production: one slow backend read would then stall the partition executor that issued it.
    /// Per operation, <see cref="ReadExecutionHint.InlineWhenIdle"/> is the production-safe form.
    /// </para>
    /// </param>
    /// <param name="name">
    /// Instance name for the <c>scheduler</c> meter tag (e.g. <c>wal</c>, <c>kahuna-backend-read</c>).
    /// Defaults to <c>unnamed</c>.
    /// </param>
    /// <param name="maxSpinBeforePark">
    /// Upper bound of the adaptive spin window: how long one idle worker may poll the ready queue
    /// before it parks. Null means <see cref="DefaultMaxSpinBeforePark"/>; <see cref="TimeSpan.Zero"/>
    /// disables spinning, so every idle worker parks at once and every hand-off is a kernel wake.
    /// </param>
    public FairReadScheduler(
        ILogger<IRaft> logger,
        int workerCount = 0,
        int maxQueueDepthPerPartition = DefaultMaxQueueDepth,
        bool concurrentPerPartition = false,
        bool inlineExecution = false,
        string? name = null,
        TimeSpan? maxSpinBeforePark = null)
    {
        this.logger = logger;
        this.maxQueueDepthPerPartition = maxQueueDepthPerPartition;
        this.concurrentPerPartition = concurrentPerPartition;
        this.inlineExecution = inlineExecution;

        Name = string.IsNullOrEmpty(name) ? "unnamed" : name;
        _schedulerTag = new KeyValuePair<string, object?>("scheduler", Name);

        TimeSpan spinBound = maxSpinBeforePark ?? DefaultMaxSpinBeforePark;
        maxSpinTicks = spinBound <= TimeSpan.Zero ? 0 : Math.Max(1, (long)(spinBound.TotalSeconds * Stopwatch.Frequency));
        _spinWindowTicks = maxSpinTicks;

        if (inlineExecution)
            workerCount = 0;
        else if (workerCount <= 0)
            workerCount = Math.Max(1, Environment.ProcessorCount);

        _workers = new Thread[workerCount];
        _slots = new WorkerSlot[workerCount];

        for (int i = 0; i < workerCount; i++)
            _slots[i] = new WorkerSlot(i);

        KommanderMetrics.RegisterReadScheduler(this);
    }

    // ── IRaftReadScheduler ─────────────────────────────────────────────────

    /// <inheritdoc/>
    /// <remarks>Routed through the state-carried path with the delegate itself as the state, so
    /// even the legacy overload queues a single combined item instead of a TCS + closure + Action.</remarks>
    public Task<T> EnqueueTask<T>(int partitionId, Func<T> operation) =>
        EnqueueTask(partitionId, operation, static op => op());

    /// <inheritdoc/>
    public Task<T> EnqueueTask<TState, T>(int partitionId, TState state, Func<TState, T> operation) =>
        EnqueueTask(partitionId, state, operation, ReadExecutionHint.Queue);

    /// <inheritdoc/>
    public Task<T> EnqueueTask<TState, T>(int partitionId, TState state, Func<TState, T> operation, ReadExecutionHint hint)
    {
        ReadWorkItem<TState, T> item = new(state, operation);

        Submit(partitionId, item, hint);

        return item.Task;
    }

    /// <inheritdoc/>
    public void EnqueueTask<TState, T>(
        int partitionId,
        TState state,
        Func<TState, T> operation,
        Action<TState, T?, Exception?> onCompleted,
        ReadExecutionHint hint = ReadExecutionHint.Queue)
    {
        Submit(partitionId, new CallbackReadWorkItem<TState, T>(state, operation, onCompleted), hint);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Batching happens at drain time: <see cref="ProcessPartition"/> executes each maximal
    /// run of <b>adjacent</b> same-executor operations in its drained batch as one
    /// <see cref="IReadBatchExecutor{TArg,T}.ExecuteBatch"/> call. Adjacency (rather than
    /// grouping across the whole drained batch) is deliberate: it never reorders a batchable
    /// op relative to an interleaved plain <see cref="EnqueueTask{T}"/> op, so ordered-mode
    /// schedulers whose plain ops require mutual FIFO ordering (WAL reads vs truncations)
    /// remain correct even if they mix in batchable reads.
    /// </remarks>
    public Task<T> EnqueueBatchableTask<TArg, T>(int partitionId, TArg arg, IReadBatchExecutor<TArg, T> executor) =>
        EnqueueBatchableTask(partitionId, arg, executor, ReadExecutionHint.Queue);

    /// <inheritdoc/>
    public Task<T> EnqueueBatchableTask<TArg, T>(int partitionId, TArg arg, IReadBatchExecutor<TArg, T> executor, ReadExecutionHint hint)
    {
        TaskBatchableOp<TArg, T> op = new(executor, arg);

        Submit(partitionId, op, hint);

        return op.Tcs.Task;
    }

    /// <inheritdoc/>
    public void EnqueueBatchableTask<TArg, TState, T>(
        int partitionId,
        TArg arg,
        IReadBatchExecutor<TArg, T> executor,
        TState state,
        Action<TState, T?, Exception?> onCompleted,
        ReadExecutionHint hint = ReadExecutionHint.Queue)
    {
        Submit(partitionId, new CallbackBatchableOp<TArg, TState, T>(executor, arg, state, onCompleted), hint);
    }

    /// <summary>
    /// Admits <paramref name="work"/>, then runs it on this thread when the admission chose the inline
    /// path, or drains the ready queue here in inline-execution (simulation) mode.
    /// </summary>
    private void Submit(int partitionId, object work, ReadExecutionHint hint)
    {
        PartitionState? inlineState = EnqueueCore(partitionId, work, hint);

        // Both run outside the admission lock, so the caller's task is already completed when
        // EnqueueTask returns and the caller's await never yields. Outside the lock on purpose: read work
        // must not run under the lifecycle read lock, which Stop() takes for writing.
        if (inlineState is not null)
            RunInline(partitionId, inlineState, work);
        else if (inlineExecution)
            DrainInline();
    }

    /// <summary>
    /// Shared admission path. Either publishes <paramref name="work"/> (an <see cref="IReadWorkItem"/>
    /// or a <see cref="BatchableOp"/>) into the partition queue and returns null, or — for an
    /// <see cref="ReadExecutionHint.InlineWhenIdle"/> operation whose partition is idle — counts it in
    /// flight and returns the partition state, and the caller must then run it through
    /// <see cref="RunInline"/>. Throws without publishing (stopping / back-pressure), in which case the
    /// caller's task object is never exposed and its callback never runs, so nothing is stranded.
    ///
    /// <para><b>Inline admission and ordering.</b> The decision is made under the partition lock, from the
    /// same state the queue path uses. Idle means nothing queued (<c>Ops.Count == 0</c>, which also means
    /// not scheduled) and, in ordered mode, nothing in flight. The inline operation is counted in
    /// <c>Depth</c> and <c>InFlightCount</c> before the lock is released, so a later admission sees the
    /// partition busy and queues behind it (ordered mode: the queued op cannot be scheduled until the
    /// inline run releases the single-flight gate). Hence FIFO holds both ways: an inline op never
    /// overtakes a queued one, and a queued op never overtakes an inline one admitted before it. In
    /// concurrent mode in-flight ops are independent by declaration, so only an empty queue is needed.
    /// Back-pressure is checked first, so an inline op counts against the limit for its duration.</para>
    /// </summary>
    private PartitionState? EnqueueCore(int partitionId, object work, ReadExecutionHint hint)
    {
        // A nested admission (from inside a scheduler-run operation or callback) never runs inline.
        bool wantsInline = hint == ReadExecutionHint.InlineWhenIdle && !inlineExecution && t_executionDepth == 0;

        if (KommanderMetrics.ReadSchedulerHandoffLatencyUs.Enabled)
            StampEnqueuedAt(work, Stopwatch.GetTimestamp());

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
            // the ready queue); the worker threads created by Start() then drain it in
            // submission order. This closes the startup race where a consumer issues a read
            // before RaftManager.JoinCluster has called Start(): the read is served rather
            // than rejected with a retry. Only _stopping rejects new work — see Stop(),
            // which drains any parked operations even when Start() was never called.
            PartitionState state = _partitions.GetOrAdd(partitionId, _ => new PartitionState());

            lock (state.Lock)
            {
                if (state.Depth >= maxQueueDepthPerPartition)
                {
                    RecordBackpressureRejection();
                    throw new ReadBackpressureExceededException(partitionId, state.Depth);
                }

                state.Depth++;
                RecordDepth(state.Depth);

                if (wantsInline && state.Ops.Count == 0 && (concurrentPerPartition || state.InFlightCount == 0))
                {
                    state.InFlightCount++;
                    return state;
                }

                state.Ops.Enqueue(work);

                TryScheduleLocked(partitionId, state);
                return null;
            }
        }
        finally
        {
            _lifecycle.ExitReadLock();
        }
    }

    /// <summary>
    /// Runs an operation admitted inline by <see cref="EnqueueCore"/> on the calling thread, then
    /// releases its partition slot and schedules anything that queued behind it meanwhile.
    ///
    /// <para>Shutdown: <see cref="Stop"/> waits for <c>InFlightCount</c> to reach zero, so an inline run
    /// in flight delays the drain exactly as a worker's batch does. The release signals the drain
    /// coordinator INSIDE the partition lock: the coordinator only observes the zero count by taking that
    /// lock, so the signal happens before <see cref="Stop"/> can return and <see cref="Dispose"/> can
    /// dispose the event.</para>
    /// </summary>
    private void RunInline(int partitionId, PartitionState state, object work)
    {
        bool timing = KommanderMetrics.ReadSchedulerRunDurationUs.Enabled || KommanderMetrics.ReadSchedulerHandoffLatencyUs.Enabled;
        long start = timing ? Stopwatch.GetTimestamp() : 0;

        if (timing)
            RecordHandoff(work, start, _pathInlineTag);

        // Counted before the run publishes its completion; see TotalReadsCompleted.
        Interlocked.Increment(ref _inlineReadsCompleted);

        t_executionDepth++;
        try
        {
            RunWork(work);
        }
        catch (Exception ex)
        {
            // Defensive: both op kinds deliver their outcome internally and never throw.
            logger.LogError(
                "[FairReadScheduler] Unhandled exception in inline run for partition {PartitionId}: {Message}",
                partitionId, ex.Message);
        }
        finally
        {
            t_executionDepth--;

            lock (state.Lock)
            {
                state.Depth--;
                state.InFlightCount--;

                TryScheduleLocked(partitionId, state);

                if (_stopping)
                    _drainWake.Set();
            }
        }

        if (KommanderMetrics.ReadSchedulerOperationsTotal.Enabled)
            KommanderMetrics.ReadSchedulerOperationsTotal.Add(1, _schedulerTag, _pathInlineTag);

        if (timing && KommanderMetrics.ReadSchedulerRunDurationUs.Enabled)
            KommanderMetrics.ReadSchedulerRunDurationUs.Record(ElapsedMicroseconds(start), _schedulerTag, _pathInlineTag);
    }

    private void RunWork(object work)
    {
        if (work is IReadWorkItem item)
            item.Run(this);
        else
            ((BatchableOp)work).RunAlone(this);
    }

    /// <summary>
    /// Atomically claims a partition into the ready queue if it has pending work that no
    /// worker owns and it is not already scheduled, and wakes one parked worker for it. MUST be
    /// called while holding <paramref name="state"/>.<see cref="PartitionState.Lock"/>: the same
    /// critical section that observes <c>Ops</c>/<c>Scheduled</c>/<c>InFlight</c> also establishes
    /// ownership (sets <c>Scheduled</c>) before releasing the lock, so two threads cannot both decide
    /// to schedule — or drain — the same partition. The ready queue is unbounded and the wake is a
    /// lock-free pop plus an event set, so neither blocks under the lock.
    /// </summary>
    /// <param name="partitionId">The partition.</param>
    /// <param name="state">Its state; its lock must be held.</param>
    /// <param name="wakeIdle">
    /// False when the caller is a thread that returns to the ready queue right after this (the tail of
    /// <see cref="ProcessPartition"/>, run by a worker or a drain loop): it takes the id itself, so a wake
    /// would only produce an empty wake-up. Any other id still queued then is handed on by
    /// <see cref="WorkerLoop"/>'s post-take check.
    /// </param>
    /// <returns><c>true</c> if this call newly scheduled the partition.</returns>
    private bool TryScheduleLocked(int partitionId, PartitionState state, bool wakeIdle = true)
    {
        if (state.Scheduled || (!concurrentPerPartition && state.InFlightCount > 0) || state.Ops.Count == 0)
            return false;

        _readyQueue.Enqueue(partitionId);
        state.Scheduled = true;

        // One id, at most one wake, and none while a worker is spinning: the spinner takes the id without
        // a kernel wake. If no worker is parked, every worker is draining and one of them sees the id on
        // its next loop; see Park for why a worker cannot miss it on its way to sleep.
        if (wakeIdle && !IsSpinnerActive)
            WakeOneIdle();
        return true;
    }

    // ── Idle-worker stack ──────────────────────────────────────────────────

    private static long PackIdleHead(long previous, int slotIndex) =>
        unchecked((long)(((ulong)previous & 0xFFFF_FFFF_0000_0000UL) + 0x1_0000_0000UL) | (uint)(slotIndex + 1));

    private void PushIdle(WorkerSlot slot)
    {
        while (true)
        {
            long head = Volatile.Read(ref _idleHead);
            slot.Next = (int)(uint)head - 1;

            if (Interlocked.CompareExchange(ref _idleHead, PackIdleHead(head, slot.Index), head) == head)
                return;
        }
    }

    private bool TryPopIdle(out WorkerSlot? slot)
    {
        while (true)
        {
            long head = Volatile.Read(ref _idleHead);
            int top = (int)(uint)head - 1;

            if (top < 0)
            {
                slot = null;
                return false;
            }

            WorkerSlot candidate = _slots[top];
            int next = Volatile.Read(ref candidate.Next);

            if (Interlocked.CompareExchange(ref _idleHead, PackIdleHead(head, next), head) == head)
            {
                slot = candidate;
                return true;
            }
        }
    }

    private void WakeOneIdle()
    {
        if (TryPopIdle(out WorkerSlot? slot))
            slot!.Event.Set();
    }

    private void WakeAllIdle()
    {
        while (TryPopIdle(out WorkerSlot? slot))
            slot!.Event.Set();
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>
    /// Starts the worker threads. Idempotent (a second call is a no-op).
    /// <para>
    /// Callers may enqueue before Start(): such operations are parked
    /// in their per-partition queues and served, in submission order, once the workers spin
    /// up. This avoids a startup race with consumers that read before the owning
    /// <c>RaftManager</c> has called Start().
    /// </para>
    /// </summary>
    public void Start()
    {
        if (_started)
            return;

        // Inline mode owns no threads; the work already ran on the enqueuing thread.
        if (inlineExecution)
        {
            _started = true;
            return;
        }

#if KOMMANDER_THREAD_FREE
        // The thread-free build has no read workers. RaftConfiguration.Validate already refuses
        // the threaded configuration, so this only guards a direct construction. _started stays
        // false so Stop() still drains on the calling thread instead of joining threads that do not exist.
        throw new PlatformNotSupportedException(
            "FairReadScheduler: the thread-free build (KOMMANDER_THREAD_FREE) supports inline execution only.");
#else
        _started = true;

        for (int i = 0; i < _workers.Length; i++)
        {
            WorkerSlot slot = _slots[i];
            _workers[i] = new Thread(() => WorkerLoop(slot))
            {
                IsBackground = true,
                Name = $"FairReadScheduler-{Name}-{i}",
            };
            _workers[i].Start();
        }
#endif
    }

    /// <summary>
    /// Stops the scheduler.
    ///
    /// <para>All operations accepted before this call will be executed (or faulted), their
    /// <see cref="Task{T}"/> results delivered and their callbacks run, before workers exit. That
    /// includes inline runs still executing on their callers: the drain waits for them.</para>
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
        // in-flight admission (READ lock) has finished publishing its work, and guarantees
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
            // Inline mode joins the same path: it owns no threads, so there is nothing to cancel
            // or join, and anything parked must still be drained so no awaiter is stranded.
            if (!_started || inlineExecution)
            {
                // Start() was never called, but callers may have parked reads (enqueue does not
                // require Start()). Drain them synchronously on this thread so their awaiters
                // complete instead of hanging forever. Safe because no worker threads exist; an
                // inline run still on its caller is waited for by the quiescence check.
                DrainRemaining(new List<object>(MaxBatchSize));
                return;
            }

#if !KOMMANDER_THREAD_FREE
            // Full fence: a worker that pushed itself idle and then reads this flag, and this thread
            // that publishes it and then pops every idle worker, cannot both miss each other.
            Interlocked.Exchange(ref _cancelRequested, 1);
            WakeAllIdle();

            foreach (Thread worker in _workers)
                worker.Join();
#endif
        }
        finally
        {
            // Publish "stop completed" — releases any concurrent waiters above. Set even on
            // an exceptional drain so waiters never hang.
            _stopCompleted.Set();
        }
    }

    // ── Worker ─────────────────────────────────────────────────────────────

#if !KOMMANDER_THREAD_FREE
    /// <summary>
    /// Worker main loop: take a ready partition, else spin (if this worker can be the one spinner and the
    /// window is open), else park.
    ///
    /// <para><b>Handing on skipped wakes.</b> Producers skip the wake while a spinner exists, so several ids
    /// can arrive with only the spinner awake to take them. After every successful take, a worker that
    /// still sees ids queued and no spinner wakes one parked worker. The woken worker does the same after
    /// its own take, so the chain reaches as many workers as there are ids. Work is never stranded either
    /// way: an awake worker returns to the queue after each partition.</para>
    /// </summary>
    private void WorkerLoop(WorkerSlot slot)
    {
        List<object> batch = new(MaxBatchSize);
        bool justWoken = false;

        while (true)
        {
            if (_readyQueue.TryDequeue(out int partitionId))
            {
                justWoken = false;

                if (!_readyQueue.IsEmpty && !IsSpinnerActive)
                    WakeOneIdle();

                ProcessPartition(partitionId, batch);
                continue;
            }

            if (justWoken)
            {
                justWoken = false;
                RecordEmptyWakeup();
            }

            if (Volatile.Read(ref _cancelRequested) != 0)
            {
                DrainRemaining(batch);
                break;
            }

            if (TrySpinForWork())
                continue;

            long parkedAt = maxSpinTicks > 0 ? Stopwatch.GetTimestamp() : 0;

            if (Park(slot))
            {
                justWoken = true;
                RecordWakeup();

                if (maxSpinTicks > 0)
                    OpenSpinWindowAfterPark(Stopwatch.GetTimestamp() - parkedAt);
            }
        }
    }

    /// <summary>
    /// Becomes the one spinner, if nobody is and the window is open, and polls the ready queue for up to
    /// the current window. True when work (or a stop request) appeared; the caller loops to take it. A
    /// miss narrows the window.
    ///
    /// <para><b>No lost wake on the way out.</b> A producer that skipped its wake read
    /// <see cref="_spinning"/> as 1 after it published its id. The spinner clears the flag with a full
    /// fence before it parks, and <see cref="Park"/> re-checks the queue after that, so it sees every id
    /// whose producer counted on it.</para>
    /// </summary>
    private bool TrySpinForWork()
    {
        long window = Volatile.Read(ref _spinWindowTicks);

        if (window <= 0 || Interlocked.CompareExchange(ref _spinning, 1, 0) != 0)
            return false;

        bool found = false;
        try
        {
            long deadline = Stopwatch.GetTimestamp() + window;

            do
            {
                if (!_readyQueue.IsEmpty || Volatile.Read(ref _cancelRequested) != 0)
                {
                    found = true;
                    break;
                }

                Thread.SpinWait(4);
            }
            while (Stopwatch.GetTimestamp() < deadline);
        }
        finally
        {
            Interlocked.Exchange(ref _spinning, 0);
        }

        if (!found)
        {
            long narrowed = window / 2;
            Volatile.Write(ref _spinWindowTicks, narrowed < maxSpinTicks / 16 ? 0 : narrowed);
        }

        return found;
    }

    /// <summary>
    /// Widens the spin window when a park ended sooner than the bound: a spin of that length would have
    /// taken the work without the kernel wake.
    /// </summary>
    private void OpenSpinWindowAfterPark(long parkedTicks)
    {
        if (parkedTicks >= maxSpinTicks)
            return;

        long window = Volatile.Read(ref _spinWindowTicks);
        long widened = window <= 0 ? Math.Max(1, maxSpinTicks / 4) : Math.Min(maxSpinTicks, window * 2);
        Volatile.Write(ref _spinWindowTicks, widened);
    }

    /// <summary>
    /// Parks the worker until a producer pops its slot and sets its event.
    ///
    /// <para><b>No lost wake.</b> A producer publishes an id (a <see cref="ConcurrentQueue{T}"/> enqueue,
    /// whose tail claim is an interlocked operation) and then pops the idle stack. The worker pushes itself
    /// on the idle stack (a compare-exchange) and then re-checks the queue. Interlocked operations are full
    /// fences, so the two store-then-load sequences cannot both miss: either the producer's pop finds the
    /// worker, or the worker's re-check finds the id. When the re-check finds work, the worker wakes one
    /// idle worker for it — possibly itself, in which case it returns without sleeping; if the stack is
    /// already empty, someone popped this worker and will set its event, so it waits for that set. The
    /// same argument covers <see cref="Stop"/>, which publishes <see cref="_cancelRequested"/> with a full
    /// fence and then pops every idle worker.</para>
    ///
    /// <para>The event's spin count is zero, so <c>Wait</c> goes straight to the kernel: no spin on park
    /// and no spin on wake.</para>
    /// </summary>
    /// <returns><c>true</c> when the worker slept and was woken by another thread.</returns>
    private bool Park(WorkerSlot slot)
    {
        slot.Event.Reset();
        PushIdle(slot);

        if (Volatile.Read(ref _cancelRequested) != 0)
        {
            // Stop may have popped the stack before this push: wake everyone still on it, this worker
            // included, and go back to the loop, which sees the flag and exits.
            WakeAllIdle();
            return false;
        }

        if (!_readyQueue.IsEmpty && TryPopIdle(out WorkerSlot? other))
        {
            if (ReferenceEquals(other, slot))
                return false;

            other!.Event.Set();
        }

        slot.Event.Wait();
        return true;
    }
#endif

    /// <summary>
    /// Runs every ready partition on the calling thread until nothing is ready.
    ///
    /// <para>Re-entrant by design. Completing an operation's task can resume a caller that
    /// enqueues another read, which calls back into this method. The per-partition
    /// <c>Scheduled</c>/<c>InFlightCount</c> guards make that safe: an inner call drains what it
    /// can and the outer loop then finds the ready queue empty.</para>
    /// </summary>
    private void DrainInline()
    {
        List<object> batch = new(MaxBatchSize);

        while (_readyQueue.TryDequeue(out int partitionId))
            ProcessPartition(partitionId, batch);
    }

    private void ProcessPartition(int partitionId, List<object> batch)
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
            while (batch.Count < MaxBatchSize && state.Ops.TryDequeue(out object? work))
                batch.Add(work);

            // Clear the scheduled flag and raise the in-flight flag BEFORE
            // releasing the lock and running reads.
            // * Scheduled=false: the partition's ID has been removed from
            //   the ready queue (we just dequeued it).
            // * InFlight=true: prevents an admission from adding the partition back
            //   to the ready queue (or running inline) while reads are in progress.
            //   Without this a concurrent enqueue would see Scheduled=false, add the
            //   partition, and a second worker could start on the same partition concurrently.
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

        bool handoff = KommanderMetrics.ReadSchedulerHandoffLatencyUs.Enabled;
        bool duration = KommanderMetrics.ReadSchedulerRunDurationUs.Enabled;

        t_executionDepth++;
        try
        {
            for (int i = 0; i < batch.Count; )
            {
                // Coalesce each maximal run of ADJACENT ops sharing one executor instance into a
                // single ExecuteBatch call. Adjacency preserves the queue's total execution order
                // relative to interleaved plain work items — see EnqueueBatchableTask remarks.
                int runLength = 1;

                if (batch[i] is BatchableOp first)
                {
                    while (i + runLength < batch.Count &&
                           batch[i + runLength] is BatchableOp next &&
                           ReferenceEquals(next.ExecutorKey, first.ExecutorKey))
                        runLength++;
                }

                long start = handoff || duration ? Stopwatch.GetTimestamp() : 0;

                if (handoff)
                {
                    for (int j = i; j < i + runLength; j++)
                        RecordHandoff(batch[j], start, _pathWorkerTag);
                }

                // Counted before the run publishes its completions; see TotalReadsCompleted.
                Interlocked.Add(ref _workerReadsCompleted, runLength);

                try
                {
                    if (batch[i] is IReadWorkItem item)
                        item.Run(this); // Delivers its own result or exception internally.
                    else
                        ((BatchableOp)batch[i]).RunGroup(batch, i, runLength, this); // Delivers each op's outcome internally.
                }
                catch (Exception ex)
                {
                    // Defensive: both op kinds deliver their outcome internally and should never
                    // let an exception escape, but log just in case.
                    logger.LogError(
                        "[FairReadScheduler] Unhandled exception for partition {PartitionId}: {Message}",
                        partitionId, ex.Message);
                }

                RecordWorkerRun(batch[i] is BatchableOp, runLength, start, duration);

                i += runLength;
            }
        }
        finally
        {
            t_executionDepth--;
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

            TryScheduleLocked(partitionId, state, wakeIdle: false);
        }

        // If we're shutting down, wake the single drain coordinator: it may be blocked
        // waiting for exactly this batch to finish (and re-queue its remainder) so it can
        // claim the partition. Only relevant during Stop(); no cost on the normal path.
        // Outside the lock is safe here: the thread running this is a worker (joined before
        // Stop returns) or the coordinator itself.
        if (_stopping)
            _drainWake.Set();
    }

    private void DrainRemaining(List<object> batch)
    {
        // Exactly ONE canceled worker coordinates the shutdown drain. Others return
        // immediately: by the time a worker reaches DrainRemaining its own in-progress batch
        // has already completed and re-queued any remainder (ProcessPartition tail), so the
        // coordinator will pick it up. A single coordinator prevents every canceled worker
        // from scanning all partitions and spinning — which would burn CPU across all
        // scheduler threads whenever one slow read holds up shutdown.
        if (Interlocked.CompareExchange(ref _draining, 1, 0) != 0)
            return;

        // The ONLY thing that executes a queued partition is an atomic dequeue of its id from the
        // ready queue, so each partition is owned by one worker at a time. This closes the
        // previous two-drainer TOCTOU where the sweep checked "Ops.Count > 0 && !InFlight"
        // under the lock, released it, and then called ProcessPartition in a separate
        // critical section — letting two drainers split one partition's queue.
        //
        // Admission is already closed (Stop() publishes _stopping under the write lock before
        // it cancels the workers), so the system drains monotonically. Loop until global quiescence:
        // no partition has queued work, none is InFlight (worker or inline caller), and the ready
        // queue is empty.
        while (true)
        {
            // Claim every eligible partition into the ready queue (atomic under each lock).
            foreach (KeyValuePair<int, PartitionState> kv in _partitions)
            {
                lock (kv.Value.Lock)
                    TryScheduleLocked(kv.Key, kv.Value);
            }

            // Drain whatever is claimable right now.
            while (_readyQueue.TryDequeue(out int partitionId))
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
                // Another worker (or an inline caller) is still executing a run it will re-queue
                // behind on completion. Block (bounded) on its completion signal instead of
                // spinning. The timeout is a safety net against a lost wakeup (the run may Set the
                // event between our scan and this Wait); worst case is a short, bounded shutdown
                // delay, not a hang or a busy loop.
                _drainWake.Reset();
                _drainWake.Wait(TimeSpan.FromMilliseconds(25));
            }
            // Else: work is pending but claimable (not held by any worker); loop straight back
            // to claim and drain it — each pass makes progress, so this is not a busy spin.
        }
    }

    // ── Callbacks and observability ────────────────────────────────────────

    /// <summary>
    /// Runs a completion callback; a throw is logged and swallowed so it cannot stop the rest of a
    /// batch, kill a worker, or escape into an inline caller's admission.
    /// </summary>
    private void InvokeCallback<TState, T>(Action<TState, T?, Exception?> onCompleted, TState state, T? result, Exception? error)
    {
        try
        {
            onCompleted(state, result, error);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[FairReadScheduler] Completion callback threw on scheduler {Scheduler}: {Message}",
                Name, ex.Message);
        }
    }

    private static void StampEnqueuedAt(object work, long timestamp)
    {
        if (work is IReadWorkItem item)
            item.EnqueuedAt = timestamp;
        else
            ((BatchableOp)work).EnqueuedAt = timestamp;
    }

    private void RecordHandoff(object work, long start, KeyValuePair<string, object?> pathTag)
    {
        long enqueuedAt = work is IReadWorkItem item ? item.EnqueuedAt : ((BatchableOp)work).EnqueuedAt;

        // 0: admitted before a listener attached, so there is no admission timestamp.
        if (enqueuedAt != 0 && KommanderMetrics.ReadSchedulerHandoffLatencyUs.Enabled)
            KommanderMetrics.ReadSchedulerHandoffLatencyUs.Record(
                (start - enqueuedAt) * 1_000_000.0 / Stopwatch.Frequency, _schedulerTag, pathTag);
    }

    private void RecordWorkerRun(bool batchable, int runLength, long start, bool duration)
    {
        if (KommanderMetrics.ReadSchedulerOperationsTotal.Enabled)
            KommanderMetrics.ReadSchedulerOperationsTotal.Add(runLength, _schedulerTag, _pathWorkerTag);

        if (batchable && KommanderMetrics.ReadSchedulerBatchSize.Enabled)
            KommanderMetrics.ReadSchedulerBatchSize.Record(runLength, _schedulerTag);

        if (duration)
            KommanderMetrics.ReadSchedulerRunDurationUs.Record(ElapsedMicroseconds(start), _schedulerTag, _pathWorkerTag);
    }

    private void RecordWakeup()
    {
        Interlocked.Increment(ref _workerWakeups);

        if (KommanderMetrics.ReadSchedulerWakeupsTotal.Enabled)
            KommanderMetrics.ReadSchedulerWakeupsTotal.Add(1, _schedulerTag);
    }

    private void RecordEmptyWakeup()
    {
        Interlocked.Increment(ref _emptyWorkerWakeups);

        if (KommanderMetrics.ReadSchedulerEmptyWakeupsTotal.Enabled)
            KommanderMetrics.ReadSchedulerEmptyWakeupsTotal.Add(1, _schedulerTag);
    }

    private void RecordBackpressureRejection()
    {
        Interlocked.Increment(ref _backpressureRejections);

        if (KommanderMetrics.ReadSchedulerBackpressureRejectionsTotal.Enabled)
            KommanderMetrics.ReadSchedulerBackpressureRejectionsTotal.Add(1, _schedulerTag);
    }

    /// <summary>Raises the high-water mark; the plain read keeps the common (not higher) case to one load.</summary>
    private void RecordDepth(int depth)
    {
        int current = Volatile.Read(ref _queueDepthHighWater);

        while (depth > current)
        {
            int seen = Interlocked.CompareExchange(ref _queueDepthHighWater, depth, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    private static double ElapsedMicroseconds(long start) =>
        (Stopwatch.GetTimestamp() - start) * 1_000_000.0 / Stopwatch.Frequency;

    // ── IDisposable ────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        // Always call Stop() — never skip on _stopping. Stop() is idempotent and, crucially,
        // BLOCKS until the owning shutdown has fully drained and joined. Skipping it when a
        // concurrent Stop() had merely *requested* shutdown (set _stopping) would let Dispose
        // tear down the worker events while a worker is still draining I/O.
        Stop();

        foreach (WorkerSlot slot in _slots)
            slot.Dispose();

        _drainWake.Dispose();
        _stopCompleted.Dispose();
        _lifecycle.Dispose();
    }
}

/// <summary>
/// Thrown by <see cref="FairReadScheduler"/> admission when a partition's
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
