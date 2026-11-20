
using Microsoft.Extensions.Logging;

namespace Kommander.Scheduling;

/// <summary>
/// Shared pool of worker threads that multiplexes many <see cref="RaftPartitionExecutor"/>
/// instances over a small fixed set of OS threads, eliminating the one-thread-per-partition
/// stack ceiling.
///
/// <para><b>Single-owner invariant:</b> each partition has a per-partition run-lock
/// (implemented in <see cref="RaftPartitionExecutor.DrainOnPool"/>).  At most one pool
/// thread holds the run-lock for a given partition at any instant, so the state machine
/// still executes serially — exactly as with a dedicated thread.</para>
///
/// <para><b>Scheduling:</b> when a partition has work, <see cref="Schedule"/> adds it to the
/// ready queue, a <see cref="ParkedWorkerQueue{T}"/>. An idle pool thread is parked on its own
/// event and costs nothing; a schedule wakes exactly one of them, or none while a worker is in
/// its short spin for the next item. The woken thread dequeues one executor and calls
/// <see cref="RaftPartitionExecutor.DrainOnPool"/>. Cooperative yield-after-quantum behaviour is
/// handled inside <c>DrainOnPool</c> via the existing per-class drain quanta.</para>
///
/// <para>The pool used to park every thread on one <c>SemaphoreSlim</c>, which spins before it
/// parks on each wait. With a serial stream of small rounds that spin was most of the pool's CPU;
/// see <see cref="ParkedWorkerQueue{T}"/>.</para>
///
/// <para>Controlled by <see cref="RaftConfiguration.EnableSharedExecutorPool"/>,
/// <see cref="RaftConfiguration.PartitionExecutorPoolSize"/> and
/// <see cref="RaftConfiguration.IdleWorkerMaxSpinBeforePark"/>.</para>
/// </summary>
public sealed class RaftExecutorPool : IDisposable
{
    private readonly Thread[] _workers;

    /// <summary>Executors with work, and the pool threads parked for them.</summary>
    private readonly ParkedWorkerQueue<RaftPartitionExecutor> _ready;

#if KOMMANDER_THREAD_FREE
    /// <summary>
    /// The host pump's wake signal: one permit per <see cref="Schedule"/>. The thread-free build has no
    /// pool threads to park, so this is its only waiter; see <see cref="WaitForWorkAsync"/>.
    /// </summary>
    private readonly SemaphoreSlim _workAvailable = new(0, int.MaxValue);
#endif

    private bool _started;
    private bool _stopped;

    /// <summary>
    /// Creates the pool with <paramref name="poolSize"/> worker threads.
    /// Threads are not started until <see cref="Start"/> is called.
    /// </summary>
    /// <param name="poolSize">
    /// Number of worker threads.  0 auto-sizes to <see cref="Environment.ProcessorCount"/>.
    /// Values below 1 are clamped to 1.
    /// </param>
    /// <param name="manualExecution">
    /// When true the pool owns no threads: <see cref="Start"/> creates none and a caller drives
    /// every drain through <see cref="PumpOnce"/> or <see cref="PumpUntilIdle"/>.
    /// <para>Only a deterministic simulation sets this. It is what turns "the executors made
    /// progress" from something the thread pool decides into a step the harness takes, which is
    /// the difference between a run that replays and one that only usually behaves the same. The
    /// single-owner invariant is unaffected: the per-partition run-lock inside
    /// <see cref="RaftPartitionExecutor.DrainOnPool"/> still admits one drainer at a time.</para>
    /// </param>
    /// <param name="logger">
    /// Sink for a failure that escapes an executor drain. Optional: a pool without one still
    /// survives the failure, it only cannot say so.
    /// </param>
    /// <param name="maxSpinBeforePark">
    /// Upper bound of how long one idle pool thread polls the ready queue before it parks. Null means
    /// the default (20 µs); <see cref="TimeSpan.Zero"/> disables the spin, so every schedule that
    /// finds no busy thread is a kernel wake. See <see cref="RaftConfiguration.IdleWorkerMaxSpinBeforePark"/>.
    /// </param>
    public RaftExecutorPool(int poolSize, bool manualExecution = false, ILogger? logger = null, TimeSpan? maxSpinBeforePark = null)
    {
        _manualExecution = manualExecution;
        _logger = logger;

#if KOMMANDER_THREAD_FREE
        // The thread-free build has no worker threads to create. RaftConfiguration.Validate
        // already refuses the threaded configuration, so this only guards a direct construction.
        if (!manualExecution)
            throw new PlatformNotSupportedException(
                "RaftExecutorPool: the thread-free build (KOMMANDER_THREAD_FREE) supports manual execution only.");

        _ = poolSize;
        _workers = [];
#else
        int p = poolSize > 0 ? poolSize : poolSize == 0 ? Environment.ProcessorCount : 1;
        _workers = new Thread[manualExecution ? 0 : p];

        for (int i = 0; i < _workers.Length; i++)
        {
            int worker = i;
            _workers[i] = new Thread(() => WorkerLoop(worker))
            {
                IsBackground = true,
                Name = $"RaftExecutorPool-{i}"
            };
        }
#endif

        _ready = new ParkedWorkerQueue<RaftPartitionExecutor>(_workers.Length, maxSpinBeforePark);
    }

    /// <summary>True when this pool owns no threads and a caller drives every drain.</summary>
    private readonly bool _manualExecution;

    private readonly ILogger? _logger;

    /// <summary>
    /// Whether this pool is externally driven. Read by <see cref="RaftPartitionExecutor.Start"/>,
    /// which must run the write-ahead-log restore on the calling thread rather than on the thread
    /// pool: a thread-pool hop there would put the restore outside the harness's control and
    /// re-introduce exactly the nondeterminism manual mode exists to remove.
    /// </summary>
    internal bool IsManualExecution => _manualExecution;

    /// <summary>
    /// Drains one ready executor on the calling thread. Returns true when one was drained.
    /// Manual mode only.
    /// </summary>
    public async ValueTask<bool> PumpOnceAsync()
    {
        if (!_manualExecution)
            throw new InvalidOperationException(
                "RaftExecutorPool.PumpOnceAsync requires manual mode; this pool owns worker threads.");

        if (!_ready.TryDequeue(out RaftPartitionExecutor? executor))
            return false;

        await executor.DrainOnPoolAsync().ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Drains ready executors until none is ready, and returns how many drains ran.
    ///
    /// <para>One drain can make another executor ready — that is how a message crosses from one
    /// partition to another — so this is a loop. <paramref name="maxDrains"/> bounds it: two
    /// partitions that keep waking each other would otherwise never return, and a simulation
    /// wants that reported as a stuck step rather than as a hang.</para>
    /// </summary>
    public async ValueTask<int> PumpUntilIdleAsync(int maxDrains = 10_000)
    {
        int drains = 0;

        while (drains < maxDrains && await PumpOnceAsync().ConfigureAwait(false))
            drains++;

        return drains;
    }

#if KOMMANDER_THREAD_FREE
    /// <summary>
    /// True when at least one executor waits in the ready queue. The host pump reads it to decide
    /// whether to start another drain. A best-effort snapshot: a concurrent schedule can make it
    /// stale at once, which only delays that executor to the pump's next pass.
    /// </summary>
    internal bool HasReadyWork => !_ready.IsEmpty;

    /// <summary>
    /// Waits until an executor is scheduled, the timeout expires, or the token is cancelled.
    ///
    /// <para>Every <see cref="Schedule"/> releases one permit. The permits are counts, not
    /// flags, so a burst of schedules leaves surplus permits that wake the host pump for nothing.
    /// That costs one empty pass each and loses no wake. The wait is asynchronous, so it never
    /// blocks the one thread of a single-threaded host.</para>
    /// </summary>
    internal Task<bool> WaitForWorkAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
        _workAvailable.WaitAsync(timeout, cancellationToken);
#endif

    /// <summary>Number of pool threads.</summary>
    public int PoolSize => _workers.Length;

    /// <summary>
    /// Times a parked pool thread was woken by a schedule. Each one is a thread hand-off with an OS
    /// wake cost; against the executor operations served it gives the hand-offs per operation.
    /// </summary>
    public long WorkerWakeups => _ready.Wakeups;

    /// <summary>Wake-ups after which the pool thread found no executor ready: another thread took it first.</summary>
    public long EmptyWorkerWakeups => _ready.EmptyWakeups;

    /// <summary>Test seam: pool threads parked, or about to park. Exact only while the pool is quiet.</summary>
    internal int IdleWorkerCount => _ready.IdleWorkerCount;

    /// <summary>
    /// Starts all pool worker threads.  Safe to call only once.
    /// <para>In manual mode there are no threads to start; the caller pumps instead.</para>
    /// </summary>
    public void Start()
    {
        if (_started)
            return;

        _started = true;

#if !KOMMANDER_THREAD_FREE
        foreach (Thread t in _workers)
            t.Start();
#endif
    }

    /// <summary>
    /// Enqueues <paramref name="executor"/> into the global ready-queue and wakes one
    /// pool thread.  The caller is responsible for ensuring the executor's <c>_inQueue</c>
    /// flag has been set (CAS 0→1) before calling this method to prevent duplicate
    /// scheduling.
    /// </summary>
    /// <param name="executor">The executor with work.</param>
    /// <param name="fromDrain">
    /// True only for the re-schedule at the end of <see cref="RaftPartitionExecutor.DrainOnPoolAsync"/>.
    /// The pool thread that ran that drain goes back to the ready queue as soon as the drain returns,
    /// so it takes the executor itself (or wakes a parked thread for it when it takes another one
    /// first). Waking a second thread there only produced a thread that found nothing.
    /// </param>
    internal void Schedule(RaftPartitionExecutor executor, bool fromDrain = false)
    {
        // A schedule that lands after Stop has no thread to wake: the idle stack is empty by then.
        // The executor behind it is stopping too (RaftManager stops every partition before the pool),
        // so nothing a thread could still have done is lost.
        if (fromDrain)
            _ready.EnqueueFromWorker(executor);
        else
            _ready.Enqueue(executor);

#if KOMMANDER_THREAD_FREE
        try
        {
            _workAvailable.Release();
        }
        catch (ObjectDisposedException)
        {
        }
#endif
    }

#if !KOMMANDER_THREAD_FREE
    private void WorkerLoop(int worker)
    {
        // Take returns false once Stop ran, whatever is still queued: see the precondition on Stop.
        while (_ready.Take(worker, handOn: true, out RaftPartitionExecutor? executor))
        {
            try
            {
                executor.DrainOnPool();
            }
            catch (Exception ex)
            {
                // Last line of defence. DrainOnPool contains its own failures; anything that still
                // reaches here would end this thread with an unhandled exception, and an unhandled
                // exception on a dedicated thread ends the process — every partition on the node
                // with it. Log it and keep serving.
                _logger?.LogError(
                    ex,
                    "[RaftExecutorPool] An exception escaped an executor drain; the pool thread continues.");
            }
        }
    }
#endif

    /// <summary>
    /// Signals all pool threads to stop and blocks until they have all exited.
    /// Outstanding executor drains that are in progress will complete normally before
    /// the thread exits.  Safe to call even if <see cref="Start"/> was never called.
    ///
    /// <para><b>Precondition:</b> every <see cref="RaftPartitionExecutor"/> that was
    /// scheduled on this pool must have been stopped (via
    /// <see cref="RaftPartitionExecutor.Stop"/>) before this method is called.
    /// Executors that are still running will continue to re-enqueue themselves onto
    /// <c>_ready</c>, and those entries will be silently abandoned when the pool
    /// threads exit — potentially losing queued work.  <see cref="RaftManager"/> upholds
    /// this ordering by stopping all partitions before stopping the pool.</para>
    /// </summary>
    public void Stop()
    {
        if (_stopped)
            return;

        _stopped = true;

        // Wakes every parked thread; each sees the request on its next take and exits. A thread
        // inside a drain finishes it first.
        _ready.RequestStop();

#if !KOMMANDER_THREAD_FREE
        // _workers is empty in manual mode: there is no thread to join.
        if (_started)
        {
            foreach (Thread t in _workers)
                t.Join();
        }
#endif
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        Stop();
        _ready.Dispose();

#if KOMMANDER_THREAD_FREE
        _workAvailable.Dispose();
#endif
    }
}
