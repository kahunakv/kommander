
using System.Collections.Concurrent;
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
/// <para><b>Scheduling:</b> when a partition has work, it is added to the global
/// <c>_ready</c> queue by <see cref="Schedule"/>.  Idle pool threads park on a
/// <c>SemaphoreSlim</c>; each release wakes one thread which dequeues one executor
/// and calls <see cref="RaftPartitionExecutor.DrainOnPool"/>.  Cooperative
/// yield-after-quantum behaviour is handled inside <c>DrainOnPool</c> via the existing
/// per-class drain quanta.</para>
///
/// <para>Controlled by <see cref="RaftConfiguration.EnableSharedExecutorPool"/> and
/// <see cref="RaftConfiguration.PartitionExecutorPoolSize"/>.</para>
/// </summary>
public sealed class RaftExecutorPool : IDisposable
{
    private readonly Thread[] _workers;
    private readonly ConcurrentQueue<RaftPartitionExecutor> _ready = new();
    private readonly SemaphoreSlim _workAvailable = new(0, int.MaxValue);
    private readonly CancellationTokenSource _cts = new();
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
    public RaftExecutorPool(int poolSize, bool manualExecution = false, ILogger? logger = null)
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
            _workers[i] = new Thread(WorkerLoop)
            {
                IsBackground = true,
                Name = $"RaftExecutorPool-{i}"
            };
        }
#endif
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
    /// <para>Reuses the wake signal that a threaded pool parks its workers on: every
    /// <see cref="Schedule"/> releases one permit, in manual mode too. The permits are counts, not
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
    internal void Schedule(RaftPartitionExecutor executor)
    {
        _ready.Enqueue(executor);

        // A schedule that lands after Dispose has no thread to wake. The executor behind it is
        // stopping too (RaftManager stops every partition before the pool), so dropping the wake
        // loses nothing a thread could still have done.
        try
        {
            _workAvailable.Release();
        }
        catch (ObjectDisposedException)
        {
        }
    }

#if !KOMMANDER_THREAD_FREE
    private void WorkerLoop()
    {
        CancellationToken token = _cts.Token;

        while (true)
        {
            try
            {
                _workAvailable.Wait(token);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (!_ready.TryDequeue(out RaftPartitionExecutor? executor))
                continue;

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
        _cts.Cancel();

        // _workers is empty in manual mode. Release(0) throws, so the wake-and-join is skipped:
        // there is no parked worker to wake and no thread to join.
        if (_started && _workers.Length > 0)
        {
            // Wake all parked workers so they observe cancellation and exit.
            _workAvailable.Release(_workers.Length);

            foreach (Thread t in _workers)
                t.Join();
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        Stop();
        _cts.Dispose();
        _workAvailable.Dispose();
    }
}
