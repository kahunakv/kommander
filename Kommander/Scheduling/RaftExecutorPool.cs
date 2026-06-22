
using System.Collections.Concurrent;

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
    public RaftExecutorPool(int poolSize)
    {
        int p = poolSize > 0 ? poolSize : poolSize == 0 ? Environment.ProcessorCount : 1;
        _workers = new Thread[p];

        for (int i = 0; i < p; i++)
        {
            _workers[i] = new Thread(WorkerLoop)
            {
                IsBackground = true,
                Name = $"RaftExecutorPool-{i}"
            };
        }
    }

    /// <summary>Number of pool threads.</summary>
    public int PoolSize => _workers.Length;

    /// <summary>Starts all pool worker threads.  Safe to call only once.</summary>
    public void Start()
    {
        if (_started)
            return;

        _started = true;

        foreach (Thread t in _workers)
            t.Start();
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
        _workAvailable.Release();
    }

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

            executor.DrainOnPool();
        }
    }

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

        if (_started)
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
