#if KOMMANDER_THREAD_FREE
using Kommander.Support.Parallelization;
using Microsoft.Extensions.Logging;

namespace Kommander.Scheduling;

/// <summary>
/// Drives one node's scheduling work as async continuations on the ambient scheduler, so the node
/// runs on a host that cannot create threads. On single-threaded WebAssembly the ambient scheduler
/// is the browser event loop. Exists only in the thread-free build (<c>KOMMANDER_THREAD_FREE</c>)
/// and runs when <see cref="RaftConfiguration.EnableHostPumpedScheduling"/> is on.
///
/// <para><b>What it pumps.</b> The node is built in the manual-execution mode that a deterministic
/// simulation uses: the shared <see cref="RaftExecutorPool"/>, the write-ahead-log write scheduler,
/// and the outbound transport dispatcher own no threads, and write-ahead-log reads run inline. This
/// pump takes the place of the simulator. Each pass starts executor drains, writes pending
/// write-ahead-log batches, and flushes the outbound transport.</para>
///
/// <para><b>Drains are started, not awaited.</b> An executor drain can wait for work that only a
/// later pass does, such as a transport flush. A pump that awaited the drain would wait inside the
/// operation it must leave to serve. So each pass reaps the drains that finished, starts a new
/// drain for every ready executor up to <see cref="MaxConcurrentDrains"/>, and keeps the others in
/// flight. This is also how the threaded pool behaves, with one drain for each pool thread. The
/// per-partition run-lock in <see cref="RaftPartitionExecutor.DrainOnPoolAsync"/> still admits one
/// drainer for each partition, so the state machine stays serial.</para>
///
/// <para><b>How it waits.</b> A pass that moved nothing waits for the first of: an executor
/// scheduled (the pool's own wake signal), an in-flight drain finished, or an idle timeout. The
/// timeout covers the work that has no signal, which is a transport message that a timer queued
/// outside an executor. The timeout doubles on each idle pass, from
/// <see cref="MinIdleWait"/> to <see cref="MaxIdleWait"/>, and any work resets it. So an idle node
/// wakes a few times a second, and a busy node never waits.</para>
///
/// <para>The pump never blocks a thread. Every wait is an await.</para>
/// </summary>
internal sealed class RaftHostPump : IDisposable
{
    /// <summary>
    /// Upper bound on executor drains in flight at one time. A drain that waits holds one slot, so
    /// the bound only matters when many partitions wait together. It plays the role of the pool
    /// size in the threaded build.
    /// </summary>
    internal const int MaxConcurrentDrains = 64;

    /// <summary>First idle wait after a pass that moved work.</summary>
    internal static readonly TimeSpan MinIdleWait = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// Longest idle wait. It bounds the extra delay of a transport message that a timer queued
    /// outside an executor, which has no wake signal of its own.
    /// </summary>
    internal static readonly TimeSpan MaxIdleWait = TimeSpan.FromMilliseconds(20);

    private readonly RaftExecutorPool pool;

    private readonly Func<int> pumpWriteAheadLog;

    private readonly Func<Task<int>> flushTransport;

    private readonly ILogger logger;

    private readonly CancellationTokenSource cts = new();

    private readonly List<Task<bool>> drains = new(MaxConcurrentDrains);

    private Task? loop;

    private int disposed;

    /// <param name="pool">The node's shared executor pool, in manual mode.</param>
    /// <param name="pumpWriteAheadLog">Writes pending write-ahead-log batches and returns how many ran.</param>
    /// <param name="flushTransport">Sends queued outbound messages and returns how many left.</param>
    /// <param name="logger">Sink for a failure that escapes a pass.</param>
    public RaftHostPump(
        RaftExecutorPool pool,
        Func<int> pumpWriteAheadLog,
        Func<Task<int>> flushTransport,
        ILogger logger)
    {
        if (!pool.IsManualExecution)
            throw new InvalidOperationException("RaftHostPump requires an executor pool in manual mode.");

        this.pool = pool;
        this.pumpWriteAheadLog = pumpWriteAheadLog;
        this.flushTransport = flushTransport;
        this.logger = logger;
    }

    /// <summary>
    /// Starts the pump loop. Idempotent. The loop yields before its first pass, so no scheduling
    /// work runs inside the caller, which is the <see cref="RaftManager"/> constructor.
    /// </summary>
    public void Start()
    {
        if (loop is not null || Volatile.Read(ref disposed) != 0)
            return;

        loop = RunAsync(cts.Token);
        FireAndForget.Observe(loop, logger, "RaftHostPump");
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        await Task.Yield();

        TimeSpan idleWait = MinIdleWait;

        // One outstanding wake wait, kept across passes. A new wait on each pass would leave the
        // old one pending, and the old one would take the next permit, so the new one would sleep
        // through a real schedule until its timeout.
        Task<bool>? wake = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            int work;

            try
            {
                work = await PassAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // A pass failure must not end the pump: a node with no pump does nothing at all.
                logger.LogError(ex, "[RaftHostPump] A scheduling pass failed; the pump continues.");
                work = 0;
            }

            if (work > 0)
            {
                idleWait = MinIdleWait;
                await Task.Yield();
                continue;
            }

            try
            {
                wake ??= pool.WaitForWorkAsync(Timeout.InfiniteTimeSpan, cancellationToken);

                Task idle = Task.Delay(idleWait, cancellationToken);

                if (drains.Count == 0)
                    await Task.WhenAny(wake, idle).ConfigureAwait(false);
                else
                    await Task.WhenAny(wake, idle, Task.WhenAny(drains)).ConfigureAwait(false);

                if (wake.IsCompleted)
                    wake = null;
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                // The pool was disposed under the wake wait. The node is gone.
                break;
            }

            if (idleWait < MaxIdleWait)
                idleWait = TimeSpan.FromTicks(Math.Min(idleWait.Ticks * 2, MaxIdleWait.Ticks));
        }
    }

    /// <summary>
    /// Runs one pass and returns how many units of work moved. The order follows
    /// <see cref="RaftManager.PumpSchedulingAsync"/>: executors first, so a tick produces its
    /// write-ahead-log writes and outbound messages; then the write-ahead log; then the transport.
    /// </summary>
    private async Task<int> PassAsync()
    {
        int work = ReapFinishedDrains();

        while (drains.Count < MaxConcurrentDrains && pool.HasReadyWork)
        {
            Task<bool> drain = pool.PumpOnceAsync().AsTask();

            if (!drain.IsCompleted)
            {
                drains.Add(drain);
                work++;
                continue;
            }

            // Another pumper took the executor first (a teardown drain runs its own pump), so the
            // ready queue is empty now.
            if (!ObserveDrain(drain))
                break;

            work++;
        }

        work += pumpWriteAheadLog();
        work += await flushTransport().ConfigureAwait(false);

        return work;
    }

    private int ReapFinishedDrains()
    {
        int finished = 0;

        for (int i = drains.Count - 1; i >= 0; i--)
        {
            if (!drains[i].IsCompleted)
                continue;

            ObserveDrain(drains[i]);
            drains.RemoveAt(i);
            finished++;
        }

        return finished;
    }

    /// <summary>
    /// Reads the outcome of a finished drain. <see cref="RaftPartitionExecutor.DrainOnPoolAsync"/>
    /// contains its own failures, so a fault here is unexpected. It is logged and the pump goes
    /// on, as a pool thread does in the threaded build.
    /// </summary>
    private bool ObserveDrain(Task<bool> drain)
    {
        if (drain.IsCompletedSuccessfully)
            return drain.Result;

        if (drain.Exception is { } ex)
            logger.LogError(ex, "[RaftHostPump] An exception escaped an executor drain; the pump continues.");

        return false;
    }

    /// <summary>
    /// Stops the loop. Does not wait for it: teardown on a single-threaded host cannot block, and
    /// the loop exits at its next await. Drains still in flight finish on their own, and the
    /// executors' own teardown pumps what is left.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        cts.Cancel();
        cts.Dispose();
    }
}
#endif
