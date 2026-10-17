using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kommander.Diagnostics;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Hand-off tests for <see cref="FairReadScheduler"/>: the idle-worker
/// handshake that replaced the <c>BlockingCollection</c> spin, the adaptive single spinner,
/// <see cref="ReadExecutionHint.InlineWhenIdle"/>, the completion-callback overloads, and the
/// per-instance counters and meters.
///
/// <para>Tests that count wake-ups build the scheduler with <c>maxSpinBeforePark: TimeSpan.Zero</c>, so
/// every hand-off is a park and a wake and the counts are exact.</para>
/// </summary>
public sealed class TestFairReadSchedulerHandoffs
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private static FairReadScheduler NewScheduler(
        int workers = 1,
        bool concurrent = false,
        int maxDepth = FairReadScheduler.DefaultMaxQueueDepth,
        TimeSpan? spin = null,
        string? name = null) =>
        new(NullLogger<IRaft>.Instance, workers, maxDepth, concurrent, name: name ?? Guid.NewGuid().ToString("N"), maxSpinBeforePark: spin);

    private static async Task WaitUntil(Func<bool> condition, string because)
    {
        Stopwatch watch = Stopwatch.StartNew();

        while (!condition())
        {
            if (watch.Elapsed > Timeout)
                Assert.Fail($"Timed out waiting: {because}");

            await Task.Delay(1, TestContext.Current.CancellationToken);
        }
    }

    // ── Idle-worker handshake ───────────────────────────────────────────────

    /// <summary>
    /// The lost-wake race: an id published between a worker's last empty dequeue and its park must be
    /// served. Awaiting each read before issuing the next puts every enqueue right where the worker is
    /// heading to sleep. A lost wake has no safety net (no timed park), so it shows as a hang.
    /// </summary>
    [Theory]
    [InlineData(0, false)]
    [InlineData(0, true)]
    [InlineData(20, false)]
    [InlineData(20, true)]
    public async Task LostWakeRace_EveryEnqueueIsServed(int spinMicroseconds, bool concurrent)
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2, concurrent: concurrent, spin: TimeSpan.FromMicroseconds(spinMicroseconds));
        scheduler.Start();

        Task loop = Task.Run(async () =>
        {
            for (int i = 0; i < 10_000; i++)
            {
                // Vary the arrival point inside the worker's drain-to-park window.
                Thread.SpinWait(i % 64);
                Assert.Equal(i, await scheduler.EnqueueTask(1, i, static v => v));
            }
        }, TestContext.Current.CancellationToken);

        await loop.WaitAsync(Timeout, TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Many producers, many partitions, many workers, random pacing: stresses the idle stack's
    /// compare-exchange (the ABA case reuses slots constantly) and the hand-on of skipped wakes.
    /// </summary>
    [Fact]
    public async Task ManyProducers_ManyWorkers_AllComplete()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 8, concurrent: true);
        scheduler.Start();

        const int producers = 8;
        const int perProducer = 5_000;
        long sum = 0;

        Task[] tasks = new Task[producers];
        for (int p = 0; p < producers; p++)
        {
            int producer = p;
            tasks[p] = Task.Run(async () =>
            {
                Random random = new(producer);
                for (int i = 0; i < perProducer; i++)
                {
                    if (random.Next(8) == 0)
                        Thread.SpinWait(random.Next(500));

                    int value = await scheduler.EnqueueTask(1 + (i + producer) % 16, 1, static v => v);
                    Interlocked.Add(ref sum, value);
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(tasks).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(producers * perProducer, Interlocked.Read(ref sum));
        Assert.Equal(producers * perProducer, scheduler.TotalReadsCompleted);
    }

    /// <summary>
    /// A burst to one partition while it is in flight is one wake: the ordered single-flight gate keeps
    /// the burst off the ready queue, and the worker's own reschedule at the end of its run does not wake
    /// a second worker (it takes the partition itself).
    /// </summary>
    [Fact]
    public async Task BurstToOnePartition_IsOneWake()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 4, spin: TimeSpan.Zero);
        scheduler.Start();

        await WaitUntil(() => scheduler.IdleWorkerCount == 4, "all workers parked");

        using ManualResetEventSlim started = new(false);
        using ManualResetEventSlim release = new(false);

        Task<int> first = scheduler.EnqueueTask(1, () =>
        {
            started.Set();
            release.Wait();
            return 0;
        });

        Assert.True(started.Wait(Timeout, TestContext.Current.CancellationToken));

        List<Task<int>> burst = [];
        for (int i = 1; i < 64; i++)
            burst.Add(scheduler.EnqueueTask(1, i, static v => v));

        release.Set();

        await first.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await Task.WhenAll(burst).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(1, scheduler.WorkerWakeups);
        Assert.Equal(0, scheduler.EmptyWorkerWakeups);
        Assert.Equal(64, scheduler.WorkerReadsCompleted);
    }

    /// <summary>One wake-up is counted per parked-worker wake, and none of them finds the queue empty.</summary>
    [Fact]
    public async Task Counters_OneWakeupPerParkedWorkerWake()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1, spin: TimeSpan.Zero);
        scheduler.Start();

        for (int i = 0; i < 10; i++)
        {
            await WaitUntil(() => scheduler.IdleWorkerCount == 1, "worker parked");
            Assert.Equal(i, await scheduler.EnqueueTask(1, i, static v => v));
        }

        await WaitUntil(() => scheduler.IdleWorkerCount == 1, "worker parked");

        Assert.Equal(10, scheduler.WorkerWakeups);
        Assert.Equal(0, scheduler.EmptyWorkerWakeups);
    }

    [Fact]
    public async Task Stop_WhileWorkersParked_Returns()
    {
        FairReadScheduler scheduler = NewScheduler(workers: 4, spin: TimeSpan.Zero);
        scheduler.Start();

        await WaitUntil(() => scheduler.IdleWorkerCount == 4, "all workers parked");

        await Task.Run(scheduler.Dispose, TestContext.Current.CancellationToken).WaitAsync(Timeout, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Stop_WhileWorkerDraining_WaitsThenDrainsEverything()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2);
        scheduler.Start();

        using ManualResetEventSlim started = new(false);
        using ManualResetEventSlim release = new(false);

        Task<int> blocked = scheduler.EnqueueTask(1, () =>
        {
            started.Set();
            release.Wait();
            return -1;
        });

        Assert.True(started.Wait(Timeout, TestContext.Current.CancellationToken));

        List<Task<int>> queued = [];
        for (int i = 0; i < 100; i++)
            queued.Add(scheduler.EnqueueTask(1 + i % 3, i, static v => v));

        Task stop = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(stop.IsCompleted, "Stop returned while a worker was still inside a read.");

        release.Set();

        await stop.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.True(blocked.IsCompletedSuccessfully);
        Assert.All(queued, t => Assert.True(t.IsCompletedSuccessfully));
    }

    // ── Adaptive spin ───────────────────────────────────────────────────────

    /// <summary>
    /// A slow stream closes the spin window: each idle period's spin misses and halves it, so a host that
    /// reads a page every few hundred µs ends up parking at once and pays no spin per read.
    /// </summary>
    [Fact]
    public async Task SlowStream_ClosesSpinWindow()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1, spin: TimeSpan.FromMicroseconds(20));
        scheduler.Start();

        Assert.True(scheduler.SpinWindowTicks > 0);

        for (int i = 0; i < 20; i++)
        {
            await scheduler.EnqueueTask(1, i, static v => v);
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, scheduler.SpinWindowTicks);
    }

    /// <summary>
    /// A fast stream re-opens a closed window: a park that a producer ends sooner than the bound widens it.
    /// The bound is generous (5 ms) so a loaded test host still wakes the worker inside it.
    /// </summary>
    [Fact]
    public async Task FastStreamAfterSlow_ReopensSpinWindow()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1, spin: TimeSpan.FromMilliseconds(5));
        scheduler.Start();

        // Close it: 5 ms bound halves to zero after five misses of 40 ms gaps.
        for (int i = 0; i < 8; i++)
        {
            await scheduler.EnqueueTask(1, i, static v => v);
            await Task.Delay(40, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, scheduler.SpinWindowTicks);

        for (int i = 0; i < 200; i++)
            await scheduler.EnqueueTask(1, i, static v => v);

        Assert.True(scheduler.SpinWindowTicks > 0, "A stream of back-to-back reads did not re-open the spin window.");
    }

    // ── InlineWhenIdle ──────────────────────────────────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InlineWhenIdle_IdlePartition_RunsOnCallerBeforeReturn(bool concurrent)
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2, concurrent: concurrent);
        scheduler.Start();

        int caller = Environment.CurrentManagedThreadId;
        Task<int> task = scheduler.EnqueueTask(1, 7, static v => v * Environment.CurrentManagedThreadId, ReadExecutionHint.InlineWhenIdle);

        Assert.True(task.IsCompletedSuccessfully, "An inline read must be complete when EnqueueTask returns.");
        Assert.Equal(7 * caller, await task);
        Assert.Equal(1, scheduler.InlineReadsCompleted);
        Assert.Equal(0, scheduler.WorkerReadsCompleted);
    }

    [Fact]
    public async Task InlineWhenIdle_BeforeStart_RunsOnCaller()
    {
        using FairReadScheduler scheduler = NewScheduler();

        Task<int> task = scheduler.EnqueueTask(1, 3, static v => v, ReadExecutionHint.InlineWhenIdle);

        Assert.True(task.IsCompletedSuccessfully);
        Assert.Equal(3, await task);
    }

    /// <summary>The Queue hint never runs on the caller, idle partition or not.</summary>
    [Fact]
    public async Task QueueHint_NeverRunsOnCaller()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2);
        scheduler.Start();

        for (int i = 0; i < 200; i++)
        {
            int caller = Environment.CurrentManagedThreadId;
            int ran = await scheduler.EnqueueTask(1 + i % 2, 0, static _ => Environment.CurrentManagedThreadId, ReadExecutionHint.Queue);
            Assert.NotEqual(caller, ran);
        }

        Assert.Equal(0, scheduler.InlineReadsCompleted);
    }

    /// <summary>
    /// An inline op admitted first completes first; a queued op admitted while it runs waits for it
    /// (ordered mode: the inline op holds the single-flight gate) and then runs on a worker.
    /// </summary>
    [Fact]
    public async Task Ordered_InlineAdmittedFirst_CompletesBeforeQueued()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2);
        scheduler.Start();

        ConcurrentQueue<string> log = new();
        using ManualResetEventSlim started = new(false);
        using ManualResetEventSlim release = new(false);

        Task<int> inline = Task.Run(() => scheduler.EnqueueTask(1, 0, _ =>
        {
            log.Enqueue("inline-start");
            started.Set();
            release.Wait();
            log.Enqueue("inline-end");
            return 1;
        }, ReadExecutionHint.InlineWhenIdle), TestContext.Current.CancellationToken);

        Assert.True(started.Wait(Timeout, TestContext.Current.CancellationToken));

        Task<int> queued = scheduler.EnqueueTask(1, 0, _ =>
        {
            log.Enqueue("queued");
            return 2;
        });

        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(queued.IsCompleted, "A queued op overtook an inline op admitted before it.");

        release.Set();

        Assert.Equal(1, await inline.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal(2, await queued.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal(["inline-start", "inline-end", "queued"], log.ToArray());
    }

    /// <summary>
    /// A queued op (parked before Start) makes an InlineWhenIdle admission queue behind it: the inline
    /// attempt cannot overtake it, and runs on a worker after it.
    /// </summary>
    [Fact]
    public async Task InlineAttempt_BehindQueuedOp_Queues()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1);

        ConcurrentQueue<string> log = new();
        int caller = Environment.CurrentManagedThreadId;

        Task<int> queued = scheduler.EnqueueTask(1, 0, _ =>
        {
            log.Enqueue("queued");
            return Environment.CurrentManagedThreadId;
        });

        Task<int> attempt = scheduler.EnqueueTask(1, 0, _ =>
        {
            log.Enqueue("attempt");
            return Environment.CurrentManagedThreadId;
        }, ReadExecutionHint.InlineWhenIdle);

        Assert.False(attempt.IsCompleted, "An inline attempt ran past a queued op.");

        scheduler.Start();

        await Task.WhenAll(queued, attempt).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(["queued", "attempt"], log.ToArray());
        Assert.NotEqual(caller, await attempt);
        Assert.Equal(0, scheduler.InlineReadsCompleted);
    }

    /// <summary>
    /// Ordered mode: an op in flight on a worker makes the inline attempt queue. Concurrent mode: in-flight
    /// ops are independent by declaration, so the attempt runs inline while the worker is still inside
    /// the same partition.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InlineAttempt_WhileWorkerInsidePartition(bool concurrent)
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2, concurrent: concurrent);
        scheduler.Start();

        using ManualResetEventSlim started = new(false);
        using ManualResetEventSlim release = new(false);

        Task<int> blocked = scheduler.EnqueueTask(1, () =>
        {
            started.Set();
            release.Wait();
            return 0;
        });

        Assert.True(started.Wait(Timeout, TestContext.Current.CancellationToken));

        int caller = Environment.CurrentManagedThreadId;
        Task<int> attempt = scheduler.EnqueueTask(1, 0, static _ => Environment.CurrentManagedThreadId, ReadExecutionHint.InlineWhenIdle);

        if (concurrent)
        {
            Assert.True(attempt.IsCompletedSuccessfully, "Concurrent mode must allow an inline run beside an in-flight worker.");
            Assert.Equal(caller, await attempt);
        }
        else
        {
            await Task.Delay(50, TestContext.Current.CancellationToken);
            Assert.False(attempt.IsCompleted, "Ordered mode ran an inline op while a worker was inside the partition.");
        }

        release.Set();

        await Task.WhenAll(blocked, attempt).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        if (!concurrent)
            Assert.NotEqual(caller, await attempt);
    }

    /// <summary>Back-pressure counts an inline op for as long as it runs, so the limit does not loosen.</summary>
    [Fact]
    public async Task Backpressure_CountsInlineOpWhileItRuns()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1, maxDepth: 1);
        scheduler.Start();

        using ManualResetEventSlim started = new(false);
        using ManualResetEventSlim release = new(false);

        Task<int> inline = Task.Run(() => scheduler.EnqueueTask(1, 0, _ =>
        {
            started.Set();
            release.Wait();
            return 1;
        }, ReadExecutionHint.InlineWhenIdle), TestContext.Current.CancellationToken);

        Assert.True(started.Wait(Timeout, TestContext.Current.CancellationToken));

        // Admission throws synchronously, before any task exists.
        Assert.Throws<ReadBackpressureExceededException>(() => { _ = scheduler.EnqueueTask(1, 0, static v => v); });
        Assert.Throws<ReadBackpressureExceededException>(() => { _ = scheduler.EnqueueTask(1, 0, static v => v, ReadExecutionHint.InlineWhenIdle); });
        Assert.Equal(2, scheduler.BackpressureRejections);

        release.Set();
        Assert.Equal(1, await inline.WaitAsync(Timeout, TestContext.Current.CancellationToken));

        Assert.Equal(5, await scheduler.EnqueueTask(1, 5, static v => v).WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal(1, scheduler.QueueDepthHighWater);
    }

    /// <summary>Stop waits for an inline op still running on its caller, started scheduler or not.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Stop_WaitsForInlineOpInFlight(bool started)
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2);
        if (started)
            scheduler.Start();

        using ManualResetEventSlim running = new(false);
        using ManualResetEventSlim release = new(false);
        int finished = 0;

        Task<int> inline = Task.Run(() => scheduler.EnqueueTask(1, 0, _ =>
        {
            running.Set();
            release.Wait();
            Volatile.Write(ref finished, 1);
            return 1;
        }, ReadExecutionHint.InlineWhenIdle), TestContext.Current.CancellationToken);

        Assert.True(running.Wait(Timeout, TestContext.Current.CancellationToken));

        // Queued behind the inline op (ordered): Stop must drain it after the inline op finishes.
        Task<int> queued = scheduler.EnqueueTask(1, 2, static v => v);

        Task stop = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(stop.IsCompleted, "Stop returned while an inline op was still running.");

        release.Set();

        await stop.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(1, Volatile.Read(ref finished));
        Assert.Equal(1, await inline);
        Assert.True(queued.IsCompletedSuccessfully);
        Assert.Equal(2, await queued);
    }

    /// <summary>
    /// An InlineWhenIdle admission from inside a scheduler-run operation queues: inline runs never nest,
    /// on the caller or on a worker.
    /// </summary>
    [Fact]
    public async Task NestedInlineAdmission_Queues()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1);
        scheduler.Start();

        Task<int>? nested = null;
        int outerThread = 0;

        Task<int> outer = scheduler.EnqueueTask(1, 0, _ =>
        {
            outerThread = Environment.CurrentManagedThreadId;
            nested = scheduler.EnqueueTask(2, 0, static _ => Environment.CurrentManagedThreadId, ReadExecutionHint.InlineWhenIdle);
            return 1;
        }, ReadExecutionHint.InlineWhenIdle);

        Assert.True(outer.IsCompletedSuccessfully);
        Assert.Equal(Environment.CurrentManagedThreadId, outerThread);

        int nestedThread = await nested!.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        Assert.NotEqual(outerThread, nestedThread);
        Assert.Equal(1, scheduler.InlineReadsCompleted);
        Assert.Equal(1, scheduler.WorkerReadsCompleted);
    }

    [Fact]
    public async Task InlineWhenIdle_Batchable_RunsBatchOfOneOnCaller()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1);
        scheduler.Start();

        RecordingExecutor executor = new();

        Task<int> task = scheduler.EnqueueBatchableTask(1, 4, executor, ReadExecutionHint.InlineWhenIdle);

        Assert.True(task.IsCompletedSuccessfully);
        Assert.Equal(40, await task);
        Assert.Equal([4], Assert.Single(executor.Calls));
        Assert.Equal(Environment.CurrentManagedThreadId, executor.Threads.Single());
    }

    /// <summary>The deterministic-simulation mode keeps its own inline drain; the hint changes nothing there.</summary>
    [Fact]
    public void InlineExecutionMode_IgnoresHint()
    {
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, inlineExecution: true);
        scheduler.Start();

        Task<int> task = scheduler.EnqueueTask(1, 9, static v => v, ReadExecutionHint.InlineWhenIdle);

        Assert.True(task.IsCompletedSuccessfully);
        Assert.Equal(0, scheduler.InlineReadsCompleted);
        Assert.Equal(1, scheduler.WorkerReadsCompleted);
    }

    // ── Completion callbacks ────────────────────────────────────────────────

    [Fact]
    public async Task Callback_RunsOnWorkerOnceWithResult()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2);
        scheduler.Start();

        CallbackProbe<int> probe = new();
        int caller = Environment.CurrentManagedThreadId;

        scheduler.EnqueueTask(1, probe, static p => p.OperationThread = Environment.CurrentManagedThreadId, static (p, result, error) => p.Record(result, error));

        await probe.Done.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(1, probe.Calls);
        Assert.Null(probe.Error);
        Assert.Equal(probe.OperationThread, probe.Result);
        Assert.Equal(probe.OperationThread, probe.CallbackThread);
        Assert.NotEqual(caller, probe.CallbackThread);
    }

    [Fact]
    public void Callback_Inline_RunsOnCallerBeforeReturn()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1);
        scheduler.Start();

        CallbackProbe<int> probe = new();

        scheduler.EnqueueTask(1, probe, static _ => 11, static (p, result, error) => p.Record(result, error), ReadExecutionHint.InlineWhenIdle);

        Assert.True(probe.Done.Task.IsCompleted, "An inline callback must run before EnqueueTask returns.");
        Assert.Equal(11, probe.Result);
        Assert.Equal(Environment.CurrentManagedThreadId, probe.CallbackThread);
    }

    [Fact]
    public async Task Callback_ReceivesOperationException()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1);
        scheduler.Start();

        CallbackProbe<int> probe = new();

        scheduler.EnqueueTask<CallbackProbe<int>, int>(1, probe, static _ => throw new InvalidOperationException("boom"), static (p, result, error) => p.Record(result, error));

        await probe.Done.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(1, probe.Calls);
        Assert.Equal(0, probe.Result);
        Assert.Equal("boom", Assert.IsType<InvalidOperationException>(probe.Error).Message);
    }

    /// <summary>A throwing callback is logged and swallowed; the rest of the drained batch still completes.</summary>
    [Fact]
    public async Task ThrowingCallback_DoesNotStopTheBatch()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1);

        RecordingExecutor executor = new();
        int delivered = 0;

        // Parked before Start, so one drain claims all of them: plain items, then one coalesced batch.
        for (int i = 0; i < 5; i++)
            scheduler.EnqueueTask(1, i, static v => v, (_, result, _) =>
            {
                Interlocked.Increment(ref delivered);
                if (result == 0)
                    throw new InvalidOperationException("callback");
            });

        for (int i = 0; i < 5; i++)
            scheduler.EnqueueBatchableTask(1, i, executor, i, (_, result, _) =>
            {
                Interlocked.Increment(ref delivered);
                if (result == 0)
                    throw new InvalidOperationException("callback");
            });

        Task<int> after = scheduler.EnqueueTask(1, 99, static v => v);

        scheduler.Start();

        Assert.Equal(99, await after.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal(10, Volatile.Read(ref delivered));
        Assert.Equal([0, 1, 2, 3, 4], Assert.Single(executor.Calls));
    }

    /// <summary>
    /// Callback and task batchable ops against one executor coalesce into one ExecuteBatch call, whatever
    /// the callback state type.
    /// </summary>
    [Fact]
    public async Task CallbackAndTaskBatchableOps_CoalesceIntoOneCall()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 1);

        RecordingExecutor executor = new();
        ConcurrentQueue<int> results = new();

        Task<int> first = scheduler.EnqueueBatchableTask(1, 1, executor);
        scheduler.EnqueueBatchableTask(1, 2, executor, results, static (r, result, _) => r.Enqueue(result));
        scheduler.EnqueueBatchableTask(1, 3, executor, "state", (_, result, _) => results.Enqueue(result));
        Task<int> last = scheduler.EnqueueBatchableTask(1, 4, executor);

        scheduler.Start();

        Assert.Equal(10, await first.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal(40, await last.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.Equal([20, 30], results.OrderBy(v => v).ToArray());
        Assert.Equal([1, 2, 3, 4], Assert.Single(executor.Calls));
    }

    /// <summary>Stop runs the callback of every accepted operation, started scheduler or not.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Stop_RunsEveryAcceptedCallback(bool started)
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2);
        using ManualResetEventSlim release = new(false);

        if (started)
        {
            scheduler.Start();
            scheduler.EnqueueTask(1, 0, _ =>
            {
                release.Wait();
                return 0;
            }, static (_, _, _) => { });
        }

        int[] calls = new int[200];

        for (int i = 0; i < calls.Length; i++)
            scheduler.EnqueueTask(1 + i % 4, i, static v => v, (_, result, _) => Interlocked.Increment(ref calls[result]));

        Task stop = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);
        release.Set();

        await stop.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.All(calls, c => Assert.Equal(1, c));
    }

    [Fact]
    public async Task DefaultInterfaceCallback_DeliversResultAndError()
    {
        IRaftReadScheduler scheduler = new StubScheduler();

        CallbackProbe<int> ok = new();
        scheduler.EnqueueTask(1, ok, static _ => 5, static (p, result, error) => p.Record(result, error));

        CallbackProbe<int> failed = new();
        scheduler.EnqueueTask<CallbackProbe<int>, int>(1, failed, static _ => throw new InvalidOperationException("x"), static (p, result, error) => p.Record(result, error));

        await Task.WhenAll(ok.Done.Task, failed.Done.Task).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(5, ok.Result);
        Assert.Null(ok.Error);
        Assert.IsType<InvalidOperationException>(failed.Error);
    }

    // ── Meters ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The meters count one operation per enqueue on each path, tagged with the instance name, and the
    /// hand-off histogram records one sample per operation.
    /// </summary>
    [Fact]
    public async Task Meters_CountPerPathAndInstance()
    {
        string name = "meters-" + Guid.NewGuid().ToString("N");
        ConcurrentDictionary<string, long> counts = new();

        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName && instrument.Name.StartsWith("raft.read_scheduler.", StringComparison.Ordinal))
                l.EnableMeasurementEvents(instrument);
        };

        void OnMeasurement<TValue>(Instrument instrument, TValue value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            string? scheduler = null, path = null;
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "scheduler") scheduler = tag.Value as string;
                if (tag.Key == "path") path = tag.Value as string;
            }

            if (scheduler != name)
                return;

            string key = path is null ? instrument.Name : $"{instrument.Name}/{path}";
            long amount = instrument is Counter<long> ? Convert.ToInt64(value) : 1;
            counts.AddOrUpdate(key, amount, (_, current) => current + amount);
        }

        listener.SetMeasurementEventCallback<long>(OnMeasurement);
        listener.SetMeasurementEventCallback<double>(OnMeasurement);
        listener.SetMeasurementEventCallback<int>(OnMeasurement);
        listener.Start();

        using FairReadScheduler scheduler = NewScheduler(workers: 1, spin: TimeSpan.Zero, name: name);
        scheduler.Start();

        for (int i = 0; i < 5; i++)
            await scheduler.EnqueueTask(1, i, static v => v, ReadExecutionHint.InlineWhenIdle);

        for (int i = 0; i < 7; i++)
        {
            await WaitUntil(() => scheduler.IdleWorkerCount == 1, "worker parked");
            await scheduler.EnqueueTask(1, i, static v => v);
        }

        await WaitUntil(() => scheduler.IdleWorkerCount == 1, "worker parked");

        Assert.Equal(5, counts.GetValueOrDefault("raft.read_scheduler.operations_total/inline"));
        Assert.Equal(7, counts.GetValueOrDefault("raft.read_scheduler.operations_total/worker"));
        Assert.Equal(7, counts.GetValueOrDefault("raft.read_scheduler.wakeups_total"));
        Assert.Equal(0, counts.GetValueOrDefault("raft.read_scheduler.empty_wakeups_total"));
        Assert.Equal(5, counts.GetValueOrDefault("raft.read_scheduler.handoff_latency_us/inline"));
        Assert.Equal(7, counts.GetValueOrDefault("raft.read_scheduler.handoff_latency_us/worker"));
        Assert.Equal(12, counts.GetValueOrDefault("raft.read_scheduler.run_duration_us/inline") + counts.GetValueOrDefault("raft.read_scheduler.run_duration_us/worker"));

        Assert.Equal(5, scheduler.InlineReadsCompleted);
        Assert.Equal(7, scheduler.WorkerReadsCompleted);
        Assert.Equal(12, scheduler.TotalReadsCompleted);
        Assert.Equal(7, scheduler.WorkerWakeups);
    }

    /// <summary>
    /// A read's count is visible as soon as its task completes: the counter moves before the completion
    /// is published, so a caller resuming on another thread cannot read a stale value.
    /// </summary>
    [Fact]
    public async Task TotalReadsCompleted_VisibleWhenTaskCompletes()
    {
        using FairReadScheduler scheduler = NewScheduler(workers: 2);
        scheduler.Start();

        for (int i = 1; i <= 500; i++)
        {
            await scheduler.EnqueueTask(1, i, static v => v);
            Assert.Equal(i, scheduler.TotalReadsCompleted);
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private sealed class CallbackProbe<T>
    {
        public readonly TaskCompletionSource Done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Calls;
        public T? Result;
        public Exception? Error;
        public int CallbackThread;
        public int OperationThread;

        public void Record(T? result, Exception? error)
        {
            Interlocked.Increment(ref Calls);
            Result = result;
            Error = error;
            CallbackThread = Environment.CurrentManagedThreadId;
            Done.TrySetResult();
        }
    }

    private sealed class RecordingExecutor : IReadBatchExecutor<int, int>
    {
        public readonly ConcurrentQueue<int[]> Calls = new();
        public readonly ConcurrentQueue<int> Threads = new();

        public int[] ExecuteBatch(int[] args)
        {
            Calls.Enqueue((int[])args.Clone());
            Threads.Enqueue(Environment.CurrentManagedThreadId);

            int[] results = new int[args.Length];
            for (int i = 0; i < args.Length; i++)
                results[i] = args[i] * 10;

            return results;
        }
    }

    /// <summary>Implements only the abstract member, so the callback overloads use the interface defaults.</summary>
    private sealed class StubScheduler : IRaftReadScheduler
    {
        public Task<T> EnqueueTask<T>(int partitionId, Func<T> operation)
        {
            try
            {
                return Task.FromResult(operation());
            }
            catch (Exception ex)
            {
                return Task.FromException<T>(ex);
            }
        }
    }
}
