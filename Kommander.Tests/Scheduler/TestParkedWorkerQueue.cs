#if !KOMMANDER_THREAD_FREE
// The thread-free build has no worker threads: the queue's worker side (Take, TryTake) is not compiled
// there, and CI builds this project in that configuration too.
using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Scheduling;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Tests for <see cref="ParkedWorkerQueue{T}"/>, the ready queue and idle-worker handshake shared by
/// <see cref="RaftExecutorPool"/> and <c>FairWalScheduler</c>: no lost wake, one wake per item, the
/// hand-on of skipped wakes, the put-back that wakes nobody, the timed wait, stop, and the adaptive
/// spin window.
///
/// <para>Tests that count wake-ups build the queue with a zero spin bound, so every hand-off is a
/// park and a wake and the counts are exact.</para>
/// </summary>
public sealed class TestParkedWorkerQueue
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>Worker threads over one queue, each running <c>Take</c> in a loop until stop.</summary>
    private sealed class Workers<T> : IDisposable
    {
        private readonly Thread[] threads;

        internal ParkedWorkerQueue<T> Queue { get; }

        internal Workers(int count, TimeSpan? spin, Action<T> handle, int extraSlots = 0)
        {
            Queue = new ParkedWorkerQueue<T>(count + extraSlots, spin);
            threads = new Thread[count];

            for (int i = 0; i < count; i++)
            {
                int worker = i;
                threads[i] = new Thread(() =>
                {
                    while (Queue.Take(worker, handOn: true, out T? item))
                        handle(item);
                })
                {
                    IsBackground = true,
                    Name = $"TestParkedWorker-{i}",
                };
                threads[i].Start();
            }
        }

        internal bool StopAndJoin(TimeSpan timeout)
        {
            Queue.RequestStop();

            foreach (Thread thread in threads)
            {
                if (!thread.Join(timeout))
                    return false;
            }

            return true;
        }

        public void Dispose()
        {
            StopAndJoin(Timeout);
            Queue.Dispose();
        }
    }

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

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    // ── Handshake ───────────────────────────────────────────────────────────

    /// <summary>
    /// The lost-wake race: an item published between a worker's last empty dequeue and its park must be
    /// taken. Awaiting each item before publishing the next puts every enqueue right where the worker is
    /// heading to sleep. A lost wake has no safety net (the park has no timeout), so it shows as a hang.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(20)]
    public async Task LostWakeRace_EveryItemIsTaken(int spinMicroseconds)
    {
        using Workers<TaskCompletionSource> workers = new(2, TimeSpan.FromMicroseconds(spinMicroseconds), static item => item.TrySetResult());

        Task loop = Task.Run(async () =>
        {
            for (int i = 0; i < 10_000; i++)
            {
                // Vary the arrival point inside the worker's take-to-park window.
                Thread.SpinWait(i % 64);

                TaskCompletionSource item = NewSignal();
                workers.Queue.Enqueue(item);
                await item.Task;
            }
        }, TestContext.Current.CancellationToken);

        await loop.WaitAsync(Timeout, TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Many producers, many workers, random pacing: stresses the idle stack's compare-exchange (slots
    /// are reused constantly, the ABA case) and the hand-on of the wakes producers skip while a worker
    /// spins.
    /// </summary>
    [Fact]
    public async Task ManyProducers_ManyWorkers_AllTaken()
    {
        long taken = 0;
        using Workers<int> workers = new(8, null, value => Interlocked.Add(ref taken, value));

        const int producers = 8;
        const int perProducer = 20_000;

        Task[] tasks = new Task[producers];
        for (int p = 0; p < producers; p++)
        {
            int producer = p;
            tasks[p] = Task.Run(() =>
            {
                Random random = new(producer);
                for (int i = 0; i < perProducer; i++)
                {
                    if (random.Next(8) == 0)
                        Thread.SpinWait(random.Next(500));

                    workers.Queue.Enqueue(1);
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(tasks).WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await WaitUntil(() => Interlocked.Read(ref taken) == producers * perProducer, "every item taken");
    }

    /// <summary>One wake-up per item handed to a parked worker, and none of them finds the queue empty.</summary>
    [Fact]
    public async Task ParkedWorker_OneWakePerItem()
    {
        using Workers<TaskCompletionSource> workers = new(1, TimeSpan.Zero, static item => item.TrySetResult());

        for (int i = 0; i < 10; i++)
        {
            await WaitUntil(() => workers.Queue.IdleWorkerCount == 1, "worker parked");

            TaskCompletionSource item = NewSignal();
            workers.Queue.Enqueue(item);
            await item.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        }

        await WaitUntil(() => workers.Queue.IdleWorkerCount == 1, "worker parked");

        Assert.Equal(10, workers.Queue.Wakeups);
        Assert.Equal(0, workers.Queue.EmptyWakeups);
    }

    /// <summary>
    /// One enqueue wakes one worker: with four parked, three stay parked. The old semaphore and
    /// blocking collection did the same in count, but each waiter spun before parking again.
    /// </summary>
    [Fact]
    public async Task Enqueue_WakesExactlyOneParkedWorker()
    {
        using ManualResetEventSlim release = new(false);
        TaskCompletionSource started = NewSignal();

        using Workers<int> workers = new(4, TimeSpan.Zero, _ =>
        {
            started.TrySetResult();
            release.Wait();
        });

        await WaitUntil(() => workers.Queue.IdleWorkerCount == 4, "all workers parked");

        workers.Queue.Enqueue(1);
        await started.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Equal(3, workers.Queue.IdleWorkerCount);
        Assert.Equal(1, workers.Queue.Wakeups);

        release.Set();
    }

    /// <summary>
    /// A put-back from a worker that returns to the queue wakes no parked worker; the item waits for a
    /// worker to come to the queue.
    /// </summary>
    [Fact]
    public async Task EnqueueFromWorker_WakesNoParkedWorker()
    {
        TaskCompletionSource item = NewSignal();
        using Workers<TaskCompletionSource> workers = new(2, TimeSpan.Zero, static taken => taken.TrySetResult());

        await WaitUntil(() => workers.Queue.IdleWorkerCount == 2, "all workers parked");

        workers.Queue.EnqueueFromWorker(item);

        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.False(item.Task.IsCompleted, "A put-back woke a parked worker.");
        Assert.Equal(2, workers.Queue.IdleWorkerCount);
        Assert.Equal(0, workers.Queue.Wakeups);

        workers.Queue.WakeOne();
        await item.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// The hand-on: several items published with one wake still get one worker each. The first worker
    /// wakes a second after its take, the second a third. This is what makes it safe for a producer to
    /// skip its wake while a worker spins.
    /// </summary>
    [Fact]
    public async Task OneWakeForSeveralItems_IsHandedOnToAWorkerPerItem()
    {
        using ManualResetEventSlim release = new(false);
        ConcurrentDictionary<int, bool> threads = new();
        int inHandler = 0;

        using Workers<int> workers = new(3, TimeSpan.Zero, _ =>
        {
            threads[Environment.CurrentManagedThreadId] = true;
            Interlocked.Increment(ref inHandler);
            release.Wait();
        });

        await WaitUntil(() => workers.Queue.IdleWorkerCount == 3, "all workers parked");

        workers.Queue.Publish(1);
        workers.Queue.Publish(2);
        workers.Queue.Publish(3);
        workers.Queue.WakeOne();

        await WaitUntil(() => Volatile.Read(ref inHandler) == 3, "one worker per item, from a single wake");

        Assert.Equal(3, threads.Count);
        Assert.Equal(3, workers.Queue.Wakeups);

        release.Set();
    }

    // ── Stop ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RequestStop_ParkedWorkersLeave()
    {
        Workers<int> workers = new(4, TimeSpan.Zero, static _ => { });

        await WaitUntil(() => workers.Queue.IdleWorkerCount == 4, "all workers parked");

        bool joined = await Task.Run(() => workers.StopAndJoin(Timeout), TestContext.Current.CancellationToken);

        Assert.True(joined, "A parked worker did not leave after RequestStop.");
        Assert.True(workers.Queue.StopRequested);

        workers.Queue.Dispose();
    }

    /// <summary>
    /// After a stop request <c>Take</c> returns false whatever the queue holds: the remainder is the
    /// owner's to drain. The executor pool relies on that to exit, and the WAL scheduler to run its
    /// own shutdown drain.
    /// </summary>
    [Fact]
    public async Task RequestStop_LeavesQueuedItemsToTheOwner()
    {
        int handled = 0;
        Workers<int> workers = new(2, TimeSpan.Zero, _ => Interlocked.Increment(ref handled));

        await WaitUntil(() => workers.Queue.IdleWorkerCount == 2, "all workers parked");

        workers.Queue.Publish(7);

        bool joined = await Task.Run(() => workers.StopAndJoin(Timeout), TestContext.Current.CancellationToken);

        Assert.True(joined);
        Assert.Equal(0, Volatile.Read(ref handled));
        Assert.True(workers.Queue.TryDequeue(out int left));
        Assert.Equal(7, left);

        workers.Queue.Dispose();
    }

    // ── Timed wait ──────────────────────────────────────────────────────────

    [Fact]
    public void TryTake_NothingArrives_TimesOut()
    {
        using ParkedWorkerQueue<int> queue = new(1, TimeSpan.Zero);

        Stopwatch watch = Stopwatch.StartNew();
        bool taken = queue.TryTake(0, 50, out _, out bool timedOut);

        Assert.False(taken);
        Assert.True(timedOut);
        Assert.True(watch.ElapsedMilliseconds >= 40, $"Returned after {watch.ElapsedMilliseconds} ms.");
        Assert.Equal(0, queue.TimedWaiterCount);
    }

    [Fact]
    public void TryTake_ItemAlreadyQueued_ReturnsAtOnce()
    {
        using ParkedWorkerQueue<int> queue = new(1, TimeSpan.Zero);
        queue.Publish(5);

        Assert.True(queue.TryTake(0, 10_000, out int item, out bool timedOut));
        Assert.Equal(5, item);
        Assert.False(timedOut);
    }

    /// <summary>
    /// An arrival goes to the worker that waits for one, not to a parked worker: the timed waiter holds
    /// work and wants this item in the same batch. Both ways of publishing wake it, the put-back of
    /// another worker included, which wakes no parked worker.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TryTake_ArrivalWakesTheTimedWaiter_NotAParkedWorker(bool fromWorker)
    {
        int handledByParked = 0;

        // Slot 0 is a parked worker; slot 1 is the timed waiter below.
        using Workers<int> workers = new(1, TimeSpan.Zero, _ => Interlocked.Increment(ref handledByParked), extraSlots: 1);

        await WaitUntil(() => workers.Queue.IdleWorkerCount == 1, "worker parked");

        Task<(bool Taken, int Item, bool TimedOut, long Ms)> waiter = Task.Run(() =>
        {
            Stopwatch watch = Stopwatch.StartNew();
            bool taken = workers.Queue.TryTake(1, 20_000, out int item, out bool timedOut);
            return (taken, item, timedOut, watch.ElapsedMilliseconds);
        }, TestContext.Current.CancellationToken);

        await WaitUntil(() => workers.Queue.TimedWaiterCount == 1, "timed waiter registered");

        if (fromWorker)
            workers.Queue.EnqueueFromWorker(42);
        else
            workers.Queue.Enqueue(42);

        (bool taken, int item, bool timedOut, long ms) = await waiter.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.True(taken);
        Assert.Equal(42, item);
        Assert.False(timedOut);
        Assert.True(ms < 10_000, $"The timed waiter was not woken by the arrival ({ms} ms).");
        Assert.Equal(0, Volatile.Read(ref handledByParked));
        Assert.Equal(0, workers.Queue.Wakeups);
        Assert.Equal(0, workers.Queue.TimedWaiterCount);
    }

    [Fact]
    public async Task TryTake_RequestStop_EndsTheWait()
    {
        using ParkedWorkerQueue<int> queue = new(1, TimeSpan.Zero);

        Task<(bool Taken, bool TimedOut)> waiter = Task.Run(() =>
        {
            bool taken = queue.TryTake(0, 20_000, out _, out bool timedOut);
            return (taken, timedOut);
        }, TestContext.Current.CancellationToken);

        await WaitUntil(() => queue.TimedWaiterCount == 1, "timed waiter registered");

        queue.RequestStop();

        (bool taken, bool timedOut) = await waiter.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.False(taken);
        Assert.True(timedOut, "A stop request must read as the end of the wait, so the caller does not wait again.");
    }

    // ── Adaptive spin ───────────────────────────────────────────────────────

    /// <summary>
    /// A slow stream closes the spin window: each idle period's spin misses and halves it, so a node
    /// with one round every few milliseconds parks at once and pays no spin per hand-off.
    /// </summary>
    [Fact]
    public async Task SlowStream_ClosesSpinWindow()
    {
        using Workers<TaskCompletionSource> workers = new(1, TimeSpan.FromMicroseconds(20), static item => item.TrySetResult());

        Assert.True(workers.Queue.SpinWindowTicks > 0);

        for (int i = 0; i < 20; i++)
        {
            TaskCompletionSource item = NewSignal();
            workers.Queue.Enqueue(item);
            await item.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, workers.Queue.SpinWindowTicks);
    }

    /// <summary>
    /// A fast stream re-opens a closed window: a park that a producer ends sooner than the bound widens it.
    /// The bound is generous (5 ms) so a loaded test host still wakes the worker inside it.
    /// </summary>
    [Fact]
    public async Task FastStreamAfterSlow_ReopensSpinWindow()
    {
        using Workers<TaskCompletionSource> workers = new(1, TimeSpan.FromMilliseconds(5), static item => item.TrySetResult());

        // Close it: the 5 ms bound halves to zero after five misses.
        for (int i = 0; i < 8; i++)
        {
            TaskCompletionSource item = NewSignal();
            workers.Queue.Enqueue(item);
            await item.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
            await Task.Delay(40, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, workers.Queue.SpinWindowTicks);

        for (int i = 0; i < 200; i++)
        {
            TaskCompletionSource item = NewSignal();
            workers.Queue.Enqueue(item);
            await item.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        }

        Assert.True(workers.Queue.SpinWindowTicks > 0, "A stream of back-to-back items did not re-open the spin window.");
    }

    /// <summary>With a zero bound no worker ever spins: the window stays closed whatever the stream does.</summary>
    [Fact]
    public async Task ZeroSpinBound_NeverOpensTheWindow()
    {
        using Workers<TaskCompletionSource> workers = new(1, TimeSpan.Zero, static item => item.TrySetResult());

        for (int i = 0; i < 200; i++)
        {
            TaskCompletionSource item = NewSignal();
            workers.Queue.Enqueue(item);
            await item.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, workers.Queue.SpinWindowTicks);
    }
}
#endif
