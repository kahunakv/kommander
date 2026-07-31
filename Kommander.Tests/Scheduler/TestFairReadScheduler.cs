using System.Collections.Concurrent;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Acceptance tests for <see cref="FairReadScheduler"/>.
///
/// Covers:
/// - Same-partition reads complete in submitted order (FIFO).
/// - Cross-partition reads can run concurrently on multiple workers.
/// - No partition is starved under concurrent load.
/// - Shutdown does not drop accepted operations.
/// - Exceptions from the delegate fault the returned Task, not the worker.
/// - Back-pressure throws when per-partition limit is reached.
/// </summary>
public sealed class TestFairReadScheduler
{
    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Operations submitted for the same partition must complete in the order
    /// they were enqueued (FIFO).
    /// </summary>
    [Fact]
    public async Task SamePartition_ReadsCompleteInSubmittedOrder()
    {
        const int opCount = 50;
        ConcurrentQueue<int> completionOrder = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        List<Task<int>> tasks = new(opCount);
        for (int i = 1; i <= opCount; i++)
        {
            int captured = i;
            tasks.Add(scheduler.EnqueueTask(1, () =>
            {
                completionOrder.Enqueue(captured);
                return captured;
            }));
        }

        int[] results = await Task.WhenAll(tasks);

        Assert.Equal(opCount, results.Length);

        // FIFO: each result at index i should equal i+1.
        for (int i = 0; i < results.Length; i++)
            Assert.Equal(i + 1, results[i]);

        // Completion order (as observed inside the worker) should also be sequential.
        int[] order = completionOrder.ToArray();
        for (int i = 0; i < order.Length - 1; i++)
            Assert.True(order[i] < order[i + 1], $"FIFO violated at index {i}: {order[i]} followed by {order[i + 1]}");
    }

    /// <summary>
    /// Operations enqueued BEFORE <see cref="FairReadScheduler.Start"/> must be parked
    /// and then served, in submission order, once the workers spin up — not rejected.
    /// Regression for the startup race where a consumer reads before
    /// <c>RaftManager.JoinCluster</c> has called Start().
    /// </summary>
    [Fact]
    public async Task EnqueueBeforeStart_ParkedThenServedInOrder()
    {
        const int opCount = 50;
        ConcurrentQueue<int> completionOrder = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);

        // Enqueue everything BEFORE Start(): must not throw, must park.
        List<Task<int>> tasks = new(opCount);
        for (int i = 1; i <= opCount; i++)
        {
            int captured = i;
            tasks.Add(scheduler.EnqueueTask(1, () =>
            {
                completionOrder.Enqueue(captured);
                return captured;
            }));
        }

        // Nothing should have run yet — no workers exist.
        Assert.All(tasks, t => Assert.False(t.IsCompleted));

        scheduler.Start();

        int[] results = await Task.WhenAll(tasks);

        Assert.Equal(opCount, results.Length);
        for (int i = 0; i < results.Length; i++)
            Assert.Equal(i + 1, results[i]);

        // FIFO preserved across the Start() boundary.
        int[] order = completionOrder.ToArray();
        for (int i = 0; i < order.Length - 1; i++)
            Assert.True(order[i] < order[i + 1], $"FIFO violated at index {i}: {order[i]} followed by {order[i + 1]}");
    }

    /// <summary>
    /// If operations are parked before Start() and the scheduler is stopped without ever
    /// starting, <see cref="FairReadScheduler.Stop"/> must drain them so their awaiters
    /// complete rather than hang forever.
    /// </summary>
    [Fact]
    public async Task StopWithoutStart_DrainsParkedOperations()
    {
        const int opCount = 20;
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 2);

        List<Task<int>> tasks = new(opCount);
        for (int i = 1; i <= opCount; i++)
        {
            int seq = i;
            tasks.Add(scheduler.EnqueueTask(i % 3, () => seq));
        }

        // Stop() without a prior Start() must still complete every parked operation.
        await Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        Assert.All(tasks, t => Assert.True(t.IsCompleted, "A parked operation was not completed by Stop()."));

        int[] results = await Task.WhenAll(tasks);
        Assert.Equal(opCount, results.Length);
    }

    /// <summary>
    /// Operations from different partitions can run concurrently on separate
    /// workers, so all tasks complete and each partition's results are correct.
    /// </summary>
    [Fact]
    public async Task CrossPartition_ReadsRunConcurrently()
    {
        const int opCount = 30;
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 2);
        scheduler.Start();

        List<Task<(int Partition, int Seq)>> tasks = new(opCount * 2);

        for (int i = 1; i <= opCount; i++)
        {
            int seq = i;
            tasks.Add(scheduler.EnqueueTask(1, () => (1, seq)));
            tasks.Add(scheduler.EnqueueTask(2, () => (2, seq)));
        }

        (int Partition, int Seq)[] results = await Task.WhenAll(tasks);

        Assert.Equal(opCount * 2, results.Length);
        Assert.Contains(results, r => r.Partition == 1);
        Assert.Contains(results, r => r.Partition == 2);

        // Per-partition FIFO: sequence numbers from each partition should be ascending.
        int[] p1 = results.Where(r => r.Partition == 1).Select(r => r.Seq).ToArray();
        int[] p2 = results.Where(r => r.Partition == 2).Select(r => r.Seq).ToArray();

        for (int i = 0; i < p1.Length - 1; i++)
            Assert.True(p1[i] < p1[i + 1], $"P1 FIFO violated at index {i}");
        for (int i = 0; i < p2.Length - 1; i++)
            Assert.True(p2[i] < p2[i + 1], $"P2 FIFO violated at index {i}");
    }

    /// <summary>
    /// With many active partitions posting work simultaneously, every partition
    /// must eventually receive all its results — no partition is starved.
    /// </summary>
    [Fact]
    public async Task NoStarvation_AllPartitionsEventuallyComplete()
    {
        const int partitions = 8;
        const int opsPerPartition = 25;

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 4);
        scheduler.Start();

        List<Task<(int Partition, int Seq)>> tasks = new(partitions * opsPerPartition);

        for (int p = 1; p <= partitions; p++)
        {
            for (int i = 1; i <= opsPerPartition; i++)
            {
                int partition = p, seq = i;
                tasks.Add(scheduler.EnqueueTask(partition, () => (partition, seq)));
            }
        }

        (int Partition, int Seq)[] results = await Task.WhenAll(tasks);

        Assert.Equal(partitions * opsPerPartition, results.Length);

        // Every partition should have all its results.
        for (int p = 1; p <= partitions; p++)
        {
            int count = results.Count(r => r.Partition == p);
            Assert.Equal(opsPerPartition, count);
        }
    }

    /// <summary>
    /// Operations accepted before <see cref="FairReadScheduler.Stop"/> is called
    /// must all complete — none are silently dropped.
    /// </summary>
    [Fact]
    public async Task Shutdown_DoesNotDropAcceptedOperations()
    {
        const int opCount = 200;
        const int partitions = 4;

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 2);
        scheduler.Start();

        List<Task<int>> tasks = new(opCount * partitions);

        for (int p = 1; p <= partitions; p++)
        {
            for (int i = 1; i <= opCount; i++)
            {
                int seq = i;
                tasks.Add(scheduler.EnqueueTask(p, () => seq));
            }
        }

        // Stop after enqueueing all operations; workers must drain.
        await Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        // All tasks must have completed (not cancelled, not still pending).
        bool allDone = tasks.All(t => t.IsCompleted);
        Assert.True(allDone, $"Some tasks were not completed after Stop().");

        int[] results = await Task.WhenAll(tasks);
        Assert.Equal(opCount * partitions, results.Length);
    }

    /// <summary>
    /// When the delegate throws, the returned <see cref="Task{T}"/> faults with
    /// that exception; the worker thread must survive and process subsequent items.
    /// </summary>
    [Fact]
    public async Task DelegateException_FaultsTaskWithoutKillingWorker()
    {
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        // First operation: throws.
        Task<int> faulted = scheduler.EnqueueTask<int>(1, () => throw new InvalidOperationException("oops"));

        // Second operation: should still execute after the faulted one.
        Task<int> ok = scheduler.EnqueueTask(1, () => 42);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await faulted);
        int result = await ok;
        Assert.Equal(42, result);
    }

    /// <summary>
    /// <see cref="FairReadScheduler.EnqueueTask{T}"/> must throw
    /// <see cref="ReadBackpressureExceededException"/> when the per-partition
    /// depth limit is exceeded.
    /// </summary>
    [Fact]
    public void Backpressure_ThrowsWhenQueueFull()
    {
        const int maxDepth = 4;

        // A blocking operation that never returns until we unblock.
        ManualResetEventSlim gate = new(false);

        using FairReadScheduler scheduler = new(
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxQueueDepthPerPartition: maxDepth);
        scheduler.Start();

        // Fill the queue up to the limit.
        List<Task<int>> pending = [];
        for (int i = 1; i <= maxDepth; i++)
        {
            pending.Add(scheduler.EnqueueTask(1, () =>
            {
                gate.Wait();
                return 0;
            }));
        }

        // One more should trip the back-pressure limit.
        ReadBackpressureExceededException ex = Assert.Throws<ReadBackpressureExceededException>(() =>
        {
            _ = scheduler.EnqueueTask(1, () => 99);
        });

        Assert.Equal(1, ex.PartitionId);

        // Unblock so workers can drain and the scheduler can stop cleanly.
        gate.Set();
        scheduler.Stop();
    }

    // ── Regression: reschedule stranding & concurrent drain ────────────────

    /// <summary>
    /// Regression for the reschedule-stranding liveness bug: a partition that already has
    /// accepted work must eventually be re-represented in the ready queue on its own —
    /// without relying on a *future* enqueue for that partition. We stall many distinct
    /// partitions in-flight to push the ready queue well past its old bounded capacity,
    /// enqueue a *second* op on an already-in-flight partition, then release everything.
    /// The follow-up op must complete even though nothing further is ever enqueued for it.
    /// </summary>
    [Fact]
    public async Task Reschedule_CompletesWithoutFutureEnqueue_UnderReadyQueuePressure()
    {
        // With the old bound (workerCount * 64), 4 workers capped distinct partitions at
        // 256; exceeding that stranded rescheduled work. Use well over that.
        const int partitions = 600;
        const int workerCount = 4;

        // Gate that holds every "first" op in flight so their partitions saturate the queue.
        ManualResetEventSlim release = new(false);

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount);
        scheduler.Start();

        // One stalled op per partition -> up to `partitions` distinct ids scheduled at once.
        List<Task<int>> firstOps = new(partitions);
        for (int p = 1; p <= partitions; p++)
        {
            firstOps.Add(scheduler.EnqueueTask(p, () =>
            {
                release.Wait();
                return p;
            }));
        }

        // Second op on an already-busy partition. It is appended to state.Ops while that
        // partition is InFlight, so it only reaches the ready queue via the post-batch
        // reschedule — the exact path that used to strand under a full queue.
        const int busyPartition = 1;
        Task<int> followUp = scheduler.EnqueueTask(busyPartition, () => 12345);

        // Follow-up must NOT be complete yet (its partition is stalled in flight).
        Assert.False(followUp.IsCompleted);

        // Release all first ops; the reschedule must carry the follow-up to completion
        // with no further enqueue for `busyPartition`.
        release.Set();

        int[] firstResults = await Task.WhenAll(firstOps).WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(partitions, firstResults.Length);

        int followResult = await followUp.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(12345, followResult);

        scheduler.Stop();
    }

    /// <summary>
    /// Regression for the concurrent-drain TOCTOU: stopping a scheduler with several workers
    /// and an unscheduled partition holding multiple batches must never let two draining
    /// workers execute that partition's ops concurrently, and must preserve FIFO order.
    /// A per-partition concurrency probe asserts max concurrency == 1; the completion order
    /// asserts FIFO.
    /// </summary>
    [Fact]
    public async Task Stop_DrainsSinglePartition_WithoutConcurrentExecution_AndInOrder()
    {
        // Several distinct partitions, each staged with more than one batch of work, all
        // parked before Start() so every partition begins Scheduled=false. Stop() must drain
        // them via the atomic-claim path.
        const int partitions = 6;
        const int opsPerPartition = 200; // > MaxBatchSize (64) -> multiple batches each
        const int workerCount = 4;

        int[] concurrent = new int[partitions + 1];
        int[] maxConcurrent = new int[partitions + 1];
        ConcurrentDictionary<int, ConcurrentQueue<int>> order = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount);

        List<Task<int>> tasks = new(partitions * opsPerPartition);
        for (int p = 1; p <= partitions; p++)
        {
            order[p] = new ConcurrentQueue<int>();
            for (int i = 1; i <= opsPerPartition; i++)
            {
                int partition = p, seq = i;
                tasks.Add(scheduler.EnqueueTask(partition, () =>
                {
                    int now = Interlocked.Increment(ref concurrent[partition]);
                    // Track the observed maximum concurrency for this partition.
                    int prevMax;
                    do { prevMax = Volatile.Read(ref maxConcurrent[partition]); }
                    while (now > prevMax &&
                           Interlocked.CompareExchange(ref maxConcurrent[partition], now, prevMax) != prevMax);

                    order[partition].Enqueue(seq);
                    Thread.SpinWait(50); // widen the window for a concurrency violation to surface
                    Interlocked.Decrement(ref concurrent[partition]);
                    return seq;
                }));
            }
        }

        // Start then immediately Stop, racing the drain against normal worker dispatch.
        scheduler.Start();
        await Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        Assert.All(tasks, t => Assert.True(t.IsCompleted, "A staged operation was not completed by Stop()."));
        await Task.WhenAll(tasks);

        for (int p = 1; p <= partitions; p++)
        {
            Assert.True(maxConcurrent[p] <= 1,
                $"Partition {p} executed with concurrency {maxConcurrent[p]} (single-worker-per-partition violated).");

            int[] seen = order[p].ToArray();
            Assert.Equal(opsPerPartition, seen.Length);
            for (int i = 0; i < seen.Length - 1; i++)
                Assert.True(seen[i] < seen[i + 1], $"P{p} FIFO violated at index {i}: {seen[i]} then {seen[i + 1]}");
        }
    }

    /// <summary>
    /// Racing <see cref="FairReadScheduler.Stop"/> against continued enqueue pressure: every
    /// task that was <em>accepted</em> (EnqueueTask returned without throwing) must complete
    /// or fault — none may remain pending after Stop() returns. Enqueues that arrive after
    /// _stopping is observed are rejected with <see cref="InvalidOperationException"/> and do
    /// not count as accepted.
    /// </summary>
    [Fact]
    public async Task Stop_RacingEnqueuePressure_CompletesEveryAcceptedTask()
    {
        const int workerCount = 3;
        const int partitions = 32;

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount);
        scheduler.Start();

        ConcurrentBag<Task<int>> accepted = new();
        CancellationTokenSource producerStop = new();

        // Producers hammer EnqueueTask across many partitions until Stop() rejects them.
        Task[] producers = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            int seq = 0;
            while (!producerStop.IsCancellationRequested)
            {
                try
                {
                    int partition = (seq % partitions) + 1;
                    int value = seq++;
                    accepted.Add(scheduler.EnqueueTask(partition, () => value));
                }
                catch (InvalidOperationException)
                {
                    break; // scheduler is stopping; enqueue was rejected (not accepted).
                }
                catch (ReadBackpressureExceededException)
                {
                    // depth limit hit for this partition; keep producing on others.
                }
            }
        })).ToArray();

        // Let some work accumulate, then stop while producers are still pushing.
        await Task.Delay(50, TestContext.Current.CancellationToken);
        scheduler.Stop();
        producerStop.Cancel();
        await Task.WhenAll(producers);

        Task<int>[] all = accepted.ToArray();
        Assert.All(all, t => Assert.True(t.IsCompleted,
            "An accepted task was still pending after Stop() returned."));

        // Every accepted task completed (none faulted for these pure delegates).
        await Task.WhenAll(all);
    }

    /// <summary>
    /// Deterministic regression for the admission/shutdown lifecycle race: an
    /// <see cref="FairReadScheduler.EnqueueTask{T}"/> that has passed the stopping check but
    /// not yet published its work must not be stranded by a concurrent
    /// <see cref="FairReadScheduler.Stop"/>. We hold one admission in flight (via the
    /// <c>OnAfterAdmissionCheck</c> seam), call Stop() on another thread, and assert Stop
    /// cannot complete while the admission is parked; once released, the accepted task must
    /// complete. On the pre-fix code (stopping checked outside any lock) Stop() would drain
    /// and join before the parked admission published, and the task would hang forever.
    /// </summary>
    [Fact]
    public async Task Stop_BlocksUntilInFlightAdmissionPublishes_ThenTaskCompletes()
    {
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 2);
        scheduler.Start();

        ManualResetEventSlim admissionEntered = new(false);
        ManualResetEventSlim releaseAdmission = new(false);

        // The next EnqueueTask will signal that it is past the stopping check (holding the
        // admission read lock) and then block until the test releases it.
        scheduler.OnAfterAdmissionCheck = () =>
        {
            admissionEntered.Set();
            releaseAdmission.Wait();
        };

        // Kick off the admission on a background thread; it parks inside the seam.
        Task<int> admitted = Task.Run(() => scheduler.EnqueueTask(7, () => 777), TestContext.Current.CancellationToken);

        try
        {
            Assert.True(admissionEntered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken), "Admission never reached the seam.");

            // Clear the seam so Stop()'s own drain path (and nothing else) is unaffected.
            scheduler.OnAfterAdmissionCheck = null;

            // Stop() on another thread must BLOCK on the lifecycle write lock while the
            // admission holds the read lock.
            Task stop = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

            // Give Stop() a chance to run; it must not complete while the admission is parked.
            await Task.WhenAny(stop, Task.Delay(TimeSpan.FromMilliseconds(300), TestContext.Current.CancellationToken));
            Assert.False(stop.IsCompleted, "Stop() completed while an admission was still in flight.");
            Assert.False(admitted.IsCompleted, "Task completed before its work was even published.");

            // Release the admission: it publishes its work, Stop() proceeds to drain it.
            releaseAdmission.Set();

            await stop.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            int result = await admitted.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.Equal(777, result);
        }
        finally
        {
            // Always release the gate so a failed assertion cannot leave the admission thread
            // parked (holding the read lock) and deadlock `using` disposal / test cleanup.
            releaseAdmission.Set();
        }
    }

    /// <summary>
    /// Deterministic regression for the concurrent-Stop lifecycle bug: when two callers race
    /// <see cref="FairReadScheduler.Stop"/>, the second (non-owning) caller must not return
    /// until the owner has fully drained and joined every worker. Otherwise a caller that
    /// treats a returned Stop() as "safe to dispose the backend" could tear down storage
    /// while a worker is still executing I/O. We stall the owner's drain on an in-flight read
    /// and assert the second Stop() stays blocked until that read completes.
    /// </summary>
    [Fact]
    public async Task ConcurrentStop_SecondCallerWaitsForOwnerToFinishDraining()
    {
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 2);
        scheduler.Start();

        ManualResetEventSlim readStarted = new(false);
        ManualResetEventSlim releaseRead = new(false);
        int readCompleted = 0;

        // A single in-flight read that blocks the drain until the test releases it.
        Task<int> blocked = scheduler.EnqueueTask(3, () =>
        {
            readStarted.Set();
            releaseRead.Wait();
            Interlocked.Exchange(ref readCompleted, 1);
            return 99;
        });

        try
        {
            Assert.True(readStarted.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken), "Blocking read never started.");

            // Two concurrent Stop() callers. Exactly one owns the drain/join; the other must
            // block until the owner is done.
            Task stopA = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);
            Task stopB = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

            // Neither may complete while the read (and thus the owner's join) is still in
            // flight — the non-owner must not sneak out early.
            await Task.WhenAny(Task.WhenAll(stopA, stopB), Task.Delay(TimeSpan.FromMilliseconds(300), TestContext.Current.CancellationToken));
            Assert.False(stopA.IsCompleted, "A Stop() caller returned before the owner finished draining.");
            Assert.False(stopB.IsCompleted, "A Stop() caller returned before the owner finished draining.");
            Assert.Equal(0, Volatile.Read(ref readCompleted));

            // Let the read finish; both Stop() callers must now return.
            releaseRead.Set();

            await Task.WhenAll(stopA, stopB).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // Both returned only after the worker actually finished the read.
            Assert.Equal(1, Volatile.Read(ref readCompleted));
            Assert.Equal(99, await blocked.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        }
        finally
        {
            releaseRead.Set();
        }
    }

    /// <summary>
    /// End-to-end fairness/liveness with worker counts &gt; 1 and enough distinct partitions
    /// to exceed the old ready-queue capacity: all accepted tasks complete and each
    /// partition runs with concurrency exactly one throughout.
    /// </summary>
    [Fact]
    public async Task ManyPartitions_ExceedingReadyCapacity_AllComplete_NoConcurrentPartition()
    {
        const int workerCount = 4;
        const int partitions = 500; // > workerCount * 64 (256)
        const int opsPerPartition = 6;

        int[] concurrent = new int[partitions + 1];
        int[] maxConcurrent = new int[partitions + 1];

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount);
        scheduler.Start();

        List<Task<int>> tasks = new(partitions * opsPerPartition);
        for (int p = 1; p <= partitions; p++)
        {
            for (int i = 1; i <= opsPerPartition; i++)
            {
                int partition = p, seq = i;
                tasks.Add(scheduler.EnqueueTask(partition, () =>
                {
                    int now = Interlocked.Increment(ref concurrent[partition]);
                    int prevMax;
                    do { prevMax = Volatile.Read(ref maxConcurrent[partition]); }
                    while (now > prevMax &&
                           Interlocked.CompareExchange(ref maxConcurrent[partition], now, prevMax) != prevMax);

                    Thread.SpinWait(20);
                    Interlocked.Decrement(ref concurrent[partition]);
                    return seq;
                }));
            }
        }

        int[] results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
        Assert.Equal(partitions * opsPerPartition, results.Length);

        for (int p = 1; p <= partitions; p++)
            Assert.True(maxConcurrent[p] <= 1,
                $"Partition {p} executed with concurrency {maxConcurrent[p]} (single-worker-per-partition violated).");

        scheduler.Stop();
    }
}
