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
}
