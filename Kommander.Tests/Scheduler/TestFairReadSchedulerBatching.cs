using System.Collections.Concurrent;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Acceptance tests for <see cref="FairReadScheduler.EnqueueBatchableTask{TArg,T}"/> —
/// coalescing of point reads into one <see cref="IReadBatchExecutor{TArg,T}.ExecuteBatch"/>
/// call per drained run.
///
/// Covers:
/// - A burst of same-executor ops drains as a single ExecuteBatch call with aligned results.
/// - Adjacency grouping: a plain EnqueueTask op interleaved between batchable ops splits the
///   run and total execution order is preserved (batch, plain, batch).
/// - Different executor instances never share a batch.
/// - A throwing executor faults every task in its group with the same exception and does not
///   kill the worker.
/// - A result array whose length mismatches the argument count faults the group.
/// - Backpressure applies to the batchable admission path.
/// - Stop() without Start() drains parked batchable ops.
/// - The IRaftReadScheduler default implementation forwards to EnqueueTask with a
///   single-element batch.
/// </summary>
public sealed class TestFairReadSchedulerBatching
{
    /// <summary>
    /// Records every ExecuteBatch invocation and maps each argument to arg * 10, so tests can
    /// assert both batch composition and index-aligned result delivery.
    /// </summary>
    private sealed class RecordingExecutor : IReadBatchExecutor<int, int>
    {
        public readonly ConcurrentQueue<int[]> Calls = new();

        public int[] ExecuteBatch(int[] args)
        {
            Calls.Enqueue((int[])args.Clone());

            int[] results = new int[args.Length];
            for (int i = 0; i < args.Length; i++)
                results[i] = args[i] * 10;

            return results;
        }
    }

    [Fact]
    public async Task ParkedBurst_CoalescesIntoSingleExecuteBatchCall()
    {
        const int opCount = 10;
        RecordingExecutor executor = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);

        // Park all ops before Start(): they sit in one partition queue, so the first drain
        // cycle (MaxBatchSize 64 > 10) claims them all in one batch — deterministically one
        // ExecuteBatch call.
        List<Task<int>> tasks = new(opCount);
        for (int i = 1; i <= opCount; i++)
            tasks.Add(scheduler.EnqueueBatchableTask(1, i, executor));

        scheduler.Start();

        int[] results = await Task.WhenAll(tasks);

        for (int i = 0; i < opCount; i++)
            Assert.Equal((i + 1) * 10, results[i]);

        int[][] calls = executor.Calls.ToArray();
        Assert.Single(calls);
        Assert.Equal(Enumerable.Range(1, opCount).ToArray(), calls[0]);

        Assert.Equal(opCount, scheduler.TotalReadsCompleted);
    }

    [Fact]
    public async Task PlainOpBetweenBatchableOps_SplitsRunAndPreservesOrder()
    {
        RecordingExecutor executor = new();
        ConcurrentQueue<string> events = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);

        // Parked submission order: B(1), B(2), plain, B(3). Adjacency grouping must yield
        // ExecuteBatch([1,2]) → plain → ExecuteBatch([3]) — the plain op is never reordered
        // relative to the batchable ops around it.
        Task<int> b1 = scheduler.EnqueueBatchableTask(1, 1, executor);
        Task<int> b2 = scheduler.EnqueueBatchableTask(1, 2, executor);
        Task<int> plain = scheduler.EnqueueTask(1, () =>
        {
            events.Enqueue("plain");
            return -1;
        });
        Task<int> b3 = scheduler.EnqueueBatchableTask(1, 3, executor);

        scheduler.Start();

        await Task.WhenAll(b1, b2, plain, b3);

        Assert.Equal(10, await b1);
        Assert.Equal(20, await b2);
        Assert.Equal(-1, await plain);
        Assert.Equal(30, await b3);

        int[][] calls = executor.Calls.ToArray();
        Assert.Equal(2, calls.Length);
        Assert.Equal([1, 2], calls[0]);
        Assert.Equal([3], calls[1]);

        // The plain op ran between the two batches: the first batch had already been recorded
        // when it executed, the second had not.
        Assert.Single(events);
    }

    [Fact]
    public async Task DistinctExecutorInstances_NeverShareABatch()
    {
        RecordingExecutor executorA = new();
        RecordingExecutor executorB = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);

        // Parked order: A(1), A(2), B(3), A(4). Grouping is by executor reference identity,
        // so the run splits at B even though every op is batchable.
        Task<int> a1 = scheduler.EnqueueBatchableTask(1, 1, executorA);
        Task<int> a2 = scheduler.EnqueueBatchableTask(1, 2, executorA);
        Task<int> b3 = scheduler.EnqueueBatchableTask(1, 3, executorB);
        Task<int> a4 = scheduler.EnqueueBatchableTask(1, 4, executorA);

        scheduler.Start();

        int[] results = await Task.WhenAll(a1, a2, b3, a4);
        Assert.Equal([10, 20, 30, 40], results);

        int[][] callsA = executorA.Calls.ToArray();
        Assert.Equal(2, callsA.Length);
        Assert.Equal([1, 2], callsA[0]);
        Assert.Equal([4], callsA[1]);

        int[][] callsB = executorB.Calls.ToArray();
        Assert.Single(callsB);
        Assert.Equal([3], callsB[0]);
    }

    private sealed class ThrowingExecutor : IReadBatchExecutor<int, int>
    {
        public int[] ExecuteBatch(int[] args)
        {
            throw new InvalidOperationException("backend read failed");
        }
    }

    [Fact]
    public async Task ExecutorException_FaultsEveryTaskInGroup_WorkerSurvives()
    {
        ThrowingExecutor throwing = new();
        RecordingExecutor healthy = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);

        Task<int> t1 = scheduler.EnqueueBatchableTask(1, 1, throwing);
        Task<int> t2 = scheduler.EnqueueBatchableTask(1, 2, throwing);

        scheduler.Start();

        InvalidOperationException ex1 = await Assert.ThrowsAsync<InvalidOperationException>(() => t1);
        InvalidOperationException ex2 = await Assert.ThrowsAsync<InvalidOperationException>(() => t2);

        Assert.Equal("backend read failed", ex1.Message);

        // Every op in the group faults with the SAME exception instance — the batch call is the
        // shared failure, equivalent to each individual read failing.
        Assert.Same(ex1, ex2);

        // The worker survived: a subsequent batchable op on the same partition still completes.
        Assert.Equal(50, await scheduler.EnqueueBatchableTask(1, 5, healthy));
    }

    private sealed class WrongLengthExecutor : IReadBatchExecutor<int, int>
    {
        public int[] ExecuteBatch(int[] args)
        {
            return new int[args.Length + 1];
        }
    }

    [Fact]
    public async Task ResultLengthMismatch_FaultsGroup()
    {
        WrongLengthExecutor executor = new();

        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);

        Task<int> t1 = scheduler.EnqueueBatchableTask(1, 1, executor);
        Task<int> t2 = scheduler.EnqueueBatchableTask(1, 2, executor);

        scheduler.Start();

        await Assert.ThrowsAsync<InvalidOperationException>(() => t1);
        await Assert.ThrowsAsync<InvalidOperationException>(() => t2);
    }

    [Fact]
    public void Backpressure_AppliesToBatchablePath()
    {
        RecordingExecutor executor = new();

        // Never started: ops park and Depth grows until the limit rejects admission.
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1, maxQueueDepthPerPartition: 4);

        List<Task<int>> accepted = new();
        for (int i = 0; i < 4; i++)
            accepted.Add(scheduler.EnqueueBatchableTask(1, i, executor));

        // Admission throws synchronously (before any task is created); the statement-body
        // lambda keeps the assertion on the synchronous Assert.Throws path.
        Assert.Throws<ReadBackpressureExceededException>(() =>
        {
            _ = scheduler.EnqueueBatchableTask(1, 99, executor);
        });
    }

    [Fact]
    public async Task StopWithoutStart_DrainsParkedBatchableOps()
    {
        RecordingExecutor executor = new();

        FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, workerCount: 1);

        Task<int> t1 = scheduler.EnqueueBatchableTask(1, 1, executor);
        Task<int> t2 = scheduler.EnqueueBatchableTask(1, 2, executor);

        scheduler.Stop();

        Assert.Equal(10, await t1);
        Assert.Equal(20, await t2);
        Assert.Single(executor.Calls.ToArray());

        scheduler.Dispose();
    }

    /// <summary>
    /// Minimal scheduler that only implements <see cref="IRaftReadScheduler.EnqueueTask{T}"/>,
    /// so calls to EnqueueBatchableTask exercise the interface's default (non-coalescing)
    /// implementation.
    /// </summary>
    private sealed class InlineScheduler : IRaftReadScheduler
    {
        public Task<T> EnqueueTask<T>(int partitionId, Func<T> operation)
        {
            return Task.FromResult(operation());
        }
    }

    [Fact]
    public async Task DefaultInterfaceImplementation_ExecutesSingleElementBatch()
    {
        RecordingExecutor executor = new();
        IRaftReadScheduler scheduler = new InlineScheduler();

        int result = await scheduler.EnqueueBatchableTask(1, 7, executor);

        Assert.Equal(70, result);

        int[][] calls = executor.Calls.ToArray();
        Assert.Single(calls);
        Assert.Equal([7], calls[0]);
    }
}
