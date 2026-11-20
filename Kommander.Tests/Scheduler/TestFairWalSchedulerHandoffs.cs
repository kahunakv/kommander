using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Data;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Hand-off tests for <see cref="FairWalScheduler"/>: the parked-worker handshake that replaced the
/// <c>BlockingCollection</c> ready-queue. No lost wake, one wake per write, no wake for a worker's own
/// put-back, an unbounded ready-queue, and a linger that still hears the arrival it waits for.
///
/// <para>Tests that count wake-ups build the scheduler with <c>maxSpinBeforePark: TimeSpan.Zero</c>, so
/// every hand-off is a park and a wake and the counts are exact.</para>
/// </summary>
public sealed class TestFairWalSchedulerHandoffs
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private static WALWriteOperation MakeOp(int partitionId, long operationId, Action<RaftWalCompletion> onComplete) =>
        new(onComplete, operationId, WALWriteOperationType.LeaderPropose,
            (partitionId, [new RaftLog { Id = operationId, Term = 1, Type = RaftLogType.Proposed }]));

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

    private static Task EnqueueAndWait(FairWalScheduler scheduler, int partitionId, long operationId)
    {
        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeOp(partitionId, operationId, _ => done.TrySetResult()));
        return done.Task;
    }

    // ── Handshake ───────────────────────────────────────────────────────────

    /// <summary>
    /// The lost-wake race: a partition made ready between a worker's last empty dequeue and its park
    /// must be written. Awaiting each completion before the next enqueue puts every enqueue right where
    /// the worker is heading to sleep. A lost wake has no safety net, so it shows as a hang.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(20)]
    public async Task LostWakeRace_EveryWriteCompletes(int spinMicroseconds)
    {
        using GatedWal wal = new(open: true);
        using FairWalScheduler scheduler = new(
            wal, NullLogger<IRaft>.Instance, workerCount: 2, maxSpinBeforePark: TimeSpan.FromMicroseconds(spinMicroseconds));
        scheduler.Start();

        Task loop = Task.Run(async () =>
        {
            for (int i = 1; i <= 10_000; i++)
            {
                Thread.SpinWait(i % 64);
                await EnqueueAndWait(scheduler, 1 + i % 3, i);
            }
        }, TestContext.Current.CancellationToken);

        await loop.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(10_000, scheduler.TotalOperationsCompleted);
    }

    /// <summary>Many producers over many partitions and four workers: every accepted write completes.</summary>
    [Fact]
    public async Task ManyProducers_ManyPartitions_AllComplete()
    {
        using GatedWal wal = new(open: true);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4);
        scheduler.Start();

        const int producers = 8;
        const int perProducer = 5_000;

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

                    await EnqueueAndWait(scheduler, 1 + (i + producer) % 16, producer * 1_000_000L + i + 1);
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(tasks).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(producers * perProducer, scheduler.TotalOperationsCompleted);
    }

    /// <summary>
    /// One wake per write for a serial stream, with four workers: three stay parked the whole time, and
    /// no woken worker finds the queue empty.
    /// </summary>
    [Fact]
    public async Task SerialWrites_OneWakeEach_NoneEmpty()
    {
        using GatedWal wal = new(open: true);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, maxSpinBeforePark: TimeSpan.Zero);
        scheduler.Start();

        for (int i = 1; i <= 10; i++)
        {
            await WaitUntil(() => scheduler.IdleWorkerCount == 4, "all workers parked");
            await EnqueueAndWait(scheduler, 1, i).WaitAsync(Timeout, TestContext.Current.CancellationToken);
        }

        await WaitUntil(() => scheduler.IdleWorkerCount == 4, "all workers parked");

        Assert.Equal(10, scheduler.WorkerWakeups);
        Assert.Equal(0, scheduler.EmptyWorkerWakeups);
    }

    /// <summary>
    /// A burst to a partition whose write is in flight is one wake in all: the in-flight flag keeps the
    /// burst off the ready-queue, and the worker's own put-back after the write does not wake a second
    /// worker — it takes the partition itself.
    /// </summary>
    [Fact]
    public async Task BurstBehindAnInFlightWrite_IsOneWake()
    {
        using GatedWal wal = new(open: false);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, maxSpinBeforePark: TimeSpan.Zero);
        scheduler.Start();
        using OpenOnExit openOnExit = new(wal);

        await WaitUntil(() => scheduler.IdleWorkerCount == 4, "all workers parked");

        List<Task> all = [EnqueueAndWait(scheduler, 1, 1)];
        await WaitUntil(() => wal.BlockedWriters == 1, "the worker is inside the engine write");

        for (int i = 2; i <= 64; i++)
            all.Add(EnqueueAndWait(scheduler, 1, i));

        wal.Release();

        await Task.WhenAll(all).WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await WaitUntil(() => scheduler.IdleWorkerCount == 4, "all workers parked");

        Assert.Equal(1, scheduler.WorkerWakeups);
        Assert.Equal(0, scheduler.EmptyWorkerWakeups);
        Assert.Equal(64, scheduler.TotalOperationsCompleted);
        Assert.Equal(2, scheduler.TotalBatchesWritten);
    }

    /// <summary>
    /// The ready-queue has no capacity: any number of partitions can be ready at once. With the bounded
    /// collection (64 per worker) the enqueue that made the next partition ready blocked, under the
    /// partition lock, until a worker freed a slot — here the only worker is held inside the engine, so
    /// that enqueue would never have returned.
    /// </summary>
    [Fact]
    public async Task MoreReadyPartitionsThanTheOldCapacity_EnqueueDoesNotBlock()
    {
        const int partitions = 400;

        using GatedWal wal = new(open: false);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();
        using OpenOnExit openOnExit = new(wal);

        int completed = 0;

        scheduler.Enqueue(MakeOp(1, 1, _ => Interlocked.Increment(ref completed)));
        await WaitUntil(() => wal.BlockedWriters == 1, "the worker is inside the engine write");

        Task enqueueAll = Task.Run(() =>
        {
            for (int p = 2; p <= partitions; p++)
                scheduler.Enqueue(MakeOp(p, p, _ => Interlocked.Increment(ref completed)));
        }, TestContext.Current.CancellationToken);

        await enqueueAll.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        wal.Release();

        await WaitUntil(() => Volatile.Read(ref completed) == partitions, "every partition written");
    }

    // ── Linger ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A lingering worker holds a batch and waits for more partitions to become ready. An enqueue that
    /// makes another partition ready must go to that worker, into the same engine write, and not to a
    /// parked worker that would write it on its own: that is the coalescing the linger exists for.
    /// </summary>
    [Fact]
    public async Task Linger_ArrivalGoesToTheLingeringWorker_NotAParkedOne()
    {
        using GatedWal wal = new(open: false);
        using FairWalScheduler scheduler = new(
            wal, NullLogger<IRaft>.Instance, workerCount: 3, groupCommitLingerMs: 30_000, maxSpinBeforePark: TimeSpan.Zero);
        scheduler.Start();
        using OpenOnExit openOnExit = new(wal);

        await WaitUntil(() => scheduler.IdleWorkerCount == 3, "all workers parked");

        // Worker A takes partition 2's first write and is held inside the engine.
        Task first = EnqueueAndWait(scheduler, 2, 21);
        await WaitUntil(() => wal.BlockedWriters == 1, "the first worker is inside the engine write");

        // Partition 2's second write queues behind it. Partition 1's write goes to worker B, whose
        // evidence gate sees that queued write and enters the timed wait. Worker C stays parked.
        Task second = EnqueueAndWait(scheduler, 2, 22);
        Task held = EnqueueAndWait(scheduler, 1, 11);
        // In the timed wait, not merely on its way there: an arrival before the worker registers is
        // handed to the parked worker, which is correct and not what this test is about.
        await WaitUntil(() => scheduler.LingeringWorkerCount == 1, "the free worker lingers");
        await WaitUntil(() => scheduler.IdleWorkerCount == 1, "the third worker is parked");

        long wakeupsBefore = scheduler.WorkerWakeups;

        // The arrival: partition 3 becomes ready while B lingers and C is parked.
        Task arrival = EnqueueAndWait(scheduler, 3, 31);

        await WaitUntil(() => scheduler.TotalLingerPartitionsGathered >= 1, "the lingering worker gathers the arrival");

        Assert.Equal(1, scheduler.IdleWorkerCount);
        Assert.Equal(wakeupsBefore, scheduler.WorkerWakeups);
        Assert.False(arrival.IsCompleted, "The arrival was written on its own instead of joining the lingering worker's batch.");

        // Stop ends the linger's wait; everything accepted is written before the workers exit.
        wal.Release();
        await Task.Run(scheduler.Stop, TestContext.Current.CancellationToken).WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await Task.WhenAll(first, second, held, arrival).WaitAsync(Timeout, TestContext.Current.CancellationToken);
    }

    // ── Stop ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Stop_WhileWorkersParked_Returns()
    {
        using GatedWal wal = new(open: true);
        FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, maxSpinBeforePark: TimeSpan.Zero);
        scheduler.Start();

        await WaitUntil(() => scheduler.IdleWorkerCount == 4, "all workers parked");

        await Task.Run(scheduler.Dispose, TestContext.Current.CancellationToken).WaitAsync(Timeout, TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Stop while one worker is inside the engine and others are parked: everything accepted before the
    /// stop completes, including what was queued behind the held write and partitions no worker had
    /// taken yet.
    /// </summary>
    [Fact]
    public async Task Stop_WithQueuedWork_CompletesEverythingAccepted()
    {
        using GatedWal wal = new(open: false);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1, maxSpinBeforePark: TimeSpan.Zero);
        scheduler.Start();
        using OpenOnExit openOnExit = new(wal);

        List<Task> all = [EnqueueAndWait(scheduler, 1, 1)];
        await WaitUntil(() => wal.BlockedWriters == 1, "the worker is inside the engine write");

        for (int i = 2; i <= 50; i++)
            all.Add(EnqueueAndWait(scheduler, 1 + i % 5, i));

        Task stop = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(stop.IsCompleted, "Stop returned while a worker was still inside a write.");

        wal.Release();

        await stop.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await Task.WhenAll(all).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(50, scheduler.TotalOperationsCompleted);
    }

    /// <summary>
    /// Opens the gate when a test ends, pass or fail. Declared after the scheduler, so it is disposed
    /// before it: a failed assertion with the gate still closed would otherwise leave the scheduler's
    /// own disposal waiting for ever on the writes the gate holds, and the failure would show as a hang.
    /// </summary>
    private sealed class OpenOnExit(GatedWal wal) : IDisposable
    {
        public void Dispose() => wal.Release();
    }

    // ── Fake WAL ────────────────────────────────────────────────────────────

    /// <summary>A WAL whose writes wait on a gate, so a test can hold a worker inside the engine.</summary>
    private sealed class GatedWal : IWAL, IDisposable
    {
        private readonly ManualResetEventSlim _gate;
        private int _blockedWriters;

        public GatedWal(bool open) => _gate = new ManualResetEventSlim(open);

        public int BlockedWriters => Volatile.Read(ref _blockedWriters);

        public void Release() => _gate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            if (_gate.IsSet)
                return RaftOperationStatus.Success;

            Interlocked.Increment(ref _blockedWriters);
            try
            {
                _gate.Wait();
            }
            finally
            {
                Interlocked.Decrement(ref _blockedWriters);
            }

            return RaftOperationStatus.Success;
        }

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public long GetLastCheckpoint(int partitionId) => -1;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public (RaftOperationStatus, long) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus, int) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() => _gate.Dispose();
    }
}
