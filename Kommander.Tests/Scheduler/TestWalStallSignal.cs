
using Kommander.Data;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// The durable-write stall signal on <see cref="FairWalScheduler"/>: the age of the oldest operation the
/// scheduler has accepted and the engine has not answered. Unlike the commit-wait EWMA, which is fed only by
/// completed batches and so reads its last value while a write hangs, the age keeps rising for as long as the
/// engine holds the write, covers operations still queued behind it, and returns to zero once the engine answers.
/// </summary>
public sealed class TestWalStallSignal
{
    private sealed class GatedWal : IWAL, IDisposable
    {
        private readonly ManualResetEventSlim gate = new(initialState: false);

        private int blocked;

        public int Blocked => Volatile.Read(ref blocked);

        public void Release() => gate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            Interlocked.Increment(ref blocked);
            try
            {
                gate.Wait();
            }
            finally
            {
                Interlocked.Decrement(ref blocked);
            }

            return RaftOperationStatus.Success;
        }

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public long GetLastCheckpoint(int partitionId) => 0;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() => gate.Dispose();
    }

    private static WALWriteOperation MakeOp(int partitionId, Action<RaftWalCompletion> onComplete) =>
        new(onComplete, operationId: 0, WALWriteOperationType.LeaderPropose, (partitionId, [new RaftLog { Id = 1 }]));

    private static async Task WaitUntilAsync(Func<bool> predicate, int timeoutMs, string what)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
                return;

            await Task.Delay(10);
        }

        Assert.True(predicate(), what);
    }

    [Fact]
    public async Task OldestPendingWriteAge_RisesWhileTheEngineHoldsTheWrite_CoversTheQueue_AndClearsOnCompletion()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int partitionId = 7;
        using GatedWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        Assert.Equal(0, scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId));
        Assert.Equal(0, scheduler.GetOldestPendingWriteAgeMs());

        int completed = 0;

        // The worker takes the first op into its batch and blocks inside the engine's Write.
        scheduler.Enqueue(MakeOp(partitionId, _ => Interlocked.Increment(ref completed)));
        await WaitUntilAsync(() => wal.Blocked == 1, 5_000, "the worker must be held inside Write");

        await Task.Delay(150, ct);
        double inFlightAge = scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId);
        Assert.InRange(inFlightAge, 100, 60_000);
        Assert.InRange(scheduler.GetOldestPendingWriteAgeMs(), inFlightAge - 5, 60_000);

        // A second op queues behind the held batch: the signal still ages the oldest (in-flight) op, not the newest.
        scheduler.Enqueue(MakeOp(partitionId, _ => Interlocked.Increment(ref completed)));
        await Task.Delay(50, ct);
        double queuedAge = scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId);
        Assert.True(queuedAge >= inFlightAge, $"age must keep rising: {queuedAge} < {inFlightAge}");

        // Other partitions are unaffected.
        Assert.Equal(0, scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId + 1));

        // The engine answers: both ops complete and the signal returns to idle.
        wal.Release();
        await WaitUntilAsync(() => Volatile.Read(ref completed) == 2, 5_000, "both operations must complete once the engine answers");
        await WaitUntilAsync(() => scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId) == 0, 5_000, "the age must return to zero when nothing is pending");
        Assert.Equal(0, scheduler.GetOldestPendingWriteAgeMs());
    }

    [Fact]
    public async Task BacklogIsReleasedBeforeTheCompletionCallbackFires()
    {
        const int partitionId = 5;
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        // The callback is the moment a caller learns its write is durable, and a caller may read the
        // backlog right then (a load report, or a test asserting "a sequential writer never builds
        // depth"). Reading both signals from inside the callback pins the ordering without a race:
        // on the worker thread the batch must already have left the depth and the stall age.
        // Releasing them after the callbacks let a GA run read depth 1 after an awaited ReplicateLogs.
        for (long i = 1; i <= 5; i++)
        {
            TaskCompletionSource<(int Depth, double AgeMs)> seen = new(TaskCreationOptions.RunContinuationsAsynchronously);
            scheduler.Enqueue(MakeOp(partitionId, _ => seen.TrySetResult((
                scheduler.GetPartitionDepth(partitionId),
                scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId)))));

            (int depthAtAck, double ageAtAck) = await seen.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(0, depthAtAck);
            Assert.Equal(0, ageAtAck);
        }
    }

    [Fact]
    public async Task GroupBatch_ReleasesEveryPartitionBeforeTheFirstCallbackOfAny()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using GatedWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        // Partition 1 holds the single worker inside Write, so partitions 2 and 3 queue up behind it
        // and are drained together as one group batch once the engine answers.
        TaskCompletionSource first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeOp(1, _ => first.TrySetResult()));
        await WaitUntilAsync(() => wal.Blocked == 1, 5_000, "the worker must be held inside Write");

        // One ack can wake a reader of ANOTHER partition (a load report covers all of them), so each
        // callback records the depth of both group members, whichever of the two fires first.
        int worstDepthSeen = 0;
        int remaining = 2;
        TaskCompletionSource rest = new(TaskCreationOptions.RunContinuationsAsynchronously);

        void OnGroupMemberComplete(RaftWalCompletion _)
        {
            int seen = Math.Max(scheduler.GetPartitionDepth(2), scheduler.GetPartitionDepth(3));
            if (seen > Volatile.Read(ref worstDepthSeen))
                Volatile.Write(ref worstDepthSeen, seen); // single worker: callbacks never overlap

            if (Interlocked.Decrement(ref remaining) == 0)
                rest.TrySetResult();
        }

        scheduler.Enqueue(MakeOp(2, OnGroupMemberComplete));
        scheduler.Enqueue(MakeOp(3, OnGroupMemberComplete));
        Assert.Equal(1, scheduler.GetPartitionDepth(2));
        Assert.Equal(1, scheduler.GetPartitionDepth(3));

        wal.Release();
        await first.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await rest.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.Equal(0, Volatile.Read(ref worstDepthSeen));
    }

    [Fact]
    public async Task HardStateOperation_PersistsOnTheWorker_AndCompletesWithItsOwnStatus()
    {
        const int partitionId = 3;
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        RaftWalCompletion? completion = null;
        WALWriteOperation op = new(c => Volatile.Write(ref completion, c), operationId: 42, WALWriteOperationType.HardState,
            (partitionId, []), term: 7, votedFor: "localhost:9999");

        scheduler.Enqueue(op);
        await WaitUntilAsync(() => Volatile.Read(ref completion) is not null, 5_000, "the hard-state operation must complete");

        RaftWalCompletion done = Volatile.Read(ref completion)!;
        Assert.Equal(RaftOperationStatus.Success, done.Status);
        Assert.Equal(WALWriteOperationType.HardState, done.OperationType);
        Assert.Equal(7, done.Term);
        Assert.Equal(42, done.OperationId);
        Assert.Equal(-1, done.MinLogIndex);

        Assert.True(((IWAL)wal).TryGetHardState(partitionId, out long term, out string? votedFor));
        Assert.Equal(7, term);
        Assert.Equal("localhost:9999", votedFor);
        Assert.Equal(0, scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId));
    }

    [Fact]
    public async Task HlcFloorOperation_PersistsOnTheWorker_AndCarriesItsValue()
    {
        const int partitionId = 4;
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        RaftWalCompletion? completion = null;
        WALWriteOperation op = new(c => Volatile.Write(ref completion, c), operationId: 43, WALWriteOperationType.HlcFloor,
            (partitionId, []), metadataValue: 123_456);

        scheduler.Enqueue(op);
        await WaitUntilAsync(() => Volatile.Read(ref completion) is not null, 5_000, "the HLC floor operation must complete");

        RaftWalCompletion done = Volatile.Read(ref completion)!;
        Assert.Equal(RaftOperationStatus.Success, done.Status);
        Assert.Equal(WALWriteOperationType.HlcFloor, done.OperationType);
        Assert.Equal(123_456, done.MetadataValue);
        Assert.Equal(-1, done.Term);
        Assert.Equal(123_456, ((IWAL)wal).GetHlcFloor(partitionId));
    }
}
