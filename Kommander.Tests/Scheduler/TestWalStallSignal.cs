
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

        await Task.Delay(150);
        double inFlightAge = scheduler.GetPartitionOldestPendingWriteAgeMs(partitionId);
        Assert.InRange(inFlightAge, 100, 60_000);
        Assert.InRange(scheduler.GetOldestPendingWriteAgeMs(), inFlightAge - 5, 60_000);

        // A second op queues behind the held batch: the signal still ages the oldest (in-flight) op, not the newest.
        scheduler.Enqueue(MakeOp(partitionId, _ => Interlocked.Increment(ref completed)));
        await Task.Delay(50);
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
}
