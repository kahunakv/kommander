using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Covers <see cref="RaftWriteAhead"/>'s enqueue ordering and rollback paths:
/// <list type="bullet">
///   <item><description>
///     The allocation-free <c>OrderById</c> fast path must produce the same ascending-id order as
///     the previous <c>OrderBy(...).ToArray()</c> — verified on the follower append path where the
///     written order and <c>LogIndex</c> are observable on the returned operation.
///   </description></item>
///   <item><description>
///     The pooled rollback snapshot buffers must restore every mutated field when the WAL scheduler
///     rejects the enqueue. The scheduler in this harness is never started, so
///     <c>FairWalScheduler.Enqueue</c> throws synchronously — a deterministic rejection that drives
///     the rollback path without needing to manufacture backpressure.
///   </description></item>
/// </list>
/// </summary>
public sealed class TestRaftWriteAheadEnqueueOrdering
{
    [Fact]
    public void EnqueuePropose_WhenSchedulerRejects_RestoresIdsTermsForUnsortedInput()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            RaftWriteAhead writeAhead = CreateWriteAhead(wal, out RaftManager manager, out RaftPartition partition);
            try
            {
                // Intentionally out of id order to exercise the OrderBy fallback in OrderById.
                List<RaftLog> logs =
                [
                    NewLog(id: 30, term: 2, RaftLogType.Proposed),
                    NewLog(id: 10, term: 3, RaftLogType.Proposed),
                    NewLog(id: 20, term: 4, RaftLogType.Proposed),
                ];
                (long id, long term, RaftLogType type)[] before = Snapshot(logs);

                // WAL scheduler was never started → Enqueue throws InvalidOperationException.
                Assert.Throws<InvalidOperationException>(() => writeAhead.EnqueuePropose(99, logs, default, false));

                // Every id and term must be restored to its pre-call value across all entries.
                AssertRestored(before, logs);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    [Fact]
    public void EnqueueCommit_WhenSchedulerRejects_RestoresTypesAndCommitIndex()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            RaftWriteAhead writeAhead = CreateWriteAhead(wal, out RaftManager manager, out RaftPartition partition);
            try
            {
                long commitIndexBefore = writeAhead.GetCommitIndex();
                List<RaftLog> logs =
                [
                    NewLog(id: 3, term: 1, RaftLogType.Proposed),
                    NewLog(id: 1, term: 1, RaftLogType.Proposed),
                    NewLog(id: 2, term: 1, RaftLogType.ProposedCheckpoint),
                ];
                (long id, long term, RaftLogType type)[] before = Snapshot(logs);

                Assert.Throws<InvalidOperationException>(() => writeAhead.EnqueueCommit(logs));

                AssertRestored(before, logs);
                Assert.Equal(commitIndexBefore, writeAhead.GetCommitIndex());
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    [Fact]
    public void EnqueueRollback_WhenSchedulerRejects_RestoresTypes()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            RaftWriteAhead writeAhead = CreateWriteAhead(wal, out RaftManager manager, out RaftPartition partition);
            try
            {
                List<RaftLog> logs =
                [
                    NewLog(id: 2, term: 1, RaftLogType.Proposed),
                    NewLog(id: 1, term: 1, RaftLogType.ProposedCheckpoint),
                ];
                (long id, long term, RaftLogType type)[] before = Snapshot(logs);

                Assert.Throws<InvalidOperationException>(() => writeAhead.EnqueueRollback(logs));

                AssertRestored(before, logs);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    [Fact]
    public async Task Commit_WhenSchedulerRejects_RestoresTypesAndCommitIndex()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            RaftWriteAhead writeAhead = CreateWriteAhead(wal, out RaftManager manager, out RaftPartition partition);
            try
            {
                long commitIndexBefore = writeAhead.GetCommitIndex();
                List<RaftLog> logs =
                [
                    NewLog(id: 2, term: 1, RaftLogType.Proposed),
                    NewLog(id: 1, term: 1, RaftLogType.Proposed),
                ];
                (long id, long term, RaftLogType type)[] before = Snapshot(logs);

                await Assert.ThrowsAsync<InvalidOperationException>(() => writeAhead.Commit(logs));

                AssertRestored(before, logs);
                Assert.Equal(commitIndexBefore, writeAhead.GetCommitIndex());
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    [Theory]
    [InlineData(new long[] { 3, 1, 2 }, new long[] { 1, 2, 3 })]   // unsorted → ascending (OrderBy fallback)
    [InlineData(new long[] { 1, 2, 3 }, new long[] { 1, 2, 3 })]   // already sorted → unchanged (fast path)
    [InlineData(new long[] { 5 }, new long[] { 5 })]               // single entry
    public void EnqueueProposeOrCommit_WritesAscendingIdOrder_AndLogIndexIsMax(long[] inputIds, long[] expectedIds)
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            RaftWriteAhead writeAhead = CreateWriteAhead(wal, out RaftManager manager, out RaftPartition partition);
            try
            {
                // Started scheduler → Enqueue succeeds and the operation is returned synchronously.
                ((FairWalScheduler)manager.WalScheduler).Start();

                List<RaftLog> logs = [.. inputIds.Select(id => NewLog(id, term: 1, RaftLogType.Proposed))];

                WALWriteOperation? operation = writeAhead.EnqueueProposeOrCommit(logs);

                Assert.NotNull(operation);
                long[] writtenIds = [.. operation!.Logs.Logs.Select(l => l.Id)];
                Assert.Equal(expectedIds, writtenIds);
                Assert.Equal(expectedIds.Max(), operation.LogIndex);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    private static RaftLog NewLog(long id, long term, RaftLogType type) => new()
    {
        Id = id,
        Term = term,
        Type = type,
        Time = new HLCTimestamp(0, 0, 0),
        LogType = "test",
        LogData = []
    };

    private static (long id, long term, RaftLogType type)[] Snapshot(List<RaftLog> logs) =>
        [.. logs.Select(l => (l.Id, l.Term, l.Type))];

    private static void AssertRestored((long id, long term, RaftLogType type)[] before, List<RaftLog> logs)
    {
        Assert.Equal(before.Length, logs.Count);
        for (int i = 0; i < logs.Count; i++)
        {
            Assert.Equal(before[i].id, logs[i].Id);
            Assert.Equal(before[i].term, logs[i].Term);
            Assert.Equal(before[i].type, logs[i].Type);
        }
    }

    private static RaftWriteAhead CreateWriteAhead(SqliteWAL wal, out RaftManager manager, out RaftPartition partition)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        partition = new(
            manager,
            wal,
            partitionId: 1,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        return new RaftWriteAhead(manager, _ => { }, partition, wal);
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-sqlite-wal-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
