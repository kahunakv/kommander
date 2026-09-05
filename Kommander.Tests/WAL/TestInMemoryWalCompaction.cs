
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Prefix compaction in <see cref="InMemoryWAL"/> switches between two removal strategies above a size
/// threshold: entry-by-entry removal for a small prefix, and a rebuild of the partition's storage for a
/// large one. Both must leave exactly the same surviving entries, the same recorded maximum log id, and
/// the same recorded checkpoint.
/// </summary>
/// <remarks>
/// The rebuild exists because removing from the head of a <c>SortedList</c> shifts the whole retained
/// tail once per removed entry, which is quadratic when a large prefix is compacted away. A test that
/// only checks a small compaction would never take the rebuild path, so the sizes below are chosen to
/// land on either side of the threshold.
/// </remarks>
public sealed class TestInMemoryWalCompaction
{
    private static RaftLog Log(long id, RaftLogType type = RaftLogType.Committed) => new()
    {
        Id = id,
        Type = type,
        Term = 1,
        LogType = "",
        LogData = [],
    };

    private static InMemoryWAL NewWal() => new(NullLogger<IRaft>.Instance);

    private static void Seed(InMemoryWAL wal, int partitionId, int count)
    {
        List<RaftLog> logs = [];

        for (long id = 1; id <= count; id++)
            logs.Add(Log(id));

        wal.Write([(partitionId, logs)]);
    }

    private static List<long> Ids(InMemoryWAL wal, int partitionId) =>
        wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToList();

    /// <summary>
    /// 4 removed in front of 4 retained stays below the rebuild threshold; 900 in front of 900 is well
    /// above it. Both must survive with the identical remaining ids.
    /// </summary>
    [Theory]
    [InlineData(8, 5)]
    [InlineData(1800, 901)]
    public void CompactionLeavesExactlyTheEntriesAtOrAboveTheCheckpoint(int total, long lastCheckpoint)
    {
        InMemoryWAL wal = NewWal();
        Seed(wal, partitionId: 1, count: total);

        (RaftOperationStatus status, int removed) =
            wal.CompactLogsOlderThan(1, lastCheckpoint, compactNumberEntries: total);

        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal((int)lastCheckpoint - 1, removed);

        List<long> expected = [];
        for (long id = lastCheckpoint; id <= total; id++)
            expected.Add(id);

        Assert.Equal(expected, Ids(wal, 1));
        Assert.Equal(total, wal.GetMaxLog(1));
        Assert.Equal(total - lastCheckpoint + 1, wal.CountPersistedLogs(1));
    }

    /// <summary>
    /// The per-pass cap bounds how much one call removes, and it must bound it from the bottom of the
    /// log — a rebuild that dropped the wrong end would still report a plausible count.
    /// </summary>
    [Fact]
    public void PassCapRemovesTheLowestIdsOnly()
    {
        InMemoryWAL wal = NewWal();
        Seed(wal, partitionId: 2, count: 2000);

        (_, int removed) = wal.CompactLogsOlderThan(2, lastCheckpoint: 1500, compactNumberEntries: 1200);

        Assert.Equal(1200, removed);
        Assert.Equal(1201, Ids(wal, 2)[0]);
        Assert.Equal(2000, wal.GetMaxLog(2));
    }

    /// <summary>
    /// A checkpoint entry sits at or above the compaction floor, so it always survives — on the rebuild
    /// path too, where the surviving entries are copied into fresh storage.
    /// </summary>
    [Fact]
    public void RebuildRetainsTheCheckpointEntry()
    {
        InMemoryWAL wal = NewWal();

        List<RaftLog> logs = [];
        for (long id = 1; id <= 1500; id++)
            logs.Add(Log(id, id == 1000 ? RaftLogType.CommittedCheckpoint : RaftLogType.Committed));
        wal.Write([(3, logs)]);

        wal.CompactLogsOlderThan(3, lastCheckpoint: 1000, compactNumberEntries: 2000);

        Assert.Equal(1000, wal.GetLastCheckpoint(3));
        Assert.Equal(1000, Ids(wal, 3)[0]);
        Assert.Equal(1500, wal.GetMaxLog(3));
    }

    /// <summary>
    /// Compacting away every entry — the checkpoint sits above the whole log — must clear the recorded
    /// maximum rather than leave it pointing at a removed id.
    /// </summary>
    [Fact]
    public void CompactingTheWholeLogClearsTheRecordedMaximum()
    {
        InMemoryWAL wal = NewWal();
        Seed(wal, partitionId: 4, count: 1500);

        (_, int removed) = wal.CompactLogsOlderThan(4, lastCheckpoint: 5000, compactNumberEntries: 5000);

        Assert.Equal(1500, removed);
        Assert.Empty(Ids(wal, 4));
        Assert.Equal(0, wal.GetMaxLog(4));
    }

    [Fact]
    public void NothingBelowTheCheckpointRemovesNothing()
    {
        InMemoryWAL wal = NewWal();
        Seed(wal, partitionId: 5, count: 10);

        (RaftOperationStatus status, int removed) =
            wal.CompactLogsOlderThan(5, lastCheckpoint: 1, compactNumberEntries: 100);

        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(0, removed);
        Assert.Equal(10, wal.CountPersistedLogs(5));
        Assert.Equal(10, wal.GetMaxLog(5));
    }

    /// <summary>
    /// Compaction is per partition: a rebuild replaces one partition's storage, so a neighbour's entries
    /// must be untouched by it.
    /// </summary>
    [Fact]
    public void RebuildDoesNotDisturbOtherPartitions()
    {
        InMemoryWAL wal = NewWal();
        Seed(wal, partitionId: 6, count: 1500);
        Seed(wal, partitionId: 7, count: 40);

        wal.CompactLogsOlderThan(6, lastCheckpoint: 900, compactNumberEntries: 2000);

        Assert.Equal(900, Ids(wal, 6)[0]);
        Assert.Equal(40, wal.CountPersistedLogs(7));
        Assert.Equal(1, Ids(wal, 7)[0]);
        Assert.Equal(40, wal.GetMaxLog(7));
    }
}
