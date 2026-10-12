using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The boundary cases of a compaction pass on <see cref="RaftWriteAhead"/>:
///
/// <list type="bullet">
///   <item>Nothing eligible (no checkpoint exists): the pass is a counted no-op — invoked, never
///         effective, nothing removed, no storage commit issued.</item>
///   <item>Entries exactly at the truncation floor: compaction deletes strictly below the
///         checkpoint, so the checkpoint row itself (the durable recovery anchor every later
///         restore chains from) survives the pass and remains readable as the last checkpoint.</item>
/// </list>
/// </summary>
public sealed class TestWalCompactionBoundaries
{
    /// <summary>
    /// With no checkpoint there is nothing to compact to: the pass must still be observable as
    /// invoked (CompactionPassCount) but never effective (EffectiveCompactionPassCount), remove
    /// nothing, and issue no storage commit — a deployment whose application never checkpoints
    /// needs the WAL to at least not churn.
    /// </summary>
    [Fact]
    public async Task Compact_WithoutACheckpoint_IsACountedNoOp()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            SeedCommittedLogs(wal, partitionId, count: 9);

            RaftWriteAhead writeAhead = CreateWriteAhead(wal, partitionId, out RaftManager manager, out RaftPartition partition);
            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync();

                Assert.Equal(1, writeAhead.CompactionPassCount);
                Assert.Equal(0, writeAhead.EffectiveCompactionPassCount);
                Assert.Equal(0, wal.LastCompactionCommitCount);
                Assert.Equal(9, wal.ReadLogsRange(partitionId, 0).Count);
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Compaction removes the committed prefix strictly below the checkpoint but keeps the
    /// checkpoint row itself: it is the floor-certifying anchor that a restart's frontier
    /// reconstruction and a lagging follower's backfill both chain from. A second pass over the
    /// same shape removes nothing further.
    /// </summary>
    [Fact]
    public async Task Compact_RetainsTheCheckpointRowAndEverythingAtOrAboveTheFloor()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            SeedCommittedLogs(wal, partitionId, count: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(wal, partitionId, out RaftManager manager, out RaftPartition partition);
            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync();

                long[] surviving = wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray();
                Assert.Equal([10L], surviving);
                Assert.Equal(RaftLogType.CommittedCheckpoint, wal.ReadLogsRange(partitionId, 0).Single().Type);
                Assert.Equal(10, wal.GetLastCheckpoint(partitionId));

                // The boundary is stable: nothing further is removable on a repeat pass.
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync();
                Assert.Equal([10L], wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray());
                Assert.Equal(10, wal.GetLastCheckpoint(partitionId));
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    // ── harness (mirrors the compaction harness) ─────────────────────────────

    private static RaftWriteAhead CreateWriteAhead(
        SqliteWAL wal,
        int partitionId,
        out RaftManager manager,
        out RaftPartition partition)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            CompactEveryOperations = 0,
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
            partitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        return new RaftWriteAhead(manager, _ => { }, partition, wal);
    }

    private static void SeedCommittedLogs(SqliteWAL wal, int partitionId, int count, long checkpointId = -1)
    {
        List<RaftLog> logs = [];

        for (long id = 1; id <= count; id++)
        {
            logs.Add(new RaftLog
            {
                Id = id,
                Term = id,
                Type = RaftLogType.Committed,
                LogType = "compaction-boundary-test",
                LogData = [1, 2, 3],
            });
        }

        if (checkpointId > 0)
        {
            logs.Add(new RaftLog
            {
                Id = checkpointId,
                Term = checkpointId,
                Type = RaftLogType.CommittedCheckpoint,
                LogType = "compaction-boundary-test-checkpoint",
                LogData = [1, 2, 3],
            });
        }

        Assert.Equal(
            RaftOperationStatus.Success,
            wal.Write(new List<(int, List<RaftLog>)> { (partitionId, logs) }));
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
