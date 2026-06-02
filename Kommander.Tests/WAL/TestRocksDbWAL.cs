using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

public sealed class TestRocksDbWAL
{
    [Fact]
    public void ReadLogsKeepsOverlappingIdsForPartitionsSharingShard()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            for (int partitionId = 0; partitionId < 9; partitionId++)
            {
                RaftOperationStatus status = wal.Write(
                    new List<(int, List<RaftLog>)>
                    {
                        (partitionId, [CreateLog(partitionId, id: 1, term: 100 + partitionId)])
                    }
                );

                Assert.Equal(RaftOperationStatus.Success, status);
            }

            for (int partitionId = 0; partitionId < 9; partitionId++)
            {
                RaftLog log = Assert.Single(wal.ReadLogs(partitionId));
                Assert.Equal(1, log.Id);
                Assert.Equal(100 + partitionId, log.Term);
                Assert.Equal($"partition-{partitionId}", log.LogType);

                RaftLog rangedLog = Assert.Single(wal.ReadLogsRange(partitionId, 1));
                Assert.Equal(1, rangedLog.Id);
                Assert.Equal(100 + partitionId, rangedLog.Term);
                Assert.Equal($"partition-{partitionId}", rangedLog.LogType);
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void IteratorOperationsStayInsidePartitionKeyRange()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            RaftOperationStatus status = wal.Write(
                new List<(int, List<RaftLog>)>
                {
                    (
                        0,
                        [
                            CreateLog(0, id: 1, term: 10),
                            CreateLog(0, id: 3, term: 13, type: RaftLogType.CommittedCheckpoint)
                        ]
                    ),
                    (
                        8,
                        [
                            CreateLog(8, id: 1, term: 80),
                            CreateLog(8, id: 2, term: 82),
                            CreateLog(8, id: 3, term: 83)
                        ]
                    )
                }
            );

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, wal.GetMaxLog(0));
            Assert.Equal(13, wal.GetCurrentTerm(0));
            Assert.Equal(3, wal.GetLastCheckpoint(0));
            Assert.Equal(3, wal.GetMaxLog(8));
            Assert.Equal(83, wal.GetCurrentTerm(8));
            Assert.Equal(-1, wal.GetLastCheckpoint(8));

            (RaftOperationStatus compactStatus, int removed) = wal.CompactLogsOlderThan(8, lastCheckpoint: 3, compactNumberEntries: 10);
            Assert.Equal(RaftOperationStatus.Success, compactStatus);
            Assert.Equal(2, removed);

            Assert.Equal([1, 3], wal.ReadLogsRange(0, 1).Select(log => log.Id));
            RaftLog remainingPartition8Log = Assert.Single(wal.ReadLogsRange(8, 1));
            Assert.Equal(3, remainingPartition8Log.Id);
            Assert.Equal(83, remainingPartition8Log.Term);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    private static RaftLog CreateLog(int partitionId, long id, long term, RaftLogType type = RaftLogType.Committed)
    {
        return new()
        {
            Id = id,
            Term = term,
            Type = type,
            LogType = $"partition-{partitionId}"
        };
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-rocksdb-wal-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
