using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

public sealed class TestSqliteWAL
{
    [Fact]
    public void MetadataCreateUpsertAndReadRoundTripsValues()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            Assert.True(wal.SetMetaData("version", "1"));
            Assert.Equal("1", wal.GetMetaData("version"));

            Assert.True(wal.SetMetaData("version", "2"));
            Assert.Equal("2", wal.GetMetaData("version"));

            Assert.True(wal.SetMetaData("empty", ""));
            Assert.Equal("", wal.GetMetaData("empty"));

            Assert.Null(wal.GetMetaData("missing"));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void WriteWithNullLogTypeReadsBackAsEmptyString()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            Assert.Equal(
                RaftOperationStatus.Success,
                wal.Write(
                    new List<(int, List<RaftLog>)>
                    {
                        (5, [CreateLog(id: 1, logType: null)])
                    }
                )
            );

            RaftLog log = Assert.Single(wal.ReadLogs(5));
            Assert.Equal("", log.LogType);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void CompactLogsOlderThanDeletesOnlyBoundedOlderLogs()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            Assert.Equal(
                RaftOperationStatus.Success,
                wal.Write(
                    new List<(int, List<RaftLog>)>
                    {
                        (
                            7,
                            [
                                CreateLog(id: 1),
                                CreateLog(id: 2),
                                CreateLog(id: 3),
                                CreateLog(id: 4),
                                CreateLog(id: 5)
                            ]
                        )
                    }
                )
            );

            Assert.Equal(RaftOperationStatus.Success, wal.CompactLogsOlderThan(7, lastCheckpoint: 5, compactNumberEntries: 2));

            Assert.Equal([3, 4, 5], wal.ReadLogs(7).Select(log => log.Id));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public async Task ConcurrentWriteAndCompactOnSamePartitionAreSerialized()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            List<Task> tasks = [];

            for (int i = 0; i < 50; i++)
            {
                long id = i + 1;
                tasks.Add(Task.Run(() =>
                {
                    Assert.Equal(
                        RaftOperationStatus.Success,
                        wal.Write(new List<(int, List<RaftLog>)> { (3, [CreateLog(id)]) })
                    );
                }, TestContext.Current.CancellationToken));

                tasks.Add(Task.Run(() =>
                {
                    Assert.Equal(
                        RaftOperationStatus.Success,
                        wal.CompactLogsOlderThan(3, lastCheckpoint: id, compactNumberEntries: 5)
                    );
                }, TestContext.Current.CancellationToken));
            }

            await Task.WhenAll(tasks);

            Assert.True(wal.GetMaxLog(3) >= 1);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    private static RaftLog CreateLog(long id, string? logType = "sqlite-test")
    {
        return new()
        {
            Id = id,
            Term = id,
            Type = RaftLogType.Committed,
            LogType = logType,
            LogData = [1, 2, 3]
        };
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
