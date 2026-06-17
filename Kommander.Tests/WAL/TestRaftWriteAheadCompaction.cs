using System.Diagnostics;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

public sealed class TestRaftWriteAheadCompaction
{
    [Fact]
    public async Task Compact_UsesSingleStorageCommitPerPass()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactNumberEntries = 2;
            const int maxEntriesPerCompaction = 100;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries,
                maxEntriesPerCompaction,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                Assert.Equal(1, wal.LastCompactionCommitCount);
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

    [Fact]
    public async Task Compact_DrainsMultipleBatchesWhenRemovableExceedsBatchSize()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactNumberEntries = 2;
            const int maxEntriesPerCompaction = 100;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries,
                maxEntriesPerCompaction,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
                Assert.Equal(removableCount + 1, wal.GetMaxLog(partitionId));
                Assert.Equal(removableCount + 1, wal.GetLastCheckpoint(partitionId));
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

    [Fact]
    public async Task Compact_StopsAtMaxEntriesPerCompactionAndLeavesRemainder()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactNumberEntries = 5;
            const int maxEntriesPerCompaction = 25;
            const int removableCount = 50;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries,
                maxEntriesPerCompaction,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(25, remainingRemovable);

                (_, int noneLeft) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, noneLeft);
                Assert.Equal(removableCount + 1, wal.GetLastCheckpoint(partitionId));
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

    [Fact]
    public async Task NotifyCommitted_TriggersCompactionAfterConfiguredCommitCount()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactEveryOperations = 5;
            const int removableCount = 20;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 5,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                for (int i = 0; i < compactEveryOperations; i++)
                    writeAhead.NotifyCommitted();

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
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

    [Fact]
    public async Task NotifyCommitted_DisabledWhenCompactEveryOperationsIsZero()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 20;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 5,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                for (int i = 0; i < 100; i++)
                    writeAhead.NotifyCommitted();

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(removableCount, remainingRemovable);
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

    [Fact]
    public async Task NotifyCommitted_DoesNotAwaitCompactionPass()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 200;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 2,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations: 1,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                writeAhead.NotifyCommitted();
                stopwatch.Stop();

                Assert.True(stopwatch.ElapsedMilliseconds < 100);

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
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

    [Fact]
    public async Task Compact_ConcurrentTriggersCoalesceToSinglePass()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 100;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 2,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                List<Task> tasks = [];

                for (int i = 0; i < 100; i++)
                {
                    tasks.Add(Task.Run(
                        () => writeAhead.Compact(),
                        TestContext.Current.CancellationToken));
                }

                await Task.WhenAll(tasks).ConfigureAwait(true);
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                Assert.Equal(1, writeAhead.CompactionPassCount);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
                Assert.Equal(removableCount + 1, wal.GetMaxLog(partitionId));
                Assert.Equal(removableCount + 1, wal.GetLastCheckpoint(partitionId));
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

    private static RaftWriteAhead CreateWriteAhead(
        SqliteWAL wal,
        int compactNumberEntries,
        int maxEntriesPerCompaction,
        int compactEveryOperations,
        int partitionId,
        out RaftManager manager,
        out RaftPartition partition)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            CompactEveryOperations = compactEveryOperations,
            CompactNumberEntries = compactNumberEntries,
            MaxEntriesPerCompaction = maxEntriesPerCompaction,
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

    private static void SeedRemovableLogs(SqliteWAL wal, int partitionId, int removableCount, long checkpointId)
    {
        List<RaftLog> logs = [];

        for (long id = 1; id <= removableCount; id++)
            logs.Add(CreateCommittedLog(id));

        logs.Add(CreateCheckpointLog(checkpointId));

        Assert.Equal(
            RaftOperationStatus.Success,
            wal.Write(new List<(int, List<RaftLog>)> { (partitionId, logs) }));
    }

    private static RaftLog CreateCommittedLog(long id)
    {
        return new()
        {
            Id = id,
            Term = id,
            Type = RaftLogType.Committed,
            LogType = "compaction-test",
            LogData = [1, 2, 3],
        };
    }

    private static RaftLog CreateCheckpointLog(long id)
    {
        return new()
        {
            Id = id,
            Term = id,
            Type = RaftLogType.CommittedCheckpoint,
            LogType = "compaction-test-checkpoint",
            LogData = [1, 2, 3],
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
