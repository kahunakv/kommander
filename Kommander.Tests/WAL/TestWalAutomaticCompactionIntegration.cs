using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// End-to-end compaction integration tests for SQLite and RocksDB.
/// Exercises automatic triggering and drain-to-bound via <see cref="RaftWriteAhead.NotifyCommitted"/>,
/// not the full Raft state-machine commit completion path (covered by Task 5 unit tests).
/// </summary>
public sealed class TestWalAutomaticCompactionIntegration
{
    private const int PartitionId = 1;

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task BoundedGrowth_StaysBoundedUnderSustainedCommits(string backend)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);
            const int compactEveryOperations = 20;
            const int totalOperations = 200;

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactEveryOperations,
                compactNumberEntries: 10,
                maxEntriesPerCompaction: 100,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                await DriveCommittedOperationsAsync(
                    wal,
                    writeAhead,
                    totalOperations,
                    logsPerOperation: 1).ConfigureAwait(true);

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                int persisted = wal.CountPersistedLogs(PartitionId);
                int entriesWithoutCompaction = EntriesWrittenPerOperation(1) * totalOperations;
                Assert.True(
                    persisted < entriesWithoutCompaction / 2,
                    $"Expected bounded WAL size (< {entriesWithoutCompaction / 2}), but found {persisted} entries after {totalOperations} operations.");
                Assert.True(
                    persisted <= compactEveryOperations * EntriesWrittenPerOperation(1) * 4,
                    $"Expected O(CompactEveryOperations) retention (<= {compactEveryOperations * EntriesWrittenPerOperation(1) * 4}), but found {persisted} entries.");
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

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task BoundedGrowth_DisabledModeAccumulatesHistory(string backend)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);
            const int totalOperations = 200;

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactEveryOperations: 0,
                compactNumberEntries: 10,
                maxEntriesPerCompaction: 100,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                await DriveCommittedOperationsAsync(
                    wal,
                    writeAhead,
                    totalOperations,
                    logsPerOperation: 1).ConfigureAwait(true);

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                Assert.Equal(0, writeAhead.CompactionPassCount);
                Assert.Equal(LastLogId(totalOperations, logsPerOperation: 1), wal.GetMaxLog(PartitionId));

                long lastCheckpoint = wal.GetLastCheckpoint(PartitionId);
                Assert.True(wal.CountRemovableLogs(PartitionId) >= totalOperations - 1);
                (_, int removableInOnePass) = wal.CompactLogsOlderThan(
                    PartitionId,
                    lastCheckpoint,
                    compactNumberEntries: totalOperations * 2);
                Assert.True(
                    removableInOnePass >= totalOperations - 1,
                    "Disabled mode should leave essentially all history below the checkpoint.");
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

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task BoundedGrowth_KeepsPaceWithRemovableBacklog(string backend)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);
            const int compactEveryOperations = 20;
            const int maxEntriesPerCompaction = 100;
            const int totalOperations = 200;

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactEveryOperations,
                compactNumberEntries: 10,
                maxEntriesPerCompaction,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                await DriveCommittedOperationsAsync(
                    wal,
                    writeAhead,
                    totalOperations,
                    logsPerOperation: 3).ConfigureAwait(true);

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                int removable = wal.CountRemovableLogs(PartitionId);
                Assert.True(
                    removable <= maxEntriesPerCompaction,
                    $"Removable backlog {removable} exceeded pass cap {maxEntriesPerCompaction}.");
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

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task BoundedGrowth_RecoverAfterCompactionReplaysCorrectly(string backend)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);
            const int totalOperations = 100;

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactEveryOperations: 10,
                compactNumberEntries: 10,
                maxEntriesPerCompaction: 100,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                await DriveCommittedOperationsAsync(
                    wal,
                    writeAhead,
                    totalOperations,
                    logsPerOperation: 1).ConfigureAwait(true);

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                long lastCheckpoint = wal.GetLastCheckpoint(PartitionId);
                Assert.True(lastCheckpoint > 0);

                List<RaftLog> tailBeforeRecover = wal.ReadLogsRange(PartitionId, lastCheckpoint);
                Assert.All(tailBeforeRecover, log => Assert.True(log.Id >= lastCheckpoint));
                Assert.Contains(
                    tailBeforeRecover,
                    log => log.Type == RaftLogType.CommittedCheckpoint && log.Id == lastCheckpoint);

                RaftWriteAhead recoveryWriteAhead = new(
                    manager,
                    _ => { },
                    partition,
                    wal);

                IReadOnlyList<RaftLog> restoredLogs = await recoveryWriteAhead.LoadRestoreLogsAsync().ConfigureAwait(true);
                await recoveryWriteAhead.CompleteRestoreAsync(restoredLogs).ConfigureAwait(true);
                long commitIndex = await recoveryWriteAhead.GetMaxLog().ConfigureAwait(true) + 1;

                Assert.True(commitIndex > lastCheckpoint);

                List<RaftLog> tailAfterRecover = wal.ReadLogsRange(PartitionId, lastCheckpoint);
                Assert.Equal(
                    tailBeforeRecover.Select(log => log.Id).ToArray(),
                    tailAfterRecover.Select(log => log.Id).ToArray());
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
    /// Simulates sustained commit load by writing directly to the WAL and notifying
    /// <see cref="RaftWriteAhead"/> — bypasses EnqueueCommit/state-machine completion.
    /// </summary>
    private static async Task DriveCommittedOperationsAsync(
        IWAL wal,
        RaftWriteAhead writeAhead,
        int totalOperations,
        int logsPerOperation)
    {
        long nextId = 1;

        for (int operation = 1; operation <= totalOperations; operation++)
        {
            List<RaftLog> batch = [];

            for (int logIndex = 0; logIndex < logsPerOperation; logIndex++)
            {
                batch.Add(CreateCommittedLog(nextId));
                nextId++;
            }

            batch.Add(CreateCheckpointLog(nextId));
            nextId++;

            Assert.Equal(
                RaftOperationStatus.Success,
                wal.Write(new List<(int, List<RaftLog>)> { (PartitionId, batch) }));

            writeAhead.NotifyCommitted();

            if (operation % 10 == 0)
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);
        }
    }

    private static int EntriesWrittenPerOperation(int logsPerOperation) => logsPerOperation + 1;

    private static long LastLogId(int totalOperations, int logsPerOperation) =>
        totalOperations * EntriesWrittenPerOperation(logsPerOperation);

    private static RaftWriteAhead CreateWriteAhead(
        IWAL wal,
        int compactEveryOperations,
        int compactNumberEntries,
        int maxEntriesPerCompaction,
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
            PartitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        return new RaftWriteAhead(manager, _ => { }, partition, wal);
    }

    private static IWAL CreateWal(string backend, string path) =>
        backend switch
        {
            "sqlite" => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance),
            "rocksdb" => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance),
            _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, "Unknown WAL backend."),
        };

    private static RaftLog CreateCommittedLog(long id)
    {
        return new()
        {
            Id = id,
            Term = id,
            Type = RaftLogType.Committed,
            LogType = "compaction-integration",
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
            LogType = "compaction-integration-checkpoint",
            LogData = [1, 2, 3],
        };
    }

    private static string CreateTempWalPath(string backend)
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-{backend}-wal-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
