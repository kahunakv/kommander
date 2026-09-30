using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
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
    public void SyncWritesCanBeDisabledForTestWal()
    {
        const int partitionId = 4;
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.False(wal.SyncWritesEnabled);

            Assert.Equal(
                RaftOperationStatus.Success,
                wal.Write(
                    new List<(int, List<RaftLog>)>
                    {
                        (partitionId, [CreateLog(id: 1)])
                    }
                )
            );

            Assert.Equal(1, Assert.Single(wal.ReadLogs(partitionId)).Id);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void CompactLogsOlderThanPass_RemovesUpToCapInSingleTransaction()
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
                                CreateLog(id: 5),
                                CreateLog(id: 6),
                                CreateLog(id: 7),
                                CreateLog(id: 8),
                                CreateLog(id: 9)
                            ]
                        )
                    }
                )
            );

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(
                7,
                lastCheckpoint: 10,
                compactNumberEntries: 2,
                maxTotalEntries: 9);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(9, removed);
            Assert.Equal(1, wal.LastCompactionCommitCount);
            Assert.Empty(wal.ReadLogs(7));
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

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(7, lastCheckpoint: 5, compactNumberEntries: 2);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(2, removed);

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
                    (RaftOperationStatus status, _) = wal.CompactLogsOlderThan(3, lastCheckpoint: id, compactNumberEntries: 5);
                    Assert.Equal(RaftOperationStatus.Success, status);
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

    // ── Sharding-specific tests ───────────────────────────────────────────────

    /// <summary>
    /// A fresh directory resolves shardCount=0 to Environment.ProcessorCount, persists that
    /// value, and returns it on reopen (even when the caller passes shardCount=0 again).
    /// </summary>
    [Fact]
    public void ShardCount_PersistsOnFirstOpen_IsRestoredOnReopen()
    {
        string path = CreateTempWalPath();
        try
        {
            int expected;
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 4))
            {
                expected = 4;
                // Write to force the metadata DB to be persisted.
                wal.Write([(1, [CreateLog(1)])]);
            }

            // Reopen with shardCount=0 (accept persisted) — data must still be readable.
            using SqliteWAL wal2 = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 0);
            Assert.Equal(1, Assert.Single(wal2.ReadLogs(1)).Id);

            // The persisted shard_count metadata must match what we seeded.
            Assert.Equal(expected.ToString(), wal2.GetMetaData("shard_count"));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Reopening with a non-zero shardCount that differs from the persisted value must throw
    /// with a message naming both values, preventing silent log orphaning.
    /// </summary>
    [Fact]
    public void ShardCount_ConflictingNonZeroOnReopen_ThrowsDescriptiveError()
    {
        string path = CreateTempWalPath();
        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 4))
                wal.Write([(1, [CreateLog(1)])]);

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 2));

            // The message should name both the configured and persisted values.
            Assert.Contains("2", ex.Message);
            Assert.Contains("4", ex.Message);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Reopening with the same non-zero shardCount as was seeded must succeed.
    /// </summary>
    [Fact]
    public void ShardCount_MatchingNonZeroOnReopen_Succeeds()
    {
        string path = CreateTempWalPath();
        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 3))
                wal.Write([(1, [CreateLog(1)])]);

            // Same count — must not throw.
            using SqliteWAL wal2 = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 3);
            Assert.Equal(1, Assert.Single(wal2.ReadLogs(1)).Id);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// When two partitions share a shard (shardCount=1), deleting one must leave the other's
    /// rows untouched and fully readable and writable.
    /// </summary>
    [Fact]
    public void DeletePartitionWAL_OnSharedShard_DoesNotAffectSiblingPartition()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            // Write to two partitions that share the single shard.
            wal.Write([(10, [CreateLog(1), CreateLog(2)]), (20, [CreateLog(1)])]);

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(10));

            // Partition 10 is gone.
            Assert.Empty(wal.ReadLogs(10));

            // Partition 20 is unaffected.
            Assert.Equal(1, Assert.Single(wal.ReadLogs(20)).Id);

            // Partition 20 can still be written to.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(20, [CreateLog(2)])]));
            Assert.Equal(2, wal.ReadLogs(20).Count);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A Write call covering P partitions all on the same shard (shardCount=1) must issue
    /// exactly one SQLite transaction regardless of partition count.
    /// </summary>
    [Fact]
    public void Write_MultiPartitionOnSingleShard_IssuesOneTransaction()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            List<(int, List<RaftLog>)> batch =
            [
                (1, [CreateLog(1)]),
                (2, [CreateLog(1)]),
                (3, [CreateLog(1)]),
                (5, [CreateLog(1)]),
            ];

            Assert.Equal(RaftOperationStatus.Success, wal.Write(batch));
            Assert.Equal(1, wal.LastWriteTransactionCount);

            // All partitions are readable.
            foreach (int p in new[] { 1, 2, 3, 5 })
                Assert.Single(wal.ReadLogs(p));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A Write call covering partitions spread across multiple shards issues exactly one
    /// transaction per shard, not one per partition.
    /// </summary>
    [Fact]
    public void Write_MultiPartitionAcrossShards_IssuesOneTransactionPerShard()
    {
        string path = CreateTempWalPath();
        try
        {
            // shardCount=2 — even partitions go to shard 0, odd to shard 1.
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 2);

            List<(int, List<RaftLog>)> batch =
            [
                (0, [CreateLog(1)]),  // shard 0
                (2, [CreateLog(1)]),  // shard 0
                (1, [CreateLog(1)]),  // shard 1
                (3, [CreateLog(1)]),  // shard 1
            ];

            Assert.Equal(RaftOperationStatus.Success, wal.Write(batch));

            // 4 partitions across 2 shards — should be 2 transactions.
            Assert.Equal(2, wal.LastWriteTransactionCount);

            foreach (int p in new[] { 0, 1, 2, 3 })
                Assert.Single(wal.ReadLogs(p));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Every entry point called after <see cref="SqliteWAL.Dispose"/> has a defined outcome and never
    /// touches a closed connection: reads throw <see cref="ObjectDisposedException"/>, status-returning
    /// calls report their failure value, and nothing reopens a shard or the metadata DB. Before the
    /// disposal fence, a late metadata read ran a command on the closed connection (the field was not
    /// cleared) and a late call on a never-opened shard created its file.
    /// </summary>
    [Fact]
    public void Dispose_IsIdempotent_AndLaterCallsFailWithoutReopening()
    {
        string path = CreateTempWalPath();

        try
        {
            ErrorCapturingLogger logger = new();
            SqliteWAL wal = new(path, "wal", logger, shardCount: 2);

            // Opens shard 0 and the metadata DB; shard 1 stays unopened.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(0, [CreateLog(id: 1)])]));
            Assert.True(wal.SetMetaData("key", "value"));

            wal.Dispose();
            wal.Dispose();

            Assert.Throws<ObjectDisposedException>(() => wal.ReadLogs(0));
            Assert.Throws<ObjectDisposedException>(() => wal.ReadLogsRange(0, 1, 10));
            Assert.Throws<ObjectDisposedException>(() => wal.GetTermAt(0, 1));
            Assert.Throws<ObjectDisposedException>(() => wal.GetCurrentTerm(0));
            Assert.Throws<ObjectDisposedException>(() => wal.GetLastCheckpoint(0));
            Assert.Throws<ObjectDisposedException>(() => wal.CountPersistedLogs(0));
            Assert.Throws<ObjectDisposedException>(() => wal.CountRemovableLogs(0));
            Assert.Throws<ObjectDisposedException>(() => wal.GetMetaData("key"));

            Assert.False(wal.SetMetaData("key", "late"));
            Assert.Equal(0, wal.GetMaxLog(0));
            Assert.Equal(RaftOperationStatus.Errored, wal.Write([(0, [CreateLog(id: 2)])]));
            Assert.Equal(RaftOperationStatus.Errored, wal.TruncateLogsAfter(0, 0));
            Assert.Equal(RaftOperationStatus.Errored, wal.TruncateProposedLogsAfter(0, 0));
            Assert.Equal((RaftOperationStatus.Errored, 0L), wal.TruncateLogsAfterAndGetMax(0, 0));
            Assert.Equal((RaftOperationStatus.Errored, false), wal.InstallSnapshotBoundary(0, 1, 1, sync: true));
            Assert.Equal((RaftOperationStatus.Errored, 0), wal.CompactLogsOlderThan(0, 1, 10));
            Assert.Equal(RaftOperationStatus.Errored, wal.DeletePartitionWAL(0));

            // A late call on a shard that was never opened must not create it.
            Assert.Throws<ObjectDisposedException>(() => wal.ReadLogs(1));
            Assert.Equal(RaftOperationStatus.Errored, wal.Write([(1, [CreateLog(id: 1)])]));
            Assert.False(File.Exists(Path.Combine(path, "raft_shard1_wal.db")));

            // Rejections after disposal are expected teardown traffic, not storage failures.
            Assert.Empty(logger.Errors);

            // Nothing written before disposal was lost, and the rejected late calls changed nothing.
            using SqliteWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance);
            Assert.Single(reopened.ReadLogs(0));
            Assert.Equal("value", reopened.GetMetaData("key"));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Regression for the Kahuna teardown NRE (<c>SqliteConnection.Close</c> inside
    /// <see cref="SqliteWAL.Dispose"/>): a host that still reads <see cref="IRaft.WalAdapter"/> while
    /// Raft is disposed. Workers hammer metadata and shard reads and writes while the WAL is disposed
    /// under them. Each call must either complete on an open connection or be rejected as disposed —
    /// never fault inside SQLite. Status-returning calls swallow storage exceptions into an error log,
    /// so a logged error fails the run too.
    /// </summary>
    [Fact]
    public void Dispose_RacingLiveCalls_CompletesOrRejectsButNeverFaultsInsideSqlite()
    {
        const int iterations = 40;

        for (int iteration = 0; iteration < iterations; iteration++)
            RunDisposeRace(iteration);
    }

    private static void RunDisposeRace(int iteration)
    {
        string path = CreateTempWalPath();

        try
        {
            ErrorCapturingLogger logger = new();
            SqliteWAL wal = new(path, "wal", logger, syncWrites: false, shardCount: 2);

            // Open both shards and the metadata DB so Dispose has live connections to close.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(0, [CreateLog(id: 1)]), (1, [CreateLog(id: 1)])]));
            Assert.True(wal.SetMetaData("key", "value"));

            Action<int>[] operations =
            [
                i => wal.GetMetaData("key"),
                i => wal.SetMetaData("key", i.ToString()),
                i => wal.ReadLogsRange(i % 2, 0, 16),
                i => wal.GetTermAt(i % 2, 1),
                i => wal.GetMaxLog(i % 2),
                i => wal.Write([(i % 2, [CreateLog(id: i + 2)])]),
            ];

            ConcurrentQueue<Exception> unexpected = new();
            using CountdownEvent warmedUp = new(operations.Length);
            int stop = 0;

            Thread[] workers = operations.Select(operation => new Thread(() =>
            {
                for (int i = 0; Volatile.Read(ref stop) == 0; i++)
                {
                    try
                    {
                        operation(i);
                    }
                    catch (ObjectDisposedException)
                    {
                        // The defined outcome for a read that arrives after disposal.
                    }
                    catch (Exception ex)
                    {
                        unexpected.Enqueue(ex);
                    }

                    if (i == 0)
                        warmedUp.Signal();
                }
            }) { IsBackground = true }).ToArray();

            foreach (Thread worker in workers)
                worker.Start();

            // Dispose only once every worker is live, so the close lands on calls in flight.
            Assert.True(warmedUp.Wait(TimeSpan.FromSeconds(10)), $"iteration {iteration}: workers did not start");
            wal.Dispose();

            // Let every worker also run against the disposed WAL for a while.
            Thread.Sleep(5);
            Volatile.Write(ref stop, 1);

            foreach (Thread worker in workers)
                Assert.True(worker.Join(TimeSpan.FromSeconds(10)), $"iteration {iteration}: worker did not stop");

            Assert.True(unexpected.IsEmpty, $"iteration {iteration}: {string.Join("\n", unexpected)}");
            Assert.True(logger.Errors.Count == 0, $"iteration {iteration}: {string.Join("\n", logger.Errors)}");
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>Records Error-level messages: the status-returning WAL calls report a storage exception only there.</summary>
    private sealed class ErrorCapturingLogger : ILogger<IRaft>
    {
        private readonly ConcurrentQueue<string> errors = new();

        public IReadOnlyCollection<string> Errors => errors.ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error)
                errors.Enqueue(formatter(state, exception));
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
