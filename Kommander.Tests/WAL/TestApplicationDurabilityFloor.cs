
using System.Collections.Concurrent;
using System.Text.Json;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Tests for the application-durability floor (<see cref="IApplicationDurabilityProvider"/>):
/// restart replay must widen below the last checkpoint down to the floor (redelivering committed
/// entries the application has not durably applied), and WAL compaction must never truncate
/// entries above the floor — closing the kill-between-checkpoint-and-flush data-loss window.
/// Uses the durable backends (SQLite/RocksDB) because only they anchor <c>ReadLogs</c> at the
/// checkpoint; <see cref="InMemoryWAL"/> always returns the full log, so the floor is moot there.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestApplicationDurabilityFloor
{
    private const int PartitionId = 1;

    /// <summary>
    /// Mutable provider stub: <c>-1</c> models "no opinion"; any other value is the durably
    /// applied index the application would read from its own storage at restart.
    /// </summary>
    private sealed class TestDurabilityProvider : IApplicationDurabilityProvider
    {
        public long DurablyAppliedIndex { get; set; } = -1;

        public long GetDurablyAppliedIndex(int partitionId) => DurablyAppliedIndex;
    }

    // ── Restore: replay widens down to the floor ─────────────────────────────

    /// <summary>
    /// Committed entries between the durability floor and the checkpoint must be redelivered via
    /// <c>OnLogRestored</c>, in log order, before the post-checkpoint tail — exactly the committed
    /// data set in (floor, commitFrontier], checkpoint excluded.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Restore_FloorBelowCheckpoint_RedeliversEntriesAboveFloor(string backend)
    {
        await RunRestoreCaseAsync(
            backend,
            durablyAppliedIndex: 5,
            expectedRestoredIds: [6, 7, 8, 9, 10, 12, 13, 14, 15]);
    }

    /// <summary>
    /// No provider (and a provider returning -1, "no opinion") must keep the checkpoint-anchored
    /// replay byte-for-byte: only entries above the checkpoint are delivered.
    /// </summary>
    [Theory]
    [InlineData("sqlite", false)]
    [InlineData("rocksdb", false)]
    [InlineData("sqlite", true)]
    [InlineData("rocksdb", true)]
    public async Task Restore_NullOrNoOpinionProvider_KeepsCheckpointAnchoredReplay(string backend, bool noOpinionProvider)
    {
        await RunRestoreCaseAsync(
            backend,
            durablyAppliedIndex: noOpinionProvider ? -1 : null,
            expectedRestoredIds: [12, 13, 14, 15]);
    }

    /// <summary>
    /// A stale-low floor (crash between the application's data flush and its floor record) means
    /// already-applied entries are redelivered. That is the documented contract: no crash, full
    /// in-order delivery, and an intact commit frontier.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Restore_StaleLowFloor_RedeliversAlreadyAppliedEntries(string backend)
    {
        await RunRestoreCaseAsync(
            backend,
            durablyAppliedIndex: 0,
            expectedRestoredIds: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15]);
    }

    /// <summary>
    /// A floor exactly at the checkpoint keeps the checkpoint-anchored read; the delivery filter
    /// has nothing to remove because everything above the checkpoint is above the floor too.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Restore_FloorAtCheckpoint_KeepsCheckpointAnchoredReplay(string backend)
    {
        await RunRestoreCaseAsync(
            backend,
            durablyAppliedIndex: 11,
            expectedRestoredIds: [12, 13, 14, 15]);
    }

    // ── Restore: the soft checkpoint narrows replay above the hard checkpoint ─

    /// <summary>
    /// A floor above the checkpoint is a soft checkpoint: only committed entries above the floor
    /// are redelivered, and the commit frontier still reconstructs to the full tail (the floor
    /// certifies its prefix like a checkpoint entry would).
    /// </summary>
    [Theory]
    [InlineData("sqlite", 12, new long[] { 13, 14, 15 })]
    [InlineData("rocksdb", 12, new long[] { 13, 14, 15 })]
    [InlineData("sqlite", 14, new long[] { 15 })]
    [InlineData("rocksdb", 14, new long[] { 15 })]
    [InlineData("sqlite", 15, new long[] { })]
    [InlineData("rocksdb", 15, new long[] { })]
    public async Task Restore_FloorAboveCheckpoint_NarrowsReplayToTailAboveFloor(string backend, long floor, long[] expectedRestoredIds)
    {
        await RunRestoreCaseAsync(
            backend,
            durablyAppliedIndex: floor,
            expectedRestoredIds: expectedRestoredIds);
    }

    /// <summary>
    /// The compaction-blocked shape from the bank-soak incident: no checkpoint exists at all (the
    /// hard-checkpoint floor never advanced) while the application durably applied a long prefix.
    /// Replay must start at the floor instead of index 1 — this is exactly the case that made
    /// cold-restart time proportional to workload runtime.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Restore_NoCheckpointWithFloor_NarrowsReplayToTailAboveFloor(string backend)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);

            TestDurabilityProvider provider = new() { DurablyAppliedIndex = 10 };

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                provider,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                await partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                // Committed 1..15, never checkpointed.
                List<RaftLog> batch = [];
                for (long id = 1; id <= 15; id++)
                    batch.Add(CreateCommittedLog(id));
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(PartitionId, batch)]));

                List<long> restoredIds = [];
                manager.OnLogRestored += (_, log) =>
                {
                    restoredIds.Add(log.Id);
                    return Task.FromResult(true);
                };

                IReadOnlyList<RaftLog> logs = await writeAhead.LoadRestoreLogsAsync().ConfigureAwait(true);

                // The read itself was narrowed — the durably-applied prefix is not even loaded.
                Assert.Equal([11L, 12L, 13L, 14L, 15L], logs.Select(log => log.Id).ToArray());

                await writeAhead.CompleteRestoreAsync(logs).ConfigureAwait(true);

                Assert.Equal([11L, 12L, 13L, 14L, 15L], restoredIds);

                // The soft floor seeds the frontier scan, so the frontier is intact.
                Assert.Equal(15, writeAhead.GetCommitIndex());
                Assert.Equal(15, writeAhead.GetPresentIndex());
                Assert.Equal(1, writeAhead.GetPresentTerm());
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
    /// When the WAL holds no entry at the floor (here: the floor points into a hole), narrowing is
    /// abandoned — the read stays conservative and the frontier reconstructs from what the WAL
    /// actually holds — but already-applied committed entries are still not redelivered.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Restore_FloorEntryMissingFromWal_FallsBackToFullReadWithDeliverySkip(string backend)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);

            TestDurabilityProvider provider = new() { DurablyAppliedIndex = 6 };

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                provider,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                await partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                // Committed 1..5 and 7..8: id 6 — exactly the floor — is a hole.
                List<RaftLog> batch = [];
                for (long id = 1; id <= 5; id++)
                    batch.Add(CreateCommittedLog(id));
                batch.Add(CreateCommittedLog(7));
                batch.Add(CreateCommittedLog(8));
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(PartitionId, batch)]));

                List<long> restoredIds = [];
                manager.OnLogRestored += (_, log) =>
                {
                    restoredIds.Add(log.Id);
                    return Task.FromResult(true);
                };

                IReadOnlyList<RaftLog> logs = await writeAhead.LoadRestoreLogsAsync().ConfigureAwait(true);

                // Fallback: the full (unnarrowed) read.
                Assert.Equal([1L, 2L, 3L, 4L, 5L, 7L, 8L], logs.Select(log => log.Id).ToArray());

                await writeAhead.CompleteRestoreAsync(logs).ConfigureAwait(true);

                // Entries at or below the floor are still not redelivered; 7 and 8 sit above the
                // floor but also above the reconstructed frontier (the hole at 6 stops the
                // contiguous prefix), so they are deferred to leader re-supply as before.
                Assert.Empty(restoredIds);
                Assert.Equal(5, writeAhead.GetCommitIndex());
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
    /// Seeds committed entries 1..10, a checkpoint at 11 and committed entries 12..15, then
    /// restores through a fresh <see cref="RaftWriteAhead"/> with the given provider floor and
    /// asserts the exact ordered <c>OnLogRestored</c> delivery and the reconstructed frontier.
    /// <paramref name="durablyAppliedIndex"/> null means "no provider configured".
    /// </summary>
    private static async Task RunRestoreCaseAsync(string backend, long? durablyAppliedIndex, long[] expectedRestoredIds)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);

            TestDurabilityProvider? provider = durablyAppliedIndex is null
                ? null
                : new TestDurabilityProvider { DurablyAppliedIndex = durablyAppliedIndex.Value };

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                provider,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                // Constructing RaftPartition spins the executor, which auto-restores the (still
                // empty) WAL through its own RaftWriteAhead. Wait it out, then seed — so the only
                // OnLogRestored deliveries observed below come from the restore under test.
                await partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                SeedCheckpointedLog(wal);

                List<long> restoredIds = [];
                manager.OnLogRestored += (_, log) =>
                {
                    restoredIds.Add(log.Id);
                    return Task.FromResult(true);
                };

                IReadOnlyList<RaftLog> logs = await writeAhead.LoadRestoreLogsAsync().ConfigureAwait(true);
                await writeAhead.CompleteRestoreAsync(logs).ConfigureAwait(true);

                Assert.Equal(expectedRestoredIds, restoredIds);

                // The floor only widens delivery below the frontier; frontier reconstruction is
                // unchanged (highest committed id is 15 in every variant).
                Assert.Equal(15, writeAhead.GetCommitIndex());
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

    // ── Compaction: the floor fences truncation and releases as it advances ──

    /// <summary>
    /// A compaction pass with a provider floor below the checkpoint must truncate nothing above
    /// the floor (only the durably-applied prefix id &lt;= floor is removable), stay fenced while
    /// the floor is stalled, and resume normally once the floor advances. RaftWriteAhead compaction
    /// is role-agnostic — this same pass runs on leaders and followers.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Compaction_FloorBelowCheckpoint_FencesTruncationUntilFloorAdvances(string backend)
    {
        string path = CreateTempWalPath(backend);

        try
        {
            IWAL wal = CreateWal(backend, path);

            // Committed 1..20 with a checkpoint at 21: without a floor, everything below 21 is removable.
            List<RaftLog> batch = [];
            for (long id = 1; id <= 20; id++)
                batch.Add(CreateCommittedLog(id));
            batch.Add(CreateCheckpointLog(21));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(PartitionId, batch)]));

            TestDurabilityProvider provider = new() { DurablyAppliedIndex = 3 };

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                provider,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                // Only the durably-applied prefix (1..3) is gone; 4..21 survive despite the checkpoint.
                Assert.Equal(
                    Enumerable.Range(4, 18).Select(i => (long)i).ToArray(),
                    wal.ReadLogsRange(PartitionId, 0).Select(log => log.Id).ToArray());

                // A second pass at the same floor is fully blocked: nothing further is removed.
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);
                Assert.Equal(18, wal.ReadLogsRange(PartitionId, 0).Count);

                // Floor advances (the application flushed through 20): compaction resumes and
                // drains to the checkpoint like the floor was never there.
                provider.DurablyAppliedIndex = 20;
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                Assert.Equal(
                    [21L],
                    wal.ReadLogsRange(PartitionId, 0).Select(log => log.Id).ToArray());
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

    // ── P0: system-config snapshot delivery order with a widened replay ──────

    /// <summary>
    /// On the system partition, a durability floor below the checkpoint replays pre-checkpoint
    /// config deltas <em>before</em> the checkpoint's embedded snapshot, and deltas above it after
    /// — so values are overwritten in commit order and the final roster is the newest one, exactly
    /// as with checkpoint-anchored replay.
    /// </summary>
    [Fact]
    public async Task Restore_P0FloorBelowCheckpoint_PreservesSnapshotDeliveryOrder()
    {
        const int partitionId = RaftSystemConfig.SystemPartition;

        string path = CreateTempWalPath("sqlite");

        try
        {
            IWAL wal = new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance);

            // Delta v2 below the checkpoint, snapshot v3 in the checkpoint, delta v4 above it.
            ConcurrentDictionary<string, string> snapshotConfig = new();
            snapshotConfig[RaftSystemConfigKeys.Members] = JsonSerializer.Serialize(CreateRoster(3));
            byte[]? snapshotPayload = RaftSystemCoordinatorHelpers.SerializeCheckpointSnapshot(snapshotConfig);
            Assert.NotNull(snapshotPayload);

            TestDurabilityProvider provider = new() { DurablyAppliedIndex = 39 };

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                provider,
                out RaftManager manager,
                out RaftPartition partition,
                partitionId: partitionId);

            try
            {
                // Let the partition's own (empty-WAL) auto-restore finish before seeding, so only
                // the restore under test delivers entries to the coordinator.
                await partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                // The sparse prefix below 40 models a compacted partition. In production the floor
                // was recorded before compaction removed the rows; a plain checkpoint write over
                // the absent prefix no longer records it, so install the boundary the way a real
                // compacted partition earned it (the row's snapshot payload is upserted below).
                Assert.Equal(
                    RaftOperationStatus.Success,
                    wal.InstallSnapshotBoundary(partitionId, snapshotIndex: 42, lastIncludedTerm: 5, sync: true).Status);

                List<RaftLog> logs =
                [
                    new()
                    {
                        Id = 40, Term = 5, Type = RaftLogType.Committed,
                        LogType = RaftSystemConfig.RaftLogType,
                        LogData = SerializeMembersDelta(CreateRoster(2)),
                    },
                    new()
                    {
                        Id = 42, Term = 5, Type = RaftLogType.CommittedCheckpoint,
                        LogType = RaftSystemConfig.CheckpointLogType,
                        LogData = snapshotPayload,
                    },
                    new()
                    {
                        Id = 43, Term = 5, Type = RaftLogType.Committed,
                        LogType = RaftSystemConfig.RaftLogType,
                        LogData = SerializeMembersDelta(CreateRoster(4)),
                    },
                ];
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, logs)]));

                IReadOnlyList<RaftLog> restoreLogs = await writeAhead.LoadRestoreLogsAsync().ConfigureAwait(true);

                // The widened read really did start below the checkpoint.
                Assert.Equal([40L, 42L, 43L], restoreLogs.Select(log => log.Id).ToArray());

                await writeAhead.CompleteRestoreAsync(restoreLogs).ConfigureAwait(true);

                await manager.SystemCoordinator.DrainAsync()
                    .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                // Commit-order overwrite: v2 (pre-checkpoint delta) → v3 (snapshot) → v4 (tail delta).
                Assert.Equal(4, manager.GetMembership().MembershipVersion);
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
    /// The system partition is never narrowed by the soft checkpoint: the coordinator's roster and
    /// partition map exist only in memory and are rebuilt from replay, and the application floor
    /// certifies nothing about them. With a floor above every entry, the checkpoint-anchored read
    /// and the full system replay must both survive unchanged.
    /// </summary>
    [Fact]
    public async Task Restore_P0FloorAboveCheckpoint_StillReplaysSystemEntries()
    {
        const int partitionId = RaftSystemConfig.SystemPartition;

        string path = CreateTempWalPath("sqlite");

        try
        {
            IWAL wal = new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance);

            ConcurrentDictionary<string, string> snapshotConfig = new();
            snapshotConfig[RaftSystemConfigKeys.Members] = JsonSerializer.Serialize(CreateRoster(3));
            byte[]? snapshotPayload = RaftSystemCoordinatorHelpers.SerializeCheckpointSnapshot(snapshotConfig);
            Assert.NotNull(snapshotPayload);

            // The application durably applied everything — irrelevant to coordinator replay.
            TestDurabilityProvider provider = new() { DurablyAppliedIndex = 43 };

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                provider,
                out RaftManager manager,
                out RaftPartition partition,
                partitionId: partitionId);

            try
            {
                await partition.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                // As above: the compacted prefix's floor is earned through the boundary install; a
                // plain checkpoint write over the absent prefix no longer records it.
                Assert.Equal(
                    RaftOperationStatus.Success,
                    wal.InstallSnapshotBoundary(partitionId, snapshotIndex: 42, lastIncludedTerm: 5, sync: true).Status);

                List<RaftLog> logs =
                [
                    new()
                    {
                        Id = 42, Term = 5, Type = RaftLogType.CommittedCheckpoint,
                        LogType = RaftSystemConfig.CheckpointLogType,
                        LogData = snapshotPayload,
                    },
                    new()
                    {
                        Id = 43, Term = 5, Type = RaftLogType.Committed,
                        LogType = RaftSystemConfig.RaftLogType,
                        LogData = SerializeMembersDelta(CreateRoster(4)),
                    },
                ];
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, logs)]));

                IReadOnlyList<RaftLog> restoreLogs = await writeAhead.LoadRestoreLogsAsync().ConfigureAwait(true);

                // Not narrowed: the checkpoint-anchored read still returns the checkpoint and the
                // system delta at 43 even though both sit at or below the floor.
                Assert.Equal([42L, 43L], restoreLogs.Select(log => log.Id).ToArray());

                await writeAhead.CompleteRestoreAsync(restoreLogs).ConfigureAwait(true);

                await manager.SystemCoordinator.DrainAsync()
                    .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                // Snapshot v3 then delta v4 both replayed: the coordinator sees the newest roster.
                Assert.Equal(4, manager.GetMembership().MembershipVersion);
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

    private static ClusterMembership CreateRoster(long version) => new()
    {
        MembershipVersion = version,
        Members =
        [
            new() { Endpoint = "localhost:9700", NodeId = 1, Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
            new() { Endpoint = "localhost:9701", NodeId = 2, Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
        ],
    };

    private static byte[] SerializeMembersDelta(ClusterMembership roster) =>
        RaftSystemCoordinatorHelpers.Serialize(new global::Kommander.System.Protos.RaftSystemMessage
        {
            Key = RaftSystemConfigKeys.Members,
            Value = JsonSerializer.Serialize(roster),
        });

    // ── Shared scaffolding ───────────────────────────────────────────────────

    /// <summary>Committed 1..10, checkpoint 11, committed 12..15 — one durable batch.</summary>
    private static void SeedCheckpointedLog(IWAL wal)
    {
        List<RaftLog> batch = [];

        for (long id = 1; id <= 10; id++)
            batch.Add(CreateCommittedLog(id));

        batch.Add(CreateCheckpointLog(11));

        for (long id = 12; id <= 15; id++)
            batch.Add(CreateCommittedLog(id));

        Assert.Equal(RaftOperationStatus.Success, wal.Write([(PartitionId, batch)]));
    }

    private static RaftWriteAhead CreateWriteAhead(
        IWAL wal,
        IApplicationDurabilityProvider? provider,
        out RaftManager manager,
        out RaftPartition partition,
        int partitionId = PartitionId)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9700,
            InitialPartitions = 0,
            CompactEveryOperations = 0,
            CompactNumberEntries = 1000,
            MaxEntriesPerCompaction = 1000,
            ApplicationDurabilityProvider = provider,
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

    private static IWAL CreateWal(string backend, string path) =>
        backend switch
        {
            "sqlite" => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance),
            "rocksdb" => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance),
            _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, "Unknown WAL backend."),
        };

    private static RaftLog CreateCommittedLog(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Committed,
        LogType = "durability-floor-test",
        LogData = [1, 2, 3],
    };

    private static RaftLog CreateCheckpointLog(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.CommittedCheckpoint,
        LogType = "durability-floor-test-checkpoint",
        LogData = [1, 2, 3],
    };

    private static string CreateTempWalPath(string backend)
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-{backend}-durability-floor-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
