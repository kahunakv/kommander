
using System.Text.Json;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.System.Protos;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Google.Protobuf;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Deterministic lifecycle tests for CreatePartition / RemovePartition (Tasks 2.5 / 2.6).
///
/// Abstract base runs against all three WAL backends (InMemory, SQLite, RocksDB) via
/// concrete subclasses at the bottom of the file.
/// All tests drive the coordinator directly (no real Raft quorum needed).
/// </summary>
public abstract class PartitionLifecycleTests
{
    // ── WAL factory ───────────────────────────────────────────────────────────

    /// <summary>Creates a fresh WAL instance. <paramref name="cleanup"/> releases any temp resources.</summary>
    protected abstract IWAL CreateWal(out Action cleanup);

    // ── Builders ──────────────────────────────────────────────────────────────

    private static RaftManager Build(IWAL wal)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0
        };
        return new(
            config,
            new StaticDiscovery([]),
            wal,
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] SerializeMessage(string key, string value)
    {
        RaftSystemMessage msg = new() { Key = key, Value = value };
        using MemoryStream ms = new();
        msg.WriteTo(ms);
        return ms.ToArray();
    }

    private static RaftSystemRequest MakeConfigReplicated(List<RaftPartitionRange> ranges, long mapVersion = 1) =>
        new(RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(
                RaftSystemConfigKeys.Partitions,
                JsonSerializer.Serialize(new RaftPartitionMap { MapVersion = mapVersion, Partitions = ranges })));

    private static RaftSystemRequest MakeConfigRestored(List<RaftPartitionRange> ranges, long mapVersion = 1) =>
        new(RaftSystemRequestType.ConfigRestored,
            SerializeMessage(
                RaftSystemConfigKeys.Partitions,
                JsonSerializer.Serialize(new RaftPartitionMap { MapVersion = mapVersion, Partitions = ranges })));

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    private static Task<(RaftOperationStatus Status, long Generation)> SendCreateAsync(
        RaftManager manager,
        int partitionId,
        RaftRoutingMode mode,
        int? start = null,
        int? end = null)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(partitionId, mode, start, end, tcs));
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static Task<(RaftOperationStatus Status, long Generation)> SendRemoveAsync(
        RaftManager manager,
        int partitionId)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(
            new RaftSystemRequest(RaftSystemRequestType.RemovePartition, partitionId) { Completion = tcs });
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Test 1 — Create an Unrouted partition, write a WAL log to it, read it back.
    /// Verifies that the new partition is live and its WAL is functional.
    /// </summary>
    [Fact]
    public async Task CreatePartition_ThenWriteLog_LogReadableFromWal()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal);
        try
        {
            // Bootstrap with one HashRange partition so IsInitialized can be set.
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 2));
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            (RaftOperationStatus status, long gen) = await SendCreateAsync(manager, partitionId: 10, RaftRoutingMode.Unrouted);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(1, gen);
            Assert.True(manager.Partitions.ContainsKey(10));

            // Write a log entry to the new partition's WAL.
            RaftOperationStatus writeStatus = wal.Write(
                [(10, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Proposed, LogType = "test" }])]);
            Assert.Equal(RaftOperationStatus.Success, writeStatus);

            // Log must be readable back.
            List<RaftLog> logs = wal.ReadLogs(10);
            Assert.Single(logs);
            Assert.Equal(1, logs[0].Id);
            Assert.Equal("test", logs[0].LogType);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Test 2 — Create a partition, write a log, then remove it.
    /// After removal: partition absent from manager dictionary and WAL reclaimed.
    /// </summary>
    [Fact]
    public async Task CreateThenRemove_PartitionAbsentFromManagerAndWalReclaimed()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal);
        try
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            int replicateCallCount = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCallCount++;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, replicateCallCount));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            // Create.
            (RaftOperationStatus createStatus, _) = await SendCreateAsync(manager, partitionId: 20, RaftRoutingMode.Unrouted);
            Assert.Equal(RaftOperationStatus.Success, createStatus);
            Assert.True(manager.Partitions.ContainsKey(20));

            // Write a WAL entry so we can verify reclamation.
            wal.Write([(20, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Proposed }])]);
            Assert.NotEmpty(wal.ReadLogs(20));

            // Remove.
            (RaftOperationStatus removeStatus, long removeGen) = await SendRemoveAsync(manager, partitionId: 20);
            Assert.Equal(RaftOperationStatus.Success, removeStatus);
            Assert.Equal(2, removeGen);

            // Partition gone from live dictionary.
            Assert.False(manager.Partitions.ContainsKey(20));

            // WAL reclaimed.
            Assert.Empty(wal.ReadLogs(20));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Test 4 — Crash-recovery: simulate a restart after the Removed tombstone was
    /// committed but before DeletePartitionWAL ran.
    ///
    /// The coordinator must re-attempt WAL reclamation when it processes ConfigRestored
    /// (or ConfigReplicated) and finds an entry with State = Removed.
    /// </summary>
    [Fact]
    public async Task RestartWithRemovedTombstone_WalReclaimedOnInitialization()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal);
        try
        {
            // Write WAL data for partition 5 (simulates data written before the crash).
            wal.Write([(5, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Proposed }])]);
            Assert.NotEmpty(wal.ReadLogs(5));

            // Simulate restart: the persisted map already has partition 5 as Removed
            // (tombstone was committed before the crash that prevented DeletePartitionWAL).
            List<RaftPartitionRange> recoveredMap =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 5, StartRange = 0, EndRange = 0,            Generation = 3, State = RaftPartitionState.Removed, RoutingMode = RaftRoutingMode.Unrouted }
            ];

            // ConfigRestored fires — coordinator calls InitializePartitions which must
            // re-attempt DeletePartitionWAL for the Removed tombstone.
            manager.SystemCoordinator.Send(MakeConfigRestored(recoveredMap, mapVersion: 3));

            // Then LeaderChanged so InitializePartitions fires again (same idempotent path).
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, "other-node:9001"));
            await WaitForIdleAsync(manager);

            // Partition 5 must NOT appear in the live dictionary (tombstone skipped by StartUserPartitions).
            Assert.False(manager.Partitions.ContainsKey(5));

            // WAL reclaimed by the recovery path inside InitializePartitions.
            Assert.Empty(wal.ReadLogs(5));

            // Partition 1 (Active) must be present and functional.
            Assert.True(manager.Partitions.ContainsKey(1));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Test 5 — Duplicate CreatePartition is idempotent: calling it twice returns the
    /// same generation and the map has exactly one entry for the partition.
    /// </summary>
    [Fact]
    public async Task CreatePartition_DuplicateCall_IdempotentSameGeneration_MapHasOneEntry()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal);
        try
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            int replicateCallCount = 0;
            List<RaftPartitionRange>? lastReplicatedRanges = null;
            manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
            {
                replicateCallCount++;
                using MemoryStream ms = new(data);
                RaftSystemMessage msg = RaftSystemMessage.Parser.ParseFrom(ms);
                RaftPartitionMap? m = JsonSerializer.Deserialize<RaftPartitionMap>(msg.Value);
                lastReplicatedRanges = m?.Partitions;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, replicateCallCount));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            // First call — creates the partition.
            (RaftOperationStatus status1, long gen1) = await SendCreateAsync(manager, partitionId: 30, RaftRoutingMode.Unrouted);
            Assert.Equal(RaftOperationStatus.Success, status1);
            Assert.Equal(1, gen1);
            Assert.Equal(1, replicateCallCount);

            // Second call — idempotent, must not replicate again.
            (RaftOperationStatus status2, long gen2) = await SendCreateAsync(manager, partitionId: 30, RaftRoutingMode.Unrouted);
            Assert.Equal(RaftOperationStatus.Success, status2);
            Assert.Equal(1, gen2);
            Assert.Equal(1, replicateCallCount); // no second replication

            // Map must have exactly one entry for partition 30.
            Assert.NotNull(lastReplicatedRanges);
            Assert.Equal(1, lastReplicatedRanges.Count(r => r.PartitionId == 30));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Removing a partition that does not exist returns Errored without replicating.
    /// Consistent guard across all WAL backends.
    /// </summary>
    [Fact]
    public async Task RemovePartition_NonExistent_ReturnsError_NothingReplicated()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal);
        try
        {
            List<RaftPartitionRange> initial =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ];
            manager.SystemCoordinator.Send(MakeConfigReplicated(initial));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, _) = await SendRemoveAsync(manager, partitionId: 999);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.False(replicateCalled);
        }
        finally { cleanup(); }
    }
}

// ── Concrete subclasses (one per WAL backend) ─────────────────────────────────

public sealed class InMemoryPartitionLifecycleTests : PartitionLifecycleTests
{
    protected override IWAL CreateWal(out Action cleanup)
    {
        cleanup = () => { };
        return new InMemoryWAL(NullLogger<IRaft>.Instance);
    }
}

public sealed class SqlitePartitionLifecycleTests : PartitionLifecycleTests
{
    protected override IWAL CreateWal(out Action cleanup)
    {
        string path = Path.Combine(Path.GetTempPath(), $"lifecycle-sqlite-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        cleanup = () =>
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        };
        return new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);
    }
}

public sealed class RocksDbPartitionLifecycleTests : PartitionLifecycleTests
{
    protected override IWAL CreateWal(out Action cleanup)
    {
        string path = Path.Combine(Path.GetTempPath(), $"lifecycle-rocksdb-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        cleanup = () =>
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        };
        return new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);
    }
}
