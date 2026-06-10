
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

    private static RaftManager Build(IWAL wal, int initialPartitions = 0)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = initialPartitions
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
    /// Test 0 — LeaderChanged followed immediately by CreatePartition in the same
    /// channel burst must succeed.
    ///
    /// Before the fix, TrySetInitialPartitions did not write to systemConfiguration
    /// after replication succeeded. Any coordinator operation already queued behind
    /// LeaderChanged (e.g. a CreatePartition sent in the same burst) would call
    /// systemConfiguration.TryGetValue, find nothing, and return Errored — because
    /// ConfigReplicated, which updates systemConfiguration, arrives asynchronously.
    ///
    /// After the fix, TrySetInitialPartitions writes to systemConfiguration before
    /// calling StartPartitions, so the queued CreatePartition finds the map immediately.
    /// </summary>
    [Fact]
    public async Task LeaderChanged_ThenCreatePartition_CreateSucceedsWithoutWaitingForConfigReplicated()
    {
        IWAL wal = CreateWal(out Action cleanup);
        // initialPartitions=1 so TrySetInitialPartitions produces a valid (non-empty) map
        // and DivideIntoRanges doesn't divide by zero with count=0.
        using RaftManager manager = Build(wal, initialPartitions: 1);
        try
        {
            int replicateCallCount = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCallCount++;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, replicateCallCount));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            // Send LeaderChanged (triggers TrySetInitialPartitions) and CreatePartition
            // in a single synchronous burst — both are in the channel before the loop
            // processes either, so ConfigReplicated has no opportunity to run between them.
            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> createTcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, "localhost:9000"));
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(10, RaftRoutingMode.Unrouted, null, null, createTcs));

            (RaftOperationStatus status, long gen) =
                await createTcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(manager.Partitions.ContainsKey(10));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Test 0b — TrySetInitialPartitions: NodeIsNotLeader must exit the retry loop
    /// immediately (one attempt only), not sleep and retry 9 times.
    ///
    /// The original code had the inner condition as != Success (always true), so
    /// NodeIsNotLeader would sleep RetryDelay × 9 then call Environment.Exit(1).
    /// The fix changes the inner condition to != NodeIsNotLeader, matching every other
    /// method in the file.  We cannot call Environment.Exit in a test, so we verify the
    /// observable side-effect: exactly one replication call was made (no retries).
    /// </summary>
    [Fact]
    public async Task TrySetInitialPartitions_NodeIsNotLeader_ExitsRetryLoopAfterOneAttempt()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal, initialPartitions: 1);
        try
        {
            int replicateCallCount = 0;
            manager.SystemCoordinator.RetryDelay = TimeSpan.Zero;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCallCount++;
                // Always return NodeIsNotLeader so the loop always hits the fast-abort path.
                return Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero, 0));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            // LeaderChanged triggers TrySetInitialPartitions.  With the bug, it would
            // sleep RetryDelay × 9 then exit.  With the fix, it exits after one attempt.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, "localhost:9000"));

            await WaitForIdleAsync(manager);

            // Exactly one replication call — no retry loop.
            Assert.Equal(1, replicateCallCount);
        }
        finally { cleanup(); }
    }

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
    /// Test 6 — CreatePartition with an inverted HashRange (start &gt; end) is rejected
    /// immediately with Errored. The inverted interval would pass the overlap check
    /// (it covers nothing) and silently corrupt the partition map, so the guard must
    /// fire before any replication.
    /// </summary>
    [Fact]
    public async Task CreatePartition_InvertedHashRange_IsRejectedWithError()
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

            // start=500, end=100 — inverted range
            (RaftOperationStatus status, _) = await SendCreateAsync(manager, partitionId: 42,
                RaftRoutingMode.HashRange, start: 500, end: 100);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.False(replicateCalled);
            Assert.False(manager.Partitions.ContainsKey(42));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Test 6b — RemovePartition fires OnPartitionMapChanged on the leader.
    /// Followers fire the event via ConfigReplicated → StartUserPartitions.  The leader
    /// must fire it too so routing-table consumers see the remove without a round-trip.
    /// The removed partition must not appear in the snapshot (StartUserPartitions skips
    /// Removed tombstones).
    /// </summary>
    [Fact]
    public async Task RemovePartition_FiresOnPartitionMapChanged_PartitionAbsentFromSnapshot()
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

            // Create partition 20.
            (RaftOperationStatus createStatus, _) = await SendCreateAsync(manager, partitionId: 20, RaftRoutingMode.Unrouted);
            Assert.Equal(RaftOperationStatus.Success, createStatus);

            // Subscribe to the event AFTER creation so we only capture the remove snapshot.
            List<IReadOnlyList<RaftPartitionRange>> snapshots = [];
            manager.OnPartitionMapChanged += snap => snapshots.Add(snap);

            // Remove partition 20.
            (RaftOperationStatus removeStatus, _) = await SendRemoveAsync(manager, partitionId: 20);
            Assert.Equal(RaftOperationStatus.Success, removeStatus);

            // Event must have fired exactly once for the remove.
            Assert.Single(snapshots);

            // The snapshot must not contain the removed partition.
            Assert.DoesNotContain(snapshots[0], r => r.PartitionId == 20);

            // Partition 1 must still be in the snapshot.
            Assert.Contains(snapshots[0], r => r.PartitionId == 1 && r.State == RaftPartitionState.Active);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Test 7 — RemovePartition on a Splitting partition is rejected with Errored.
    /// Removing a Splitting source would orphan the paired Splitting target permanently:
    /// crash-recovery in InitializePartitions looks for (Splitting, Splitting) pairs and
    /// would never find a match, leaving the target stuck in Splitting forever.
    ///
    /// P1 is the only Splitting partition in the map (P2 is Active). Crash-recovery
    /// iterates Splitting partitions = [P1] and looks for a second Splitting entry with
    /// EndRange+1 == P1.StartRange — none exists, so no SplitPartitionCommit is enqueued
    /// and P1 remains Splitting when the remove attempt arrives.
    /// </summary>
    [Fact]
    public async Task RemovePartition_SplittingPartition_IsRejectedWithError()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal);
        try
        {
            // P1 is solo Splitting — crash-recovery cannot find a pair, so no Phase 2 fires.
            List<RaftPartitionRange> splittingMap =
            [
                new() { PartitionId = 1, StartRange = 0,   EndRange = 499,          Generation = 2, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 2, StartRange = 500, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active,    RoutingMode = RaftRoutingMode.HashRange }
            ];

            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, 0));
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            manager.SystemCoordinator.Send(MakeConfigReplicated(splittingMap, mapVersion: 2));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, _) = await SendRemoveAsync(manager, partitionId: 1);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.False(replicateCalled);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Test 8 — RemovePartition on a Draining partition is rejected with Errored.
    /// Removing a Draining source bypasses the MergePartitionCommit phase entirely:
    /// the survivor never absorbs the source's hash range, leaving the range unowned.
    ///
    /// P2 is Draining HashRange [1000,1999], which is not adjacent to P1 [0,499].
    /// Crash-recovery adjacency requires EndRange+1 == StartRange; the gap means no
    /// viable survivor is found, so InitializePartitions logs a warning and skips P2.
    /// P2 remains Draining, and the RemovePartition guard fires as expected.
    /// </summary>
    [Fact]
    public async Task RemovePartition_DrainingPartition_IsRejectedWithError()
    {
        IWAL wal = CreateWal(out Action cleanup);
        using RaftManager manager = Build(wal);
        try
        {
            // P2 is Draining HashRange with a gap from P1 — crash-recovery finds no
            // adjacent Active HashRange partner and skips P2, leaving it in Draining state.
            List<RaftPartitionRange> drainingMap =
            [
                new() { PartitionId = 1, StartRange = 0,    EndRange = 499,  Generation = 1, State = RaftPartitionState.Active,   RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 2, StartRange = 1000, EndRange = 1999, Generation = 2, State = RaftPartitionState.Draining, RoutingMode = RaftRoutingMode.HashRange }
            ];

            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.Errored, HLCTimestamp.Zero, 0));
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            manager.SystemCoordinator.Send(MakeConfigReplicated(drainingMap, mapVersion: 2));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };

            (RaftOperationStatus status, _) = await SendRemoveAsync(manager, partitionId: 2);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.False(replicateCalled);
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
