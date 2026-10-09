
using System.Text.Json;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.System.Protos;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Google.Protobuf;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// A partition id that was handed out once is never handed out again, even when the tombstone
/// that spent it is gone from the committed map.
///
/// <para>
/// The committed map carries <see cref="RaftPartitionMap.HighestPartitionIdEver"/>: a floor that
/// every id-minting path raises in the same log entry as the entry it protects, and that nothing
/// lowers. The allocator steps past it, and <c>TryCreatePartition</c> / <c>TrySplitPartition</c>
/// refuse any id at or below it whose entry is absent. The in-order install guard
/// (<see cref="TestSystemConfigApplyOrder"/>) stops the known way a tombstone gets lost; the
/// floor makes any future way harmless.
/// </para>
///
/// All tests use the coordinator-override harness (no real Raft quorum).
/// </summary>
public sealed class TestHighestPartitionIdEver
{
    private static RaftManager Build()
    {
        RaftManager manager = new(
            new RaftConfiguration { Host = "localhost", Port = 9000, InitialPartitions = 0 },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        return manager;
    }

    private static byte[] SerializeMessage(string key, string value)
    {
        RaftSystemMessage msg = new() { Key = key, Value = value };
        using MemoryStream ms = new();
        msg.WriteTo(ms);
        return ms.ToArray();
    }

    private static RaftSystemRequest MakeDelivery(RaftSystemRequestType type, RaftPartitionMap map, long logIndex) =>
        new(type, SerializeMessage(RaftSystemConfigKeys.Partitions, JsonSerializer.Serialize(map)))
        {
            LogIndex = logIndex
        };

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    private static Task<(RaftOperationStatus Status, long Generation)> SendCreateAsync(RaftManager manager, int partitionId)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(partitionId, RaftRoutingMode.Unrouted, null, null, tcs));
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static Task<(RaftOperationStatus Status, long Generation)> SendRemoveAsync(RaftManager manager, int partitionId)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(
            new RaftSystemRequest(RaftSystemRequestType.RemovePartition, partitionId) { Completion = tcs });
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static Task<(RaftOperationStatus Status, long Generation)> SendSplitAsync(RaftManager manager, int partitionId, RaftSplitPlan plan)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(partitionId, plan, tcs));
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// Routes replication back into the manager with no quorum, numbering commits from
    /// <paramref name="firstLogIndex"/>, and records every committed payload.
    /// </summary>
    private static List<byte[]> OverrideCoordinatorIo(RaftManager manager, long firstLogIndex = 101)
    {
        List<byte[]> committed = [];
        long nextLogIndex = firstLogIndex;

        manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
        {
            committed.Add(data);
            return Task.FromResult(
                new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, nextLogIndex++));
        };
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

        return committed;
    }

    private static RaftPartitionMap CommittedMap(byte[] payload)
    {
        RaftSystemMessage message = RaftSystemMessage.Parser.ParseFrom(payload);
        Assert.Equal(RaftSystemConfigKeys.Partitions, message.Key);
        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(message.Value);
        Assert.NotNull(map);
        return map;
    }

    private static RaftPartitionRange Active(int id, int start = 0, int end = 0, RaftRoutingMode mode = RaftRoutingMode.Unrouted) =>
        new() { PartitionId = id, StartRange = start, EndRange = end, Generation = 1, State = RaftPartitionState.Active, RoutingMode = mode };

    private static RaftPartitionMap SeedMap(int highestPartitionIdEver = 0, params RaftPartitionRange[] ranges) =>
        new() { MapVersion = 1, HighestPartitionIdEver = highestPartitionIdEver, Partitions = [..ranges] };

    // ── Unit: the allocator formula ──────────────────────────────────────────

    [Fact]
    public void NextAvailablePartitionId_TakesTheMaximumOfFloorAndEntries()
    {
        List<RaftPartitionRange> entries = [Active(1), Active(5)];

        Assert.Equal(6, RaftPartitionMap.NextAvailablePartitionId(entries));
        Assert.Equal(6, RaftPartitionMap.NextAvailablePartitionId(entries, highestPartitionIdEver: 3));
        Assert.Equal(10, RaftPartitionMap.NextAvailablePartitionId(entries, highestPartitionIdEver: 9));
        Assert.Equal(10, RaftPartitionMap.NextAvailablePartitionId([], highestPartitionIdEver: 9));
        Assert.Equal(RaftSystemConfig.SystemPartition + 1, RaftPartitionMap.NextAvailablePartitionId([]));
    }

    [Fact]
    public void RecordPartitionId_NeverLowersTheFloor()
    {
        RaftPartitionMap map = SeedMap(highestPartitionIdEver: 7);

        map.RecordPartitionId(3);
        Assert.Equal(7, map.HighestPartitionIdEver);

        map.RecordPartitionId(9);
        Assert.Equal(9, map.HighestPartitionIdEver);

        Assert.True(map.IsPartitionIdSpent(9));
        Assert.True(map.IsPartitionIdSpent(1));
        Assert.False(map.IsPartitionIdSpent(10));
    }

    /// <summary>
    /// The field is new. A map written before it existed must still load, with the floor off, and
    /// a map with the field must round-trip through the system-partition serializer.
    /// </summary>
    [Fact]
    public void Serialization_IsBackwardCompatible()
    {
        const string legacyJson = """{"MapVersion":4,"Partitions":[{"PartitionId":1,"StartRange":0,"EndRange":0,"Generation":1,"State":0,"RoutingMode":1,"Replicas":[],"ReplicationFactor":0}]}""";
        RaftPartitionMap? legacy = JsonSerializer.Deserialize(legacyJson, SystemJsonContext.Default.RaftPartitionMap);
        Assert.NotNull(legacy);
        Assert.Equal(0, legacy.HighestPartitionIdEver);
        Assert.Single(legacy.Partitions);

        RaftPartitionMap map = SeedMap(highestPartitionIdEver: 12, Active(1));
        string json = JsonSerializer.Serialize(map, SystemJsonContext.Default.RaftPartitionMap);
        Assert.Contains("\"HighestPartitionIdEver\":12", json);
        RaftPartitionMap? roundTripped = JsonSerializer.Deserialize(json, SystemJsonContext.Default.RaftPartitionMap);
        Assert.NotNull(roundTripped);
        Assert.Equal(12, roundTripped.HighestPartitionIdEver);
    }

    // ── Coordinator: minting raises the floor in the same entry ──────────────

    /// <summary>
    /// Every create commits the floor in the same log entry as the new partition, and a removal
    /// leaves the floor where it is.
    /// </summary>
    [Fact]
    public async Task Create_CommitsTheFloorWithTheEntry_AndRemoveKeepsIt()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(RaftSystemRequestType.ConfigReplicated, SeedMap(0, Active(1)), logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status2, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, status2);
        Assert.Equal(2, CommittedMap(committed[^1]).HighestPartitionIdEver);

        (RaftOperationStatus status5, _) = await SendCreateAsync(manager, 5);
        Assert.Equal(RaftOperationStatus.Success, status5);
        Assert.Equal(5, CommittedMap(committed[^1]).HighestPartitionIdEver);

        (RaftOperationStatus removeStatus, _) = await SendRemoveAsync(manager, 5);
        Assert.Equal(RaftOperationStatus.Success, removeStatus);
        RaftPartitionMap afterRemove = CommittedMap(committed[^1]);
        Assert.Equal(5, afterRemove.HighestPartitionIdEver);
        Assert.Contains(afterRemove.Partitions, r => r.PartitionId == 5 && r.State == RaftPartitionState.Removed);

        Assert.Equal(6, manager.GetNextAvailablePartitionId());
    }

    /// <summary>
    /// The hardening target: the committed map lost the tombstone of id 2 but still carries the
    /// floor. The allocator must step past 2, creating 2 must be refused, and the next mutation
    /// must keep the floor.
    /// </summary>
    [Fact]
    public async Task LostTombstone_IdStaysSpent_AllocatorStepsPastIt_AndCreateIsRefused()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        // Id 2 was handed out once; its entry is gone, the floor remains.
        manager.SystemCoordinator.Send(MakeDelivery(RaftSystemRequestType.ConfigReplicated, SeedMap(2, Active(1)), logIndex: 5));
        await WaitForIdleAsync(manager);

        Assert.Equal(3, manager.GetNextAvailablePartitionId());

        (RaftOperationStatus recreateStatus, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Errored, recreateStatus);
        Assert.Empty(committed);
        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == 2);

        (RaftOperationStatus nextStatus, _) = await SendCreateAsync(manager, manager.GetNextAvailablePartitionId());
        Assert.Equal(RaftOperationStatus.Success, nextStatus);
        RaftPartitionMap after = CommittedMap(committed[^1]);
        Assert.Equal(3, after.HighestPartitionIdEver);
        Assert.Contains(after.Partitions, r => r.PartitionId == 3);
        Assert.Equal(4, manager.GetNextAvailablePartitionId());
    }

    /// <summary>
    /// The floor alone must drive the allocator: a map whose only live entry sits far below the
    /// floor still allocates above the floor, on a follower-style delivery and after a restore.
    /// </summary>
    [Fact]
    public async Task FloorAboveEveryEntry_DrivesTheAllocator_OnDeliveryAndOnRestore()
    {
        using RaftManager manager = Build();
        OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(RaftSystemRequestType.ConfigReplicated, SeedMap(9, Active(1)), logIndex: 5));
        await WaitForIdleAsync(manager);
        Assert.Equal(10, manager.GetNextAvailablePartitionId());

        manager.SystemCoordinator.Send(MakeDelivery(RaftSystemRequestType.ConfigRestored, SeedMap(14, Active(1)), logIndex: 6));
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));
        await WaitForIdleAsync(manager);
        Assert.Equal(15, manager.GetNextAvailablePartitionId());
    }

    /// <summary>
    /// A map written before the field existed carries a floor of 0: an explicit id that no entry
    /// holds is still accepted, exactly as before, and the create raises the floor from there.
    /// </summary>
    [Fact]
    public async Task LegacyMapWithoutTheField_AcceptsAnyUnusedId_AndStartsTheFloor()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(RaftSystemRequestType.ConfigReplicated, SeedMap(0, Active(1), Active(5)), logIndex: 5));
        await WaitForIdleAsync(manager);
        Assert.Equal(6, manager.GetNextAvailablePartitionId());

        (RaftOperationStatus status, _) = await SendCreateAsync(manager, 3);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(3, CommittedMap(committed[^1]).HighestPartitionIdEver);
        Assert.Equal(6, manager.GetNextAvailablePartitionId());

        // From here on the floor is live: 2 was never handed out but sits below the floor and the
        // map cannot tell it from a lost tombstone, so it is refused.
        (RaftOperationStatus belowFloor, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Errored, belowFloor);
    }

    // ── Coordinator: split minting ───────────────────────────────────────────

    /// <summary>
    /// A split child is minted through the same floor: the automatic target lands above the floor,
    /// and the floor commits with the Phase 1 entry.
    /// </summary>
    [Fact]
    public async Task Split_AllocatesTheChildAboveTheFloor_AndCommitsTheFloor()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(
            RaftSystemRequestType.ConfigReplicated,
            SeedMap(9, Active(1, 0, 999, RaftRoutingMode.HashRange)),
            logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, _) = await SendSplitAsync(
            manager, 1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true });
        Assert.Equal(RaftOperationStatus.Success, status);

        RaftPartitionMap phase1 = CommittedMap(committed[0]);
        Assert.Contains(phase1.Partitions, r => r.PartitionId == 10 && r.State == RaftPartitionState.Splitting);
        Assert.Equal(10, phase1.HighestPartitionIdEver);
        Assert.Equal(11, manager.GetNextAvailablePartitionId());
    }

    /// <summary>An explicit split target at or below the floor is a spent id and is refused.</summary>
    [Fact]
    public async Task Split_RefusesASpentExplicitTarget()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(
            RaftSystemRequestType.ConfigReplicated,
            SeedMap(9, Active(1, 0, 999, RaftRoutingMode.HashRange)),
            logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, _) = await SendSplitAsync(
            manager, 1, new RaftSplitPlan { TargetPartitionId = 4, TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true });
        Assert.Equal(RaftOperationStatus.Errored, status);
        Assert.Empty(committed);
        Assert.Single(manager.GetPartitionMap());
        Assert.Equal(RaftPartitionState.Active, manager.GetPartitionMap()[0].State);
    }
}
