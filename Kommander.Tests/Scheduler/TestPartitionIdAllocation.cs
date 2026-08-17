
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
/// Tests for <see cref="IRaft.GetNextAvailablePartitionId"/> — the allocator's view of the
/// partition map.
///
/// <para>
/// The distinction under test: <see cref="IRaft.GetPartitionMap"/> hides entries in
/// <see cref="RaftPartitionState.Removed"/> because its callers route, place and back up, and must
/// never act on a retired range. An allocator needs the opposite: a removed entry is kept in the
/// map forever and <c>TryCreatePartition</c> refuses to recreate its id, so that id is spent and
/// must be stepped over. Picking <c>max(id) + 1</c> over the routing view reuses it and fails
/// permanently.
/// </para>
///
/// All tests use the coordinator-override harness (no real Raft quorum).
/// </summary>
public sealed class TestPartitionIdAllocation
{
    // ── Builder ───────────────────────────────────────────────────────────────

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

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    private static Task<(RaftOperationStatus Status, long Generation)> SendCreateAsync(
        RaftManager manager,
        int partitionId)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(partitionId, RaftRoutingMode.Unrouted, null, null, tcs));
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

    /// <summary>Routes replication and map application back into the manager, no quorum required.</summary>
    private static void OverrideCoordinatorIo(RaftManager manager)
    {
        int replicateCallCount = 0;
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
        {
            replicateCallCount++;
            return Task.FromResult(
                new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, replicateCallCount));
        };
        manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// A tombstone holding the highest id still moves the allocator past it, even though the
    /// routing view no longer reports that range at all.
    /// </summary>
    [Fact]
    public async Task TombstonedId_IsSkippedByAllocator_WhileRoutingViewHidesIt()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 3, State = RaftPartitionState.Active,  RoutingMode = RaftRoutingMode.HashRange },
            new() { PartitionId = 3, StartRange = 0, EndRange = 0,            Generation = 2, State = RaftPartitionState.Removed, RoutingMode = RaftRoutingMode.Unrouted }
        ]));
        await WaitForIdleAsync(manager);

        // The routing view sees only the live range...
        IReadOnlyList<RaftPartitionRange> map = manager.GetPartitionMap();
        Assert.Single(map);
        Assert.DoesNotContain(map, r => r.PartitionId == 3);

        // ...while the allocator steps past the retired one instead of handing id 3 back out.
        Assert.Equal(4, manager.GetNextAvailablePartitionId());
    }

    /// <summary>
    /// The end-to-end shape of the bug this exists to prevent: create a partition, retire it, and
    /// the next allocation must not land back on the retired id — because recreating it is refused.
    /// </summary>
    [Fact]
    public async Task CreateThenRemove_AllocatorStaysPastRetiredId_AndRecreateIsRefused()
    {
        using RaftManager manager = Build();

        manager.SystemCoordinator.Send(MakeConfigReplicated(
        [
            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
        ]));
        await WaitForIdleAsync(manager);

        Assert.Equal(2, manager.GetNextAvailablePartitionId());

        OverrideCoordinatorIo(manager);

        int allocated = manager.GetNextAvailablePartitionId();
        (RaftOperationStatus createStatus, _) = await SendCreateAsync(manager, allocated);
        Assert.Equal(RaftOperationStatus.Success, createStatus);
        Assert.Equal(allocated + 1, manager.GetNextAvailablePartitionId());

        (RaftOperationStatus removeStatus, _) = await SendRemoveAsync(manager, allocated);
        Assert.Equal(RaftOperationStatus.Success, removeStatus);

        // The retired id is gone from routing but still spent: the allocator must not offer it again.
        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == allocated);
        Assert.Equal(allocated + 1, manager.GetNextAvailablePartitionId());

        // And this is why — the id can never be recreated.
        (RaftOperationStatus recreateStatus, _) = await SendCreateAsync(manager, allocated);
        Assert.Equal(RaftOperationStatus.Errored, recreateStatus);

        // Whereas the id the allocator does offer is free to create.
        (RaftOperationStatus nextStatus, _) = await SendCreateAsync(manager, manager.GetNextAvailablePartitionId());
        Assert.Equal(RaftOperationStatus.Success, nextStatus);
    }

    /// <summary>
    /// A host that never applied a committed map answers from its hosted partitions, mirroring the
    /// fallback <see cref="IRaft.GetPartitionMap"/> uses for the same harness shape.
    /// </summary>
    [Fact]
    public void NoCommittedMap_FallsBackToHostedPartitions()
    {
        using RaftManager manager = Build();

        manager.Partitions.TryAdd(1, new RaftPartition(
            manager, manager.WalAdapter, partitionId: 1, startRange: 0, endRange: 0, NullLogger<IRaft>.Instance));
        manager.Partitions.TryAdd(7, new RaftPartition(
            manager, manager.WalAdapter, partitionId: 7, startRange: 0, endRange: 0, NullLogger<IRaft>.Instance));

        Assert.Equal(8, manager.GetNextAvailablePartitionId());
    }

    /// <summary>
    /// With nothing allocated at all the answer is still a usable id: the system partition (0) is
    /// reserved and can never be created.
    /// </summary>
    [Fact]
    public void EmptyMap_NeverOffersTheSystemPartitionId()
    {
        using RaftManager manager = Build();

        Assert.Equal(RaftSystemConfig.SystemPartition + 1, manager.GetNextAvailablePartitionId());
    }
}
