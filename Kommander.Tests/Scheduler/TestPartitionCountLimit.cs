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
/// <see cref="RaftConfiguration.MaxPartitions"/> bounds how many data partitions the committed map
/// may hold. At the bound, a create of a new id and a split both answer
/// <see cref="RaftOperationStatus.PartitionLimitReached"/> before the map changes: nothing is
/// committed, the map version stays, and the id is not spent. A create of an id that is already
/// Active stays idempotent at the bound, and a removal frees a slot.
///
/// All tests use the coordinator-override harness (no real Raft quorum).
/// </summary>
public sealed class TestPartitionCountLimit
{
    private static RaftManager Build(int maxPartitions)
    {
        RaftManager manager = new(
            new RaftConfiguration { Host = "localhost", Port = 9000, InitialPartitions = 0, MaxPartitions = maxPartitions },
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

    private static RaftSystemRequest MakeDelivery(RaftPartitionMap map, long logIndex) =>
        new(RaftSystemRequestType.ConfigReplicated, SerializeMessage(RaftSystemConfigKeys.Partitions, JsonSerializer.Serialize(map)))
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

    private static List<byte[]> OverrideCoordinatorIo(RaftManager manager)
    {
        List<byte[]> committed = [];
        long nextLogIndex = 101;

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

    private static RaftPartitionRange Range(
        int id, RaftPartitionState state = RaftPartitionState.Active, int start = 0, int end = 0,
        RaftRoutingMode mode = RaftRoutingMode.Unrouted) =>
        new() { PartitionId = id, StartRange = start, EndRange = end, Generation = 1, State = state, RoutingMode = mode };

    private static RaftPartitionMap SeedMap(int highestPartitionIdEver, params RaftPartitionRange[] ranges) =>
        new() { MapVersion = 1, HighestPartitionIdEver = highestPartitionIdEver, Partitions = [..ranges] };

    // ── Unit: what counts ────────────────────────────────────────────────────

    /// <summary>
    /// Splitting and Draining entries still hold a WAL and elections, so they count. Removed
    /// tombstones and the system partition do not.
    /// </summary>
    [Fact]
    public void CountPartitions_CountsEveryLiveEntry_ExceptTheSystemPartition()
    {
        RaftPartitionMap map = SeedMap(
            6,
            Range(RaftSystemConfig.SystemPartition),
            Range(1),
            Range(2, RaftPartitionState.Splitting),
            Range(3, RaftPartitionState.Draining),
            Range(4, RaftPartitionState.Removed),
            Range(5));

        Assert.Equal(4, PartitionCountLimit.CountPartitions(map));
    }

    // ── Unit: startup validation ─────────────────────────────────────────────

    [Fact]
    public void Validate_RefusesANegativeBound()
    {
        RaftConfiguration config = new() { MaxPartitions = -1 };
        Assert.Throws<RaftException>(config.Validate);
    }

    /// <summary>The bootstrap layout is never refused at run time, so startup refuses one above the bound.</summary>
    [Fact]
    public void Validate_RefusesInitialPartitionsAboveTheBound()
    {
        RaftConfiguration above = new() { InitialPartitions = 5, MaxPartitions = 4 };
        RaftException ex = Assert.Throws<RaftException>(above.Validate);
        Assert.Contains("MaxPartitions", ex.Message);

        RaftConfiguration atBound = new() { InitialPartitions = 4, MaxPartitions = 4 };
        atBound.Validate();

        RaftConfiguration unbounded = new() { InitialPartitions = 500, MaxPartitions = 0 };
        unbounded.Validate();
    }

    // ── Coordinator: create ──────────────────────────────────────────────────

    /// <summary>
    /// At the bound, a new create commits nothing: no entry, no map version bump, and the id is not
    /// spent, so the allocator still offers it.
    /// </summary>
    [Fact]
    public async Task Create_AtTheBound_IsRefused_AndSpendsNothing()
    {
        using RaftManager manager = Build(maxPartitions: 2);
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(SeedMap(2, Range(1), Range(2)), logIndex: 5));
        await WaitForIdleAsync(manager);

        Assert.Equal(3, manager.GetNextAvailablePartitionId());

        (RaftOperationStatus status, long generation) = await SendCreateAsync(manager, 3);
        Assert.Equal(RaftOperationStatus.PartitionLimitReached, status);
        Assert.Equal(0, generation);
        Assert.Empty(committed);
        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == 3);
        Assert.Equal(3, manager.GetNextAvailablePartitionId());

        // A retry at the bound gets the same answer.
        (RaftOperationStatus again, _) = await SendCreateAsync(manager, 3);
        Assert.Equal(RaftOperationStatus.PartitionLimitReached, again);
        Assert.Empty(committed);
    }

    /// <summary>A create of an id that is already Active adds nothing, so it stays open at the bound.</summary>
    [Fact]
    public async Task Create_OfAnActiveId_StaysIdempotent_AtTheBound()
    {
        using RaftManager manager = Build(maxPartitions: 2);
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(SeedMap(2, Range(1), Range(2)), logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, long generation) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(1, generation);
        Assert.Empty(committed);
    }

    /// <summary>
    /// A removal frees a slot: the id refused at the bound is created afterwards, and the bound
    /// holds again once the map is full.
    /// </summary>
    [Fact]
    public async Task Remove_FreesASlot_ForTheRefusedId()
    {
        using RaftManager manager = Build(maxPartitions: 2);
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(SeedMap(2, Range(1), Range(2)), logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus refused, _) = await SendCreateAsync(manager, 3);
        Assert.Equal(RaftOperationStatus.PartitionLimitReached, refused);

        (RaftOperationStatus removeStatus, _) = await SendRemoveAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, removeStatus);

        (RaftOperationStatus created, _) = await SendCreateAsync(manager, 3);
        Assert.Equal(RaftOperationStatus.Success, created);
        RaftPartitionMap after = CommittedMap(committed[^1]);
        Assert.Contains(after.Partitions, r => r.PartitionId == 3 && r.State == RaftPartitionState.Active);
        Assert.Equal(3, after.HighestPartitionIdEver);

        (RaftOperationStatus full, _) = await SendCreateAsync(manager, 4);
        Assert.Equal(RaftOperationStatus.PartitionLimitReached, full);
    }

    /// <summary>With the bound off (0), creates are not limited.</summary>
    [Fact]
    public async Task Create_WithNoBound_IsNotLimited()
    {
        using RaftManager manager = Build(maxPartitions: 0);
        OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(SeedMap(1, Range(1)), logIndex: 5));
        await WaitForIdleAsync(manager);

        for (int id = 2; id <= 6; id++)
        {
            (RaftOperationStatus status, _) = await SendCreateAsync(manager, id);
            Assert.Equal(RaftOperationStatus.Success, status);
        }
    }

    // ── Coordinator: split ───────────────────────────────────────────────────

    /// <summary>
    /// A split adds a partition, so at the bound it is refused before the source range is touched:
    /// nothing commits, the source stays Active over its whole range, and the child id is not spent.
    /// </summary>
    [Fact]
    public async Task Split_AtTheBound_IsRefused_AndLeavesTheSourceUntouched()
    {
        using RaftManager manager = Build(maxPartitions: 1);
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(
            SeedMap(1, Range(1, start: 0, end: 999, mode: RaftRoutingMode.HashRange)),
            logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, _) = await SendSplitAsync(
            manager, 1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true });
        Assert.Equal(RaftOperationStatus.PartitionLimitReached, status);
        Assert.Empty(committed);

        RaftPartitionRange source = Assert.Single(manager.GetPartitionMap());
        Assert.Equal(RaftPartitionState.Active, source.State);
        Assert.Equal(0, source.StartRange);
        Assert.Equal(999, source.EndRange);
        Assert.Equal(1, source.Generation);
        Assert.Equal(2, manager.GetNextAvailablePartitionId());
    }

    /// <summary>Below the bound a split goes through as before.</summary>
    [Fact]
    public async Task Split_BelowTheBound_Succeeds()
    {
        using RaftManager manager = Build(maxPartitions: 2);
        List<byte[]> committed = OverrideCoordinatorIo(manager);

        manager.SystemCoordinator.Send(MakeDelivery(
            SeedMap(1, Range(1, start: 0, end: 999, mode: RaftRoutingMode.HashRange)),
            logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus status, _) = await SendSplitAsync(
            manager, 1, new RaftSplitPlan { TargetRoutingMode = RaftRoutingMode.HashRange, AutoCommit = true });
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Contains(CommittedMap(committed[0]).Partitions, r => r.PartitionId == 2);
    }
}
