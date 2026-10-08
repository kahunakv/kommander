
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
/// The system coordinator installs each committed system entry at most once and never lets an
/// older entry overwrite a newer one.
///
/// <para>
/// Two deliveries reach <c>systemConfiguration</c> for every entry the leader commits: the handler
/// installs the entry as soon as its proposal commits, and the log applicator delivers the same
/// entry again as <see cref="RaftSystemRequestType.ConfigReplicated"/>. A handler can hold the
/// coordinator loop for a long time, so that second delivery can arrive after later entries were
/// installed. Installing it then moves the local map backwards, and the next handler rewrites the
/// whole map from the stale base, discarding every entry committed in between — a partition
/// tombstone among them, which hands a spent partition id out a second time.
/// </para>
///
/// All tests use the coordinator-override harness (no real Raft quorum).
/// </summary>
public sealed class TestSystemConfigApplyOrder
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

    private static RaftSystemRequest MakeConfigReplicated(List<RaftPartitionRange> ranges, long mapVersion, long logIndex) =>
        new(RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(
                RaftSystemConfigKeys.Partitions,
                JsonSerializer.Serialize(new RaftPartitionMap { MapVersion = mapVersion, Partitions = ranges })))
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

    /// <summary>
    /// Routes replication back into the manager with no quorum. Each commit gets the next log
    /// index above <paramref name="firstLogIndex"/>, and the committed payloads are recorded so a
    /// test can replay one of them the way the log applicator would.
    /// </summary>
    private static List<byte[]> OverrideCoordinatorIo(RaftManager manager, long firstLogIndex)
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

    private static List<RaftPartitionRange> SeedRanges() =>
    [
        new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
    ];

    /// <summary>
    /// The exact shape of the production failure: create a partition, remove it, then receive the
    /// applicator's late delivery of the create entry. The tombstone must survive, the allocator
    /// must step past the id, and a second creation of the id must be refused.
    /// </summary>
    [Fact]
    public async Task LateDeliveryOfOlderEntry_KeepsTheTombstoneAndTheAllocatorPastIt()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideCoordinatorIo(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(SeedRanges(), mapVersion: 1, logIndex: 5));
        await WaitForIdleAsync(manager);
        Assert.Equal(5, manager.SystemCoordinator.AppliedSystemLogIndexForTest);

        (RaftOperationStatus createStatus, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, createStatus);
        Assert.Equal(101, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
        Assert.Contains(manager.GetPartitionMap(), r => r.PartitionId == 2);

        (RaftOperationStatus removeStatus, _) = await SendRemoveAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, removeStatus);
        Assert.Equal(102, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == 2);
        Assert.Equal(3, manager.GetNextAvailablePartitionId());

        // The applicator delivers the create entry only now, behind the removal.
        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.ConfigReplicated, committed[0]) { LogIndex = 101 });
        await WaitForIdleAsync(manager);

        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == 2);
        Assert.Equal(3, manager.GetNextAvailablePartitionId());
        Assert.Equal(102, manager.SystemCoordinator.AppliedSystemLogIndexForTest);

        // The spent id stays spent...
        (RaftOperationStatus recreateStatus, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Errored, recreateStatus);

        // ...and the next mutation builds on the map that still carries the tombstone.
        (RaftOperationStatus nextStatus, _) = await SendCreateAsync(manager, manager.GetNextAvailablePartitionId());
        Assert.Equal(RaftOperationStatus.Success, nextStatus);
        Assert.Equal(4, manager.GetNextAvailablePartitionId());
        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == 2);
        Assert.Contains(manager.GetPartitionMap(), r => r.PartitionId == 3);
    }

    /// <summary>The applicator's delivery of the entry the handler just installed is a no-op.</summary>
    [Fact]
    public async Task DeliveryOfTheInstalledEntry_IsIgnored()
    {
        using RaftManager manager = Build();
        List<byte[]> committed = OverrideCoordinatorIo(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(SeedRanges(), mapVersion: 1, logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus createStatus, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, createStatus);

        manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.ConfigReplicated, committed[0]) { LogIndex = 101 });
        await WaitForIdleAsync(manager);

        Assert.Equal(101, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
        Assert.Contains(manager.GetPartitionMap(), r => r.PartitionId == 2);
        Assert.Equal(3, manager.GetNextAvailablePartitionId());
    }

    /// <summary>A delivery above the installed index is installed, as on any follower.</summary>
    [Fact]
    public async Task NewerDelivery_IsInstalled()
    {
        using RaftManager manager = Build();
        OverrideCoordinatorIo(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(SeedRanges(), mapVersion: 1, logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus createStatus, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, createStatus);

        List<RaftPartitionRange> newer = SeedRanges();
        newer.Add(new() { PartitionId = 2, StartRange = 0, EndRange = 0, Generation = 2, State = RaftPartitionState.Removed, RoutingMode = RaftRoutingMode.Unrouted });
        newer.Add(new() { PartitionId = 7, StartRange = 0, EndRange = 0, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.Unrouted });

        manager.SystemCoordinator.Send(MakeConfigReplicated(newer, mapVersion: 9, logIndex: 150));
        await WaitForIdleAsync(manager);

        Assert.Equal(150, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == 2);
        Assert.Contains(manager.GetPartitionMap(), r => r.PartitionId == 7);
        Assert.Equal(8, manager.GetNextAvailablePartitionId());
    }

    /// <summary>
    /// A delivery that carries no index cannot be ordered: it is installed and moves nothing, so
    /// senders that predate the index (tests, legacy paths) keep their behavior.
    /// </summary>
    [Fact]
    public async Task DeliveryWithoutIndex_IsInstalledAndMovesNothing()
    {
        using RaftManager manager = Build();
        OverrideCoordinatorIo(manager, firstLogIndex: 101);

        manager.SystemCoordinator.Send(MakeConfigReplicated(SeedRanges(), mapVersion: 1, logIndex: 5));
        await WaitForIdleAsync(manager);

        (RaftOperationStatus createStatus, _) = await SendCreateAsync(manager, 2);
        Assert.Equal(RaftOperationStatus.Success, createStatus);
        Assert.Equal(101, manager.SystemCoordinator.AppliedSystemLogIndexForTest);

        manager.SystemCoordinator.Send(MakeConfigReplicated(SeedRanges(), mapVersion: 1, logIndex: 0));
        await WaitForIdleAsync(manager);

        Assert.Equal(101, manager.SystemCoordinator.AppliedSystemLogIndexForTest);
        Assert.DoesNotContain(manager.GetPartitionMap(), r => r.PartitionId == 2);
    }
}
