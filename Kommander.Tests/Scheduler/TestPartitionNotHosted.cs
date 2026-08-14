using System.Text.Json;
using Kommander.Discovery;
using Kommander.System;
using Kommander.System.Protos;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Google.Protobuf;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Coverage for the typed "partition not hosted here" contract introduced for replica placement.
/// Under placement most partitions are not materialized on most nodes, so the per-partition APIs
/// must let a consumer distinguish that retryable routing condition from a genuinely unknown
/// partition id without matching exception message strings:
///
/// <para>A partition the committed map places on <i>other</i> nodes raises
/// <see cref="PartitionNotHostedException"/> (a <see cref="RaftException"/> subtype, so existing
/// catch-alls keep working), while an id absent from the committed map keeps the plain
/// <see cref="RaftException"/> — that case is a caller error and retrying elsewhere cannot help.
/// <see cref="IRaft.HostsPartition"/> is the matching fast-path predicate, reading the same
/// materialized-partition dictionary the APIs resolve through.</para>
///
/// <para>Maps are injected through the coordinator channel, mirroring
/// <see cref="TestReplicaPlacement"/> — no live cluster, so the committed map cannot be
/// overwritten by concurrent map applications mid-test.</para>
/// </summary>
public sealed class TestPartitionNotHosted
{
    private const string Local = "localhost:9000";

    private const int HostedPartition = 1;

    private const int ForeignPartition = 2;

    private const int UnknownPartition = 999;

    private static RaftManager Build()
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
        };
        return new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );
    }

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

    private static RaftReplica Replica(string endpoint) =>
        new() { Endpoint = endpoint, Role = RaftReplicaRole.Voter, SinceGeneration = 1 };

    private static RaftPartitionRange PlacedRange(int partitionId, int startRange, params RaftReplica[] replicas) =>
        new()
        {
            PartitionId = partitionId,
            StartRange = startRange,
            EndRange = startRange + 99,
            Generation = 1,
            State = RaftPartitionState.Active,
            RoutingMode = RaftRoutingMode.HashRange,
            Replicas = [.. replicas]
        };

    /// <summary>
    /// Applies a two-range placed map: <see cref="HostedPartition"/> includes the local endpoint,
    /// <see cref="ForeignPartition"/> lives entirely on other nodes.
    /// </summary>
    private static async Task ApplyPlacedMapAsync(RaftManager manager)
    {
        List<RaftPartitionRange> ranges =
        [
            PlacedRange(HostedPartition, 0, Replica(Local), Replica("b:1"), Replica("c:1")),
            PlacedRange(ForeignPartition, 100, Replica("b:1"), Replica("c:1"), Replica("d:1"))
        ];

        manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
        await WaitForIdleAsync(manager);

        Assert.True(manager.IsInitialized);
    }

    [Fact]
    public async Task NotHostedPartition_ThrowsTypedException_FromEveryLeadershipApi()
    {
        using RaftManager manager = Build();

        await ApplyPlacedMapAsync(manager);

        // Every leadership API surfaces the typed, retryable condition, carrying the id.
        PartitionNotHostedException ex = await Assert.ThrowsAsync<PartitionNotHostedException>(
            async () => await manager.AmILeaderQuick(ForeignPartition));
        Assert.Equal(ForeignPartition, ex.PartitionId);

        // The historical message prefix survives for consumers still matching on it, and the
        // subtype stays catchable as the base RaftException.
        Assert.Contains("Invalid partition", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.IsAssignableFrom<RaftException>(ex);

        await Assert.ThrowsAsync<PartitionNotHostedException>(
            async () => await manager.AmILeader(ForeignPartition, TestContext.Current.CancellationToken));

        await Assert.ThrowsAsync<PartitionNotHostedException>(
            async () => await manager.ConfirmLeadershipAsync(ForeignPartition, TestContext.Current.CancellationToken));

        await Assert.ThrowsAsync<PartitionNotHostedException>(
            async () => await manager.WaitForLeader(ForeignPartition, TestContext.Current.CancellationToken));

        // ConfirmLocalApplicationAsync fails closed instead of throwing.
        Assert.False(await manager.ConfirmLocalApplicationAsync(ForeignPartition, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task HostsPartition_TracksMaterializationAcrossMapChanges()
    {
        using RaftManager manager = Build();

        // Nothing is materialized before the first map application.
        Assert.False(manager.HostsPartition(HostedPartition));
        Assert.False(manager.HostsPartition(ForeignPartition));

        await ApplyPlacedMapAsync(manager);

        Assert.True(manager.HostsPartition(HostedPartition));
        Assert.False(manager.HostsPartition(ForeignPartition));
        Assert.False(manager.HostsPartition(UnknownPartition));

        // A later map moves the hosted range entirely onto other nodes: the local partition is
        // stopped and the same APIs flip to the typed not-hosted condition.
        List<RaftPartitionRange> moved =
        [
            PlacedRange(HostedPartition, 0, Replica("b:1"), Replica("c:1"), Replica("d:1")),
            PlacedRange(ForeignPartition, 100, Replica("b:1"), Replica("c:1"), Replica("d:1"))
        ];
        moved[0].Generation = 2;

        manager.SystemCoordinator.Send(MakeConfigReplicated(moved, mapVersion: 2));
        await WaitForIdleAsync(manager);

        Assert.False(manager.HostsPartition(HostedPartition));

        PartitionNotHostedException ex = await Assert.ThrowsAsync<PartitionNotHostedException>(
            async () => await manager.AmILeaderQuick(HostedPartition));
        Assert.Equal(HostedPartition, ex.PartitionId);
    }

    /// <summary>
    /// An id absent from the committed map must keep throwing the plain, exact
    /// <see cref="RaftException"/>: it is a caller error, not a routing condition, and must not
    /// be reclassified as retryable. (xUnit's ThrowsAsync asserts the exact type, so a
    /// <see cref="PartitionNotHostedException"/> here would fail the assertion.)
    /// </summary>
    [Fact]
    public async Task UnknownPartition_KeepsPlainRaftException()
    {
        using RaftManager manager = Build();

        await ApplyPlacedMapAsync(manager);

        RaftException ex = await Assert.ThrowsAsync<RaftException>(
            async () => await manager.AmILeaderQuick(UnknownPartition));

        Assert.IsNotType<PartitionNotHostedException>(ex);
        Assert.Contains("Invalid partition", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
