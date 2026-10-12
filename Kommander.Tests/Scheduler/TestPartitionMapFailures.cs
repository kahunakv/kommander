
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
/// Failure-leg tests for the partition map service's create path and its
/// dynamic-creation placement picker.
///
/// Tests cover:
///   1. Creating a partition id whose map entry exists in a non-Active state is
///      rejected without corrupting the entry or replicating anything
///   2. Creating a HashRange partition without hash bounds is rejected
///   3. Dynamic creation when the committed voter count does not exceed the
///      replication factor degrades to legacy full replication (empty replica set)
///      instead of short-placing the range
/// </summary>
public sealed class TestPartitionMapFailures
{
    private const string Local = "localhost:9000";

    // ── Builders ───────────────────────────────────────────────────────────────

    private static RaftManager Build(int replicationFactor = 0) => new(
        new RaftConfiguration { Host = "localhost", Port = 9000, InitialPartitions = 0, ReplicationFactor = replicationFactor },
        new StaticDiscovery([]),
        new InMemoryWAL(NullLogger<IRaft>.Instance),
        new Kommander.Communication.Memory.InMemoryCommunication(),
        new HybridLogicalClock(),
        NullLogger<IRaft>.Instance);

    // ── Helpers ────────────────────────────────────────────────────────────────

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

    private static RaftSystemRequest MakeMembersReplicated(params string[] voterEndpoints)
    {
        ClusterMembership membership = new()
        {
            MembershipVersion = 1,
            Members =
            [
                .. voterEndpoints.Select((endpoint, i) => new ClusterMember
                {
                    Endpoint = endpoint, NodeId = i + 1, Role = ClusterMemberRole.Voter, JoinedVersion = 1
                })
            ]
        };

        return new RaftSystemRequest(
            RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(RaftSystemConfigKeys.Members, JsonSerializer.Serialize(membership)));
    }

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    private static Task<(RaftOperationStatus Status, long Generation)> SendCreateAsync(
        RaftManager manager, int partitionId, RaftRoutingMode mode, int? start = null, int? end = null)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(partitionId, mode, start, end, tcs));
        return tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // ── Test 1: create against a non-Active entry ──────────────────────────────

    [Fact]
    public async Task CreatePartition_EntryInNonActiveState_IsRejectedWithoutCorruption()
    {
        RaftManager manager = Build();
        using (manager)
        {
            // P7 sits mid-split: its map entry exists but is not Active, so the id is
            // neither free nor safely re-creatable.
            manager.SystemCoordinator.Send(MakeConfigReplicated(
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active,    RoutingMode = RaftRoutingMode.HashRange },
                new() { PartitionId = 7, StartRange = 0, EndRange = 0,            Generation = 4, State = RaftPartitionState.Splitting, RoutingMode = RaftRoutingMode.Unrouted }
            ]));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            (RaftOperationStatus status, long generation) =
                await SendCreateAsync(manager, partitionId: 7, RaftRoutingMode.Unrouted);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.Equal(0, generation);
            Assert.False(replicateCalled, "A rejected duplicate create must never replicate a map");

            // The existing entry is byte-for-byte untouched: same state, same generation.
            List<RaftPartitionRange> ranges = [..manager.GetPartitionMap()];
            RaftPartitionRange entry = ranges.Single(r => r.PartitionId == 7);
            Assert.Equal(RaftPartitionState.Splitting, entry.State);
            Assert.Equal(4, entry.Generation);
            Assert.Equal(2, ranges.Count);
        }
    }

    // ── Test 2: HashRange without bounds ───────────────────────────────────────

    [Fact]
    public async Task CreatePartition_HashRangeModeWithoutBounds_IsRejected()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Send(MakeConfigReplicated(
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ]));
            await WaitForIdleAsync(manager);

            bool replicateCalled = false;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                replicateCalled = true;
                return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            };
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            // HashRange mode with no bounds would produce an entry whose routing
            // interval is undefined; the guard must fire before replication.
            (RaftOperationStatus status, long generation) =
                await SendCreateAsync(manager, partitionId: 9, RaftRoutingMode.HashRange, start: null, end: null);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.Equal(0, generation);
            Assert.False(replicateCalled);
            Assert.False(manager.Partitions.ContainsKey(9));
            Assert.Single(manager.GetPartitionMap());
        }
    }

    // ── Test 3: voter pool not above the replication factor ────────────────────

    [Fact]
    public async Task CreatePartition_VoterCountNotAboveReplicationFactor_KeepsFullReplication()
    {
        // Two committed voters at RF=3: a per-range replica set cannot be formed
        // without reusing a node, so the new range must fall back to the legacy
        // empty set (every voter hosts it) rather than being short-placed.
        RaftManager manager = Build(replicationFactor: 3);
        using (manager)
        {
            manager.SystemCoordinator.ConfirmPartitionLeadershipOverride = static (_, _) => Task.FromResult(true);
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            manager.SystemCoordinator.Send(MakeMembersReplicated(Local, "b:1"));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
            [
                new()
                {
                    PartitionId = 1,
                    StartRange = 0,
                    EndRange = int.MaxValue,
                    Generation = 1,
                    State = RaftPartitionState.Active,
                    RoutingMode = RaftRoutingMode.HashRange,
                    Replicas =
                    [
                        new() { Endpoint = Local, Role = RaftReplicaRole.Voter, SinceGeneration = 1 },
                        new() { Endpoint = "b:1", Role = RaftReplicaRole.Voter, SinceGeneration = 1 }
                    ]
                }
            ]));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, long generation) =
                await SendCreateAsync(manager, partitionId: 5, RaftRoutingMode.Unrouted);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(1, generation);

            // Empty replica set: the founding placement degrades to full replication.
            Assert.Empty(manager.GetPartitionReplicas(5));
            Assert.True(manager.Partitions.ContainsKey(5)); // legacy ranges are hosted everywhere
        }
    }
}
