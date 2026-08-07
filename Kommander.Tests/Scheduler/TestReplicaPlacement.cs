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
/// Unit tests for per-partition replica placement: the committed
/// AddReplica / PromoteReplica / RemoveReplica lifecycle on the coordinator, partial
/// materialization in <c>StartUserPartitions</c> (a node hosts only ranges it replicates),
/// the per-partition peer/quorum seam, and initial placement at the configured
/// replication factor.
///
/// All tests run without a real Raft cluster: replication is intercepted with
/// <see cref="RaftSystemCoordinator.ReplicateOverride"/> and maps are injected through the
/// coordinator channel, mirroring <see cref="TestRaftSystemCoordinator"/>.
/// </summary>
public sealed class TestReplicaPlacement
{
    private const string Local = "localhost:9000";

    private static RaftManager Build(int replicationFactor = 0, List<RaftNode>? peers = null, int initialPartitions = 0)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = initialPartitions,
            ReplicationFactor = replicationFactor
        };
        return new(
            config,
            new StaticDiscovery(peers ?? []),
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

    private static void AcceptReplication(RaftManager manager, List<byte[]>? payloads = null) =>
        manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
        {
            payloads?.Add(data);
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
        };

    private static RaftReplica Replica(string endpoint, RaftReplicaRole role = RaftReplicaRole.Voter, long since = 1) =>
        new() { Endpoint = endpoint, Role = role, SinceGeneration = since };

    private static List<RaftPartitionRange> PlacedRange(
        int partitionId, long generation, params RaftReplica[] replicas) =>
        [
            new()
            {
                PartitionId = partitionId,
                StartRange = 0,
                EndRange = int.MaxValue,
                Generation = generation,
                State = RaftPartitionState.Active,
                RoutingMode = RaftRoutingMode.HashRange,
                Replicas = [.. replicas]
            }
        ];

    private static async Task<(RaftOperationStatus Status, long Generation)> SendReplicaChange(
        RaftManager manager, RaftSystemRequestType type, int partitionId, string endpoint)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(type, partitionId, endpoint, 0, tcs));
        return await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static RaftPartitionRange MapEntry(RaftManager manager, int partitionId) =>
        Assert.Single(manager.GetPartitionMap(), r => r.PartitionId == partitionId);

    // ── Partial materialization ──────────────────────────────────────────────

    [Fact]
    public async Task StartUserPartitions_HostsOnlyRangesWithLocalReplica()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                ..PlacedRange(1, 1, Replica(Local), Replica("b:1"), Replica("c:1")),
                ..PlacedRange(2, 1, Replica("b:1"), Replica("c:1"), Replica("d:1"))
            ];
            ranges[1].StartRange = 100; // avoid overlapping hash ranges in the same map

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            Assert.True(manager.IsInitialized);
            Assert.True(manager.Partitions.ContainsKey(1));   // local is a replica
            Assert.False(manager.Partitions.ContainsKey(2));  // local is not — never materialized

            // Routing still sees the whole committed map, hosted or not.
            Assert.Equal(2, manager.GetPartitionMap().Count);
            Assert.Equal(1, manager.GetPartitionGeneration(2));
        }
    }

    [Fact]
    public async Task StartUserPartitions_EmptyReplicas_LegacyFullReplication_HostsEverything()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<RaftPartitionRange> ranges =
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue }
            ];

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            Assert.True(manager.Partitions.ContainsKey(1));
            Assert.Empty(manager.GetPartitionReplicas(1)); // legacy: empty set = every voter
        }
    }

    [Fact]
    public async Task StartUserPartitions_LocalReplicaRemoved_StopsHostingPartition()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);
            Assert.True(manager.Partitions.ContainsKey(1));

            // Committed map drops the local replica (final RemoveReplica commit as observed by
            // the departing node): the partition must leave the routing dictionaries.
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 3, Replica("b:1"), Replica("c:1"), Replica("d:1")), mapVersion: 2));
            await WaitForIdleAsync(manager);

            Assert.False(manager.Partitions.ContainsKey(1));

            // The committed map (and its generation) remains visible for routing/forwarding.
            Assert.Equal(3, manager.GetPartitionGeneration(1));
        }
    }

    // ── Per-partition peer set and quorum seam ───────────────────────────────

    [Fact]
    public async Task PartitionPeersAndVoters_ResolveFromReplicaSet()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1,
                    Replica(Local),
                    Replica("b:1"),
                    Replica("c:1", RaftReplicaRole.Learner))));
            await WaitForIdleAsync(manager);

            // Peers = replica set minus self; the learner is a peer (it must receive appends).
            Assert.Equal(["b:1", "c:1"], manager.GetPartitionPeers(1).Select(n => n.Endpoint).Order());

            // Quorum counts only Voter replicas: the learner and non-replicas are excluded.
            Assert.True(manager.IsPartitionVoter(1, Local));
            Assert.True(manager.IsPartitionVoter(1, "b:1"));
            Assert.False(manager.IsPartitionVoter(1, "c:1"));
            Assert.False(manager.IsPartitionVoter(1, "d:1"));

            IReadOnlyList<RaftReplica> replicas = manager.GetPartitionReplicas(1);
            Assert.Equal(3, replicas.Count);
            Assert.Equal(RaftReplicaRole.Learner, Assert.Single(replicas, r => r.Endpoint == "c:1").Role);
        }
    }

    [Fact]
    public async Task GetEffectiveReplicationFactor_PrefersRangeOverride()
    {
        RaftManager manager = Build(replicationFactor: 3);
        using (manager)
        {
            List<RaftPartitionRange> ranges = PlacedRange(1, 1, Replica(Local), Replica("b:1"));
            ranges[0].ReplicationFactor = 5;

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            Assert.Equal(5, manager.GetEffectiveReplicationFactor(1));
            Assert.Equal(3, manager.GetEffectiveReplicationFactor(99)); // unknown → global default
        }
    }

    // ── Replica lifecycle: AddReplica ────────────────────────────────────────

    [Fact]
    public async Task AddReplica_AppendsLearnerAndBumpsGeneration()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 5, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.AddReplica, 1, "d:1");

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(6, generation); // one committed mutation, one generation bump

            RaftPartitionRange entry = MapEntry(manager, 1);
            RaftReplica added = Assert.Single(entry.Replicas, r => r.Endpoint == "d:1");
            Assert.Equal(RaftReplicaRole.Learner, added.Role);
            Assert.Equal(6, added.SinceGeneration);
        }
    }

    [Fact]
    public async Task AddReplica_SecondTransitional_RejectedSingleMover()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1,
                    Replica(Local),
                    Replica("b:1"),
                    Replica("c:1", RaftReplicaRole.Learner))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, _) =
                await SendReplicaChange(manager, RaftSystemRequestType.AddReplica, 1, "d:1");

            Assert.Equal(RaftOperationStatus.ConcurrentMembershipChange, status);
            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "d:1");
        }
    }

    [Fact]
    public async Task AddReplica_OnLegacyRange_Rejected()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                [new RaftPartitionRange { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue }]));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, _) =
                await SendReplicaChange(manager, RaftSystemRequestType.AddReplica, 1, "d:1");

            Assert.Equal(RaftOperationStatus.Errored, status);
        }
    }

    [Fact]
    public async Task AddReplica_AlreadyPresent_IdempotentSuccess()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 7, Replica(Local), Replica("b:1"))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.AddReplica, 1, "b:1");

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(7, generation); // no mutation, no generation bump
        }
    }

    // ── Replica lifecycle: PromoteReplica ────────────────────────────────────

    [Fact]
    public async Task PromoteReplica_LearnerBecomesVoter()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2,
                    Replica(Local),
                    Replica("b:1"),
                    Replica("c:1", RaftReplicaRole.Learner))));
            await WaitForIdleAsync(manager);

            Assert.False(manager.IsPartitionVoter(1, "c:1"));

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.PromoteReplica, 1, "c:1");

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, generation);

            // The promotion commit is the quorum-entry point.
            Assert.True(manager.IsPartitionVoter(1, "c:1"));
            Assert.Equal(RaftReplicaRole.Voter, Assert.Single(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1").Role);
        }
    }

    [Fact]
    public async Task PromoteReplica_NonReplica_Errored()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, _) =
                await SendReplicaChange(manager, RaftSystemRequestType.PromoteReplica, 1, "zz:1");

            Assert.Equal(RaftOperationStatus.Errored, status);
        }
    }

    // ── Replica lifecycle: RemoveReplica ─────────────────────────────────────

    [Fact]
    public async Task RemoveReplica_TwoCommits_ThenReplicaGone()
    {
        RaftManager manager = Build();
        using (manager)
        {
            List<byte[]> payloads = [];
            AcceptReplication(manager, payloads);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.RemoveReplica, 1, "c:1");

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, generation); // Removing (gen 2) then dropped (gen 3)

            // Exactly two committed map mutations — the §6 two-step discipline. The intermediate
            // commit must show c:1 as Removing (out of quorum, still serving).
            Assert.Equal(2, payloads.Count);
            RaftPartitionMap intermediate = DecodeMap(payloads[0]);
            RaftReplica removing = Assert.Single(
                intermediate.Partitions.Single(r => r.PartitionId == 1).Replicas, r => r.Endpoint == "c:1");
            Assert.Equal(RaftReplicaRole.Removing, removing.Role);

            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
            Assert.False(manager.IsPartitionVoter(1, "c:1"));
        }
    }

    [Fact]
    public async Task RemoveReplica_LastVoter_RejectedInsufficientVoters()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, _) =
                await SendReplicaChange(manager, RaftSystemRequestType.RemoveReplica, 1, Local);

            Assert.Equal(RaftOperationStatus.InsufficientVoters, status);
            Assert.Contains(MapEntry(manager, 1).Replicas, r => r.Endpoint == Local);
        }
    }

    [Fact]
    public async Task RemoveReplica_AbsentEndpoint_IdempotentSuccess()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 4, Replica(Local), Replica("b:1"))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.RemoveReplica, 1, "gone:1");

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(4, generation);
        }
    }

    [Fact]
    public async Task RemoveReplica_ResumesInterruptedRemoval()
    {
        // A crash between the two commits leaves a Removing replica. Re-driving RemoveReplica
        // must skip phase 1 (no second Removing commit) and finish with the single final drop.
        RaftManager manager = Build();
        using (manager)
        {
            List<byte[]> payloads = [];
            AcceptReplication(manager, payloads);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica(Local), Replica("b:1"), Replica("c:1", RaftReplicaRole.Removing))));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, long generation) =
                await SendReplicaChange(manager, RaftSystemRequestType.RemoveReplica, 1, "c:1");

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, generation);
            Assert.Single(payloads); // only the final drop was committed
            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
        }
    }

    // ── SetReplicationFactor ─────────────────────────────────────────────────

    [Fact]
    public async Task SetReplicationFactor_CommitsOverrideWithoutGenerationBump()
    {
        RaftManager manager = Build(replicationFactor: 3);
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 9, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.SetReplicationFactor, 1)
            {
                ReplicationFactorValue = 5,
                Completion = tcs
            });
            (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(9, generation); // routing unchanged — fence deliberately not invalidated
            Assert.Equal(5, manager.GetEffectiveReplicationFactor(1));
        }
    }

    // ── Initial placement ────────────────────────────────────────────────────

    [Fact]
    public async Task InitialPlacement_SixNodesFourRangesRf3_AssignsEvenSpread()
    {
        // The spec's canonical example, end to end: the P0 leader bootstraps 4 ranges over a
        // 6-node discovery set at RF=3 — each range gets 3 distinct replicas, each node 2 ranges.
        List<RaftNode> peers = [.. Enumerable.Range(1, 5).Select(i => new RaftNode($"peer{i}:1"))];
        RaftManager manager = Build(replicationFactor: 3, peers: peers, initialPartitions: 4);
        using (manager)
        {
            AcceptReplication(manager);

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await WaitForIdleAsync(manager);

            IReadOnlyList<RaftPartitionRange> map = manager.GetPartitionMap();
            Assert.Equal(4, map.Count);

            Dictionary<string, int> perNode = new(StringComparer.Ordinal);
            foreach (RaftPartitionRange range in map)
            {
                Assert.Equal(3, range.Replicas.Count);
                Assert.Equal(3, range.Replicas.Select(r => r.Endpoint).Distinct().Count());
                Assert.All(range.Replicas, r => Assert.Equal(RaftReplicaRole.Voter, r.Role));
                foreach (RaftReplica replica in range.Replicas)
                    perNode[replica.Endpoint] = perNode.GetValueOrDefault(replica.Endpoint) + 1;
            }

            Assert.Equal(6, perNode.Count);
            Assert.All(perNode.Values, count => Assert.Equal(2, count));

            // This node only materializes the ranges it replicates.
            foreach (RaftPartitionRange range in map)
                Assert.Equal(
                    range.Replicas.Any(r => r.Endpoint == manager.LocalEndpoint),
                    manager.Partitions.ContainsKey(range.PartitionId));
        }
    }

    [Fact]
    public async Task InitialPlacement_SubRfCluster_KeepsFullReplication()
    {
        // 1 local + 1 peer at RF=3: below the floor — every range keeps the legacy empty
        // replica set and is hosted everywhere.
        RaftManager manager = Build(replicationFactor: 3, peers: [new RaftNode("peer1:1")], initialPartitions: 2);
        using (manager)
        {
            AcceptReplication(manager);

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await WaitForIdleAsync(manager);

            IReadOnlyList<RaftPartitionRange> map = manager.GetPartitionMap();
            Assert.Equal(2, map.Count);
            Assert.All(map, range => Assert.Empty(range.Replicas));
            Assert.Equal(2, manager.Partitions.Count);
        }
    }

    private static RaftPartitionMap DecodeMap(byte[] payload)
    {
        RaftSystemMessage message = RaftSystemMessage.Parser.ParseFrom(payload);
        return JsonSerializer.Deserialize<RaftPartitionMap>(message.Value)!;
    }
}
