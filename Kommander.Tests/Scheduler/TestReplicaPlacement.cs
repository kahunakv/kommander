using System.Text.Json;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.System.Protos;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
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

    private static RaftManager Build(
        int replicationFactor = 0, List<RaftNode>? peers = null, int initialPartitions = 0,
        bool enablePlacementRebalancer = false,
        Kommander.Communication.Memory.InMemoryCommunication? communication = null,
        TimeSpan? learnerPromotionStableWindow = null)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = initialPartitions,
            ReplicationFactor = replicationFactor,
            EnablePlacementRebalancer = enablePlacementRebalancer
        };
        if (learnerPromotionStableWindow is { } window)
            config.LearnerPromotionStableWindow = window;
        return new(
            config,
            new StaticDiscovery(peers ?? []),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            communication ?? new Kommander.Communication.Memory.InMemoryCommunication(),
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
            (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

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

    // ── Placement-controller pass (RunPlacementPass) ─────────────────────────

    /// <summary>
    /// Makes the harness manager pass <c>AmILeaderQuick(P0)</c> — the gate the placement pass
    /// self-checks. The harness never joins a cluster, so there is no system partition to lead;
    /// the test hook stands in for it. For data ranges the hook mirrors the real contract
    /// faithfully: <c>AmILeaderQuick</c> throws the typed <see cref="PartitionNotHostedException"/>
    /// for a range this node does not host (see <see cref="TestPartitionNotHosted"/>), so any
    /// placement code path that calls it blind on a non-hosted range fails in these tests exactly
    /// as it did in production.
    /// </summary>
    private static void ForceP0Leadership(RaftManager manager) =>
        manager._amILeaderQuickHookForTesting = partitionId =>
            partitionId == RaftSystemConfig.SystemPartition
                ? ValueTask.FromResult(true)
                : manager.HostsPartition(partitionId)
                    ? ValueTask.FromResult(false)
                    : throw new PartitionNotHostedException(partitionId);

    private static RaftSystemRequest MakeMembersReplicated(params string[] voterEndpoints)
    {
        Kommander.System.ClusterMembership membership = new()
        {
            MembershipVersion = 1,
            Members =
            [
                .. voterEndpoints.Select((endpoint, i) => new Kommander.System.ClusterMember
                {
                    Endpoint = endpoint, NodeId = i + 1, Role = Kommander.System.ClusterMemberRole.Voter, JoinedVersion = 1
                })
            ]
        };

        return new RaftSystemRequest(
            RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(RaftSystemConfigKeys.Members, JsonSerializer.Serialize(membership)));
    }

    /// <summary>
    /// Drains the coordinator twice: the placement pass self-enqueues its planned replica
    /// mutations behind the first drain sentinel, so a second drain is needed before the
    /// committed map reflects them.
    /// </summary>
    private static async Task RunEnqueuedPassToCompletionAsync(RaftManager manager)
    {
        await WaitForIdleAsync(manager);
        await WaitForIdleAsync(manager);
    }

    [Fact]
    public async Task RunPlacementPass_RebalancerOff_DrivesInterruptedRemovalToFinalDrop()
    {
        // The documented contract of EnablePlacementRebalancer=false: no new moves are planned,
        // but in-flight transitions still converge. Before placement got its own scheduling this
        // was silently false — nothing ever dispatched the pass without the leader balancer.
        RaftManager manager = Build(); // rebalancer off
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica(Local), Replica("b:1"), Replica("c:1", RaftReplicaRole.Removing))));
            await WaitForIdleAsync(manager);

            ForceP0Leadership(manager);

            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            // The pass re-drove the interrupted two-commit removal to its final drop.
            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
        }
    }

    [Fact]
    public async Task SetReplicationFactor_CommitKicksPassAndConvergesOverride()
    {
        // End-to-end through the event-driven kick: committing an RF override must trigger a
        // placement pass immediately (no timer tick is ever sent here), and repeated passes must
        // converge the range from 3 voters to the overridden target of 1.
        RaftManager manager = Build(replicationFactor: 3, enablePlacementRebalancer: true);
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(Local, "b:1", "c:1"));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            ForceP0Leadership(manager);

            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.SetReplicationFactor, 1)
            {
                ReplicationFactorValue = 1,
                Completion = tcs
            });
            (RaftOperationStatus status, _) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(RaftOperationStatus.Success, status);

            // No RunPlacementPass is sent by the test: the commit itself kicked one, which
            // planned a single trim (one move per range per pass) and committed the two-step
            // removal of the ordinal-first non-leader victim.
            await RunEnqueuedPassToCompletionAsync(manager);
            Assert.Equal(2, MapEntry(manager, 1).Replicas.Count);
            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "b:1");

            // The next pass (the 5 s timer in production) trims the remaining excess voter.
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            RaftReplica survivor = Assert.Single(MapEntry(manager, 1).Replicas);
            Assert.Equal(Local, survivor.Endpoint);
        }
    }

    // ── Ranges the P0 leader does not host ───────────────────────────────────

    /// <summary>
    /// Minimal remote node for the in-memory transport: answers only the follower-lag probe
    /// (<see cref="ICommunication"/> routes <c>GetRemoteFollowerLag</c> through
    /// <see cref="IRaft.GetFollowerLagAsync"/> on the target) and throws for everything else.
    /// </summary>
    private sealed class FollowerLagRaft : IRaft
    {
        public required Func<int, string, long?> Lag { get; init; }

        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) =>
            ValueTask.FromResult(Lag(partitionId, followerEndpoint));

        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public bool Joined => throw new NotImplementedException();
        public IWAL WalAdapter => throw new NotImplementedException();
        public ICommunication Communication => throw new NotImplementedException();
        public IDiscovery Discovery => throw new NotImplementedException();
        public RaftConfiguration Configuration => throw new NotImplementedException();
        public HybridLogicalClock HybridLogicalClock => throw new NotImplementedException();
        public IRaftReadScheduler ReadScheduler => throw new NotImplementedException();
        public IRaftWalScheduler WalScheduler => throw new NotImplementedException();
        public bool IsInitialized => throw new NotImplementedException();
        public ClusterMemberRole LocalRole => throw new NotImplementedException();

        public event Action<int>? OnRestoreStarted { add { } remove { } }
        public event Action<int>? OnRestoreFinished { add { } remove { } }
        public event Action<int, RaftLog>? OnReplicationError { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add { } remove { } }
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add { } remove { } }
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }
        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }

        public Task JoinCluster(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task UpdateNodes() => throw new NotImplementedException();
        public ClusterMembership GetMembership() => throw new NotImplementedException();
        public IList<RaftNode> GetNodes() => throw new NotImplementedException();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => throw new NotImplementedException();
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => throw new NotImplementedException();
        public Task Handshake(HandshakeRequest request) => throw new NotImplementedException();
        public void RequestVote(RequestVotesRequest request) => throw new NotImplementedException();
        public void Vote(VoteRequest request) => throw new NotImplementedException();
        public void AppendLogs(AppendLogsRequest request) => throw new NotImplementedException();
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void SetMinRetainIndex(int partitionId, long index) => throw new NotImplementedException();
        public long GetCommitIndex(int partitionId) => throw new NotImplementedException();
        public long GetStaleProposedSkippedCount(int partitionId) => throw new NotImplementedException();
        public IDisposable AcquireRetentionHold(int partitionId, long index) => throw new NotImplementedException();
        public string GetLocalEndpoint() => throw new NotImplementedException();
        public int GetLocalNodeId() => throw new NotImplementedException();
        public string GetLocalNodeName() => throw new NotImplementedException();
        public ValueTask<bool> AmILeaderQuick(int partitionId) => throw new NotImplementedException();
        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, TimeSpan timeout, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public long GetPartitionGeneration(int partitionId) => throw new NotImplementedException();
        public bool HostsPartition(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftReplica> GetPartitionReplicas(int partitionId) => throw new NotImplementedException();
        public string? GetPartitionLeaderHint(int partitionId) => throw new NotImplementedException();
        public int GetEffectiveReplicationFactor(int partitionId) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SetReplicationFactorAsync(int partitionId, int replicationFactor, CancellationToken ct = default) => throw new NotImplementedException();
        public double GetPartitionLogOpsPerSecond(int partitionId) => throw new NotImplementedException();
        public int GetPartitionWalQueueDepth(int partitionId) => throw new NotImplementedException();
        public double GetPartitionCommitWaitMs(int partitionId) => throw new NotImplementedException();
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => throw new NotImplementedException();
        public int GetPartitionKey(string partitionKey) => throw new NotImplementedException();
        public int GetPrefixPartitionKey(string prefixPartitionKey) => throw new NotImplementedException();
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => throw new NotImplementedException();
        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => throw new NotImplementedException();
        public void RegisterPartitionStateTransfer(IRaftPartitionStateTransfer? transfer) => throw new NotImplementedException();
        public IReadOnlyList<RaftSnapshotStatus> GetSnapshotStatuses(int partitionId) => throw new NotImplementedException();
    }

    /// <summary>
    /// Seeds the gossip-reduced leader hint for a range this node does not host, by ingesting a
    /// load report in which <paramref name="leaderEndpoint"/> claims <paramref name="partitionId"/>.
    /// Requires <see cref="RaftConfiguration.LoadReportsEnabled"/> (implied by placement).
    /// </summary>
    private static async Task SeedLeaderHintAsync(RaftManager manager, string leaderEndpoint, int partitionId)
    {
        manager.SystemCoordinator.Send(new RaftSystemRequest(new NodeLoadReport
        {
            Endpoint = leaderEndpoint,
            ReportVersion = 1,
            Time = manager.HybridLogicalClock.SendOrLocalEvent(0),
            Leaderships = [new PartitionLoad { PartitionId = partitionId }]
        }));
        await WaitForIdleAsync(manager);
    }

    /// <summary>
    /// The Kahuna 1.2.3-validation incident: the P0 leader repairs a range it does not host.
    /// The learner-lag probe must take the remote branch — leader endpoint from the gossiped
    /// hint, lag from the remote leader's follower-progress table — and promote the caught-up
    /// learner. Before the fix the pass threw at the leadership check and the replica set
    /// stayed {Voter, Learner} forever.
    /// </summary>
    [Fact]
    public async Task RunPlacementPass_LearnerOnRangeNotHostedByP0Leader_PromotesViaRemoteLag()
    {
        Kommander.Communication.Memory.InMemoryCommunication communication = new();
        // RF > 0 turns on load-report ingest (the hint's data source under placement).
        RaftManager manager = Build(
            replicationFactor: 3, communication: communication,
            learnerPromotionStableWindow: TimeSpan.Zero);
        using (manager)
        {
            communication.SetNodes(new Dictionary<string, IRaft>
            {
                // The range leader: reports itself and the learner at the same committed index.
                ["b:1"] = new FollowerLagRaft { Lag = (_, _) => 100 }
            });

            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica("b:1"), Replica("c:1", RaftReplicaRole.Learner))));
            await WaitForIdleAsync(manager);
            Assert.False(manager.HostsPartition(1));

            await SeedLeaderHintAsync(manager, "b:1", 1);
            ForceP0Leadership(manager);

            // Pass 1 observes the learner caught up and opens the stable window; pass 2
            // promotes (window is zero here). Neither throws.
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            RaftReplica promoted = Assert.Single(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
            Assert.Equal(RaftReplicaRole.Voter, promoted.Role);
        }
    }

    /// <summary>
    /// Per-range fault isolation: one range whose lag probe fails must not abort the pass —
    /// before the fix the exception escaped <c>RunPlacementPassAsync</c> and starved every
    /// other range's transition re-drive on every tick.
    /// </summary>
    [Fact]
    public async Task RunPlacementPass_OneRangeProbeThrows_OtherRangesStillDriven()
    {
        Kommander.Communication.Memory.InMemoryCommunication communication = new();
        RaftManager manager = Build(replicationFactor: 3, communication: communication);
        using (manager)
        {
            communication.SetNodes(new Dictionary<string, IRaft>
            {
                ["b:1"] = new FollowerLagRaft { Lag = (_, _) => throw new InvalidOperationException("probe boom") }
            });

            AcceptReplication(manager);
            List<RaftPartitionRange> ranges =
            [
                // Range 1: learner on a non-hosted range whose remote probe throws.
                ..PlacedRange(1, 2, Replica("b:1"), Replica("c:1", RaftReplicaRole.Learner)),
                // Range 2: interrupted removal that must still be re-driven to its final drop.
                ..PlacedRange(2, 2, Replica(Local), Replica("b:1"), Replica("d:1", RaftReplicaRole.Removing))
            ];
            ranges[1].StartRange = 100; // avoid overlapping hash ranges in the same map

            manager.SystemCoordinator.Send(MakeConfigReplicated(ranges));
            await WaitForIdleAsync(manager);

            await SeedLeaderHintAsync(manager, "b:1", 1);
            ForceP0Leadership(manager);

            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            // Range 1 was skipped for this pass; range 2's Removing replica completed its drop.
            Assert.DoesNotContain(MapEntry(manager, 2).Replicas, r => r.Endpoint == "d:1");
            Assert.Contains(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
        }
    }

    private static RaftPartitionMap DecodeMap(byte[] payload)
    {
        RaftSystemMessage message = RaftSystemMessage.Parser.ParseFrom(payload);
        return JsonSerializer.Deserialize<RaftPartitionMap>(message.Value)!;
    }
}
