
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
/// Unit tests for the graceful-decommission drain state machine on the coordinator:
/// the <c>SetMemberRole</c> transition (<c>Voter → Leaving</c> and the rollback), its guards
/// (one drain at a time on roster state, quorum-safety, idempotency, <c>MemberNotFound</c> for
/// the removal-wins race), the placement pass's drain sweep (a fully evacuated <c>Leaving</c>
/// member is removed with no live waiter — the crash-resumption path), the shed-driven
/// evacuation of a leaver's replica, the drop of a transitional learner sitting on a leaver,
/// and the peer-set regression guard (a <c>Leaving</c> member must stay in
/// <c>manager.Nodes</c> or every drain wedges).
///
/// All tests run without a real Raft cluster: replication is intercepted with
/// <see cref="RaftSystemCoordinator.ReplicateOverride"/> and state is injected through the
/// coordinator channel, mirroring <see cref="TestReplicaPlacement"/>.
/// </summary>
public sealed class TestDecommissionDrain
{
    private const string Local = "localhost:9000";

    private static RaftManager Build(
        int replicationFactor = 0, List<RaftNode>? peers = null, int initialPartitions = 0,
        bool enablePlacementRebalancer = false)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = initialPartitions,
            ReplicationFactor = replicationFactor,
            EnablePlacementRebalancer = enablePlacementRebalancer
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

    private static RaftSystemRequest MakeMembersReplicated(params (string Endpoint, ClusterMemberRole Role)[] members)
    {
        ClusterMembership membership = new()
        {
            MembershipVersion = 1,
            Members =
            [
                .. members.Select((m, i) => new ClusterMember
                {
                    Endpoint = m.Endpoint, NodeId = i + 1, Role = m.Role, JoinedVersion = 1
                })
            ]
        };

        return new RaftSystemRequest(
            RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(RaftSystemConfigKeys.Members, JsonSerializer.Serialize(membership)));
    }

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    private static void AcceptReplication(RaftManager manager) =>
        manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

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

    private static void ForceP0Leadership(RaftManager manager) =>
        manager._amILeaderQuickHookForTesting =
            partitionId => ValueTask.FromResult(partitionId == RaftSystemConfig.SystemPartition);

    /// <summary>
    /// Drains the coordinator twice: the placement pass self-enqueues its planned mutations
    /// behind the first drain sentinel, so a second drain is needed before committed state
    /// reflects them.
    /// </summary>
    private static async Task RunEnqueuedPassToCompletionAsync(RaftManager manager)
    {
        await WaitForIdleAsync(manager);
        await WaitForIdleAsync(manager);
    }

    private static async Task<(RaftOperationStatus Status, long Version)> SendSetRole(
        RaftManager manager, string endpoint, ClusterMemberRole targetRole)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        manager.SystemCoordinator.Send(new RaftSystemRequest(
            RaftSystemRequestType.SetMemberRole,
            endpoint,
            0,
            manager.SystemCoordinator.GetMembership().MembershipVersion,
            targetRole,
            tcs));

        return await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static ClusterMemberRole RoleOf(RaftManager manager, string endpoint) =>
        Assert.Single(manager.SystemCoordinator.GetMembership().Members, m => m.Endpoint == endpoint).Role;

    private static RaftPartitionRange MapEntry(RaftManager manager, int partitionId) =>
        Assert.Single(manager.GetPartitionMap(), r => r.PartitionId == partitionId);

    // ── SetMemberRole transition guards ──────────────────────────────────────

    [Fact]
    public async Task SetMemberRole_VoterToLeaving_CommitsReversibly()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter), ("c:1", ClusterMemberRole.Voter)));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, long version) = await SendSetRole(manager, "b:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(2L, version);
            Assert.Equal(ClusterMemberRole.Leaving, RoleOf(manager, "b:1"));

            // Rollback: the reverse transition restores Voter (and with it the campaign gates).
            (status, version) = await SendSetRole(manager, "b:1", ClusterMemberRole.Voter);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3L, version);
            Assert.Equal(ClusterMemberRole.Voter, RoleOf(manager, "b:1"));
        }
    }

    [Fact]
    public async Task SetMemberRole_RetryAfterLostResponse_IsIdempotent()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter)));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus first, long v1) = await SendSetRole(manager, "b:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.Success, first);

            // A retried commit (lost response) reports success at the current version and does
            // not bump the roster again.
            (RaftOperationStatus second, long v2) = await SendSetRole(manager, "b:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.Success, second);
            Assert.Equal(v1, v2);
        }
    }

    [Fact]
    public async Task SetMemberRole_SecondConcurrentDrain_IsRefused()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter), ("c:1", ClusterMemberRole.Voter)));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus first, _) = await SendSetRole(manager, "b:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.Success, first);

            // The guard is roster state, not the in-flight latch: the first drain's commit
            // window closed long ago, yet the second drain must still be refused for as long
            // as b:1 stays Leaving.
            (RaftOperationStatus second, _) = await SendSetRole(manager, "c:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.DrainInProgress, second);
            Assert.Equal(ClusterMemberRole.Voter, RoleOf(manager, "c:1"));

            // After the first drain rolls back, the second may proceed.
            await SendSetRole(manager, "b:1", ClusterMemberRole.Voter);
            (RaftOperationStatus third, _) = await SendSetRole(manager, "c:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.Success, third);
        }
    }

    [Fact]
    public async Task SetMemberRole_LastVoter_RefusedInsufficientVoters()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Learner)));
            await WaitForIdleAsync(manager);

            // The transition removes Local from the roster-level quorum at its commit point —
            // a Learner does not backfill the voter set, so this would leave zero voters.
            (RaftOperationStatus status, _) = await SendSetRole(manager, Local, ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.InsufficientVoters, status);
            Assert.Equal(ClusterMemberRole.Voter, RoleOf(manager, Local));
        }
    }

    [Fact]
    public async Task SetMemberRole_UnknownEndpoint_MemberNotFound()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated((Local, ClusterMemberRole.Voter)));
            await WaitForIdleAsync(manager);

            // MemberNotFound is what the rollback observes when the placement pass committed
            // the final removal first (removal-wins): the caller must treat it as departed.
            (RaftOperationStatus status, _) = await SendSetRole(manager, "gone:1", ClusterMemberRole.Voter);
            Assert.Equal(RaftOperationStatus.MemberNotFound, status);
        }
    }

    [Fact]
    public async Task SetMemberRole_LearnerCannotDrain()
    {
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Learner)));
            await WaitForIdleAsync(manager);

            (RaftOperationStatus status, _) = await SendSetRole(manager, "b:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.Errored, status);

            // Learner → Voter stays on the PromoteMember path, not SetMemberRole.
            (status, _) = await SendSetRole(manager, "b:1", ClusterMemberRole.Voter);
            Assert.Equal(RaftOperationStatus.Errored, status);
        }
    }

    // ── Placement pass: drain sweep and evacuation ───────────────────────────

    [Fact]
    public async Task PlacementPass_FullyEvacuatedLeaver_IsRemovedWithoutAWaiter()
    {
        // Crash-resumption: a Leaving member no range names must converge to removal on the
        // next pass even when the departing node's RequestLeaveAsync waiter is long gone.
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter), ("c:1", ClusterMemberRole.Leaving)));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"))));
            await WaitForIdleAsync(manager);

            ForceP0Leadership(manager);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            Assert.DoesNotContain(manager.SystemCoordinator.GetMembership().Members, m => m.Endpoint == "c:1");
        }
    }

    [Fact]
    public async Task PlacementPass_LeaverStillNamed_IsNotRemoved()
    {
        // The auto-removal must gate on "no range names it" — never remove a member whose
        // evacuation is still in flight.
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter), ("c:1", ClusterMemberRole.Leaving)));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            ForceP0Leadership(manager);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            Assert.Contains(manager.SystemCoordinator.GetMembership().Members, m => m.Endpoint == "c:1");
        }
    }

    [Fact]
    public async Task PlacementPass_EvacuatesLeaverViaShed_ThenRemovesIt()
    {
        // End-to-end drain at the coordinator level, shed path: with RF 2 and two healthy
        // survivors, the leaver's replica is dead weight the planner sheds as a repair; the
        // following pass observes the map no longer names the leaver and commits its removal.
        RaftManager manager = Build(replicationFactor: 2, enablePlacementRebalancer: true);
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter), ("c:1", ClusterMemberRole.Voter)));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            ForceP0Leadership(manager);

            // Committing Voter → Leaving kicks a pass by itself (no explicit RunPlacementPass):
            // it plans the shed of c:1's replica (repair class — c:1 left the candidate set).
            (RaftOperationStatus status, _) = await SendSetRole(manager, "c:1", ClusterMemberRole.Leaving);
            Assert.Equal(RaftOperationStatus.Success, status);
            await RunEnqueuedPassToCompletionAsync(manager);

            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
            // The evacuation and the removal are separate commits, in that order — the map must
            // stop naming the leaver first, so at this point c:1 may still be in the roster.

            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            Assert.DoesNotContain(manager.SystemCoordinator.GetMembership().Members, m => m.Endpoint == "c:1");
            Assert.Equal(2, MapEntry(manager, 1).Replicas.Count);
        }
    }

    [Fact]
    public async Task PlacementPass_TransitionalLearnerOnLeaver_IsDroppedNotPromoted()
    {
        // A learner replica sitting on the draining node is work the drain would undo:
        // promoting it makes the leaver a voter the next pass must evacuate again. It must be
        // dropped instead. Runs with the rebalancer OFF: transitional handling is part of the
        // always-on pass contract.
        RaftManager manager = Build();
        using (manager)
        {
            AcceptReplication(manager);
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter), ("c:1", ClusterMemberRole.Leaving)));
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 2, Replica(Local), Replica("b:1"), Replica("c:1", RaftReplicaRole.Learner))));
            await WaitForIdleAsync(manager);

            ForceP0Leadership(manager);
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RunPlacementPass));
            await RunEnqueuedPassToCompletionAsync(manager);

            Assert.DoesNotContain(MapEntry(manager, 1).Replicas, r => r.Endpoint == "c:1");
        }
    }

    // ── Peer-set regression guard ─────────────────────────────────────────────

    [Fact]
    public async Task UpdateNodes_LeavingMember_StaysInPeerSet()
    {
        // The single highest-risk item of the drain: the moment Leaving commits, the departing
        // node must STAY in manager.Nodes. Dropping it severs heartbeats and replication both
        // ways, the evacuating learner can never catch up, and every drain wedges. This test
        // fails on the ClusterHandler.UpdateNodes filter alone — no end-to-end timeout needed.
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                (Local, ClusterMemberRole.Voter), ("b:1", ClusterMemberRole.Voter), ("c:1", ClusterMemberRole.Leaving)));
            // UpdateNodes no-ops on a manager that hosts nothing; give it one hosted range.
            manager.SystemCoordinator.Send(MakeConfigReplicated(
                PlacedRange(1, 1, Replica(Local), Replica("b:1"), Replica("c:1"))));
            await WaitForIdleAsync(manager);

            try
            {
                await manager.UpdateNodes();
            }
            catch (RaftException)
            {
                // The harness has no system partition, so the promotion/eviction checks that run
                // AFTER the peer-set refresh throw. Nodes has already been rebuilt at that point,
                // which is all this test observes.
            }

            Assert.Contains(manager.GetNodes(), n => n.Endpoint == "c:1");
            Assert.Contains(manager.GetNodes(), n => n.Endpoint == "b:1");
            Assert.DoesNotContain(manager.GetNodes(), n => n.Endpoint == Local);
        }
    }
}
