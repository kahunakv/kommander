
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
/// Failure-leg tests for the system coordinator's membership machinery and
/// checkpoint-restore path.
///
/// Tests cover:
///   1. Membership replication refused by lost leadership — the failure surfaces,
///      the change latch is released, and a retry commits
///   2. Transient membership replication failures are retried until committed
///   3. Membership seeding is skipped when a roster is already committed, and a
///      second leadership term never re-seeds over the first
///   4. Dead-member eviction commits for an unreachable member, but a live-but-
///      silent member is resurrected by the last-chance probe and kept
///   5. Removing the local voter completes without wedging the coordinator loop
///   6. A restored checkpoint snapshot rebuilds the map and roster; a malformed
///      payload is dropped without wedging; an older snapshot cannot regress a
///      newer committed roster
/// </summary>
public sealed class TestSystemCoordinatorFailures
{
    // ── Builders ───────────────────────────────────────────────────────────────

    private static RaftManager Build(
        int initialPartitions = 0,
        TimeSpan? deadMemberEvictionGrace = null,
        Kommander.Communication.Memory.InMemoryCommunication? communication = null,
        int port = 9000)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions
        };
        if (deadMemberEvictionGrace is { } grace)
            config.DeadMemberEvictionGrace = grace;

        return new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            communication ?? new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance
        );
    }

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

    private static RaftSystemRequest MakeMembersReplicated(long version, params (string Endpoint, int NodeId)[] voters)
    {
        ClusterMembership membership = new()
        {
            MembershipVersion = version,
            Members =
            [
                .. voters.Select(v => new ClusterMember
                {
                    Endpoint = v.Endpoint, NodeId = v.NodeId, Role = ClusterMemberRole.Voter, JoinedVersion = 1
                })
            ]
        };

        return new RaftSystemRequest(
            RaftSystemRequestType.ConfigReplicated,
            SerializeMessage(RaftSystemConfigKeys.Members, JsonSerializer.Serialize(membership)));
    }

    /// <summary>Serializes a checkpoint snapshot payload the way a P0 checkpoint proposal would carry it.</summary>
    private static byte[] SerializeCheckpointSnapshot(params (string Key, string Value)[] entries)
    {
        RaftSystemCheckpointSnapshot snapshot = new();
        foreach ((string key, string value) in entries)
            snapshot.Entries.Add(new RaftSystemMessage { Key = key, Value = value });

        using MemoryStream ms = new();
        snapshot.WriteTo(ms);
        return ms.ToArray();
    }

    private static Task WaitForIdleAsync(RaftManager manager) =>
        manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));

    private static void ForceP0Leadership(RaftManager manager) =>
        manager._amILeaderQuickHookForTesting =
            partitionId => ValueTask.FromResult(partitionId == RaftSystemConfig.SystemPartition);

    /// <summary>Records every replicated payload's config key so tests can assert what was committed.</summary>
    private static List<string> CaptureReplicatedKeys(RaftManager manager)
    {
        List<string> keys = [];
        manager.SystemCoordinator.ReplicateOverride = (_, data, _, _) =>
        {
            lock (keys)
            {
                using MemoryStream ms = new(data);
                keys.Add(RaftSystemMessage.Parser.ParseFrom(ms).Key);
            }
            return Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));
        };
        return keys;
    }

    private static int CountKeys(List<string> keys, string key)
    {
        lock (keys)
            return keys.Count(k => string.Equals(k, key, StringComparison.Ordinal));
    }

    private static async Task<(RaftOperationStatus Status, long Version)> SendMembershipChange(
        RaftManager manager, RaftSystemRequestType type, string endpoint, int nodeId, long expectedVersion)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.SystemCoordinator.Send(new RaftSystemRequest(type, endpoint, nodeId, expectedVersion, tcs));
        return await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // ── Membership replication failures ────────────────────────────────────────

    [Fact]
    public async Task AddMember_ReplicationRefusedByLostLeadership_SurfacesFailureAndReleasesChangeLatch()
    {
        RaftManager manager = Build();
        using (manager)
        {
            int calls = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                Interlocked.Increment(ref calls);
                return Task.FromResult(new RaftReplicationResult(false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero, 0));
            };

            (RaftOperationStatus status, long version) =
                await SendMembershipChange(manager, RaftSystemRequestType.AddMember, "node1:9001", 1, 0);

            Assert.Equal(RaftOperationStatus.NodeIsNotLeader, status);
            Assert.Equal(0, version);
            Assert.Equal(1, calls); // hand the result back, stop — no retry storm

            // Nothing was committed: the roster is still the empty pre-seed one.
            Assert.Equal(0, manager.SystemCoordinator.GetMembership().MembershipVersion);

            // The in-flight latch must be released even on failure, otherwise every
            // later change would be refused as concurrent forever.
            await WaitForIdleAsync(manager);
            Assert.False(manager.SystemCoordinator.MembershipChangePendingForTest);

            // A retry on recovered leadership commits the learner.
            manager.SystemCoordinator.ReplicateOverride = static (_, _, _, _) =>
                Task.FromResult(new RaftReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, 1));

            (RaftOperationStatus retryStatus, long retryVersion) =
                await SendMembershipChange(manager, RaftSystemRequestType.AddMember, "node1:9001", 1, 0);

            Assert.Equal(RaftOperationStatus.Success, retryStatus);
            Assert.Equal(1, retryVersion);
            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Equal(1, roster.MembershipVersion);
            Assert.Equal(ClusterMemberRole.Learner, Assert.Single(roster.Members).Role);
        }
    }

    [Fact]
    public async Task AddMember_TransientReplicationFailures_RetriedUntilCommitted()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.RetryDelay = TimeSpan.Zero;

            int calls = 0;
            manager.SystemCoordinator.ReplicateOverride = (_, _, _, _) =>
            {
                int n = Interlocked.Increment(ref calls);
                bool success = n >= 3;
                return Task.FromResult(new RaftReplicationResult(
                    success,
                    success ? RaftOperationStatus.Success : RaftOperationStatus.Errored,
                    HLCTimestamp.Zero,
                    n));
            };

            (RaftOperationStatus status, long version) =
                await SendMembershipChange(manager, RaftSystemRequestType.AddMember, "node1:9001", 1, 0);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(1, version);
            Assert.Equal(3, calls); // two transient failures, third attempt commits

            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Single(roster.Members);
            Assert.Equal("node1:9001", roster.Members[0].Endpoint);
        }
    }

    // ── Membership seeding ─────────────────────────────────────────────────────

    [Fact]
    public async Task MembershipSeed_RosterAlreadyCommitted_LeadershipDoesNotReseed()
    {
        RaftManager manager = Build();
        using (manager)
        {
            // A restart that restored its map, then learned the cluster's roster.
            manager.SystemCoordinator.Send(new RaftSystemRequest(
                RaftSystemRequestType.ConfigRestored,
                SerializeMessage(
                    RaftSystemConfigKeys.Partitions,
                    JsonSerializer.Serialize(new RaftPartitionMap
                    {
                        MapVersion = 4,
                        Partitions =
                        [
                            new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 2, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
                        ]
                    }))));
            manager.SystemCoordinator.Send(MakeMembersReplicated(5, ("localhost:9000", 1), ("b:1", 2)));
            await WaitForIdleAsync(manager);

            List<string> keys = CaptureReplicatedKeys(manager);
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            // This node wins P0: it must re-assert the restored map but never write a
            // fresh version-1 roster over the cluster's committed version-5 one.
            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await WaitForIdleAsync(manager);
            await WaitForIdleAsync(manager);

            Assert.Equal(1, CountKeys(keys, RaftSystemConfigKeys.Partitions));
            Assert.Equal(0, CountKeys(keys, RaftSystemConfigKeys.Members));

            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Equal(5, roster.MembershipVersion);
            Assert.Equal(2, roster.Members.Count);
            Assert.True(manager.IsInitialized);
        }
    }

    [Fact]
    public async Task MembershipSeed_SecondLeadershipTerm_DoesNotReseedTheRoster()
    {
        RaftManager manager = Build(initialPartitions: 1);
        using (manager)
        {
            List<string> keys = CaptureReplicatedKeys(manager);
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await WaitForIdleAsync(manager);
            await WaitForIdleAsync(manager);

            manager.SystemCoordinator.Send(
                new RaftSystemRequest(RaftSystemRequestType.LeaderChanged, manager.LocalEndpoint));
            await WaitForIdleAsync(manager);
            await WaitForIdleAsync(manager);

            // First term: initial map + one roster seed. Second term: the map is
            // re-asserted for late joiners, the roster is not seeded again.
            Assert.Equal(2, CountKeys(keys, RaftSystemConfigKeys.Partitions));
            Assert.Equal(1, CountKeys(keys, RaftSystemConfigKeys.Members));

            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Equal(1, roster.MembershipVersion);
            ClusterMember member = Assert.Single(roster.Members);
            Assert.Equal(manager.LocalEndpoint, member.Endpoint);
            Assert.Equal(ClusterMemberRole.Voter, member.Role);
        }
    }

    // ── Dead-member eviction ───────────────────────────────────────────────────

    [Fact]
    public async Task EvictDeadMembers_UnreachableMemberPastGrace_CommitsItsRemoval()
    {
        RaftManager manager = Build(deadMemberEvictionGrace: TimeSpan.Zero);
        using (manager)
        {
            manager.SystemCoordinator.Send(MakeMembersReplicated(
                1, ("localhost:9000", 1), ("ghost:9000", 2), ("other:9001", 3)));
            await WaitForIdleAsync(manager);

            List<string> keys = CaptureReplicatedKeys(manager);
            ForceP0Leadership(manager);

            // The member went Silent and then Dead; nothing answers for it on the
            // transport, so the last-chance probe cannot resurrect it.
            manager.Liveness.MarkSuspect("ghost:9000");
            manager.Liveness.AdvanceExpiry(DateTimeOffset.UtcNow, TimeSpan.Zero);

            await manager.SystemCoordinator.EvictDeadMembersAsync();
            await WaitForIdleAsync(manager);
            await WaitForIdleAsync(manager);

            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Equal(2, roster.MembershipVersion);
            Assert.DoesNotContain(roster.Members, m => m.Endpoint == "ghost:9000");
            Assert.Equal(2, roster.Members.Count);

            // Exactly one membership commit: the eviction's RemoveMember.
            Assert.Equal(1, CountKeys(keys, RaftSystemConfigKeys.Members));
        }
    }

    [Fact]
    public async Task EvictDeadMembers_LiveButSilentMember_LastChanceProbeResurrectsAndKeepsIt()
    {
        Kommander.Communication.Memory.InMemoryCommunication communication = new();
        RaftManager manager = Build(deadMemberEvictionGrace: TimeSpan.Zero, communication: communication);
        using (manager)
        using (RaftManager silent = Build(communication: communication, port: 9100))
        {
            communication.SetNodes(new Dictionary<string, IRaft> { [silent.LocalEndpoint] = silent });

            manager.SystemCoordinator.Send(MakeMembersReplicated(1, ("localhost:9000", 1), (silent.LocalEndpoint, 2)));
            await WaitForIdleAsync(manager);

            List<string> keys = CaptureReplicatedKeys(manager);
            ForceP0Leadership(manager);

            // Gossip went quiet long enough for the member to be declared Dead — but
            // the process is alive and still answers a direct ping.
            manager.Liveness.MarkSuspect(silent.LocalEndpoint);
            manager.Liveness.AdvanceExpiry(DateTimeOffset.UtcNow, TimeSpan.Zero);
            Assert.Equal(Kommander.Gossip.MemberLivenessState.Dead, manager.Liveness.GetState(silent.LocalEndpoint));

            await manager.SystemCoordinator.EvictDeadMembersAsync();
            await WaitForIdleAsync(manager);

            // The probe resurrected the member: no removal was committed.
            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Equal(1, roster.MembershipVersion);
            Assert.Equal(2, roster.Members.Count);
            Assert.Equal(0, CountKeys(keys, RaftSystemConfigKeys.Members));
            Assert.Equal(Kommander.Gossip.MemberLivenessState.Alive, manager.Liveness.GetState(silent.LocalEndpoint));
        }
    }

    // ── Self-removal ───────────────────────────────────────────────────────────

    [Fact]
    public async Task RemoveMember_SelfRemovedFromVoterSet_CommitsWithoutWedgingTheCoordinator()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.Send(MakeMembersReplicated(1, ("localhost:9000", 1), ("other:9001", 2)));
            await WaitForIdleAsync(manager);

            CaptureReplicatedKeys(manager);
            ForceP0Leadership(manager);

            (RaftOperationStatus status, long version) =
                await SendMembershipChange(manager, RaftSystemRequestType.RemoveMember, manager.LocalEndpoint, 1, 1);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(2, version);

            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Single(roster.Members);
            Assert.Equal("other:9001", roster.Members[0].Endpoint);

            // The step-down of a self-removed node runs fire-and-forget; it must
            // never wedge the loop that every later system message flows through.
            await WaitForIdleAsync(manager);
            await Task.Delay(100);
            Assert.False(manager.SystemCoordinator.LoopTask.IsFaulted);
        }
    }

    // ── Restored checkpoint snapshots ──────────────────────────────────────────

    [Fact]
    public async Task RestoredCheckpointSnapshot_RebuildsPartitionMapAndRoster()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            // A node whose P0 WAL was compacted past the original records restores
            // them from the checkpoint payload instead.
            byte[] payload = SerializeCheckpointSnapshot(
                (RaftSystemConfigKeys.Partitions, JsonSerializer.Serialize(new RaftPartitionMap
                {
                    MapVersion = 6,
                    Partitions =
                    [
                        new() { PartitionId = 1, StartRange = 0, EndRange = 499, Generation = 3, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange },
                        new() { PartitionId = 2, StartRange = 500, EndRange = 999, Generation = 3, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
                    ]
                })),
                (RaftSystemConfigKeys.Members, JsonSerializer.Serialize(new ClusterMembership
                {
                    MembershipVersion = 3,
                    Members =
                    [
                        new() { Endpoint = "localhost:9000", NodeId = 1, Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
                        new() { Endpoint = "b:1", NodeId = 2, Role = ClusterMemberRole.Voter, JoinedVersion = 2 }
                    ]
                })));

            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.ConfigCheckpointRestored, payload));
            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.RestoreCompleted));
            await WaitForIdleAsync(manager);

            Assert.True(manager.IsInitialized);
            Assert.Equal(2, manager.GetPartitionMap().Count);
            Assert.True(manager.Partitions.ContainsKey(1));
            Assert.True(manager.Partitions.ContainsKey(2));

            ClusterMembership roster = manager.SystemCoordinator.GetMembership();
            Assert.Equal(3, roster.MembershipVersion);
            Assert.Equal(2, roster.Members.Count);
        }
    }

    [Fact]
    public async Task RestoredCheckpointSnapshot_MalformedPayload_IsDroppedAndCoordinatorKeepsWorking()
    {
        RaftManager manager = Build();
        using (manager)
        {
            manager.SystemCoordinator.StartPartitionsOverride = ranges => manager.StartUserPartitions(ranges);

            manager.SystemCoordinator.Send(new RaftSystemRequest(
                RaftSystemRequestType.ConfigCheckpointRestored, [0xFF, 0xFF, 0xFF, 0xFF]));
            await WaitForIdleAsync(manager);

            // The corrupt payload installed nothing...
            Assert.Empty(manager.GetPartitionMap());
            Assert.Equal(0, manager.SystemCoordinator.GetMembership().MembershipVersion);

            // ...and the loop is still healthy: a subsequent valid map materializes.
            manager.SystemCoordinator.Send(MakeConfigReplicated(
            [
                new() { PartitionId = 1, StartRange = 0, EndRange = int.MaxValue, Generation = 1, State = RaftPartitionState.Active, RoutingMode = RaftRoutingMode.HashRange }
            ]));
            await WaitForIdleAsync(manager);

            Assert.Single(manager.GetPartitionMap());
        }
    }

    [Fact]
    public async Task RestoredCheckpointSnapshot_OlderThanCommittedRoster_DoesNotRegressMembership()
    {
        RaftManager manager = Build();
        using (manager)
        {
            // The locally committed roster is already at version 9.
            manager.SystemCoordinator.Send(MakeMembersReplicated(9, ("localhost:9000", 1)));
            await WaitForIdleAsync(manager);

            byte[] stalePayload = SerializeCheckpointSnapshot(
                (RaftSystemConfigKeys.Members, JsonSerializer.Serialize(new ClusterMembership
                {
                    MembershipVersion = 3,
                    Members = [new() { Endpoint = "localhost:9000", NodeId = 1, Role = ClusterMemberRole.Voter, JoinedVersion = 1 }]
                })));

            manager.SystemCoordinator.Send(new RaftSystemRequest(RaftSystemRequestType.ConfigCheckpointRestored, stalePayload));
            await WaitForIdleAsync(manager);

            Assert.Equal(9, manager.SystemCoordinator.GetMembership().MembershipVersion);
        }
    }
}
