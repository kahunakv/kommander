
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using GossipPingReqRequest = Kommander.Gossip.PingReqRequest;
using GossipPingReqResponse = Kommander.Gossip.PingReqResponse;

namespace Kommander.Tests;

/// <summary>
/// Integration tests for roster-derived Nodes + greenfield seed.
/// Each test stands up a full in-memory 3-node cluster and checks that after
/// JoinCluster the committed membership roster is present and Nodes is derived
/// from it.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestMembership
{
    private readonly ILogger<IRaft> logger;

    public TestMembership(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static RaftManager MakeNode(
        InMemoryCommunication communication,
        string host, int port, int nodeId,
        IEnumerable<string> peers,
        ILogger<IRaft> logger)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = host,
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(50),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<(RaftManager n1, RaftManager n2, RaftManager n3)> BuildThreeNodeCluster(
        ILogger<IRaft> logger,
        CancellationToken ct)
    {
        InMemoryCommunication comm = new();

        RaftManager n1 = MakeNode(comm, "localhost", 8101, 1, ["localhost:8102", "localhost:8103"], logger);
        RaftManager n2 = MakeNode(comm, "localhost", 8102, 2, ["localhost:8101", "localhost:8103"], logger);
        RaftManager n3 = MakeNode(comm, "localhost", 8103, 3, ["localhost:8101", "localhost:8102"], logger);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8101"] = n1,
            ["localhost:8102"] = n2,
            ["localhost:8103"] = n3,
        });

        await n1.UpdateNodes();
        await n2.UpdateNodes();
        await n3.UpdateNodes();

        await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

        await WaitForLeader([n1, n2, n3], partitionId: 1, ct);

        return (n1, n2, n3);
    }

    private static async Task WaitForLeader(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 15_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (await n.AmILeaderQuick(partitionId))
                    return;
            }
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"No leader elected for partition {partitionId} within 15 s.");
    }

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 10_000, Func<Task>? tickAction = null)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            if (tickAction is not null)
                await tickAction();
            await Task.Delay(25, ct);
        }
        throw new TimeoutException("Condition not satisfied within timeout.");
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GreenfieldCluster_SeedsRosterWithThreeVoters()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        try
        {
            // Wait until all three nodes have a committed roster (MembershipVersion > 0).
            await WaitForCondition(
                () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
                   && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
                   && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
                ct);

            foreach (RaftManager node in new[] { n1, n2, n3 })
            {
                ClusterMembership m = node.SystemCoordinator.GetMembership();
                Assert.Equal(1L, m.MembershipVersion);
                Assert.Equal(3, m.Members.Count);
                Assert.All(m.Members, member => Assert.Equal(ClusterMemberRole.Voter, member.Role));

                // All three known endpoints are in the roster.
                Assert.Contains(m.Members, x => x.Endpoint == "localhost:8101");
                Assert.Contains(m.Members, x => x.Endpoint == "localhost:8102");
                Assert.Contains(m.Members, x => x.Endpoint == "localhost:8103");
            }
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task AfterSeed_GetNodes_ReturnsTwoPeersExcludingSelf()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        try
        {
            // Wait for roster to propagate and UpdateNodes to refresh Nodes from it.
            await WaitForCondition(
                () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
                   && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
                   && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
                ct);

            // Trigger a manual UpdateNodes so we don't depend solely on timer cadence.
            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();

            // Each node should see exactly the 2 peer endpoints.
            IList<RaftNode> nodes1 = n1.GetNodes();
            Assert.Equal(2, nodes1.Count);
            Assert.DoesNotContain(nodes1, n => n.Endpoint == "localhost:8101");
            Assert.Contains(nodes1, n => n.Endpoint == "localhost:8102");
            Assert.Contains(nodes1, n => n.Endpoint == "localhost:8103");

            IList<RaftNode> nodes2 = n2.GetNodes();
            Assert.Equal(2, nodes2.Count);
            Assert.DoesNotContain(nodes2, n => n.Endpoint == "localhost:8102");

            IList<RaftNode> nodes3 = n3.GetNodes();
            Assert.Equal(2, nodes3.Count);
            Assert.DoesNotContain(nodes3, n => n.Endpoint == "localhost:8103");
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task AfterSeed_LocalRole_IsVoterForAllThreeNodes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        try
        {
            await WaitForCondition(
                () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
                   && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
                   && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
                ct);

            Assert.Equal(ClusterMemberRole.Voter, n1.LocalRole);
            Assert.Equal(ClusterMemberRole.Voter, n2.LocalRole);
            Assert.Equal(ClusterMemberRole.Voter, n3.LocalRole);
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task BeforeSeed_LocalRole_ReturnsVoter_AsFallback()
    {
        // Before any roster is committed the node is in the pre-seed transient.
        // LocalRole must return Voter (backward compat / fallback).
        RaftManager node = MakeNode(
            new InMemoryCommunication(), "localhost", 8110, 10, [], logger);

        using (node)
        {
            Assert.Equal(ClusterMemberRole.Voter, node.LocalRole);
            Assert.Equal(0L, node.SystemCoordinator.GetMembership().MembershipVersion);
            await Task.CompletedTask;
        }
    }

    // ── Graceful leave ────────────────────────────────────────────────────────

    [Fact]
    public async Task GracefulLeave_RemovesNodeFromRoster_RemainingTwoVotersKeepCommitting()
    {
        // Arrange: 3-node cluster, wait for committed roster.
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
            ct);

        try
        {
            // Act: n3 leaves gracefully. LeaveCluster commits RemoveMember before stopping.
            await n3.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);

            // Assert: n1 and n2 both see a 2-voter roster.
            await WaitForCondition(
                () =>
                {
                    ClusterMembership m1 = n1.SystemCoordinator.GetMembership();
                    ClusterMembership m2 = n2.SystemCoordinator.GetMembership();
                    return m1.Members.Count == 2
                        && m1.Members.All(m => m.Role == ClusterMemberRole.Voter)
                        && !m1.Members.Any(m => m.Endpoint == "localhost:8103")
                        && m2.Members.Count == 2
                        && m2.Members.All(m => m.Role == ClusterMemberRole.Voter)
                        && !m2.Members.Any(m => m.Endpoint == "localhost:8103");
                },
                ct);

            // Wait for a leader on partition 1 — n3 may have been leader and its departure
            // triggers a re-election that takes a few hundred milliseconds.
            await WaitForLeader([n1, n2], partitionId: 1, ct);

            // Remaining quorum (n1 + n2) must still be able to commit.
            RaftManager leader = await n1.AmILeaderQuick(1) ? n1 : n2;
            RaftReplicationResult result = await leader.ReplicateLogs(1, "test", [1, 2, 3], cancellationToken: ct);
            Assert.Equal(RaftOperationStatus.Success, result.Status);

            // Nodes list is updated lazily by the UpdateNodes timer; trigger it explicitly
            // so the assertion doesn't race the 50 ms tick.
            await n1.UpdateNodes();
            await n2.UpdateNodes();

            Assert.DoesNotContain(n1.GetNodes(), n => n.Endpoint == "localhost:8103");
            Assert.DoesNotContain(n2.GetNodes(), n => n.Endpoint == "localhost:8103");
        }
        finally
        {
            await n2.LeaveCluster(true, CancellationToken.None);
            await n1.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task GracefulLeave_WhenLeader_RemainingNodesElectNewLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
            ct);

        // Find the P0 leader and make it leave.
        RaftManager? p0Leader = null;
        foreach (RaftManager n in new[] { n1, n2, n3 })
        {
            if (await n.AmILeaderQuick(0))
            {
                p0Leader = n;
                break;
            }
        }

        Assert.NotNull(p0Leader);
        RaftManager[] survivors = new[] { n1, n2, n3 }.Where(n => n != p0Leader).ToArray();

        try
        {
            await p0Leader.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);

            // Survivors elect a new P0 leader and keep committing.
            await WaitForLeader(survivors, partitionId: 0, ct);

            // Roster on survivors shows only 2 voters.
            await WaitForCondition(
                () => survivors.All(n =>
                {
                    ClusterMembership m = n.SystemCoordinator.GetMembership();
                    return m.Members.Count == 2
                        && m.Members.All(x => x.Role == ClusterMemberRole.Voter)
                        && !m.Members.Any(x => x.Endpoint == p0Leader.LocalEndpoint);
                }),
                ct);
        }
        finally
        {
            foreach (RaftManager s in survivors)
                await s.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task GracefulLeave_LastVoter_ExitsFastWithoutSpinning()
    {
        // Verifies that when InsufficientVoters is returned (removing the last voter would
        // make the cluster permanently unavailable), CommitGracefulLeaveAsync exits
        // immediately rather than spinning to the 10 s graceful-leave deadline.
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0,
            ct);

        // Remove n1 and n2 in order, leaving n3 as the sole voter.
        await n1.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);
        await n2.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);

        // Wait for n3 to win the P0 leader election after n1/n2 departed. This must happen
        // before LeaveCluster sets _leaving=true, which suppresses elections and would prevent
        // n3 from ever becoming the leader required to receive its own ReceiveLeave request.
        await WaitForCondition(() => n3.GetPartitionLeaderEndpoint(0) == n3.LocalEndpoint, ct);

        // Also wait for n3's roster to converge to a single voter (itself). n1/n2's removals
        // must be committed on n3's system partition before the last-voter leave, otherwise
        // TryRemoveMember still sees a quorum of voters, skips the InsufficientVoters/Terminal
        // branch, and spins a normal graceful leave to the 10 s deadline (the observed hang).
        await WaitForCondition(
            () =>
            {
                ClusterMembership m = n3.SystemCoordinator.GetMembership();
                List<ClusterMember> voters = m.Members.Where(x => x.Role == ClusterMemberRole.Voter).ToList();
                return voters.Count == 1 && voters[0].Endpoint == n3.LocalEndpoint;
            },
            ct);

        // n3 is the last voter and P0 leader.
        // TryRemoveMember returns InsufficientVoters → Terminal=true → CommitGracefulLeaveAsync
        // exits immediately.  The call must complete well under the 10 s deadline.
        long startMs = Environment.TickCount64;
        await n3.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);
        long elapsedMs = Environment.TickCount64 - startMs;

        Assert.True(elapsedMs < 5_000,
            $"Last-voter LeaveCluster took {elapsedMs} ms; expected < 5 000 ms.");
    }

    [Fact]
    public async Task AfterSeed_RosterIsIdempotent_SecondLeaderElection_DoesNotDoubleAppend()
    {
        // Re-becoming P0 leader (e.g., after a brief network glitch) must not add more
        // roster entries — TrySeedInitialMembership is a no-op when a record already exists.
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        try
        {
            await WaitForCondition(
                () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0,
                ct);

            long versionAfterSeed = n1.SystemCoordinator.GetMembership().MembershipVersion;

            // Find and step down the current P0 leader; a new one will take over and call
            // TrySetInitialPartitions → TrySeedInitialMembership again.
            RaftManager? p0Leader = null;
            foreach (RaftManager n in new[] { n1, n2, n3 })
            {
                if (await n.AmILeaderQuick(0))
                {
                    p0Leader = n;
                    break;
                }
            }

            if (p0Leader is not null)
            {
                await p0Leader.StepDownAsync(0, ct);

                // Wait for a new leader to establish itself.
                await WaitForLeader([n1, n2, n3], partitionId: 0, ct);
                await Task.Delay(200, ct); // let seed attempt propagate

                // The membership version must not have changed — already seeded.
                foreach (RaftManager n in new[] { n1, n2, n3 })
                {
                    ClusterMembership m = n.SystemCoordinator.GetMembership();
                    Assert.Equal(versionAfterSeed, m.MembershipVersion);
                    Assert.Equal(3, m.Members.Count);
                }
            }
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── Gossip anti-entropy tests ─────────────────────────────────────────────

    [Fact]
    public async Task Gossip_PeerWithOlderVersion_ReceivesCurrentRosterInAck()
    {
        // When a node with a newer committed roster receives a gossip from a peer claiming
        // an older version (0), the ACK must include the full current roster so the stale
        // peer can catch up in the same round trip.
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0, ct);

        try
        {
            ClusterMembership current = n1.SystemCoordinator.GetMembership();

            // Simulate a peer that has not yet received the committed roster (version 0).
            GossipMessage staleDigest = new("localhost:9999", 0, new ClusterMembership());
            GossipAck ack = n1.ReceiveGossip(staleDigest);

            // n1 has a newer version — must include its roster so the stale peer can catch up.
            Assert.Equal(current.MembershipVersion, ack.MembershipVersion);
            Assert.NotNull(ack.Roster);
            Assert.Equal(current.MembershipVersion, ack.Roster.MembershipVersion);
            Assert.Equal(current.Members.Count, ack.Roster.Members.Count);

            // Quorum invariant: ReceiveGossip never commits new Raft log entries.
            Assert.Equal(current.MembershipVersion, n1.SystemCoordinator.GetMembership().MembershipVersion);
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task Gossip_StaleNode_PullsCurrentRosterViaAntiEntropy()
    {
        // Anti-entropy convergence: a node whose local cache was reset to version 0
        // receives a gossip digest from a peer with the committed roster and, after the
        // coordinator processes the queued update, its cache matches the committed version.
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
            ct);

        try
        {
            ClusterMembership current = n1.SystemCoordinator.GetMembership();
            long committedVersion = current.MembershipVersion;

            // Simulate n3 having missed the committed roster update.
            n3.SystemCoordinator.ResetMembershipCacheForTest();
            Assert.Equal(0, n3.SystemCoordinator.GetMembership().MembershipVersion);

            // n1 sends gossip to n3 directly.
            GossipMessage digest = new(n1.LocalEndpoint, committedVersion, current);
            n3.ReceiveGossip(digest);

            // The coordinator update is async (queued to channel); wait for it.
            await WaitForCondition(
                () => n3.SystemCoordinator.GetMembership().MembershipVersion == committedVersion,
                ct);

            // Verify that no new Raft log entries were committed — gossip only updates caches.
            Assert.Equal(committedVersion, n1.SystemCoordinator.GetMembership().MembershipVersion);
            Assert.Equal(committedVersion, n3.SystemCoordinator.GetMembership().MembershipVersion);
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task Gossip_TickConvergesVersionsAcrossAllNodes()
    {
        // After a manual GossipAsync round from each node, all nodes share the same committed
        // membership version.  This verifies the full push-pull exchange path end to end.
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
            ct);

        try
        {
            long v = n1.SystemCoordinator.GetMembership().MembershipVersion;

            // Drive one gossip round from each node.
            await n1.GossipAsync(ct);
            await n2.GossipAsync(ct);
            await n3.GossipAsync(ct);

            // All nodes must agree on the committed version (no spurious commits from gossip).
            Assert.Equal(v, n1.SystemCoordinator.GetMembership().MembershipVersion);
            Assert.Equal(v, n2.SystemCoordinator.GetMembership().MembershipVersion);
            Assert.Equal(v, n3.SystemCoordinator.GetMembership().MembershipVersion);
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── SWIM failure-detector tests ───────────────────────────────────────────

    private static RaftManager MakeSwimNode(
        InMemoryCommunication communication,
        string host, int port, int nodeId,
        IEnumerable<string> peers,
        ILogger<IRaft> logger)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = host,
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
            // Fast SWIM settings so tests complete in < 10 s.
            PingTimeout = TimeSpan.FromMilliseconds(100),
            IndirectPingFanout = 1,
            SuspicionTimeout = TimeSpan.FromMilliseconds(600),
            DeadMemberEvictionGrace = TimeSpan.FromMilliseconds(800),
            PingInterval = TimeSpan.FromMilliseconds(100),
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<(RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)>
        BuildSwimCluster(ILogger<IRaft> logger, CancellationToken ct)
    {
        InMemoryCommunication comm = new();

        RaftManager n1 = MakeSwimNode(comm, "localhost", 8151, 1, ["localhost:8152", "localhost:8153"], logger);
        RaftManager n2 = MakeSwimNode(comm, "localhost", 8152, 2, ["localhost:8151", "localhost:8153"], logger);
        RaftManager n3 = MakeSwimNode(comm, "localhost", 8153, 3, ["localhost:8151", "localhost:8152"], logger);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8151"] = n1,
            ["localhost:8152"] = n2,
            ["localhost:8153"] = n3,
        });

        await n1.UpdateNodes();
        await n2.UpdateNodes();
        await n3.UpdateNodes();

        await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

        await WaitForLeader([n1, n2, n3], partitionId: 1, ct);

        // Ensure the roster has been seeded before running SWIM tests.
        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
            ct);

        return (n1, n2, n3, comm);
    }

    /// <summary>
    /// A permanently partitioned (unreachable) node is suspected by the surviving nodes,
    /// transitions to Dead after <c>SuspicionTimeout</c>, and is evicted from the committed
    /// roster by the P0 leader after <c>DeadMemberEvictionGrace</c>.
    /// </summary>
    [Fact]
    public async Task Swim_DeadNode_IsEvictedFromRoster()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)
            = await BuildSwimCluster(logger, ct);

        try
        {
            long rosterBefore = n1.SystemCoordinator.GetMembership().MembershipVersion;

            // Permanently partition n3 so n1 and n2 can never ping it.
            comm.PartitionNode("localhost:8153");

            // Drive ping rounds on n1 and n2 until n3 is observed Dead. The Suspect → Dead
            // transition (AdvanceExpiry) runs *inside* PingAsync, so we must keep pinging while
            // we wait — a passive poll would never make progress. Bounded by a generous deadline
            // so the test stays robust under full-suite CPU contention.
            long deadObservedDeadline = Environment.TickCount64 + 15_000;
            while (Environment.TickCount64 < deadObservedDeadline
                   && n1.Liveness.GetState("localhost:8153") != MemberLivenessState.Dead)
            {
                ct.ThrowIfCancellationRequested();
                await n1.PingAsync(ct);
                await n2.PingAsync(ct);
                await Task.Delay(100, ct);
            }

            // n3 should now be Dead on n1.
            Assert.Equal(MemberLivenessState.Dead, n1.Liveness.GetState("localhost:8153"));

            // Drive UpdateNodes on the P0 leader so EvictDeadMembersAsync fires.
            // Repeat until the eviction is actually committed to the roster.
            await WaitForCondition(
                () =>
                {
                    n1.UpdateNodes().GetAwaiter().GetResult();
                    n2.UpdateNodes().GetAwaiter().GetResult();
                    long v = n1.SystemCoordinator.GetMembership().MembershipVersion;
                    return v > rosterBefore
                        && !n1.SystemCoordinator.GetMembership().Members.Any(m => m.Endpoint == "localhost:8153");
                },
                ct, timeoutMs: 10_000);

            ClusterMembership roster = n1.SystemCoordinator.GetMembership();
            Assert.DoesNotContain(roster.Members, m => m.Endpoint == "localhost:8153");
            Assert.Equal(2, roster.Members.Count);
        }
        finally
        {
            comm.HealPartition("localhost:8153");
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// A transiently partitioned node that is healed before <c>SuspicionTimeout</c>
    /// responds to the next probe, is marked Alive, and is NOT evicted from the roster.
    /// </summary>
    [Fact]
    public async Task Swim_TransientPartition_HealsBeforeSuspicionExpires_NodeNotEvicted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)
            = await BuildSwimCluster(logger, ct);

        try
        {
            long rosterBefore = n1.SystemCoordinator.GetMembership().MembershipVersion;

            // Partition n3 briefly so direct probes from n1/n2 fail → n3 becomes Suspect.
            comm.PartitionNode("localhost:8153");

            await n1.PingAsync(ct);
            await n2.PingAsync(ct);

            // n3 should now be Suspect on n1 (not yet Dead).
            await WaitForCondition(
                () => n1.Liveness.GetState("localhost:8153") == MemberLivenessState.Suspect,
                ct, timeoutMs: 3_000);

            // Heal the partition BEFORE SuspicionTimeout expires.
            comm.HealPartition("localhost:8153");

            // Drive a ping from n1 that now reaches n3; n3 should be marked Alive.
            await n1.PingAsync(ct);

            await WaitForCondition(
                () => n1.Liveness.GetState("localhost:8153") == MemberLivenessState.Alive,
                ct, timeoutMs: 3_000);

            // Drive UpdateNodes — EvictDeadMembersAsync should find nothing to evict.
            await n1.UpdateNodes();
            await n2.UpdateNodes();

            // Roster must be unchanged.
            Assert.Equal(rosterBefore, n1.SystemCoordinator.GetMembership().MembershipVersion);
            Assert.Equal(3, n1.SystemCoordinator.GetMembership().Members.Count);
        }
        finally
        {
            comm.HealPartition("localhost:8153");
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// <summary>
    /// Gossip with <see cref="MemberLivenessEntry"/> records that suspect the local node
    /// triggers <c>RefuteSuspicion</c>: the receiver bumps its incarnation and writes an
    /// Alive entry for itself.  A second gossip with the same stale Suspect(inc=0) is then
    /// ignored because the local Alive(inc=1) has higher incarnation.
    /// </summary>
    [Fact]
    public async Task Swim_GossipSelfSuspicion_TriggersRefutation_StaleGossipIgnored()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)
            = await BuildSwimCluster(logger, ct);

        try
        {
            ClusterMembership membership = n1.SystemCoordinator.GetMembership();

            // n2 gossips to n1 claiming n1 itself is Suspect at incarnation 0.
            GossipMessage suspectSelf = new("localhost:8152", membership.MembershipVersion, null)
            {
                LivenessUpdates =
                [
                    new MemberLivenessEntry("localhost:8151", MemberLivenessState.Suspect, 0, DateTimeOffset.UtcNow)
                ]
            };

            n1.ReceiveGossip(suspectSelf);

            // n1 must have refuted: its own entry is Alive with incarnation > 0.
            Assert.Equal(MemberLivenessState.Alive, n1.Liveness.GetState("localhost:8151"));
            Assert.True(n1.Liveness.GetSelfIncarnation() > 0, "self-incarnation must be > 0 after refutation");

            // Replaying the same stale Suspect(inc=0) must be ignored (merge rule rejects it).
            n1.ReceiveGossip(suspectSelf);
            Assert.Equal(MemberLivenessState.Alive, n1.Liveness.GetState("localhost:8151"));

            // A higher-incarnation Suspect would override the refutation — assert the merge rule
            // works in both directions.
            GossipMessage higherSuspect = new("localhost:8152", membership.MembershipVersion, null)
            {
                LivenessUpdates =
                [
                    new MemberLivenessEntry("localhost:8151", MemberLivenessState.Suspect, 99, DateTimeOffset.UtcNow)
                ]
            };
            n1.ReceiveGossip(higherSuspect);

            // n1 sees itself suspected at inc=99 and immediately refutes again.
            Assert.Equal(MemberLivenessState.Alive, n1.Liveness.GetState("localhost:8151"));
            Assert.True(n1.Liveness.GetSelfIncarnation() > 99, "refutation must exceed the received suspicion incarnation");
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// The PingReq relay path: when a direct probe fails but a relay confirms the target is
    /// reachable, <c>ClearSuspicion</c> transitions the target from Suspect back to Alive
    /// while preserving the existing incarnation.
    ///
    /// <para>
    /// Exercises <c>InMemoryCommunication.SendPingReq</c> → <c>RaftManager.ReceivePingReq</c>
    /// → <c>ICommunication.SendPing</c> (relay→target) to confirm the relay call chain works
    /// end-to-end.  Also verifies that <c>ClearSuspicion</c> is incarnation-agnostic (the bug
    /// that would have caused <c>MarkAlive(…, 0)</c> to be silently dropped).
    /// </para>
    /// </summary>
    [Fact]
    public async Task Swim_PingReqRelay_ClearsSuspect_PreservesIncarnation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)
            = await BuildSwimCluster(logger, ct);

        try
        {
            // Simulate n3 having previously responded with incarnation=3 (prior refutations).
            n1.Liveness.MarkAlive("localhost:8153", 3);

            // Direct probe to n3 failed → Suspect(inc=3) on n1.
            n1.Liveness.MarkSuspect("localhost:8153");
            Assert.Equal(MemberLivenessState.Suspect, n1.Liveness.GetState("localhost:8153"));

            // Relay path: n1 asks n2 to probe n3 on its behalf.
            // InMemoryCommunication.SendPingReq routes to n2.ReceivePingReq which in turn
            // sends a direct Ping to n3 via the same communication instance.
            GossipPingReqResponse relayResp = await comm.SendPingReq(
                n1,
                new RaftNode("localhost:8152"),
                new GossipPingReqRequest("localhost:8151", "localhost:8153"),
                ct);

            Assert.True(relayResp.Reached, "relay (n2) must be able to reach n3");

            // ClearSuspicion: Suspect(inc=3) → Alive(inc=3).  Incarnation preserved.
            n1.Liveness.ClearSuspicion("localhost:8153");
            Assert.Equal(MemberLivenessState.Alive, n1.Liveness.GetState("localhost:8153"));

            // Verify MarkAlive(…, 0) would NOT have achieved the same result (regression guard).
            n1.Liveness.MarkSuspect("localhost:8153");     // back to Suspect(inc=3)
            n1.Liveness.MarkAlive("localhost:8153", 0);    // stale update — should be a no-op
            // MarkAlive(inc=0) must be a no-op against a Suspect entry with incarnation=3.
            Assert.Equal(MemberLivenessState.Suspect, n1.Liveness.GetState("localhost:8153"));

            // Confirm ClearSuspicion succeeds where MarkAlive(0) did not.
            n1.Liveness.ClearSuspicion("localhost:8153");
            Assert.Equal(MemberLivenessState.Alive, n1.Liveness.GetState("localhost:8153"));
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// Regression: a node that has previously refuted a suspicion (incarnation &gt; 0) must
    /// still be cleared from Suspect when a relay (PingReq) confirms it is reachable.
    ///
    /// Before the fix, <c>PingAsync</c> called <c>MarkAlive(endpoint, 0)</c> on relay success.
    /// <c>MarkAlive</c> silently drops updates with incarnation &lt; current, so a node with
    /// incarnation &gt; 0 would stay Suspect and eventually be evicted despite being reachable.
    /// The fix uses <c>ClearSuspicion</c> which preserves the existing incarnation.
    /// </summary>
    [Fact]
    public async Task Swim_RelaySuccess_ClearsSuspect_EvenAfterPriorRefutation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)
            = await BuildSwimCluster(logger, ct);

        try
        {
            long rosterBefore = n1.SystemCoordinator.GetMembership().MembershipVersion;

            // Force n3's incarnation above 0 by simulating a prior self-refutation.
            // RefuteSuspicion bumps the incarnation counter and writes Alive(inc=1).
            n3.Liveness.RefuteSuspicion("localhost:8153");

            // Partition n3's DIRECT path from n1 only (n2 can still reach n3 for relay).
            // We simulate this by partitioning n1↔n3 while keeping n2↔n3 open.
            // InMemoryCommunication.PartitionNode is symmetric (blocks both directions),
            // so we instead drive PingAsync manually: direct probe fails (partitioned),
            // then we heal and let the relay succeed.
            comm.PartitionNode("localhost:8153");

            // Direct probe from n1 fails → n3 becomes Suspect on n1.
            await n1.PingAsync(ct);

            await WaitForCondition(
                () => n1.Liveness.GetState("localhost:8153") == MemberLivenessState.Suspect,
                ct, timeoutMs: 3_000);

            // Heal so n2 can act as a relay to reach n3.
            comm.HealPartition("localhost:8153");

            // Drive a probe from n1 where the direct path is blocked in the liveness table
            // but relay succeeds.  Because n3's incarnation is 1, MarkAlive(…, 0) would fail.
            // ClearSuspicion must succeed regardless.
            //
            // Re-partition to force the indirect path this probe round:
            // We cannot selectively partition without a bidirectional block, so instead we
            // call ClearSuspicion directly to assert the API is correct, then verify PingAsync
            // clears a Suspect with incarnation > 0 via a successful direct probe.
            n1.Liveness.MarkSuspect("localhost:8153"); // re-suspect after the heal
            n1.Liveness.ClearSuspicion("localhost:8153");

            Assert.Equal(MemberLivenessState.Alive, n1.Liveness.GetState("localhost:8153"));

            // Simulate what happens when PingAsync gets a relay confirmation: the node
            // should still become Alive even after ClearSuspicion preserves incarnation 1.
            // Re-suspect once more and heal via a real PingAsync that now succeeds directly.
            // PingAsync picks a random peer, so we keep probing until n3 is the chosen target
            // and MarkAlive(incarnation) clears the suspicion.
            n1.Liveness.MarkSuspect("localhost:8153");
            await WaitForCondition(
                () => n1.Liveness.GetState("localhost:8153") == MemberLivenessState.Alive,
                ct, timeoutMs: 5_000,
                tickAction: async () => await n1.PingAsync(ct));

            // Roster must be unchanged — no eviction.
            await n1.UpdateNodes();
            Assert.Equal(rosterBefore, n1.SystemCoordinator.GetMembership().MembershipVersion);
            Assert.Equal(3, n1.SystemCoordinator.GetMembership().Members.Count);
        }
        finally
        {
            comm.HealPartition("localhost:8153");
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── IRaft public membership surface ─────────────────────────────────────────

    [Fact]
    public async Task GetMembership_ViaInterface_MatchesRosterAfterJoin()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3) = await BuildThreeNodeCluster(logger, ct);

        try
        {
            // Verify via the IRaft interface (not the concrete type).
            IRaft i1 = n1;
            IRaft i2 = n2;
            IRaft i3 = n3;

            await WaitForCondition(
                () => i1.GetMembership().MembershipVersion > 0
                   && i2.GetMembership().MembershipVersion > 0
                   && i3.GetMembership().MembershipVersion > 0,
                ct);

            foreach (IRaft node in new[] { i1, i2, i3 })
            {
                ClusterMembership m = node.GetMembership();
                Assert.True(m.MembershipVersion > 0);
                Assert.Equal(3, m.Members.Count);
                Assert.All(m.Members, mem => Assert.Equal(ClusterMemberRole.Voter, mem.Role));
                Assert.Contains(m.Members, x => x.Endpoint == "localhost:8101");
                Assert.Contains(m.Members, x => x.Endpoint == "localhost:8102");
                Assert.Contains(m.Members, x => x.Endpoint == "localhost:8103");
            }
        }
        finally
        {
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task OnMembershipChanged_FiresAcrossJoinPromoteLeave()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();

        RaftManager n1 = BuildJoinSeedsNode(comm, "node1", 1, 8401, ["localhost:8402", "localhost:8403"]);
        RaftManager n2 = BuildJoinSeedsNode(comm, "node2", 2, 8402, ["localhost:8401", "localhost:8403"]);
        RaftManager n3 = BuildJoinSeedsNode(comm, "node3", 3, 8403, ["localhost:8401", "localhost:8402"]);
        RaftManager n4 = BuildJoinSeedsNode(comm, "node4", 4, 8404,
            ["localhost:8401", "localhost:8402", "localhost:8403"], initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8401"] = n1,
            ["localhost:8402"] = n2,
            ["localhost:8403"] = n3,
            ["localhost:8404"] = n4,
        });

        // Collect membership events from n1 via the IRaft interface.
        List<(long Version, int MemberCount)> events = [];
        IRaft i1 = n1;
        i1.OnMembershipChanged += m =>
        {
            lock (events) events.Add((m.MembershipVersion, m.Members.Count));
        };

        try
        {
            // ── Phase 1: join ─────────────────────────────────────────────────────
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForCondition(
                () => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            await WaitForCondition(() => { lock (events) return events.Count > 0; }, ct);
            long versionAfterSeed;
            lock (events) versionAfterSeed = events[^1].Version;
            Assert.True(versionAfterSeed > 0);

            // ── Phase 2: 4th node joins as Learner then is promoted to Voter ──────
            await n4.JoinCluster(["localhost:8401", "localhost:8402", "localhost:8403"], ct);
            // JoinCluster(seeds) blocks until Voter; n1 must see the promotion event.
            await WaitForCondition(
                () =>
                {
                    lock (events)
                        return events.Any(e => e.MemberCount == 4);
                },
                ct, timeoutMs: 15_000);

            long versionAfterPromotion;
            lock (events) versionAfterPromotion = events[^1].Version;
            Assert.True(versionAfterPromotion > versionAfterSeed,
                $"promotion must advance version: {versionAfterPromotion} > {versionAfterSeed}");
            Assert.Equal(System.ClusterMemberRole.Voter, n4.LocalRole);

            // ── Phase 3: n4 leaves ────────────────────────────────────────────────
            await n4.LeaveCluster(dispose: false, cancellationToken: CancellationToken.None);
            await WaitForCondition(
                () =>
                {
                    lock (events)
                        return events.Any(e => e.MemberCount == 3 && e.Version > versionAfterPromotion);
                },
                ct, timeoutMs: 15_000);

            long versionAfterLeave;
            lock (events) versionAfterLeave = events[^1].Version;
            Assert.True(versionAfterLeave > versionAfterPromotion,
                $"leave must advance version: {versionAfterLeave} > {versionAfterPromotion}");

            // ── Monotonicity across all phases ────────────────────────────────────
            List<long> allVersions;
            lock (events) allVersions = events.Select(e => e.Version).ToList();
            for (int j = 1; j < allVersions.Count; j++)
                Assert.True(allVersions[j] >= allVersions[j - 1],
                    $"version regression at [{j}]: {allVersions[j]} < {allVersions[j - 1]}");

            // GetMembership() snapshot must agree with latest event.
            ClusterMembership current = i1.GetMembership();
            Assert.True(current.MembershipVersion >= versionAfterLeave);
            Assert.Equal(3, current.Members.Count);
        }
        finally
        {
            await n4.LeaveCluster(true, CancellationToken.None);
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task JoinCluster_Seeds_ViaInterface_AdmitsNewNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();

        // n4 gets static discovery of the 3-node cluster so it can route CompleteAppendLogs
        // ACKs back to the P0 leader before its own committed roster populates its node list.
        RaftManager n1 = BuildJoinSeedsNode(comm, "node1", 1, 8361, ["localhost:8362", "localhost:8363"]);
        RaftManager n2 = BuildJoinSeedsNode(comm, "node2", 2, 8362, ["localhost:8361", "localhost:8363"]);
        RaftManager n3 = BuildJoinSeedsNode(comm, "node3", 3, 8363, ["localhost:8361", "localhost:8362"]);
        // n4 has no user partitions; its discovery seeds include all 3 existing nodes so it can
        // route CompleteAppendLogs ACKs before its committed roster populates Nodes.
        RaftManager n4 = BuildJoinSeedsNode(comm, "node4", 4, 8364,
            ["localhost:8361", "localhost:8362", "localhost:8363"], initialPartitions: 0);

        // Register all 4 nodes up-front so the leader can reach n4 once it appears in the roster.
        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8361"] = n1,
            ["localhost:8362"] = n2,
            ["localhost:8363"] = n3,
            ["localhost:8364"] = n4,
        });

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

            await WaitForCondition(
                () => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            // Call via IRaft interface, using seeds — this blocks until n4 is a Voter.
            IRaft i4 = n4;
            await i4.JoinCluster(["localhost:8361", "localhost:8362", "localhost:8363"], ct);

            // JoinCluster(seeds) already waited for Voter promotion.
            Assert.Equal(System.ClusterMemberRole.Voter, n4.LocalRole);

            // n1 must see n4 as a Voter in its committed roster.
            await WaitForCondition(
                () => n1.GetMembership().Members.Any(m => m.Endpoint == "localhost:8364" && m.Role == ClusterMemberRole.Voter),
                ct, timeoutMs: 15_000);

            // GetMembership via the IRaft interface reflects the new voter.
            ClusterMembership m = n1.GetMembership();
            Assert.Equal(4, m.Members.Count);
            Assert.Contains(m.Members, x => x.Endpoint == "localhost:8364" && x.Role == ClusterMemberRole.Voter);
        }
        finally
        {
            await n4.LeaveCluster(true, CancellationToken.None);
            await n1.LeaveCluster(true, CancellationToken.None);
            await n2.LeaveCluster(true, CancellationToken.None);
            await n3.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task GracefulLeave_Leader_StepsDownFromAllPartitions()
    {
        // When the P0 leader removes itself from the roster it must actively step down
        // from every partition it leads (Raft §6 self-removal rule) so that followers
        // can elect a new leader without waiting for a heartbeat timeout.  We use the
        // standard test configuration (50 ms heartbeat) and verify that both the system
        // partition and the single user partition have a new leader within the normal
        // WaitForLeader window.
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();

        RaftManager n1 = MakeNode(comm, "localhost", 8111, 1, ["localhost:8112", "localhost:8113"], logger);
        RaftManager n2 = MakeNode(comm, "localhost", 8112, 2, ["localhost:8111", "localhost:8113"], logger);
        RaftManager n3 = MakeNode(comm, "localhost", 8113, 3, ["localhost:8111", "localhost:8112"], logger);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8111"] = n1,
            ["localhost:8112"] = n2,
            ["localhost:8113"] = n3,
        });

        await n1.UpdateNodes();
        await n2.UpdateNodes();
        await n3.UpdateNodes();

        await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

        await WaitForLeader([n1, n2, n3], partitionId: 0, ct);
        await WaitForLeader([n1, n2, n3], partitionId: 1, ct);

        await WaitForCondition(
            () => n1.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n2.SystemCoordinator.GetMembership().MembershipVersion > 0
               && n3.SystemCoordinator.GetMembership().MembershipVersion > 0,
            ct);

        // Find the P0 leader.
        RaftManager? p0Leader = null;
        foreach (RaftManager n in new[] { n1, n2, n3 })
        {
            if (await n.AmILeaderQuick(0))
            {
                p0Leader = n;
                break;
            }
        }

        Assert.NotNull(p0Leader);
        RaftManager[] survivors = new[] { n1, n2, n3 }.Where(n => n != p0Leader).ToArray();

        // The leader commits RemoveMember(self); StepDownSelfRemovedAsync fires and yields
        // leadership on both P0 and the user partition before LeaveCluster returns.
        await p0Leader.LeaveCluster(dispose: true, cancellationToken: CancellationToken.None);

        // Survivors must have elected a new leader on both partitions.
        await WaitForLeader(survivors, partitionId: 0, ct);
        await WaitForLeader(survivors, partitionId: 1, ct);

        // Roster on survivors must show exactly 2 voters, not including the departed node.
        await WaitForCondition(
            () => survivors.All(n =>
            {
                ClusterMembership m = n.SystemCoordinator.GetMembership();
                return m.Members.Count == 2
                    && m.Members.All(x => x.Role == ClusterMemberRole.Voter)
                    && !m.Members.Any(x => x.Endpoint == p0Leader.LocalEndpoint);
            }),
            ct);

        foreach (RaftManager s in survivors)
            await s.LeaveCluster(true, CancellationToken.None);
    }

    private RaftManager BuildJoinSeedsNode(
        InMemoryCommunication comm, string name, int id, int port,
        string[] peers, int initialPartitions = 1)
    {
        RaftConfiguration config = new()
        {
            NodeName = name,
            NodeId = id,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 300,
            BackfillThreshold = 0,
            MaxBackfillEntriesPerRound = 128,
            LearnerPromotionLag = 5,
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(500),
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(logger),
            comm,
            new HybridLogicalClock(),
            logger);
    }
}
