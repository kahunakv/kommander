
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

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

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 10_000)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
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
            await n1.LeaveCluster(true);
            await n2.LeaveCluster(true);
            await n3.LeaveCluster(true);
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
            await n1.LeaveCluster(true);
            await n2.LeaveCluster(true);
            await n3.LeaveCluster(true);
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
            await n1.LeaveCluster(true);
            await n2.LeaveCluster(true);
            await n3.LeaveCluster(true);
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

    // ── Task 6: graceful leave ────────────────────────────────────────────────

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
            await n3.LeaveCluster(dispose: true);

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
            await n2.LeaveCluster(true);
            await n1.LeaveCluster(true);
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
            await p0Leader.LeaveCluster(dispose: true);

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
                await s.LeaveCluster(true);
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
        await n1.LeaveCluster(dispose: true);
        await n2.LeaveCluster(dispose: true);

        // n3 is the last voter.  Either:
        //   (a) n3 is the P0 leader → TryRemoveMember returns InsufficientVoters → fast exit, or
        //   (b) the cached leader (n1/n2) is stopped → IsStopped returns LeaveResponse(false, null) → fast exit.
        // In both cases the call must complete well under the 10 s deadline.
        long startMs = Environment.TickCount64;
        await n3.LeaveCluster(dispose: true);
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
            await n1.LeaveCluster(true);
            await n2.LeaveCluster(true);
            await n3.LeaveCluster(true);
        }
    }
}
