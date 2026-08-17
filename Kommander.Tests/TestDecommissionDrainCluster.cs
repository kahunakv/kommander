
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
/// Cluster-level tests for the graceful-decommission drain: <see cref="RaftManager.RequestLeaveAsync"/>
/// with per-partition replica placement active must evacuate the departing node's replicas onto
/// survivors <b>before</b> the roster removal commits (asserted on ordering, not just the end
/// state), roll back to Voter when nothing can evacuate within the timeout, and complete when
/// the departing node is the P0 leader itself (whose Leaving commit steps it down from every
/// partition it leads — the new P0 leader's passes finish the drain).
///
/// Each test stands up a full in-memory 3-node cluster with ReplicationFactor 2 over 2 initial
/// partitions, so every node hosts at least one replica (4 slots over 3 nodes).
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestDecommissionDrainCluster
{
    private readonly ILogger<IRaft> logger;

    public TestDecommissionDrainCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static RaftManager MakeNode(
        InMemoryCommunication communication,
        int port, int nodeId,
        IEnumerable<string> peers,
        ILogger<IRaft> logger,
        TimeSpan placementPassInterval,
        TimeSpan drainTimeout)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            ReplicationFactor = 2,
            EnablePlacementRebalancer = true,
            PlacementPassInterval = placementPassInterval,
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(200),
            DecommissionDrainTimeout = drainTimeout,
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

    private static async Task<RaftManager[]> BuildPlacedCluster(
        ILogger<IRaft> logger,
        int basePort,
        TimeSpan placementPassInterval,
        TimeSpan drainTimeout,
        CancellationToken ct)
    {
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        [
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)], logger, placementPassInterval, drainTimeout),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)], logger, placementPassInterval, drainTimeout),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)], logger, placementPassInterval, drainTimeout),
        ];

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = nodes[0],
            [Ep(2)] = nodes[1],
            [Ep(3)] = nodes[2],
        });

        foreach (RaftManager n in nodes)
            await n.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

        // Roster seeded and the initial placement committed: 2 ranges × 2 replicas.
        await WaitForCondition(
            () => nodes.All(n =>
                n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                n.GetPartitionMap().Count == 2 &&
                n.GetPartitionMap().All(r => r.Replicas.Count == 2)),
            ct, timeoutMs: 20_000);

        return nodes;
    }

    private static async Task<RaftManager> P0Leader(RaftManager[] nodes, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 15_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (await n.AmILeaderQuick(RaftSystemConfig.SystemPartition))
                    return n;
            }
            await Task.Delay(25, ct);
        }
        throw new TimeoutException("No P0 leader elected within 15 s.");
    }

    private static bool NamedInMap(RaftManager observer, string endpoint) =>
        observer.GetPartitionMap().Any(r => r.Replicas.Any(x => x.Endpoint == endpoint));

    private static bool InRoster(RaftManager observer, string endpoint) =>
        observer.SystemCoordinator.GetMembership().Members.Any(m => m.Endpoint == endpoint);

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 10_000)
    {
        timeoutMs = TestTimeouts.Scale(timeoutMs);
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(25, ct);
        }
        throw new TimeoutException("Condition not satisfied within timeout.");
    }

    private static async Task TearDown(RaftManager[] nodes)
    {
        foreach (RaftManager n in nodes)
            await n.LeaveCluster(true, CancellationToken.None);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RequestLeave_EvacuatesReplicasBeforeRemovalCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RaftManager[] nodes = await BuildPlacedCluster(
            logger, basePort: 8150,
            placementPassInterval: TimeSpan.FromMilliseconds(250),
            drainTimeout: TimeSpan.FromSeconds(60),
            ct);

        try
        {
            // Leaver: a node named in the map that is NOT the P0 leader (the leader case has its
            // own test — it additionally exercises the Leaving-commit step-down).
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            RaftManager survivor = nodes.First(n => !ReferenceEquals(n, leaver));
            string leaverEndpoint = leaver.LocalEndpoint;

            // Ordering probe: at the moment the survivor applies the roster that no longer names
            // the leaver, its already-applied map must not name the leaver either — committed
            // P0 entries apply in log order, so post-hoc repair cannot fake this.
            bool removalObserved = false;
            bool mapDrainedAtRemoval = false;
            survivor.OnMembershipChanged += membership =>
            {
                if (!removalObserved && membership.Members.All(m => m.Endpoint != leaverEndpoint))
                {
                    removalObserved = true;
                    mapDrainedAtRemoval = !NamedInMap(survivor, leaverEndpoint);
                }
            };

            LeaveClusterResult result = await leaver.RequestLeaveAsync(ct);

            Assert.Equal(LeaveClusterOutcome.Committed, result.Outcome);
            Assert.True(result.Drained);

            await WaitForCondition(() => removalObserved, ct);
            Assert.True(mapDrainedAtRemoval,
                "The roster removal was applied while the committed map still named the leaver — the drain did not precede the removal.");

            // End state on a survivor: leaver out of roster and map, and every range back at
            // RF 2 on the two survivors — evacuated, not just dropped.
            await WaitForCondition(() => !InRoster(survivor, leaverEndpoint), ct);
            Assert.False(NamedInMap(survivor, leaverEndpoint));
            Assert.All(survivor.GetPartitionMap(), r => Assert.Equal(2, r.Replicas.Count));
        }
        finally
        {
            await TearDown(nodes);
        }
    }

    [Fact]
    public async Task RequestLeave_NothingEvacuates_TimesOutAndRollsBackToVoter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Placement timer disabled: the Leaving commit kicks exactly one pass, which only ADDS a
        // learner (single mover per range); with no further passes the promotion never happens,
        // so the drain cannot complete and must roll back at the (short) timeout.
        RaftManager[] nodes = await BuildPlacedCluster(
            logger, basePort: 8160,
            placementPassInterval: TimeSpan.Zero,
            drainTimeout: TimeSpan.FromMilliseconds(1_500),
            ct);

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            string leaverEndpoint = leaver.LocalEndpoint;

            LeaveClusterResult result = await leaver.RequestLeaveAsync(ct);

            Assert.Equal(LeaveClusterOutcome.DrainTimedOut, result.Outcome);
            Assert.False(result.Drained);

            // The rollback restored Voter — on the committed roster and through LocalRole, which
            // is what releases the campaign gates (a node stuck at Leaving never campaigns again).
            await WaitForCondition(
                () => nodes.All(n =>
                {
                    ClusterMember? m = n.SystemCoordinator.GetMembership().Members.FirstOrDefault(x => x.Endpoint == leaverEndpoint);
                    return m is { Role: ClusterMemberRole.Voter };
                }),
                ct);
            Assert.Equal(ClusterMemberRole.Voter, leaver.LocalRole);

            // Nothing departed: the node is still a roster member and still holds its replicas.
            Assert.True(InRoster(leaver, leaverEndpoint));
            Assert.True(NamedInMap(leaver, leaverEndpoint));
        }
        finally
        {
            await TearDown(nodes);
        }
    }

    [Fact]
    public async Task RequestLeave_P0LeaderItself_DrainsAndDeparts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RaftManager[] nodes = await BuildPlacedCluster(
            logger, basePort: 8170,
            placementPassInterval: TimeSpan.FromMilliseconds(250),
            drainTimeout: TimeSpan.FromSeconds(60),
            ct);

        try
        {
            // The P0 leader is always named in the map here (4 replica slots over 3 nodes).
            RaftManager leaver = await P0Leader(nodes, ct);
            string leaverEndpoint = leaver.LocalEndpoint;
            Assert.True(NamedInMap(leaver, leaverEndpoint));

            // Committing its own Voter → Leaving makes ReplicateMembership step it down from
            // every partition it leads (Raft §6 conservative choice); the drain then depends on
            // the surviving voters electing a new P0 leader whose passes finish the evacuation
            // and on this node's waiter observing committed state it no longer produces.
            LeaveClusterResult result = await leaver.RequestLeaveAsync(ct);

            Assert.Equal(LeaveClusterOutcome.Committed, result.Outcome);
            Assert.True(result.Drained);

            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];
            await WaitForCondition(
                () => survivors.All(n => !InRoster(n, leaverEndpoint) && !NamedInMap(n, leaverEndpoint)),
                ct);

            // A new P0 leader exists among the survivors.
            RaftManager newLeader = await P0Leader(survivors, ct);
            Assert.NotEqual(leaverEndpoint, newLeader.LocalEndpoint);
        }
        finally
        {
            await TearDown(nodes);
        }
    }
}
