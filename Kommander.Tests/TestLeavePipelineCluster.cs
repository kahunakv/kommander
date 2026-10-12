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
/// The departure pipeline on real in-process clusters: a healthy leave that commits and
/// propagates (the departing node watches its own roster until the removal lands), a leave
/// whose acknowledgements cannot come back (the removal still commits on the survivors and the
/// departure stays bounded instead of hanging), the leader-side role-transition and removal
/// handlers (hint for non-leaders, commit for the leader, idempotency for re-delivered and
/// unknown requests), and the decommission latch that survives an unconfirmed drain rollback
/// until a committed roster provably names the node a full voter again.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLeavePipelineCluster
{
    private readonly ILogger<IRaft> logger;

    public TestLeavePipelineCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    // ── Cluster assembly ──────────────────────────────────────────────────────

    private RaftManager MakeNode(
        InMemoryCommunication communication,
        int port, int nodeId,
        IEnumerable<string> peers,
        int initialPartitions,
        int replicationFactor,
        bool placementRebalancer,
        TimeSpan placementPassInterval,
        TimeSpan drainTimeout)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
            ReplicationFactor = replicationFactor,
            EnablePlacementRebalancer = placementRebalancer,
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

    private async Task<(RaftManager[] Nodes, InMemoryCommunication Comm)> AssembleCluster(
        int basePort,
        int initialPartitions,
        int replicationFactor,
        bool placementRebalancer,
        TimeSpan placementPassInterval,
        TimeSpan drainTimeout,
        CancellationToken ct)
    {
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        {
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)], initialPartitions,
                replicationFactor, placementRebalancer, placementPassInterval, drainTimeout),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)], initialPartitions,
                replicationFactor, placementRebalancer, placementPassInterval, drainTimeout),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)], initialPartitions,
                replicationFactor, placementRebalancer, placementPassInterval, drainTimeout),
        };

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = nodes[0],
            [Ep(2)] = nodes[1],
            [Ep(3)] = nodes[2],
        });

        foreach (RaftManager n in nodes)
            await n.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

        await WaitForCondition(
            () => nodes.All(n =>
                n.IsInitialized &&
                n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                (replicationFactor <= 0 ||
                 (n.GetPartitionMap().Count == initialPartitions &&
                  n.GetPartitionMap().All(r => r.Replicas.Count == replicationFactor)))),
            ct, timeoutMs: 20_000);

        return (nodes, comm);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

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

    private static async Task<RaftManager> P0Leader(RaftManager[] nodes, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (await n.AmILeaderQuick(RaftSystemConfig.SystemPartition))
                    return n;
            }
            await Task.Delay(25, ct);
        }
        throw new TimeoutException("No system-partition leader within 15 s.");
    }

    private static bool InRoster(RaftManager observer, string endpoint, ClusterMemberRole? role = null) =>
        observer.SystemCoordinator.GetMembership().Members.Any(m =>
            m.Endpoint == endpoint && (role is null || m.Role == role));

    private static bool InNodeList(RaftManager observer, string endpoint) =>
        observer.GetNodes().Any(n => n.Endpoint == endpoint);

    private static int NodeIdOf(RaftManager observer, string endpoint) =>
        observer.SystemCoordinator.GetMembership().Members
            .First(m => m.Endpoint == endpoint).NodeId;

    private static async Task TearDown(RaftManager[] nodes)
    {
        foreach (RaftManager n in nodes)
        {
            if (n.Joined)
                await n.LeaveCluster(true, CancellationToken.None);
            else
                n.Dispose();
        }
    }

    // ── Healthy graceful leave ────────────────────────────────────────────────

    /// <summary>
    /// A leave on a healthy cluster commits and propagates well inside the departure deadline:
    /// the leaver's own roster drops it (it watched for the removal rather than running the
    /// deadline out), the survivors drop it from roster and node list, and the departed node is
    /// torn down (no longer joined, reporting the local Leaving role) while a survivor carries
    /// on leading the system partition.
    /// </summary>
    [Fact]
    public async Task LeaveCluster_HealthyCluster_CommitsAndPropagatesBeforeTheDeadline()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, _) = await AssembleCluster(
            basePort: 8380, initialPartitions: 1, replicationFactor: 0,
            placementRebalancer: false, placementPassInterval: TimeSpan.Zero, drainTimeout: TimeSpan.FromSeconds(30),
            ct);

        try
        {
            RaftManager leaver = await P0Leader(nodes, ct);
            // The system-partition leader leaving is the harder case: it commits its own removal.
            string leaverEndpoint = leaver.LocalEndpoint;
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];

            // The internal deadline the leave loop tolerates is 10 s; a healthy round trip is
            // milliseconds, so anything near the deadline means the propagation wait degenerated
            // into the deadline spin. The bound stays unscaled on purpose: it must stay below
            // the fixed deadline it is compared against.
            ValueStopwatch sw = ValueStopwatch.StartNew();
            await leaver.LeaveCluster(dispose: false, cancellationToken: ct)
                       .WaitAsync(TimeSpan.FromSeconds(6), ct);
            long elapsedMs = sw.GetElapsedMilliseconds();

            Assert.True(elapsedMs < 6_000, $"Healthy leave took {elapsedMs} ms — it ran into a wait bound.");

            // The leaver observed its own removal locally.
            Assert.False(InRoster(leaver, leaverEndpoint));
            Assert.Equal(ClusterMemberRole.Leaving, leaver.LocalRole);
            Assert.False(leaver.Joined);

            // Survivors dropped the leaver from the committed roster and from the node list
            // callers see (the roster-driven refresh of the peer list).
            await WaitForCondition(
                () => survivors.All(s => !InRoster(s, leaverEndpoint) && !InNodeList(s, leaverEndpoint)),
                ct, timeoutMs: 15_000);

            await P0Leader(survivors, ct);
        }
        finally
        {
            await TearDown(nodes);
        }
    }

    // ── Leave whose acknowledgements cannot return ────────────────────────────

    /// <summary>
    /// When every reply from the system-partition leader to the leaver is dropped on the wire,
    /// the very first leave request still reaches the leader and commits the removal — the
    /// survivors drop the leaver from roster and node list — while the leaver, which can never
    /// see an acknowledgement, gives up in a bounded time instead of hanging and tears itself
    /// down all the same.
    /// </summary>
    [Fact]
    public async Task LeaveCluster_LeaderRepliesDropped_StillCommitsOnSurvivorsAndStaysBounded()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssembleCluster(
            basePort: 8390, initialPartitions: 1, replicationFactor: 0,
            placementRebalancer: false, placementPassInterval: TimeSpan.Zero, drainTimeout: TimeSpan.FromSeconds(30),
            ct);

        try
        {
            RaftManager leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, leader));
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];
            string leaverEndpoint = leaver.LocalEndpoint;

            // Cut only the leader → leaver direction: the leaver's requests are delivered (so
            // the leader commits the removal), but every reply — the leave acknowledgement and
            // the roster replication alike — is lost on the way back.
            comm.BlockLink(leader.LocalEndpoint, leaver.LocalEndpoint);

            ValueStopwatch sw = ValueStopwatch.StartNew();
            await leaver.LeaveCluster(dispose: false, cancellationToken: ct)
                       .WaitAsync(TimeSpan.FromMilliseconds(TestTimeouts.Scale(30_000)), ct);
            long elapsedMs = sw.GetElapsedMilliseconds();

            // Bounded: either the leader became unknown (the leaver stops hearing heartbeats and
            // the empty-leader cap kicks in) or the deadline expired — both far below the guard.
            Assert.True(elapsedMs < TestTimeouts.Scale(30_000),
                $"LeaveCluster took {elapsedMs} ms — it hung past every internal bound.");

            // The removal committed even though no acknowledgement ever came back: the survivors
            // no longer count the leaver, in roster or node list.
            await WaitForCondition(
                () => survivors.All(s => !InRoster(s, leaverEndpoint) && !InNodeList(s, leaverEndpoint)),
                ct, timeoutMs: 15_000);

            Assert.False(leaver.Joined);
        }
        finally
        {
            comm.HealAll();
            await TearDown(nodes);
        }
    }

    // ── Leader-side role-transition and removal handlers ─────────────────────

    /// <summary>
    /// The role-transition handler answers from wherever it runs: a non-leader points the
    /// caller at the leader it knows, the leader commits the transition (version bump, role
    /// visible cluster-wide), a re-delivered transition for a role the member already holds is
    /// an idempotent success with no version churn, and the removal handler treats an endpoint
    /// that is not in the roster as already gone rather than an error.
    /// </summary>
    [Fact]
    public async Task MemberRoleHandlers_LeaderCommitsFollowerHintsAndUnknownEndpointsAreIdempotent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, _) = await AssembleCluster(
            basePort: 8400, initialPartitions: 1, replicationFactor: 0,
            placementRebalancer: false, placementPassInterval: TimeSpan.Zero, drainTimeout: TimeSpan.FromSeconds(30),
            ct);

        try
        {
            RaftManager leader = await P0Leader(nodes, ct);
            RaftManager demoted = nodes.First(n => !ReferenceEquals(n, leader));
            RaftManager other = nodes.First(n => !ReferenceEquals(n, leader) && !ReferenceEquals(n, demoted));
            string demotedEndpoint = demoted.LocalEndpoint;
            int demotedNodeId = NodeIdOf(leader, demotedEndpoint);
            long versionBefore = leader.SystemCoordinator.GetMembership().MembershipVersion;

            // The hint assertions read the follower's cached leader; let its view settle on the
            // leader the test just observed so a transient post-election window cannot turn the
            // hint into a null.
            await WaitForCondition(
                () => other.GetPartitionLeaderEndpoint(RaftSystemConfig.SystemPartition) == leader.LocalEndpoint,
                ct, timeoutMs: 15_000);

            // A non-leader must not commit anything; it hands back the leader it knows.
            SetMemberRoleResponse hinted = await other.ReceiveSetMemberRole(
                new SetMemberRoleRequest(demotedEndpoint, demotedNodeId, ClusterMemberRole.Leaving), ct);
            Assert.False(hinted.Success);
            Assert.Equal(RaftOperationStatus.NodeIsNotLeader, hinted.Status);
            Assert.Equal(leader.LocalEndpoint, hinted.LeaderHint);

            // Convergence of the committed roster and every OTHER node's cache. The demoted
            // node's own view of its demotion is not this test's contract (a Leaving node's
            // cache refresh rides gossip), so it is excluded from the wait.
            //
            // The Leaving role is transient here: with no replication factor no range names the
            // demoted node, so the placement pass the commit kicks completes its drain at once
            // and removes it, and the unlatched node then auto-rejoins. Polling the live cache
            // can miss that window entirely on a slow host, so each survivor records every
            // roster it advances through and the wait asks whether one of them named the
            // demoted node Leaving.
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, demoted))];
            int[] sawLeaving = new int[survivors.Length];
            Action<ClusterMembership>[] observers = new Action<ClusterMembership>[survivors.Length];
            for (int i = 0; i < survivors.Length; i++)
            {
                int slot = i;
                observers[i] = roster =>
                {
                    if (roster.Members.Any(m => m.Endpoint == demotedEndpoint && m.Role == ClusterMemberRole.Leaving))
                        Volatile.Write(ref sawLeaving[slot], 1);
                };
                survivors[i].OnMembershipChanged += observers[i];
            }

            try
            {
                // The leader commits the demotion; the version advances and every node sees the role.
                SetMemberRoleResponse demotedResponse = await leader.ReceiveSetMemberRole(
                    new SetMemberRoleRequest(demotedEndpoint, demotedNodeId, ClusterMemberRole.Leaving), ct);
                Assert.True(demotedResponse.Success);
                Assert.Equal(RaftOperationStatus.Success, demotedResponse.Status);
                Assert.Equal(versionBefore + 1, demotedResponse.MembershipVersion);

                await WaitForCondition(
                    () => Enumerable.Range(0, survivors.Length).All(i =>
                        Volatile.Read(ref sawLeaving[i]) == 1 ||
                        InRoster(survivors[i], demotedEndpoint, ClusterMemberRole.Leaving)),
                    ct);
            }
            finally
            {
                for (int i = 0; i < survivors.Length; i++)
                    survivors[i].OnMembershipChanged -= observers[i];
            }

            // A direct Leaving→Voter restore is the drain-rollback flow's transition (covered
            // by the leave-latch test in this file), not the raw handler's; it is deliberately
            // not attempted here.

            // The removal handler mirrors both behaviours for departures: hint from a
            // non-leader, and idempotent success for an endpoint already absent from the roster
            // (a retried removal must not spin).
            LeaveResponse leaveHint = await other.ReceiveLeave(
                new LeaveRequest(demotedEndpoint, demotedNodeId), ct);
            Assert.False(leaveHint.Success);
            Assert.Equal(leader.LocalEndpoint, leaveHint.LeaderHint);

        }
        finally
        {
            await TearDown(nodes);
        }
    }

    // ── The decommission latch across an unconfirmed drain rollback ──────────

    /// <summary>
    /// A decommission drain whose rollback cannot be confirmed keeps the leave latch raised:
    /// the leaver, isolated before its drain times out, rolls back with no reachable answer and
    /// still reports itself as departing. Healing the isolation alone does not release the
    /// latch — the roster still names the node Leaving. Only when a committed roster newer than
    /// the drain's own commit names the node a Voter again (here: the operator's rollback
    /// committed on the leader) does the roster observer release the latch, returning the node
    /// to a full member that could auto-rejoin after a later eviction.
    /// </summary>
    [Fact]
    public async Task DrainRollbackUnconfirmed_LeaveLatchHeldUntilACommittedRosterNamesTheNodeVoterAgain()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssembleCluster(
            basePort: 8410, initialPartitions: 2, replicationFactor: 2,
            placementRebalancer: true, placementPassInterval: TimeSpan.Zero,
            drainTimeout: TimeSpan.FromSeconds(2),
            ct);

        try
        {
            RaftManager leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n =>
                !ReferenceEquals(n, leader) &&
                n.GetPartitionMap().Any(r => r.Replicas.Any(x => x.Endpoint == n.LocalEndpoint)));
            string leaverEndpoint = leaver.LocalEndpoint;
            int leaverNodeId = NodeIdOf(leader, leaverEndpoint);

            Task<LeaveClusterResult> leaveAttempt = leaver.RequestLeaveAsync(ct);

            // Let the Voter → Leaving commit land and reach the leaver (it arms the roster
            // observer's watermark), then isolate the leaver completely: the drain can observe
            // no progress and the timed-out rollback cannot reach the leader at all.
            await WaitForCondition(
                () => InRoster(leaver, leaverEndpoint, ClusterMemberRole.Leaving), ct, timeoutMs: 15_000);
            comm.PartitionNode(leaverEndpoint);

            LeaveClusterResult outcome = await leaveAttempt
                .WaitAsync(TimeSpan.FromMilliseconds(TestTimeouts.Scale(45_000)), ct);

            Assert.Equal(LeaveClusterOutcome.DrainTimedOut, outcome.Outcome);
            Assert.False(outcome.Drained);

            // The rollback never confirmed — the roster the leaver can still see names it
            // Leaving — so the departure latch stays raised: auto-rejoin must not run while the
            // drain may still land.
            Assert.True(leaver.IsLeaveRequested, "The latch was released although the rollback never confirmed.");
            Assert.True(InRoster(leaver, leaverEndpoint, ClusterMemberRole.Leaving));

            // Healing alone changes nothing: the committed roster still names the node Leaving,
            // so no roster that proves a rollback can arrive and the latch must stay.
            comm.HealPartition(leaverEndpoint);
            await Task.Delay(TestTimeouts.Scale(1_000), ct);
            Assert.True(leaver.IsLeaveRequested, "The latch was released by healing alone, without a Voter roster.");

            // The operator's rollback, committed on the leader, produces exactly that roster:
            // newer than the drain's Leaving commit and naming the node a Voter again.
            SetMemberRoleResponse rollback = await leader.ReceiveSetMemberRole(
                new SetMemberRoleRequest(leaverEndpoint, leaverNodeId, ClusterMemberRole.Voter), ct);
            Assert.True(rollback.Success, $"Operator rollback failed: {rollback.Status}");

            await WaitForCondition(
                () => !leaver.IsLeaveRequested && InRoster(leaver, leaverEndpoint, ClusterMemberRole.Voter),
                ct, timeoutMs: 15_000);
            Assert.Equal(ClusterMemberRole.Voter, leaver.LocalRole);
        }
        finally
        {
            comm.HealAll();
            await TearDown(nodes);
        }
    }
}
