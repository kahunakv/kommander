
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// The rolled-back-leave eviction trap (Kahuna nightly <c>append / partition,placement,range</c>,
/// 2026-10-11): a node whose graceful leave timed out and rolled back to Voter kept the sticky
/// leave latch raised, so when a later network partition got it evicted as a dead member it
/// never auto-rejoined — it stayed NotMember while still leading its ranges, the replacement
/// learner the planner added could never be fed, and the move stalled for the rest of the run.
/// <para>
/// These tests drive exactly that chain on an in-memory cluster of 4 nodes at RF 3: a drain
/// whose timeout is shorter than the learner-promotion stable window cannot complete (the
/// evacuation needs a promotion, and a promotion needs that window), so it times out and rolls
/// back by construction; the latch must be released; a subsequent SWIM eviction must then
/// trigger auto-rejoin, and the placement map must settle with no transitional replica left.
/// </para>
/// <para>
/// The 2026-10-13 recurrence took the other exit of the drain: the <c>Voter → Leaving</c> commit
/// itself timed out against a P0 leader that was mid-election, so no roster ever named the node
/// Leaving, the latch stayed raised with nothing armed to release it, and the next eviction
/// parked the node the same way. The <c>DrainCommitTimedOut</c> tests drive that exit by
/// dropping the leaver's messages to the P0 leader (one way, so it keeps hearing heartbeats and
/// neither campaigns nor gets evicted) until the request reports <c>Timeout</c>.
/// </para>
/// <para>
/// The 2026-10-27 recurrence took a third exit: the drain-leg commit landed a few seconds
/// <i>after</i> the request had timed out, so the node sat <c>Leaving</c> with nobody driving the
/// drain, and a later partition evicted it while <c>Leaving</c>. The member list of the roster
/// that excluded it reads the same as the drain's own removal would, and every version-keyed
/// rule had already been defeated once by a gossip push that skipped versions. The fix makes
/// the P0 leader record <i>why</i> each member left in the committed roster
/// (<see cref="ClusterMembership.Departures"/>); the <c>DrainCommitLandedLate</c>,
/// <c>RosterSkipsPastTheEviction</c> and <c>EvictedMidDrain</c> tests drive the three shapes
/// that record settles: evicted while <c>Leaving</c>, evicted then learned through a roster that
/// skipped the eviction, and evicted while the request's own drain wait was still running.
/// </para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLeaveRollbackRejoin
{
    private readonly ILogger<IRaft> logger;

    public TestLeaveRollbackRejoin(ITestOutputHelper outputHelper)
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
        TimeSpan drainTimeout,
        TimeSpan promotionStableWindow)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            // RF 3 over 4 nodes: the initial placement needs more nodes than the factor, and a
            // Leaving node's replica has exactly one survivor to move to.
            ReplicationFactor = 3,
            EnablePlacementRebalancer = true,
            PlacementPassInterval = TimeSpan.FromMilliseconds(250),
            LearnerPromotionLag = 64,
            // The evacuation promotes a learner only after it has been caught up for this whole
            // window; a drain timeout below it therefore always fires first — the shape every
            // Kahuna decommission takes (every drain in every run ends DrainTimedOut).
            LearnerPromotionStableWindow = promotionStableWindow,
            DecommissionDrainTimeout = drainTimeout,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
            // SWIM settings mirror TestEvictionRejoin: a partitioned node is Dead within about a
            // second and evicted shortly after.
            PingTimeout = TimeSpan.FromMilliseconds(100),
            IndirectPingFanout = 1,
            SuspicionTimeout = TimeSpan.FromMilliseconds(600),
            DeadMemberEvictionGrace = TimeSpan.FromMilliseconds(800),
            PingInterval = TimeSpan.FromMilliseconds(100),
            GossipInterval = TimeSpan.FromMilliseconds(200),
            EnableAutoRejoin = true,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    /// <summary>
    /// Four in-memory nodes at RF 3 over two ranges. <paramref name="promotionStableWindow"/>
    /// defaults to 1 s; the eviction tests that need the eviction (about 2 s after the isolation
    /// with the SWIM settings below) to beat the drain's evacuation raise it, so no learner can be
    /// promoted — and no drain completed — before the node is out.
    /// </summary>
    private static async Task<(RaftManager[] Nodes, InMemoryCommunication Comm)> BuildCluster(
        ILogger<IRaft> logger, int basePort, TimeSpan drainTimeout, CancellationToken ct, TimeSpan? promotionStableWindow = null)
    {
        TimeSpan window = promotionStableWindow ?? TimeSpan.FromSeconds(1);
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        [
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3), Ep(4)], logger, drainTimeout, window),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3), Ep(4)], logger, drainTimeout, window),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2), Ep(4)], logger, drainTimeout, window),
            MakeNode(comm, basePort + 4, 4, [Ep(1), Ep(2), Ep(3)], logger, drainTimeout, window),
        ];

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = nodes[0],
            [Ep(2)] = nodes[1],
            [Ep(3)] = nodes[2],
            [Ep(4)] = nodes[3],
        });

        foreach (RaftManager n in nodes)
            await n.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

        try
        {
            await WaitForCondition(() => nodes.All(n => MapSettled(n, 3)), ct, timeoutMs: 20_000);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"Initial placement never settled: {DescribeMaps(nodes)}");
        }

        return (nodes, comm);
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

    /// <summary>
    /// Every range has exactly <paramref name="rf"/> replicas and none of them is transitional
    /// (Learner or Removing): no move is in flight. This is the end state the Kahuna
    /// <c>:stalled-move</c> checker waits for.
    /// </summary>
    private static bool MapSettled(RaftManager observer, int rf)
    {
        IReadOnlyList<RaftPartitionRange> map = observer.GetPartitionMap();
        return observer.SystemCoordinator.GetMembership().MembershipVersion > 0
            && map.Count == 2
            && map.All(r => r.Replicas.Count == rf && r.Replicas.All(x => x.Role == RaftReplicaRole.Voter));
    }

    private static string DescribeMaps(IEnumerable<RaftManager> nodes) =>
        string.Join(" | ", nodes.Select(n =>
            $"{n.LocalEndpoint} rosterV={n.SystemCoordinator.GetMembership().MembershipVersion} map=[" +
            string.Join("; ", n.GetPartitionMap().Select(r =>
                $"p{r.PartitionId} {r.State} rf={r.ReplicationFactor} {{" +
                string.Join(",", r.Replicas.Select(x => $"{x.Endpoint}:{x.Role}")) + "}")) + "]"));

    private static bool NamedInMap(RaftManager observer, string endpoint) =>
        observer.GetPartitionMap().Any(r => r.Replicas.Any(x => x.Endpoint == endpoint));

    private static ClusterMember? RosterEntry(RaftManager observer, string endpoint) =>
        observer.SystemCoordinator.GetMembership().Members.FirstOrDefault(m => m.Endpoint == endpoint);

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

    /// <summary>
    /// Runs the leaver through a drain that cannot evacuate and asserts the rollback: the
    /// outcome is <see cref="LeaveClusterOutcome.DrainTimedOut"/>, every node sees the leaver as
    /// Voter again, and — the fix — the leave latch is released.
    /// </summary>
    private static async Task DrainTimesOutAndReleasesLatch(RaftManager[] nodes, RaftManager leaver, CancellationToken ct)
    {
        string leaverEndpoint = leaver.LocalEndpoint;

        LeaveClusterResult result = await leaver.RequestLeaveAsync(ct);

        Assert.Equal(LeaveClusterOutcome.DrainTimedOut, result.Outcome);
        Assert.False(result.Drained);

        await WaitForCondition(
            () => nodes.All(n => RosterEntry(n, leaverEndpoint) is { Role: ClusterMemberRole.Voter }),
            ct);
        Assert.Equal(ClusterMemberRole.Voter, leaver.LocalRole);

        // Red before the fix: the latch stayed raised for the life of the process, and the
        // auto-rejoin driver consulted it first.
        Assert.False(leaver.IsLeaveRequested,
            "A drain rolled back to Voter must release the leave request: the node is a full member again.");
    }

    /// <summary>
    /// Isolates <paramref name="victim"/>, drives the survivors' SWIM probes until it is Dead,
    /// then drives their eviction timers until the committed roster excludes it.
    /// </summary>
    private static async Task PartitionUntilEvicted(
        InMemoryCommunication comm, RaftManager[] survivors, RaftManager victim, CancellationToken ct)
    {
        string ep = victim.LocalEndpoint;
        comm.PartitionNode(ep);

        long deadline = Environment.TickCount64 + TestTimeouts.Scale(15_000);
        while (Environment.TickCount64 < deadline
               && survivors.Any(s => s.Liveness.GetState(ep) != MemberLivenessState.Dead))
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager s in survivors)
                await s.PingAsync(ct);
            await Task.Delay(50, ct);
        }
        Assert.All(survivors, s => Assert.Equal(MemberLivenessState.Dead, s.Liveness.GetState(ep)));

        await WaitForCondition(
            () =>
            {
                foreach (RaftManager s in survivors)
                    s.UpdateNodes().GetAwaiter().GetResult();
                return survivors.All(s => RosterEntry(s, ep) is null);
            },
            ct, timeoutMs: 15_000);
    }

    /// <summary>
    /// Forces the drain-leg exit of the 2026-10-13 incident: the leaver's messages to the P0
    /// leader are dropped (one way) so its <c>SetMemberRole(Leaving)</c> never arrives, the
    /// request reports <see cref="LeaveClusterOutcome.Timeout"/>, and no committed roster names
    /// the leaver Leaving. The latch is deliberately still raised on return — the commit could
    /// still land — and the link is restored. Returns the roster version at which the request
    /// gave up: the watermark the next committed roster is judged against.
    /// </summary>
    private static async Task<long> DrainCommitTimesOutAndKeepsLatch(
        InMemoryCommunication comm, RaftManager[] nodes, RaftManager leaver, RaftManager p0Leader, CancellationToken ct)
    {
        string leaverEndpoint = leaver.LocalEndpoint;
        long versionBefore = leaver.GetMembership().MembershipVersion;

        comm.BlockLink(leaverEndpoint, p0Leader.LocalEndpoint);
        try
        {
            LeaveClusterResult result = await leaver.RequestLeaveAsync(ct);

            Assert.Equal(LeaveClusterOutcome.Timeout, result.Outcome);
            Assert.False(result.Left);
            Assert.False(result.Drained);
        }
        finally
        {
            comm.UnblockLink(leaverEndpoint, p0Leader.LocalEndpoint);
        }

        Assert.All(nodes, n => Assert.True(RosterEntry(n, leaverEndpoint) is { Role: ClusterMemberRole.Voter },
            "the Leaving commit must never have landed: every node still names the leaver Voter"));
        Assert.Equal(versionBefore, leaver.GetMembership().MembershipVersion);
        Assert.True(leaver.IsLeaveRequested,
            "an unconfirmed drain request keeps the latch until a committed roster settles it");

        return versionBefore;
    }

    /// <summary>
    /// Admits a fifth node to a running four-node cluster. The join commits a new roster that
    /// still names every existing member Voter — the shape of the roster advance (an unrelated
    /// eviction, v14) that followed the timed-out request in the incident. Every node in
    /// <paramref name="allNodes"/> stays in the transport map (an isolated one keeps its filter);
    /// only <paramref name="observers"/> are waited on, so a caller can advance the roster while
    /// one node is partitioned.
    /// </summary>
    private static async Task<RaftManager> JoinFifthNode(
        InMemoryCommunication comm, RaftManager[] allNodes, RaftManager[] observers, ILogger<IRaft> logger, int basePort, TimeSpan drainTimeout, CancellationToken ct)
    {
        string Ep(int i) => $"localhost:{basePort + i}";
        RaftManager fifth = MakeNode(comm, basePort + 5, 5, [Ep(1), Ep(2), Ep(3), Ep(4)], logger, drainTimeout, TimeSpan.FromSeconds(1));

        Dictionary<string, IRaft> all = new();
        foreach (RaftManager n in allNodes)
            all[n.LocalEndpoint] = n;
        all[fifth.LocalEndpoint] = fifth;
        comm.SetNodes(all);

        await fifth.UpdateNodes();
        await fifth.JoinCluster(observers.Select(n => n.LocalEndpoint).ToList(), ct);

        await WaitForCondition(
            () => observers.All(n => RosterEntry(n, fifth.LocalEndpoint) is not null),
            ct, timeoutMs: 30_000);

        return fifth;
    }

    /// <summary>
    /// Commits <c>Voter → Leaving</c> for <paramref name="leaver"/> straight on the P0 leader's
    /// coordinator: the drain-leg commit of a request that already gave up, landing late (the
    /// 2026-10-27 shape — in the incident the leader committed it 5.5 s after the caller's
    /// deadline). Returns the version it committed at.
    /// </summary>
    private static async Task<long> CommitLeavingFromLeader(RaftManager p0Leader, RaftManager leaver, CancellationToken ct)
    {
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        p0Leader.SystemCoordinator.Send(new RaftSystemRequest(
            RaftSystemRequestType.SetMemberRole,
            leaver.LocalEndpoint,
            leaver.LocalNodeId,
            p0Leader.GetMembership().MembershipVersion,
            ClusterMemberRole.Leaving,
            tcs));

        (RaftOperationStatus status, long version) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(TestTimeouts.Scale(10)), ct);
        Assert.Equal(RaftOperationStatus.Success, status);
        return version;
    }

    /// <summary>
    /// Asserts that the survivors' committed roster records <paramref name="endpoint"/> evicted
    /// at <paramref name="evictedVersion"/>: the P0 leader wrote the reason at the removal, and
    /// the record is what the evicted node decides on after the heal.
    /// </summary>
    private static void AssertRecordedEvicted(RaftManager observer, string endpoint, long evictedVersion)
    {
        ClusterDeparture? record = observer.GetMembership().DepartureOf(endpoint);
        Assert.NotNull(record);
        Assert.Equal(ClusterDepartureReason.Evicted, record.Reason);
        Assert.Equal(evictedVersion, record.Version);
    }

    /// <summary>
    /// The heal-and-rejoin tail shared by the eviction tests: the evicted leaver learns the
    /// excluding roster through its own gossip, must auto-rejoin, return to Voter, and the map
    /// must settle at full RF with no transitional replica.
    /// </summary>
    private static async Task HealAndExpectRejoin(
        InMemoryCommunication comm, RaftManager[] nodes, RaftManager[] survivors, RaftManager leaver, long evictedVersion, CancellationToken ct,
        int settleTimeoutMs = 30_000)
    {
        string leaverEndpoint = leaver.LocalEndpoint;
        comm.HealPartition(leaverEndpoint);

        await WaitForCondition(
            () =>
            {
                leaver.GossipAsync(ct).GetAwaiter().GetResult();
                return survivors.All(s => RosterEntry(s, leaverEndpoint) is not null);
            },
            ct, timeoutMs: 30_000);

        Assert.True(survivors[0].GetMembership().MembershipVersion > evictedVersion,
            "re-admission must commit a new roster version");

        await WaitForCondition(() => leaver.LocalRole == ClusterMemberRole.Voter, ct, timeoutMs: 30_000);

        try
        {
            await WaitForCondition(() => nodes.All(n => MapSettled(n, 3)), ct, timeoutMs: settleTimeoutMs);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"The map never settled after the rejoin: {DescribeMaps(nodes)}");
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The full incident chain. After the rolled-back drain the leaver is partitioned past the
    /// eviction grace and evicted; on heal it must learn the excluding roster through gossip,
    /// auto-rejoin, be promoted back to Voter, and the placement map must settle with every
    /// range at full RF and no transitional replica — the planner's repair of the evicted
    /// replica completes instead of stalling behind a NotMember range leader.
    /// </summary>
    [Fact]
    public async Task RequestLeave_DrainRolledBack_EvictedLater_AutoRejoinsAndMapSettles()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) =
            await BuildCluster(logger, basePort: 8880, drainTimeout: TimeSpan.FromMilliseconds(300), ct);

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];
            string leaverEndpoint = leaver.LocalEndpoint;

            await DrainTimesOutAndReleasesLatch(nodes, leaver, ct);

            long versionBeforeEviction = survivors[0].GetMembership().MembershipVersion;
            await PartitionUntilEvicted(comm, survivors, leaver, ct);
            long evictedVersion = survivors[0].GetMembership().MembershipVersion;
            Assert.True(evictedVersion > versionBeforeEviction);
            Assert.Equal(3, survivors[0].GetMembership().Members.Count);

            // Heal. The leaver's own gossip digest draws the excluding roster; applying it flips
            // the leaver to NotMember, which must now trigger auto-rejoin.
            comm.HealPartition(leaverEndpoint);

            await WaitForCondition(
                () =>
                {
                    leaver.GossipAsync(ct).GetAwaiter().GetResult();
                    return survivors.All(s => RosterEntry(s, leaverEndpoint) is not null);
                },
                ct, timeoutMs: 30_000);

            Assert.True(survivors[0].GetMembership().MembershipVersion > evictedVersion,
                "re-admission must commit a new roster version");

            // Back to a full Voter on its own view (roster learner promotion is quick: the node
            // is already caught up on P0).
            await WaitForCondition(() => leaver.LocalRole == ClusterMemberRole.Voter, ct, timeoutMs: 30_000);

            // No stalled move: the planner re-adds the leaver's replicas and promotes them.
            await WaitForCondition(() => nodes.All(n => MapSettled(n, 3)), ct, timeoutMs: 30_000);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }

    /// <summary>
    /// The other release path: a leave refused because another drain is in progress committed
    /// nothing and must not leave its latch raised either. The refused node's latch is
    /// independent of the first leaver's, which keeps its latch if its departure completed and
    /// releases it if its drain rolled back.
    /// </summary>
    [Fact]
    public async Task RequestLeave_RefusedDrainInProgress_ReleasesLeaveRequest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // A longer drain here: the second request must land while the first is still Leaving.
        (RaftManager[] nodes, InMemoryCommunication comm) =
            await BuildCluster(logger, basePort: 8890, drainTimeout: TimeSpan.FromSeconds(3), ct);

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager first = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            RaftManager second = nodes.First(n => !ReferenceEquals(n, p0Leader) && !ReferenceEquals(n, first));
            string firstEndpoint = first.LocalEndpoint;

            Task<LeaveClusterResult> firstLeave = first.RequestLeaveAsync(ct);

            // The first drain is in force once the committed roster shows it Leaving.
            await WaitForCondition(
                () => RosterEntry(second, firstEndpoint) is { Role: ClusterMemberRole.Leaving },
                ct);

            LeaveClusterResult refused = await second.RequestLeaveAsync(ct);
            Assert.Equal(LeaveClusterOutcome.RefusedDrainInProgress, refused.Outcome);
            Assert.False(second.IsLeaveRequested,
                "A refused leave committed nothing: the latch must be released so the node stays a full member.");
            Assert.Equal(ClusterMemberRole.Voter, second.LocalRole);

            // The first leaver's own latch follows its own outcome, independently of the refusal:
            // a departure that completed keeps it raised (sticky by design — the node must not
            // re-admit itself), a drain that timed out and rolled back releases it.
            LeaveClusterResult firstResult = await firstLeave;
            if (firstResult.Left)
            {
                Assert.Equal(LeaveClusterOutcome.Committed, firstResult.Outcome);
                Assert.True(first.IsLeaveRequested);
            }
            else
            {
                Assert.Equal(LeaveClusterOutcome.DrainTimedOut, firstResult.Outcome);
                await WaitForCondition(
                    () => nodes.All(n => RosterEntry(n, firstEndpoint) is { Role: ClusterMemberRole.Voter }),
                    ct);
                Assert.False(first.IsLeaveRequested);
            }
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }

    /// <summary>
    /// The 2026-10-13 chain. The drain's Leaving commit times out and no roster ever names the
    /// leaver Leaving. A later, unrelated roster advance (here a fifth node joining; in the
    /// incident the eviction of another node) still names the leaver Voter: that must release
    /// the latch, so that when the leaver is evicted afterwards it auto-rejoins and the planner's
    /// repair completes. Red before the fix: the request left the roster observer unarmed, the
    /// latch stayed raised for the life of the process, and the leaver stayed NotMember.
    /// </summary>
    [Fact]
    public async Task RequestLeave_DrainCommitTimedOut_LaterRosterNamesVoter_ReleasesLatchAndAutoRejoinsAfterEviction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int basePort = 8900;
        TimeSpan drainTimeout = TimeSpan.FromMilliseconds(300);
        (RaftManager[] nodes, InMemoryCommunication comm) = await BuildCluster(logger, basePort, drainTimeout, ct);
        RaftManager? fifth = null;

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            string leaverEndpoint = leaver.LocalEndpoint;

            long unconfirmedAt = await DrainCommitTimesOutAndKeepsLatch(comm, nodes, leaver, p0Leader, ct);

            fifth = await JoinFifthNode(comm, nodes, nodes, logger, basePort, drainTimeout, ct);
            RaftManager[] all = [.. nodes, fifth];

            // The join committed a roster newer than the unconfirmed request that still names
            // the leaver Voter: the Leaving commit did not land by then, so the request is
            // settled and the node is the full member it always was.
            await WaitForCondition(() => leaver.GetMembership().MembershipVersion > unconfirmedAt, ct);
            await WaitForCondition(() => !leaver.IsLeaveRequested, ct);

            RaftManager[] survivors = [.. all.Where(n => !ReferenceEquals(n, leaver))];
            long versionBeforeEviction = survivors[0].GetMembership().MembershipVersion;
            await PartitionUntilEvicted(comm, survivors, leaver, ct);
            long evictedVersion = survivors[0].GetMembership().MembershipVersion;
            Assert.True(evictedVersion > versionBeforeEviction);

            await HealAndExpectRejoin(comm, all, survivors, leaver, evictedVersion, ct);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                n.Dispose();
            fifth?.Dispose();
        }
    }

    /// <summary>
    /// The same unconfirmed request, with the eviction as the very next roster. No roster in
    /// between could have named the leaver Voter, and none named it Leaving either — roster
    /// versions advance by one per commit, so the exclusion cannot be the drain's own removal.
    /// The latch must be released on that roster and the auto-rejoin check that follows it must
    /// re-admit the node.
    /// </summary>
    [Fact]
    public async Task RequestLeave_DrainCommitTimedOut_EvictedByTheNextRoster_AutoRejoins()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) =
            await BuildCluster(logger, basePort: 8910, drainTimeout: TimeSpan.FromMilliseconds(300), ct);

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];

            long unconfirmedAt = await DrainCommitTimesOutAndKeepsLatch(comm, nodes, leaver, p0Leader, ct);

            await PartitionUntilEvicted(comm, survivors, leaver, ct);
            long evictedVersion = survivors[0].GetMembership().MembershipVersion;
            Assert.Equal(unconfirmedAt + 1, evictedVersion);
            Assert.Equal(3, survivors[0].GetMembership().Members.Count);

            await HealAndExpectRejoin(comm, nodes, survivors, leaver, evictedVersion, ct);
            Assert.False(leaver.IsLeaveRequested);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }

    /// <summary>
    /// The 2026-10-27 chain. The drain-leg commit times out, then lands anyway: the leader
    /// commits <c>Voter → Leaving</c> after the caller gave up, the node confirms it from the
    /// roster and keeps the latch (the drain is in force), and nobody drives the drain. A
    /// partition then evicts the node while it is <c>Leaving</c>. Before the fix that exclusion
    /// was read as the drain's own removal and the node parked as NotMember for good; now the
    /// roster's departure record says it was evicted, the latch is released and the node
    /// auto-rejoins. The promotion window is longer than the eviction so the evacuation cannot
    /// complete first (the eviction, not the drain, must be what removes the node).
    /// </summary>
    [Fact]
    public async Task RequestLeave_DrainCommitLandedLate_EvictedWhileLeaving_AutoRejoins()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await BuildCluster(
            logger, basePort: 8920, drainTimeout: TimeSpan.FromMilliseconds(300), ct, promotionStableWindow: TimeSpan.FromSeconds(6));

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];
            string leaverEndpoint = leaver.LocalEndpoint;

            long unconfirmedAt = await DrainCommitTimesOutAndKeepsLatch(comm, nodes, leaver, p0Leader, ct);

            // The commit lands late. The node sees itself Leaving and keeps the latch: a drain
            // is in force, even though the request that asked for it has already returned.
            long leavingVersion = await CommitLeavingFromLeader(p0Leader, leaver, ct);
            Assert.Equal(unconfirmedAt + 1, leavingVersion);
            await WaitForCondition(() => leaver.LocalRole == ClusterMemberRole.Leaving, ct);
            Assert.True(leaver.IsLeaveRequested, "a committed Leaving confirms the drain and keeps the latch");

            await PartitionUntilEvicted(comm, survivors, leaver, ct);
            long evictedVersion = survivors[0].GetMembership().MembershipVersion;
            Assert.True(evictedVersion > leavingVersion);
            AssertRecordedEvicted(survivors[0], leaverEndpoint, evictedVersion);

            await HealAndExpectRejoin(comm, nodes, survivors, leaver, evictedVersion, ct, settleTimeoutMs: 90_000);
            Assert.False(leaver.IsLeaveRequested, "an eviction voids the decommission");
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }

    /// <summary>
    /// The departure record survives roster versions the evicted node never sees. The drain-leg
    /// commit times out (no Leaving ever committed), the node is evicted, and while it is still
    /// isolated the roster moves on (a fifth node joins). On heal the node learns the newest
    /// roster straight from gossip — the eviction version itself is skipped, which defeated the
    /// version-adjacency rule — but that roster still records the eviction, so the latch is
    /// released and the node auto-rejoins.
    /// </summary>
    [Fact]
    public async Task RequestLeave_DrainCommitTimedOut_RosterSkipsPastTheEviction_AutoRejoins()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int basePort = 8930;
        TimeSpan drainTimeout = TimeSpan.FromMilliseconds(300);
        (RaftManager[] nodes, InMemoryCommunication comm) = await BuildCluster(logger, basePort, drainTimeout, ct);
        RaftManager? fifth = null;

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];
            string leaverEndpoint = leaver.LocalEndpoint;

            long unconfirmedAt = await DrainCommitTimesOutAndKeepsLatch(comm, nodes, leaver, p0Leader, ct);

            await PartitionUntilEvicted(comm, survivors, leaver, ct);
            long evictedVersion = survivors[0].GetMembership().MembershipVersion;
            Assert.Equal(unconfirmedAt + 1, evictedVersion);
            AssertRecordedEvicted(survivors[0], leaverEndpoint, evictedVersion);

            // The roster advances past the eviction while the leaver is still cut off: the
            // first roster it will see excludes it at a version that is not the eviction's.
            fifth = await JoinFifthNode(comm, nodes, survivors, logger, basePort, drainTimeout, ct);
            RaftManager[] all = [.. nodes, fifth];
            RaftManager[] survivorsAndFifth = [.. survivors, fifth];
            long advancedTo = survivors[0].GetMembership().MembershipVersion;
            Assert.True(advancedTo >= evictedVersion + 1);
            Assert.Equal(unconfirmedAt, leaver.GetMembership().MembershipVersion);
            AssertRecordedEvicted(survivors[0], leaverEndpoint, evictedVersion);

            await HealAndExpectRejoin(comm, all, survivorsAndFifth, leaver, advancedTo, ct);
            Assert.False(leaver.IsLeaveRequested);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                n.Dispose();
            fifth?.Dispose();
        }
    }

    /// <summary>
    /// Evicted while the request's own drain wait is running. The drain commits <c>Leaving</c>
    /// and waits for the evacuation; the node is then isolated past the eviction grace. Before
    /// the fix the wait read the exclusion as "the pass finished the drain and removed me" and
    /// reported <see cref="LeaveClusterOutcome.Committed"/> with <c>Drained</c> — a departure the
    /// caller would act on by stopping a node that still hosts every replica — and the node
    /// parked as NotMember. Now the request ends as <see cref="LeaveClusterOutcome.Evicted"/>
    /// (not <c>Left</c>) and the node auto-rejoins. The promotion window is longer than the
    /// eviction, so the drain cannot complete before the node is out.
    /// </summary>
    [Fact]
    public async Task RequestLeave_EvictedMidDrain_ReportsEvictedAndAutoRejoins()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await BuildCluster(
            logger, basePort: 8940, drainTimeout: TimeSpan.FromSeconds(20), ct, promotionStableWindow: TimeSpan.FromSeconds(6));

        try
        {
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftManager leaver = nodes.First(n => !ReferenceEquals(n, p0Leader) && NamedInMap(n, n.LocalEndpoint));
            RaftManager[] survivors = [.. nodes.Where(n => !ReferenceEquals(n, leaver))];
            string leaverEndpoint = leaver.LocalEndpoint;

            Task<LeaveClusterResult> leave = leaver.RequestLeaveAsync(ct);

            await WaitForCondition(
                () => nodes.All(n => RosterEntry(n, leaverEndpoint) is { Role: ClusterMemberRole.Leaving }),
                ct);
            Assert.False(leave.IsCompleted, "the drain wait must still be running when the node is isolated");

            await PartitionUntilEvicted(comm, survivors, leaver, ct);
            long evictedVersion = survivors[0].GetMembership().MembershipVersion;
            AssertRecordedEvicted(survivors[0], leaverEndpoint, evictedVersion);

            await HealAndExpectRejoin(comm, nodes, survivors, leaver, evictedVersion, ct, settleTimeoutMs: 90_000);

            LeaveClusterResult result = await leave.WaitAsync(TimeSpan.FromSeconds(TestTimeouts.Scale(10)), ct);
            Assert.Equal(LeaveClusterOutcome.Evicted, result.Outcome);
            Assert.False(result.Left, "an eviction is not a departure the caller may stop the node on");
            Assert.False(result.Drained);
            Assert.False(leaver.IsLeaveRequested);
            Assert.Equal(ClusterMemberRole.Voter, leaver.LocalRole);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }
}
