
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Tests for the dead-member eviction race and the auto-rejoin path:
/// <list type="bullet">
///   <item>Liveness: a Dead entry is overridable by <see cref="LivenessTable.Resurrect"/>
///     (direct-probe proof of life) and the resurrection survives stale Dead rumors at the old
///     incarnation; without this the eviction grace timer fires against demonstrably live nodes.</item>
///   <item>Eviction last-chance probe: a node that returns before the grace expires is never
///     evicted — the P0 leader re-verifies reachability at commit time.</item>
///   <item>Auto-rejoin: an evicted node that discovers a committed roster excluding itself
///     re-runs the Join flow automatically instead of parking as NotMember forever.</item>
/// </list>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestEvictionRejoin
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    // ── Liveness table unit tests ─────────────────────────────────────────────

    [Fact]
    public void Resurrect_DeadEntry_BecomesAliveAndOutranksStaleDeadRumors()
    {
        LivenessTable t = new();

        // Drive node-a to Dead at incarnation 0 via suspicion expiry.
        t.MarkSuspect("node-a");
        t.AdvanceExpiry(DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1));
        Assert.Equal(MemberLivenessState.Dead, t.GetState("node-a"));

        // MarkAlive alone must NOT clear Dead (rumor-level sticky behavior preserved).
        t.MarkAlive("node-a", 0);
        Assert.Equal(MemberLivenessState.Dead, t.GetState("node-a"));

        // Resurrect (direct-probe evidence) clears it with a bumped incarnation.
        t.Resurrect("node-a", 0);
        Assert.Equal(MemberLivenessState.Alive, t.GetState("node-a"));

        // A stale Dead rumor at the old incarnation must not re-kill the resurrected entry.
        t.ApplyUpdates("self", [new("node-a", MemberLivenessState.Dead, 0, DateTimeOffset.UtcNow)]);
        Assert.Equal(MemberLivenessState.Alive, t.GetState("node-a"));

        // A genuinely newer Dead observation (higher incarnation) still wins.
        t.ApplyUpdates("self", [new("node-a", MemberLivenessState.Dead, 99, DateTimeOffset.UtcNow)]);
        Assert.Equal(MemberLivenessState.Dead, t.GetState("node-a"));
    }

    [Fact]
    public void Resurrect_UnknownOrAliveEntry_BehavesLikeMarkAlive()
    {
        LivenessTable t = new();

        // Unknown endpoint: plain alive observation.
        t.Resurrect("node-b", 3);
        Assert.Equal(MemberLivenessState.Alive, t.GetState("node-b"));

        // Stale incarnation against an existing higher entry is dropped (MarkAlive semantics).
        t.Resurrect("node-b", 1);
        Assert.Equal(MemberLivenessState.Alive, t.GetState("node-b"));
    }

    // ── SWIM cluster harness (mirrors TestMembership's SWIM settings) ─────────

    private static RaftManager MakeSwimNode(
        InMemoryCommunication communication,
        string host, int port, int nodeId,
        IEnumerable<string> peers,
        ILogger<IRaft> logger,
        IWAL? wal = null,
        bool enableAutoRejoin = true,
        bool enableQuiescence = false)
    {
        RaftConfiguration config = new()
        {
            QuiesceAfter = TimeSpan.FromMilliseconds(300),
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
            // Quiescence validation requires PingInterval < StartElectionTimeout.
            StartElectionTimeout = enableQuiescence ? 200 : 100,
            EnableQuiescence = enableQuiescence,
            EndElectionTimeout = enableQuiescence ? 400 : 250,
            PingTimeout = TimeSpan.FromMilliseconds(100),
            IndirectPingFanout = 1,
            SuspicionTimeout = TimeSpan.FromMilliseconds(600),
            DeadMemberEvictionGrace = TimeSpan.FromMilliseconds(800),
            PingInterval = TimeSpan.FromMilliseconds(100),
            GossipInterval = TimeSpan.FromMilliseconds(200),
            // Promote a re-joining learner quickly: it is already caught up.
            LearnerPromotionLag = 64,
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(300),
            EnableAutoRejoin = enableAutoRejoin,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            wal ?? new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private async Task<(RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)>
        BuildSwimCluster(int basePort, CancellationToken ct)
    {
        InMemoryCommunication comm = new();

        string ep1 = $"localhost:{basePort}", ep2 = $"localhost:{basePort + 1}", ep3 = $"localhost:{basePort + 2}";

        RaftManager n1 = MakeSwimNode(comm, "localhost", basePort, 1, [ep2, ep3], logger);
        RaftManager n2 = MakeSwimNode(comm, "localhost", basePort + 1, 2, [ep1, ep3], logger);
        RaftManager n3 = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger);

        comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = n3 });

        await n1.UpdateNodes();
        await n2.UpdateNodes();
        await n3.UpdateNodes();

        await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));

        await WaitForCondition(
            () => n1.GetMembership().MembershipVersion > 0
               && n2.GetMembership().MembershipVersion > 0
               && n3.GetMembership().MembershipVersion > 0,
            ct);

        return (n1, n2, n3, comm);
    }

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 15_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    // ── Eviction race ─────────────────────────────────────────────────────────

    /// <summary>
    /// A node that goes Dead but becomes reachable again before the eviction grace expires must
    /// NOT be evicted: the last-chance probe on the eviction path (and the resurrect-on-probe
    /// path) verifies reality before the one-way RemoveMember commit.
    /// </summary>
    [Fact]
    public async Task Eviction_NodeReturnsBeforeGraceExpires_IsNotEvicted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)
            = await BuildSwimCluster(8830, ct);

        string ep3 = "localhost:8832";

        try
        {
            long rosterBefore = n1.GetMembership().MembershipVersion;

            // Partition n3 and drive pings until it is observed Dead on n1 and n2.
            comm.PartitionNode(ep3);

            long deadline = Environment.TickCount64 + 15_000;
            while (Environment.TickCount64 < deadline
                   && (n1.Liveness.GetState(ep3) != MemberLivenessState.Dead
                       || n2.Liveness.GetState(ep3) != MemberLivenessState.Dead))
            {
                ct.ThrowIfCancellationRequested();
                await n1.PingAsync(ct);
                await n2.PingAsync(ct);
                await Task.Delay(50, ct);
            }
            Assert.Equal(MemberLivenessState.Dead, n1.Liveness.GetState(ep3));

            // The node "returns" (heals) while the grace clock is still running.
            comm.HealPartition(ep3);

            // Let the grace fully elapse and give the timers (UpdateNodes → EvictDeadMembersAsync)
            // ample opportunity to fire the eviction they would previously have committed.
            await Task.Delay(2_000, ct);
            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await Task.Delay(500, ct);

            // No eviction: the roster is unchanged and still contains n3.
            Assert.Equal(rosterBefore, n1.GetMembership().MembershipVersion);
            Assert.Contains(n1.GetMembership().Members, m => m.Endpoint == ep3);
            Assert.Equal(3, n1.GetMembership().Members.Count);
        }
        finally
        {
            comm.HealPartition(ep3);
            n1.Dispose(); n2.Dispose(); n3.Dispose();
        }
    }

    // ── Auto-rejoin ───────────────────────────────────────────────────────────

    /// <summary>
    /// The full incident scenario: a node partitioned long enough to be genuinely evicted
    /// (roster v2 without it) heals, learns of its eviction via its own outbound gossip ack,
    /// and automatically re-runs the Join flow — ending back in the committed roster instead of
    /// parked as NotMember serving terminal errors forever.
    /// </summary>
    [Fact]
    public async Task Eviction_EvictedNodeHeals_AutoRejoinsRoster()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)
            = await BuildSwimCluster(8840, ct);

        string ep3 = "localhost:8842";

        try
        {
            // Partition n3 and drive pings until Dead everywhere.
            comm.PartitionNode(ep3);

            long deadline = Environment.TickCount64 + 15_000;
            while (Environment.TickCount64 < deadline
                   && (n1.Liveness.GetState(ep3) != MemberLivenessState.Dead
                       || n2.Liveness.GetState(ep3) != MemberLivenessState.Dead))
            {
                ct.ThrowIfCancellationRequested();
                await n1.PingAsync(ct);
                await n2.PingAsync(ct);
                await Task.Delay(50, ct);
            }

            // Drive UpdateNodes until the eviction commits (the last-chance probe fails while
            // n3 is still partitioned, so the eviction goes through as before).
            await WaitForCondition(
                () =>
                {
                    n1.UpdateNodes().GetAwaiter().GetResult();
                    n2.UpdateNodes().GetAwaiter().GetResult();
                    return !n1.GetMembership().Members.Any(m => m.Endpoint == ep3)
                        && n1.GetMembership().MembershipVersion > 1;
                },
                ct, timeoutMs: 15_000);

            long evictedVersion = n1.GetMembership().MembershipVersion;
            Assert.Equal(2, n1.GetMembership().Members.Count);

            // n3 returns. Its own outbound gossip digest (stale roster v1) draws an ack carrying
            // the v2 roster; applying it flips n3 to NotMember and triggers auto-rejoin.
            comm.HealPartition(ep3);

            await WaitForCondition(
                () =>
                {
                    n3.GossipAsync(ct).GetAwaiter().GetResult();
                    return n1.GetMembership().Members.Any(m => m.Endpoint == ep3);
                },
                // 30 s: the auto-rejoin driver runs on background timers whose cadence stretches
                // under the residual load of the class's two ~30 s quiesced-restart tests; 20 s
                // was observed to flake when this test ran right after them.
                ct, timeoutMs: 30_000);

            Assert.True(n1.GetMembership().MembershipVersion > evictedVersion,
                "re-admission must commit a new roster version");

            // n3 must observe its own re-admission and leave NotMember (Learner or, after
            // promotion, Voter).
            await WaitForCondition(
                () => n3.LocalRole != ClusterMemberRole.NotMember,
                ct, timeoutMs: 30_000);

            Assert.Contains(n3.GetMembership().Members, m => m.Endpoint == ep3);
        }
        finally
        {
            comm.HealPartition(ep3);
            n1.Dispose(); n2.Dispose(); n3.Dispose();
        }
    }

    /// <summary>
    /// The boot-into-evicted deadlock (1.0.2 field report): a node whose own WAL already
    /// contains the roster that evicts it replays "v1 includes me" → "v2 excludes me" during
    /// startup restore, BEFORE initialization completes — and no further roster change ever
    /// arrives to re-fire the edge-triggered check. The original driver's <c>IsInitialized</c>
    /// guard dropped that one-and-only trigger, parking the node as NotMember forever.
    /// The fix keys the trigger on "was ever in a committed roster" (set by the v1 replay)
    /// instead, so the restart must auto-rejoin with no external stimulus.
    /// </summary>
    [Fact]
    public async Task Eviction_NodeRestartsIntoEvictedRoster_AutoRejoinsDuringBoot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        const int basePort = 8850;
        string ep1 = $"localhost:{basePort}", ep2 = $"localhost:{basePort + 1}", ep3 = $"localhost:{basePort + 2}";

        InMemoryWAL innerWal3 = new(logger);
        NonDisposingWAL wal3 = new(innerWal3);

        RaftManager n1 = MakeSwimNode(comm, "localhost", basePort, 1, [ep2, ep3], logger);
        RaftManager n2 = MakeSwimNode(comm, "localhost", basePort + 1, 2, [ep1, ep3], logger);
        // Auto-rejoin disabled on the first incarnation so the eviction record lands in its WAL
        // and it parks as NotMember without fighting back — the pre-restart incident state.
        RaftManager n3 = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger, wal3, enableAutoRejoin: false);
        RaftManager? restarted = null;

        comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = n3 });

        try
        {
            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForCondition(
                () => n1.GetMembership().MembershipVersion > 0
                   && n2.GetMembership().MembershipVersion > 0
                   && n3.GetMembership().MembershipVersion > 0,
                ct);

            // Commit the eviction while n3 is still live and reachable, so the v2 record is
            // replicated into n3's own WAL (exactly how the incident's RemoveMember reached the
            // node before its restart).
            RaftManager p0Leader = n1;
            long deadline = Environment.TickCount64 + 15_000;
            while (Environment.TickCount64 < deadline && !await n1.AmILeaderQuick(0) && !await n2.AmILeaderQuick(0))
                await Task.Delay(50, ct);
            p0Leader = await n1.AmILeaderQuick(0) ? n1
                     : await n2.AmILeaderQuick(0) ? n2
                     : await n3.AmILeaderQuick(0) ? n3 : throw new TimeoutException("no P0 leader");

            ClusterMember member3 = p0Leader.GetMembership().Members.First(m => m.Endpoint == ep3);
            p0Leader.SystemCoordinator.Send(new RaftSystemRequest(
                RaftSystemRequestType.RemoveMember, ep3, member3.NodeId,
                p0Leader.GetMembership().MembershipVersion));

            // n3 must observe its own eviction (the v2 record committed through its WAL).
            await WaitForCondition(
                () => n3.GetMembership().MembershipVersion > 1
                   && !n3.GetMembership().Members.Any(m => m.Endpoint == ep3),
                ct);
            Assert.Equal(ClusterMemberRole.NotMember, n3.LocalRole);

            // Restart n3 over the SAME WAL with auto-rejoin enabled: restore replays
            // v1-includes-me then v2-excludes-me before initialization completes.
            n3.Dispose();
            restarted = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger, wal3);
            comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = restarted });

            await restarted.UpdateNodes();
            RaftManager toJoin = restarted;
            Task joinTask = Task.Run(() => toJoin.JoinCluster(ct), ct);

            // The restarted node must re-enter the committed roster with no external stimulus.
            await WaitForCondition(
                () => n1.GetMembership().Members.Any(m => m.Endpoint == ep3),
                ct, timeoutMs: 20_000);

            await WaitForCondition(
                () => restarted.LocalRole != ClusterMemberRole.NotMember,
                ct, timeoutMs: 20_000);

            Assert.Contains(restarted.GetMembership().Members, m => m.Endpoint == ep3);

            try { await joinTask.WaitAsync(TimeSpan.FromSeconds(30), ct); } catch (TimeoutException) { /* rejoin verified above */ }
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); restarted?.Dispose();
        }
    }

    /// <summary>
    /// The replication-starvation leg of the incident: a member that is evicted and later
    /// readmitted returns with reset state, but the partition leader still holds the
    /// caught-up progress it recorded before the eviction. With quiescence enabled that stale
    /// progress is fatal — <c>HasLaggingPeer</c> reads the member as converged, the leader never
    /// un-quiesces, and the only catch-up path (heartbeats → backfill) never runs: the member
    /// starves forever with its partitions unassembled. Readmission must therefore discard the
    /// leader's per-follower progress (<c>ResetFollowerProgress</c>), making the member read as
    /// lagging so heartbeats re-arm and backfill re-anchors from its real (empty) frontier.
    /// The assertion is end-to-end: the freshly restarted member's acked committed index on the
    /// leader must reach the leader's own committed index — replication genuinely resumed.
    /// </summary>
    [Fact]
    public async Task Readmission_ResetsStaleFollowerProgress_ReplicationResumes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        const int basePort = 8860;
        string ep1 = $"localhost:{basePort}", ep2 = $"localhost:{basePort + 1}", ep3 = $"localhost:{basePort + 2}";

        RaftManager n1 = MakeSwimNode(comm, "localhost", basePort, 1, [ep2, ep3], logger, enableQuiescence: true);
        RaftManager n2 = MakeSwimNode(comm, "localhost", basePort + 1, 2, [ep1, ep3], logger, enableQuiescence: true);
        RaftManager n3 = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger, enableQuiescence: true);
        RaftManager? restarted = null;

        comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = n3 });

        try
        {
            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForCondition(
                () => n1.GetMembership().MembershipVersion > 0
                   && n2.GetMembership().MembershipVersion > 0
                   && n3.GetMembership().MembershipVersion > 0,
                ct);

            // The starvation requires the SAME leader to retain ep3's caught-up progress across
            // the eviction, so partition 1 must be led by a survivor: step n3 down until it is.
            RaftManager leader = await WaitForPartitionLeader(1, ct, n1, n2, n3);
            long stepDownDeadline = Environment.TickCount64 + 15_000;
            while (leader == n3)
            {
                if (Environment.TickCount64 > stepDownDeadline)
                    throw new TimeoutException("Partition 1 leadership never settled on a survivor.");
                await n3.StepDownAsync(1, ct);
                await Task.Delay(100, ct);
                leader = await WaitForPartitionLeader(1, ct, n1, n2, n3);
            }

            // Replicate data on partition 1 and wait until every node acked it, so the leader
            // records ep3 as fully caught up — the progress that must not survive readmission.
            for (int i = 0; i < 5; i++)
            {
                RaftReplicationResult result = await leader.ReplicateLogs(1, "test", [1, 2, 3], cancellationToken: ct);
                Assert.Equal(RaftOperationStatus.Success, result.Status);
            }

            long leaderCommitted = await leader.GetFollowerCommittedIndexAsync(1, leader.LocalEndpoint);
            await WaitForFollowerIndex(leader, ep3, leaderCommitted, ct);

            // Partition ep3 and drive it to Dead, then let the eviction commit.
            comm.PartitionNode(ep3);
            long deadline = Environment.TickCount64 + 15_000;
            while (Environment.TickCount64 < deadline
                   && (n1.Liveness.GetState(ep3) != MemberLivenessState.Dead
                       || n2.Liveness.GetState(ep3) != MemberLivenessState.Dead))
            {
                ct.ThrowIfCancellationRequested();
                await n1.PingAsync(ct);
                await n2.PingAsync(ct);
                await Task.Delay(50, ct);
            }
            await WaitForCondition(
                () =>
                {
                    n1.UpdateNodes().GetAwaiter().GetResult();
                    n2.UpdateNodes().GetAwaiter().GetResult();
                    return !n1.GetMembership().Members.Any(m => m.Endpoint == ep3)
                        && n1.GetMembership().MembershipVersion > 1;
                },
                ct, timeoutMs: 15_000);

            // Wait until partition 1's leader has demonstrably quiesced — the starvation needs a
            // leader whose heartbeats are suppressed while it still holds ep3's stale caught-up
            // progress. (The leader cannot have moved: both survivors kept following it.)
            // Settled proposals are retained for 30 s and block the quiesce gate, so this
            // legitimately takes just over that long after the last write.
            RaftManager quiescedLeader = await WaitForPartitionLeader(1, ct, n1, n2);
            try
            {
                await WaitForCondition(
                    () => quiescedLeader.GetPartitionViewAsync(1, ct).GetAwaiter().GetResult()?.Quiesced == true,
                    ct, timeoutMs: 45_000);
            }
            catch (TimeoutException)
            {
                RaftPartitionView? v = await quiescedLeader.GetPartitionViewAsync(1, ct);
                long peerIdx = await quiescedLeader.GetFollowerCommittedIndexAsync(1, quiescedLeader.Nodes[0].Endpoint);
                long selfIdx = await quiescedLeader.GetFollowerCommittedIndexAsync(1, quiescedLeader.LocalEndpoint);
                throw new TimeoutException(
                    $"P1 never quiesced: quiesced={v?.Quiesced}, self={selfIdx}, peer={peerIdx}, view={v}, nodes=[{string.Join(',', quiescedLeader.Nodes.Select(x => x.Endpoint))}], rosterV={quiescedLeader.GetMembership().MembershipVersion}");
            }

            // The member returns with RESET state (fresh WAL) — the field shape of a node whose
            // rejoin re-initialized its log — and heals; gossip pushes it the excluding roster,
            // triggering auto-rejoin.
            n3.Dispose();
            restarted = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger,
                wal: new InMemoryWAL(logger), enableQuiescence: true);
            comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = restarted });
            comm.HealPartition(ep3);

            await restarted.UpdateNodes();
            RaftManager toJoin = restarted;
            // A state-reset node has no roster in its WAL, so admission is the seed-based join
            // (the discovery-based overload only waits; the auto-rejoin driver needs a roster).
            Task joinTask = Task.Run(() => toJoin.JoinCluster([ep1, ep2], ct), ct);

            // Drive the survivors' SWIM probes so the returned endpoint is resurrected from Dead
            // (admission gates on liveness), and surface a failed join instead of spinning on it.
            await WaitForCondition(
                () =>
                {
                    if (joinTask.IsFaulted)
                        joinTask.GetAwaiter().GetResult();
                    n1.PingAsync(ct).GetAwaiter().GetResult();
                    n2.PingAsync(ct).GetAwaiter().GetResult();
                    n1.UpdateNodes().GetAwaiter().GetResult();
                    return n1.GetMembership().Members.Any(m => m.Endpoint == ep3);
                },
                ct, timeoutMs: 20_000);

            // The decisive assertion: the readmitted, reset member must actually RECEIVE the
            // partition's log — judged from its OWN partition view, never from the leader's
            // per-follower map (that map holds the very stale entry the bug leaves behind, so
            // reading it back would make this assertion vacuous). Without the progress reset the
            // quiesced leader believes ep3 is caught up and never sends it another entry, so the
            // fresh node's own commit index stays at zero.
            RaftManager leaderAfter = await WaitForPartitionLeader(1, ct, n1, n2);
            long leaderAfterCommitted = await leaderAfter.GetFollowerCommittedIndexAsync(1, leaderAfter.LocalEndpoint);
            Assert.True(leaderAfterCommitted >= leaderCommitted);

            try
            {
                await WaitForCondition(
                    () => toJoin.GetPartitionViewAsync(1, ct).GetAwaiter().GetResult()?.CommitIndex >= leaderAfterCommitted,
                    ct, timeoutMs: 30_000);
            }
            catch (TimeoutException)
            {
                RaftPartitionView? lv = await leaderAfter.GetPartitionViewAsync(1, ct);
                RaftPartitionView? jv = await toJoin.GetPartitionViewAsync(1, ct);
                long staleIdx = await leaderAfter.GetFollowerCommittedIndexAsync(1, ep3);
                throw new TimeoutException(
                    $"rejoined node starved: leaderView=[quiesced={lv?.Quiesced} {lv}], joinView=[{jv}], leaderStaleIdxForEp3={staleIdx}, joinRole={toJoin.LocalRole}, nodesOnLeader=[{string.Join(',', leaderAfter.Nodes.Select(x => x.Endpoint))}]");
            }

            try { await joinTask.WaitAsync(TimeSpan.FromSeconds(30), ct); } catch (TimeoutException) { /* replication verified above */ }
        }
        finally
        {
            comm.HealPartition(ep3);
            n1.Dispose(); n2.Dispose(); n3.Dispose(); restarted?.Dispose();
        }
    }

    /// <summary>
    /// The quiesced-follower-restart variant of the starvation: a member restarts WITHOUT being
    /// evicted (fast restart, roster unchanged — so no readmission event fires) while the
    /// partition leaders are quiesced. Its in-memory quiesce flag and leader knowledge died with
    /// the process; nobody heartbeats it, so it loops pre-vote rounds that the leader denies
    /// ("we are the leader") forever, and its partitions never assemble. The leader's deny path
    /// must treat a member's pre-vote as evidence it cannot see the leader and un-quiesce, so
    /// one heartbeat interval re-teaches it. Asserted from the restarted node's own partition
    /// view: it must re-learn the leader and reach the leader's commit frontier.
    /// </summary>
    [Fact]
    public async Task QuiescedLeader_FollowerRestartsWithoutEviction_RelearnsLeaderAndConverges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        const int basePort = 8870;
        string ep1 = $"localhost:{basePort}", ep2 = $"localhost:{basePort + 1}", ep3 = $"localhost:{basePort + 2}";

        InMemoryWAL innerWal3 = new(logger);
        NonDisposingWAL wal3 = new(innerWal3);

        RaftManager n1 = MakeSwimNode(comm, "localhost", basePort, 1, [ep2, ep3], logger, enableQuiescence: true);
        RaftManager n2 = MakeSwimNode(comm, "localhost", basePort + 1, 2, [ep1, ep3], logger, enableQuiescence: true);
        RaftManager n3 = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger, wal3, enableQuiescence: true);
        RaftManager? restarted = null;

        comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = n3 });

        try
        {
            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForCondition(
                () => n1.GetMembership().MembershipVersion > 0
                   && n2.GetMembership().MembershipVersion > 0
                   && n3.GetMembership().MembershipVersion > 0,
                ct);

            // Keep partition 1 leadership on a node that will NOT restart.
            RaftManager leader = await WaitForPartitionLeader(1, ct, n1, n2, n3);
            long stepDownDeadline = Environment.TickCount64 + 15_000;
            while (leader == n3)
            {
                if (Environment.TickCount64 > stepDownDeadline)
                    throw new TimeoutException("Partition 1 leadership never settled on a survivor.");
                await n3.StepDownAsync(1, ct);
                await Task.Delay(100, ct);
                leader = await WaitForPartitionLeader(1, ct, n1, n2, n3);
            }

            for (int i = 0; i < 5; i++)
            {
                RaftReplicationResult result = await leader.ReplicateLogs(1, "test", [1, 2, 3], cancellationToken: ct);
                Assert.Equal(RaftOperationStatus.Success, result.Status);
            }

            long leaderCommitted = await leader.GetFollowerCommittedIndexAsync(1, leader.LocalEndpoint);
            await WaitForFollowerIndex(leader, ep3, leaderCommitted, ct);

            // Wait for demonstrable quiescence (settled proposals retained ~30 s block the gate).
            await WaitForCondition(
                () => leader.GetPartitionViewAsync(1, ct).GetAwaiter().GetResult()?.Quiesced == true,
                ct, timeoutMs: 45_000);

            // Fast restart over the SAME WAL: no eviction, roster unchanged, no readmission event.
            n3.Dispose();
            restarted = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger, wal3, enableQuiescence: true);
            comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = restarted });

            await restarted.UpdateNodes();
            RaftManager toJoin = restarted;
            Task joinTask = Task.Run(() => toJoin.JoinCluster(ct), ct);

            // The restarted member must re-learn partition 1's leader and reach its frontier —
            // judged from its OWN view. Without the deny-path wake the quiesced leader never
            // heartbeats it and this times out with the node stuck in pre-vote rounds.
            await WaitForCondition(
                () =>
                {
                    RaftPartitionView? v = toJoin.GetPartitionViewAsync(1, ct).GetAwaiter().GetResult();
                    return v is not null && v.CommitIndex >= leaderCommitted && !string.IsNullOrEmpty(v.Leader);
                },
                ct, timeoutMs: 30_000);

            try { await joinTask.WaitAsync(TimeSpan.FromSeconds(30), ct); } catch (TimeoutException) { /* convergence verified above */ }
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); restarted?.Dispose();
        }
    }

    /// <summary>
    /// The self-leader variant of the quiesced restart: the node that restarts IS the partition's
    /// quiesced leader. Its leadership state dies with the process, but the survivors' quiesced
    /// pre-vote gate defers to SWIM — and the restarted process is Alive as a node — so without
    /// the candidate-is-expected-leader exemption in VoteAsync both survivors deny the ex-leader
    /// its own vacated leadership forever, their own election trigger stays calm for the same
    /// reason, and the partition wedges leaderless with nobody heartbeating the restarted node
    /// (the GA-only failure mode of the sibling test above: leadership migrated onto the
    /// restarting node during the idle window before quiescence). With the exemption the
    /// ex-leader's pre-vote reaches quorum and a normal election re-establishes a leader.
    /// Asserted from the restarted node's own partition view.
    /// </summary>
    [Fact]
    public async Task QuiescedLeader_LeaderItselfRestarts_LeadershipReestablishedAndConverges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        const int basePort = 8960;
        string ep1 = $"localhost:{basePort}", ep2 = $"localhost:{basePort + 1}", ep3 = $"localhost:{basePort + 2}";

        InMemoryWAL innerWal3 = new(logger);
        NonDisposingWAL wal3 = new(innerWal3);

        RaftManager n1 = MakeSwimNode(comm, "localhost", basePort, 1, [ep2, ep3], logger, enableQuiescence: true);
        RaftManager n2 = MakeSwimNode(comm, "localhost", basePort + 1, 2, [ep1, ep3], logger, enableQuiescence: true);
        RaftManager n3 = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger, wal3, enableQuiescence: true);
        RaftManager? restarted = null;

        comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = n3 });

        try
        {
            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForCondition(
                () => n1.GetMembership().MembershipVersion > 0
                   && n2.GetMembership().MembershipVersion > 0
                   && n3.GetMembership().MembershipVersion > 0,
                ct);

            // Force partition 1 leadership ONTO the node that will restart. ForceLeaderForTesting
            // runs a real election at term+1, which the peers grant (equal logs, unvoted term), so
            // this settles deterministically where a step-down lottery would not.
            await WaitForPartitionLeader(1, ct, n1, n2, n3);
            long forceDeadline = Environment.TickCount64 + 15_000;
            while (!await n3.AmILeaderQuick(1))
            {
                if (Environment.TickCount64 > forceDeadline)
                    throw new TimeoutException("Partition 1 leadership never settled on n3.");
                await n3.ForceLeaderForTestingAsync(1, ct);
                await Task.Delay(100, ct);
            }

            for (int i = 0; i < 5; i++)
            {
                RaftReplicationResult result = await n3.ReplicateLogs(1, "test", [1, 2, 3], cancellationToken: ct);
                Assert.Equal(RaftOperationStatus.Success, result.Status);
            }

            long leaderCommitted = await n3.GetFollowerCommittedIndexAsync(1, n3.LocalEndpoint);
            await WaitForFollowerIndex(n3, ep1, leaderCommitted, ct);
            await WaitForFollowerIndex(n3, ep2, leaderCommitted, ct);

            // Wait for demonstrable quiescence (settled proposals retained ~30 s block the gate).
            await WaitForCondition(
                () => n3.GetPartitionViewAsync(1, ct).GetAwaiter().GetResult()?.Quiesced == true,
                ct, timeoutMs: 45_000);

            // Fast restart of the quiesced LEADER over the SAME WAL: no eviction, roster unchanged.
            n3.Dispose();
            restarted = MakeSwimNode(comm, "localhost", basePort + 2, 3, [ep1, ep2], logger, wal3, enableQuiescence: true);
            comm.SetNodes(new Dictionary<string, IRaft> { [ep1] = n1, [ep2] = n2, [ep3] = restarted });

            await restarted.UpdateNodes();
            RaftManager toJoin = restarted;
            Task joinTask = Task.Run(() => toJoin.JoinCluster(ct), ct);

            // The partition must re-establish a leader visible from the restarted node's own view
            // (typically the ex-leader reclaims it via a normal election) and reach the old commit
            // frontier. Without the candidate-is-expected-leader exemption this stalls forever in
            // denied pre-vote rounds.
            await WaitForCondition(
                () =>
                {
                    RaftPartitionView? v = toJoin.GetPartitionViewAsync(1, ct).GetAwaiter().GetResult();
                    return v is not null && v.CommitIndex >= leaderCommitted && !string.IsNullOrEmpty(v.Leader);
                },
                ct, timeoutMs: 30_000);

            try { await joinTask.WaitAsync(TimeSpan.FromSeconds(30), ct); } catch (TimeoutException) { /* convergence verified above */ }
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); restarted?.Dispose();
        }
    }

    /// <summary>Polls until the leader's recorded committed index for <paramref name="endpoint"/> on partition 1 reaches <paramref name="target"/>.</summary>
    private static async Task WaitForFollowerIndex(RaftManager leader, string endpoint, long target, CancellationToken ct, int timeoutMs = 15_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (await leader.GetFollowerCommittedIndexAsync(1, endpoint) >= target)
                return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Follower {endpoint} never reached committed index {target}.");
    }

    /// <summary>Polls until one of the candidates leads <paramref name="partitionId"/> and returns it.</summary>
    private static async Task<RaftManager> WaitForPartitionLeader(int partitionId, CancellationToken ct, params RaftManager[] candidates)
    {
        long deadline = Environment.TickCount64 + 15_000;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager candidate in candidates)
                if (await candidate.AmILeaderQuick(partitionId))
                    return candidate;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"No leader for partition {partitionId} among candidates.");
    }

    /// <summary>
    /// Delegates everything to the inner WAL but ignores Dispose, so the same in-memory WAL
    /// instance can survive a <see cref="RaftManager"/> restart (the manager disposes its WAL).
    /// </summary>
    private sealed class NonDisposingWAL : IWAL
    {
        private readonly InMemoryWAL inner;

        public NonDisposingWAL(InMemoryWAL inner) => this.inner = inner;

        public Kommander.Data.RaftOperationStatus Write(List<(int, List<Kommander.Data.RaftLog>)> logs) => inner.Write(logs);
        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);
        public List<Kommander.Data.RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public List<Kommander.Data.RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (Kommander.Data.RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
            int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
            inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public Kommander.Data.RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public Kommander.Data.RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (Kommander.Data.RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() { /* survives manager restarts */ }
    }
}
