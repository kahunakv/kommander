using System.Text;
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
/// Removing the LEADER replica of a placed range, on a real in-process four-node cluster at
/// replication factor 3 with quiescence ON (the production default) — the k8s-operator rig's
/// shape. The Kahuna 1.12.7 e2e incident (2026-10-18): once the placement trim started shedding
/// most-loaded nodes it removed range leaders, the removal's leadership hand-off ran on the P0
/// leader's own replica and silently answered NodeIsNotLeader whenever the P0 leader did not
/// lead the range, so the leader was dropped while leading — and the remaining quiesced
/// followers, which gate elections on SWIM liveness of the leader's node, never elected anyone:
/// the node was alive, it just no longer hosted the range. Partition 3 stayed unavailable for
/// minutes.
/// <para>Two tests pin the two halves of the fix. The first asserts the removal hands leadership
/// off, confirmed, before it marks the leader Removing — through the remote path, since the P0
/// leader is made to differ from the range leader. The second disables the hand-off (the pre-fix
/// behaviour) and asserts the quiesced survivors still elect a new leader promptly, because a
/// leader that left the replica set is vacated regardless of SWIM.</para>
/// <para>No write lands on the range before the removal: with this configuration a range that
/// took a write did not quiesce again within the test's patience, and the quiesced state is the
/// incident's precondition. Writes are issued after the removal only, to prove the range serves.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLeaderReplicaRemovalCluster
{
    private const int StartElectionTimeoutMs = 500;
    private const int EndElectionTimeoutMs = 1000;

    // The quiesced-follower safety sweep runs every UpdateNodesInterval. It is kept long here on
    // purpose: a survivor that notices the vacancy only through the sweep cannot meet the election
    // bound below, so the bound also pins the map-application wake (RaftManager marks the
    // partition hot when the committed replica set drops its known leader).
    private static readonly TimeSpan UpdateNodesInterval = TimeSpan.FromSeconds(5);

    private readonly ILogger<IRaft> logger;

    public TestLeaderReplicaRemovalCluster(ITestOutputHelper outputHelper)
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
        IEnumerable<string> peers)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            ReplicationFactor = 3,
            // Rebalancer OFF: the only replica-set change is the one under test, and nothing
            // repairs the range back to RF afterwards.
            EnablePlacementRebalancer = false,
            PlacementPassInterval = TimeSpan.FromMilliseconds(250),
            // Quiescence ON with a short idle window: the range quiesces before the removal, so
            // its followers sit in the SWIM-gated election mode the incident needs.
            EnableQuiescence = true,
            QuiesceAfter = TimeSpan.FromMilliseconds(150),
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = UpdateNodesInterval,
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            PingInterval = TimeSpan.FromMilliseconds(200),
            StartElectionTimeout = StartElectionTimeoutMs,
            EndElectionTimeout = EndElectionTimeoutMs,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private async Task<(RaftManager[] Nodes, InMemoryCommunication Comm)> AssemblePlacedCluster(
        int basePort, CancellationToken ct)
    {
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        {
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3), Ep(4)]),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3), Ep(4)]),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2), Ep(4)]),
            MakeNode(comm, basePort + 4, 4, [Ep(1), Ep(2), Ep(3)]),
        };

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

        // Roster seeded and the initial placement committed: two ranges, three voter replicas each.
        await WaitForCondition(
            () => nodes.All(n =>
                n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                n.GetPartitionMap().Count == 2 &&
                n.GetPartitionMap().All(r => r.Replicas.Count == 3)),
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

    /// <summary>The node that leads the range among <paramref name="nodes"/>, or null when none does within <paramref name="timeoutMs"/>.</summary>
    private static async Task<RaftManager?> TryLeaderOf(RaftManager[] nodes, int partitionId, int timeoutMs, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(timeoutMs))
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (n.HostsPartition(partitionId) && await n.AmILeaderQuick(partitionId))
                    return n;
            }
            await Task.Delay(25, ct);
        }
        return null;
    }

    private static async Task<RaftManager> LeaderOf(RaftManager[] nodes, int partitionId, CancellationToken ct) =>
        await TryLeaderOf(nodes, partitionId, 15_000, ct)
        ?? throw new TimeoutException($"No leader elected for partition {partitionId} within 15 s.");

    private static int NodeIdOf(RaftManager node) =>
        int.Parse(node.GetLocalNodeName().AsSpan("node".Length), global::System.Globalization.CultureInfo.InvariantCulture);

    private static RaftManager[] HostsOf(RaftManager[] nodes, int partitionId) =>
        nodes.Where(n => n.HostsPartition(partitionId)).ToArray();

    private static RaftManager[] FollowersOf(RaftManager[] nodes, int partitionId, RaftManager leader) =>
        nodes.Where(n => !ReferenceEquals(n, leader) && n.HostsPartition(partitionId)).ToArray();

    /// <summary>
    /// Returns the current system-partition leader and the range leader, after moving the
    /// range's leadership off the system-partition leader when the two coincide: the removal's
    /// hand-off must take the remote path (a suggestion to the leader node), the one that was a
    /// silent no-op before the fix.
    /// </summary>
    private static async Task<(RaftManager Coordinator, RaftManager RangeLeader)> SeparateCoordinatorFromRangeLeaderAsync(
        RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            RaftManager coordinator = await P0Leader(nodes, ct);
            RaftManager rangeLeader = await LeaderOf(nodes, partitionId, ct);

            if (!ReferenceEquals(coordinator, rangeLeader))
                return (coordinator, rangeLeader);

            string target = nodes[0].GetPartitionReplicas(partitionId)
                .First(r => r.Role == RaftReplicaRole.Voter && r.Endpoint != coordinator.LocalEndpoint)
                .Endpoint;

            RaftOperationStatus status = await coordinator.TransferLeadershipAsync(partitionId, target, ct);
            Assert.True(
                status is RaftOperationStatus.Success or RaftOperationStatus.LeaderAlreadyElected,
                $"Pre-test leadership transfer of partition {partitionId} failed: {status}");

            ValueStopwatch sw = ValueStopwatch.StartNew();
            while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(10_000) && await coordinator.AmILeaderQuick(partitionId))
                await Task.Delay(25, ct);
        }

        throw new InvalidOperationException("Could not separate the system-partition leader from the range leader.");
    }

    // ── The hand-off before the removal ──────────────────────────────────────

    /// <summary>
    /// Removing the range leader through a system-partition leader that does not lead the range:
    /// the removal must hand the leadership to another voter first and confirm it (the remote
    /// path: a transfer suggestion to the leader node, then the quorum-confirmed leader must
    /// differ), the departed host stops hosting, and the range keeps serving writes.
    /// </summary>
    [Fact]
    public async Task RemovingTheLeaderReplica_HandsLeadershipOffBeforeTheRemoval()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssemblePlacedCluster(basePort: 8380, ct);

        try
        {
            int partitionId = nodes[0].GetPartitionMap().Min(r => r.PartitionId);
            Assert.Equal(3, HostsOf(nodes, partitionId).Length);

            (RaftManager coordinator, RaftManager rangeLeader) = await SeparateCoordinatorFromRangeLeaderAsync(nodes, partitionId, ct);

            // Observe the real hand-off through the hook.
            List<(string Leader, string Target, RaftOperationStatus Status)> handOffs = [];
            foreach (RaftManager n in nodes)
            {
                RaftManager node = n;
                node.SystemCoordinator.HandOffPartitionLeadershipOverride = async (id, leader, target, token) =>
                {
                    RaftOperationStatus status = await node.SystemCoordinator.HandOffPartitionLeadershipCoreAsync(id, leader, target, token);
                    lock (handOffs)
                        handOffs.Add((leader, target, status));
                    return status;
                };
            }

            // The incident's precondition: the range is idle and its followers are quiesced.
            RaftManager[] followers = FollowersOf(nodes, partitionId, rangeLeader);
            Assert.Equal(2, followers.Length);
            await WaitForCondition(() => followers.All(n => n.IsPartitionQuiesced(partitionId)), ct, timeoutMs: 10_000);

            RaftPartitionLifecycleResult removed = await coordinator.RemoveReplicaAsync(
                partitionId, rangeLeader.LocalEndpoint, NodeIdOf(rangeLeader), ct);
            Assert.True(removed.Success, $"RemoveReplica of the range leader failed: {removed.Status}");

            (string Leader, string Target, RaftOperationStatus Status) handOff;
            lock (handOffs)
                handOff = Assert.Single(handOffs);

            Assert.Equal(rangeLeader.LocalEndpoint, handOff.Leader);
            Assert.NotEqual(rangeLeader.LocalEndpoint, handOff.Target);
            Assert.Equal(RaftOperationStatus.Success, handOff.Status);

            await WaitForCondition(() => !rangeLeader.HostsPartition(partitionId), ct, timeoutMs: 20_000);
            await WaitForCondition(
                () => nodes.All(n => n.GetPartitionReplicas(partitionId).Count == 2 &&
                                    n.GetPartitionReplicas(partitionId).All(r => r.Endpoint != rangeLeader.LocalEndpoint)),
                ct, timeoutMs: 20_000);

            RaftManager newLeader = await LeaderOf(followers, partitionId, ct);
            Assert.NotSame(rangeLeader, newLeader);

            RaftReplicationResult after = await newLeader.ReplicateLogs(
                partitionId, "leader-removal", Encoding.UTF8.GetBytes("after-hand-off"), cancellationToken: ct);
            Assert.True(after.Success, $"Write after the leader's removal failed: {after.Status}");
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── The safety net: a vacated leadership is detected without SWIM ────────

    /// <summary>
    /// The pre-fix removal, reproduced through the hook: the hand-off answers Success and moves
    /// nothing, so the leader is dropped while leading and its node stays alive. The quiesced
    /// survivors have no heartbeat timer and SWIM says the old leader's node is up; before the
    /// fix they stayed calm forever and denied each other's pre-votes. They must now treat the
    /// departed leader as vacated, elect one of themselves within the election bound, and serve
    /// writes again.
    /// </summary>
    [Fact]
    public async Task RemovedLeaderWithoutHandOff_QuiescedFollowersElectANewLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssemblePlacedCluster(basePort: 8390, ct);

        try
        {
            int partitionId = nodes[0].GetPartitionMap().Min(r => r.PartitionId);
            Assert.Equal(3, HostsOf(nodes, partitionId).Length);

            RaftManager rangeLeader = await LeaderOf(nodes, partitionId, ct);
            RaftManager[] survivors = FollowersOf(nodes, partitionId, rangeLeader);
            Assert.Equal(2, survivors.Length);

            foreach (RaftManager n in nodes)
                n.SystemCoordinator.HandOffPartitionLeadershipOverride = (_, _, _, _) => Task.FromResult(RaftOperationStatus.Success);

            await WaitForCondition(() => survivors.All(n => n.IsPartitionQuiesced(partitionId)), ct, timeoutMs: 10_000);

            RaftManager coordinator = await P0Leader(nodes, ct);
            RaftPartitionLifecycleResult removed = await coordinator.RemoveReplicaAsync(
                partitionId, rangeLeader.LocalEndpoint, NodeIdOf(rangeLeader), ct);
            Assert.True(removed.Success, $"RemoveReplica of the range leader failed: {removed.Status}");
            ValueStopwatch sinceRemoval = ValueStopwatch.StartNew();

            await WaitForCondition(() => !rangeLeader.HostsPartition(partitionId), ct, timeoutMs: 20_000);

            // The vacancy is noticed on the next tick, but a follower's campaign is held back
            // while its last leader contact is younger than its election timeout — and the
            // quiesce marker that put it in this state is that contact, re-sent on every
            // check-quorum probe. So: up to one election timeout of back-off, then the pre-vote
            // and the election, measured from the removal's commit; the bound leaves one more
            // election timeout for scheduler noise. Both controls (vacancy gates off; map wake
            // off, which waits for the 5 s sweep) still fail this bound.
            int bound = 3 * EndElectionTimeoutMs;
            RaftManager? newLeader = await TryLeaderOf(survivors, partitionId, bound, ct);
            long elapsedMs = sinceRemoval.GetElapsedMilliseconds();
            Assert.True(newLeader is not null,
                $"the survivors elected no leader for partition {partitionId} within {bound} ms of the leader replica's removal");
            Assert.True(elapsedMs <= TestTimeouts.Scale(bound),
                $"the survivors took {elapsedMs} ms to elect a leader after the leader replica left (bound {bound} ms)");

            RaftReplicationResult after = await newLeader!.ReplicateLogs(
                partitionId, "leader-removal", Encoding.UTF8.GetBytes("after-vacancy"), cancellationToken: ct);
            Assert.True(after.Success, $"Write after the vacated leadership failed: {after.Status}");
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }
}
