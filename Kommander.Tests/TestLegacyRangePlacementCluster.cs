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
/// Conversion of legacy fully replicated ranges on a real in-process cluster. A cluster that
/// bootstraps at or below its replication factor (the common 3-node, RF 3 shape) gets empty
/// replica sets from <c>AssignInitial</c>, and before the placement pass learned to convert them
/// nothing ever did: every node that joined added one more copy of every range. Here the P0
/// leader's pass must give each range an explicit set of the three founding voters, a fourth
/// voter must then add no copy, and a per-range factor turned on later over a factor-0 cluster
/// must converge the range to its target by removing copies — all while writes keep landing.
/// The rebalancer is on in every test, because the conversion is gated on it.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLegacyRangePlacementCluster
{
    private readonly ILogger<IRaft> logger;

    public TestLegacyRangePlacementCluster(ITestOutputHelper outputHelper)
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
        int replicationFactor,
        int initialPartitions)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
            ReplicationFactor = replicationFactor,
            EnablePlacementRebalancer = true,
            // Fast passes: the conversion and the trims that follow are driven by this timer.
            PlacementPassInterval = TimeSpan.FromMilliseconds(250),
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(500),
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

    /// <summary>Waits for (and returns) the current leader of a range among the nodes that host it.</summary>
    private static async Task<RaftManager> LeaderOf(RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                if (n.HostsPartition(partitionId) && await n.AmILeaderQuick(partitionId))
                    return n;
            }
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"No leader elected for partition {partitionId} within 15 s.");
    }

    private static bool IsPlacedOnExactly(RaftPartitionRange range, IReadOnlySet<string> endpoints) =>
        range.Replicas.Count == endpoints.Count &&
        range.Replicas.All(r => r.Role == RaftReplicaRole.Voter && endpoints.Contains(r.Endpoint));

    // ── Bootstrap at RF: convert, then grow without adding copies ────────────

    /// <summary>
    /// Three founders at RF 3 bootstrap with empty replica sets (voters do not exceed the factor).
    /// The pass converts every range to the explicit set of the three founders — the same nodes
    /// that already held it, so no data moves — and reports nothing awaiting placement. A fourth
    /// node then joins and becomes a roster Voter, and that is where the old behavior broke: it
    /// used to host a fourth copy of every range forever. Now every range keeps exactly its three
    /// founders, the joiner hosts none of them, and it still writes to them by forwarding.
    /// </summary>
    [Fact]
    public async Task ThreeNodesAtRf3_LegacyRangesGetTheFoundingVoters_AndAFourthVoterAddsNoCopy()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int basePort = 8460;
        string Ep(int i) => $"localhost:{basePort + i}";
        InMemoryCommunication comm = new();

        RaftManager[] founders =
        {
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)], replicationFactor: 3, initialPartitions: 2),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)], replicationFactor: 3, initialPartitions: 2),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)], replicationFactor: 3, initialPartitions: 2),
        };

        // The joiner's discovery seeds name the founders so it can route acks back to the P0
        // leader before the committed roster populates its node list, and it is registered with
        // the transport up front so the P0 leader can replicate to it once it is admitted (the
        // same shape as TestFourNodeJoin).
        RaftManager joiner = MakeNode(comm, basePort + 4, 4, [Ep(1), Ep(2), Ep(3)], replicationFactor: 3, initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = founders[0],
            [Ep(2)] = founders[1],
            [Ep(3)] = founders[2],
            [Ep(4)] = joiner,
        });

        try
        {
            foreach (RaftManager n in founders)
                await n.UpdateNodes();

            await Task.WhenAll(founders.Select(n => n.JoinCluster(ct)));

            await WaitForCondition(
                () => founders.All(n =>
                    n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                    n.GetPartitionMap().Count == 2),
                ct, timeoutMs: 20_000);

            HashSet<string> founderEndpoints = founders.Select(n => n.LocalEndpoint).ToHashSet(StringComparer.Ordinal);

            // The pass converts: every range names exactly the three founders, all Voter, and
            // nothing waits for placement on any node.
            await WaitForCondition(
                () => founders.All(n =>
                    n.GetPartitionsAwaitingPlacement().Count == 0 &&
                    n.GetPartitionMap().All(r => IsPlacedOnExactly(r, founderEndpoints))),
                ct, timeoutMs: 20_000);

            // Every founder still hosts every range (no data moved) and the ranges still commit.
            foreach (RaftPartitionRange range in founders[0].GetPartitionMap())
            {
                Assert.All(founders, n => Assert.True(n.HostsPartition(range.PartitionId)));

                RaftManager leader = await LeaderOf(founders, range.PartitionId, ct);
                RaftReplicationResult write = await leader.ReplicateLogs(
                    range.PartitionId, "legacy-convert",
                    Encoding.UTF8.GetBytes($"after-convert-{range.PartitionId}"), cancellationToken: ct);
                Assert.True(write.Success, $"Write after conversion failed on {range.PartitionId}: {write.Status}");
            }

            // A fourth node joins through a seed and becomes a roster Voter.
            await joiner.JoinCluster([Ep(1)], ct);

            RaftManager[] all = [.. founders, joiner];
            await WaitForCondition(
                () => all.All(n =>
                    n.SystemCoordinator.GetMembership().Members.Count(m => m.Role == ClusterMemberRole.Voter) == 4),
                ct, timeoutMs: 30_000);

            // ...and adds no copy: several passes later every range still has exactly its three
            // founders, on every node's view, and the joiner hosts none of them.
            await Task.Delay(TestTimeouts.Scale(1_500), ct);

            foreach (RaftManager n in all)
            {
                Assert.Empty(n.GetPartitionsAwaitingPlacement());
                Assert.Equal(2, n.GetPartitionMap().Count);
                Assert.All(n.GetPartitionMap(), r => Assert.True(
                    IsPlacedOnExactly(r, founderEndpoints),
                    $"{n.LocalEndpoint} sees partition {r.PartitionId} on [{string.Join(",", r.Replicas.Select(x => $"{x.Endpoint}:{x.Role}"))}]"));
            }

            Assert.All(founders[0].GetPartitionMap(), r => Assert.False(joiner.HostsPartition(r.PartitionId)));

            // The joiner still reaches a range it does not host: the proposal is forwarded to
            // the range's replicas and commits there.
            int partitionId = founders[0].GetPartitionMap()[0].PartitionId;
            _ = await LeaderOf(founders, partitionId, ct);

            RaftReplicationResult forwarded = await joiner.ReplicateLogs(
                partitionId, "legacy-convert", "from-joiner"u8.ToArray(), cancellationToken: ct);
            Assert.True(forwarded.Success, $"Forwarded write from the joiner failed: {forwarded.Status}");
        }
        finally
        {
            comm.HealAll();
            await joiner.LeaveCluster(true, CancellationToken.None);
            foreach (RaftManager n in founders)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── Factor turned on later over a factor-0 cluster ───────────────────────

    /// <summary>
    /// A factor-0 cluster runs full replication by choice: its ranges have empty replica sets and
    /// nothing is reported as awaiting placement. Turning a factor of 2 on for one range (the
    /// per-range override) must converge that range to two voters — the pass first records the
    /// three voters it already has, then the planner trims one through the two-commit removal —
    /// so the evacuated node stops hosting it and reclaims its WAL, while the range keeps
    /// committing and the evacuated node still reaches it by forwarding. The sibling range, which
    /// got no factor, stays fully replicated.
    /// </summary>
    [Fact]
    public async Task ThreeNodesAtRf0_PerRangeFactorTurnedOnLater_ConvertsThenTrimsToTarget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int basePort = 8470;
        string Ep(int i) => $"localhost:{basePort + i}";
        InMemoryCommunication comm = new();

        RaftManager[] nodes =
        {
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)], replicationFactor: 0, initialPartitions: 2),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)], replicationFactor: 0, initialPartitions: 2),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)], replicationFactor: 0, initialPartitions: 2),
        };

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = nodes[0],
            [Ep(2)] = nodes[1],
            [Ep(3)] = nodes[2],
        });

        try
        {
            foreach (RaftManager n in nodes)
                await n.UpdateNodes();

            await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

            await WaitForCondition(
                () => nodes.All(n =>
                    n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                    n.GetPartitionMap().Count == 2),
                ct, timeoutMs: 20_000);

            // Factor 0 is full replication by choice: empty sets, nothing awaiting placement, and
            // several passes leave them alone.
            await Task.Delay(TestTimeouts.Scale(750), ct);
            foreach (RaftManager n in nodes)
            {
                Assert.Empty(n.GetPartitionsAwaitingPlacement());
                Assert.All(n.GetPartitionMap(), r => Assert.Empty(r.Replicas));
            }

            int partitionId = nodes[0].GetPartitionMap()[0].PartitionId;
            int siblingId = nodes[0].GetPartitionMap()[1].PartitionId;

            RaftManager initialLeader = await LeaderOf(nodes, partitionId, ct);
            RaftReplicationResult seed = await initialLeader.ReplicateLogs(
                partitionId, "rf-later", "seed"u8.ToArray(), cancellationToken: ct);
            Assert.True(seed.Success, $"Seed write failed: {seed.Status}");

            // Turn a factor of 2 on for this range only. The commit kicks a pass: the range gets
            // its three voters recorded, then one is trimmed.
            RaftPartitionLifecycleResult factorSet = await (await P0Leader(nodes, ct)).SetReplicationFactorAsync(partitionId, 2, ct);
            Assert.True(factorSet.Success, $"SetReplicationFactor failed: {factorSet.Status}");

            await WaitForCondition(
                () => nodes.All(n =>
                    n.GetPartitionReplicas(partitionId).Count == 2 &&
                    n.GetPartitionReplicas(partitionId).All(r => r.Role == RaftReplicaRole.Voter)),
                ct, timeoutMs: 30_000);

            RaftManager victim = nodes.Single(n =>
                nodes[0].GetPartitionReplicas(partitionId).All(r => r.Endpoint != n.LocalEndpoint));

            // The evacuated host is out of the range completely and its WAL for the range is gone.
            await WaitForCondition(() => !victim.HostsPartition(partitionId), ct, timeoutMs: 20_000);
            await WaitForCondition(() => victim.WalAdapter.GetMaxLog(partitionId) == 0, ct, timeoutMs: 20_000);

            // Fixed point: more passes change nothing.
            await Task.Delay(TestTimeouts.Scale(1_000), ct);
            foreach (RaftManager n in nodes)
            {
                Assert.Equal(2, n.GetPartitionReplicas(partitionId).Count);
                Assert.Empty(n.GetPartitionsAwaitingPlacement());
            }

            // The range keeps committing on its two replicas, and the evacuated node still
            // reaches it by forwarding.
            RaftManager survivingLeader = await LeaderOf(nodes, partitionId, ct);
            RaftReplicationResult afterTrim = await survivingLeader.ReplicateLogs(
                partitionId, "rf-later", "after-trim"u8.ToArray(), cancellationToken: ct);
            Assert.True(afterTrim.Success, $"Write after the trim failed: {afterTrim.Status}");

            RaftReplicationResult forwarded = await victim.ReplicateLogs(
                partitionId, "rf-later", "forwarded"u8.ToArray(), cancellationToken: ct);
            Assert.True(forwarded.Success, $"Forwarded write from the evacuated node failed: {forwarded.Status}");

            // The sibling range got no factor and stays fully replicated on every node.
            foreach (RaftManager n in nodes)
            {
                Assert.Empty(n.GetPartitionReplicas(siblingId));
                Assert.True(n.HostsPartition(siblingId));
            }

            RaftManager siblingLeader = await LeaderOf(nodes, siblingId, ct);
            RaftReplicationResult siblingWrite = await siblingLeader.ReplicateLogs(
                siblingId, "rf-later", "sibling"u8.ToArray(), cancellationToken: ct);
            Assert.True(siblingWrite.Success, $"Sibling-range write failed: {siblingWrite.Status}");
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }
}
