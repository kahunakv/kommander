using System.Collections.Concurrent;
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
/// Per-partition placement on a real in-process three-node cluster (replication factor 2 over
/// two ranges, so every range lives on exactly two nodes): a node that does not host a range
/// must still be able to write to it by forwarding to the range's replicas, and moving a range's
/// replicas (add, promote, remove, re-add) must evacuate the departed host — stopping its
/// partition and reclaiming its WAL — while the range keeps serving writes throughout.
/// The placement rebalancer stays off so every replica-set change comes from the explicit
/// mutation under test and nothing fights it afterwards.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestReplicaMoveCluster
{
    private readonly ILogger<IRaft> logger;

    public TestReplicaMoveCluster(ITestOutputHelper outputHelper)
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
            ReplicationFactor = 2,
            EnablePlacementRebalancer = false,
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

    private async Task<(RaftManager[] Nodes, InMemoryCommunication Comm)> AssemblePlacedCluster(
        int basePort, CancellationToken ct)
    {
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        {
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)]),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)]),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)]),
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

        // Roster seeded and the initial placement committed: two ranges, two voter replicas each.
        await WaitForCondition(
            () => nodes.All(n =>
                n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                n.GetPartitionMap().Count == 2 &&
                n.GetPartitionMap().All(r => r.Replicas.Count == 2)),
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

    /// <summary>Async-poll variant for conditions that themselves need an await to evaluate.</summary>
    private static async Task WaitForAsyncCondition(Func<Task<bool>> cond, CancellationToken ct, int timeoutMs = 10_000)
    {
        timeoutMs = TestTimeouts.Scale(timeoutMs);
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (await cond()) return;
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

    /// <summary>Waits for (and returns) the current leader of a placed range.</summary>
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

    private static int NodeIdOf(RaftManager node) => int.Parse(node.GetLocalNodeName().AsSpan("node".Length), global::System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Records (endpoint, partition, payload) for every committed entry a node applies.</summary>
    private sealed class DeliveryLog
    {
        private readonly ConcurrentBag<(string Endpoint, int PartitionId, string Payload)> delivered = [];

        public Task<bool> Record(string endpoint, int partitionId, RaftLog? log)
        {
            if (log?.LogData is { } data)
                delivered.Add((endpoint, partitionId, Encoding.UTF8.GetString(data)));
            return Task.FromResult(true);
        }

        public void Attach(RaftManager node)
        {
            string endpoint = node.LocalEndpoint;
            node.OnReplicationReceived += (partitionId, log) => Record(endpoint, partitionId, log);
        }

        public int DistinctEndpoints(int partitionId, string payload)
        {
            HashSet<string> endpoints = new(StringComparer.Ordinal);
            foreach ((string endpoint, int pid, string deliveredPayload) in delivered)
            {
                if (pid == partitionId && deliveredPayload == payload)
                    endpoints.Add(endpoint);
            }
            return endpoints.Count;
        }
    }

    /// <summary>
    /// Picks a range somebody does not host, together with that node: under factor-2 placement
    /// over three nodes (four replica slots) exactly one node hosts both ranges, so the other
    /// two each miss one.
    /// </summary>
    private static (int PartitionId, RaftManager NonHost) FindNonHostedPair(RaftManager[] nodes)
    {
        foreach (RaftPartitionRange range in nodes[0].GetPartitionMap())
        {
            RaftManager? nonHost = nodes.FirstOrDefault(n => !n.HostsPartition(range.PartitionId));
            if (nonHost is not null)
                return (range.PartitionId, nonHost);
        }
        throw new InvalidOperationException("Every node hosts every range; forwarding cannot be exercised.");
    }

    // ── Forwarding from a non-replica ─────────────────────────────────────────

    /// <summary>
    /// A proposal made on a node that does not host the range is forwarded to the range's
    /// replicas: with the cluster healthy it lands on the leader and commits with the payload
    /// delivered on the range's replicas; with the leader cut off from the proposer the
    /// reachable replica answers "not leader" and that verdict — not a hang or a wrong success —
    /// is what the caller sees; with the proposer cut off from every replica the write path
    /// reports the typed not-hosted diagnosis; and once links heal a forwarded write commits
    /// again. The leader's per-follower committed-index view advances with the write, while a
    /// never-seen endpoint and a non-hosting node's view both report "no data".
    /// </summary>
    [Fact]
    public async Task ProposalFromNonReplica_ForwardsToLeader_CommitsAndReportsFailuresHonestly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssemblePlacedCluster(basePort: 8350, ct);

        try
        {
            (int partitionId, RaftManager proposer) = FindNonHostedPair(nodes);
            Assert.False(proposer.HostsPartition(partitionId));

            DeliveryLog delivery = new();
            foreach (RaftManager n in nodes)
                delivery.Attach(n);

            byte[] payload = Encoding.UTF8.GetBytes($"forwarded-{partitionId}");

            // Settle the range's leadership before forwarding: a write issued into a
            // post-assembly leadership hand-off reaches a replica that has just stepped down.
            _ = await LeaderOf(nodes, partitionId, ct);

            // Healthy path: the proposal is forwarded, commits, and the range's replicas apply it.
            RaftReplicationResult forwarded = await proposer.ReplicateLogs(
                partitionId, "fwd-write", payload, cancellationToken: ct);
            Assert.True(forwarded.Success, $"Forwarded write failed: {forwarded.Status}");
            Assert.True(forwarded.LogIndex > 0);

            await WaitForCondition(
                () => delivery.DistinctEndpoints(partitionId, Encoding.UTF8.GetString(payload)) >= 2, ct);

            // The leader's follower-progress view reflects the committed write; unknown peers and
            // nodes that do not host the range have nothing to report.
            RaftManager rangeLeader = await LeaderOf(nodes, partitionId, ct);
            RaftReplica otherVoter = nodes[0].GetPartitionReplicas(partitionId)
                .First(r => r.Role == RaftReplicaRole.Voter && r.Endpoint != rangeLeader.LocalEndpoint);

            await WaitForAsyncCondition(
                async () => await rangeLeader.GetFollowerCommittedIndexAsync(partitionId, otherVoter.Endpoint) >= forwarded.LogIndex, ct);
            Assert.Equal(-1, await rangeLeader.GetFollowerCommittedIndexAsync(partitionId, "localhost:1"));
            Assert.Equal(-1, await proposer.GetFollowerCommittedIndexAsync(partitionId, otherVoter.Endpoint));

            // Leader unreachable from the proposer: the reachable replica answers "not leader"
            // and that rejection is the honest result — the write did not commit.
            comm.BlockLinkBothWays(proposer.LocalEndpoint, rangeLeader.LocalEndpoint);
            RaftReplicationResult refused = await proposer.ReplicateLogs(
                partitionId, "fwd-write", payload, cancellationToken: ct);
            Assert.False(refused.Success);
            Assert.Equal(RaftOperationStatus.NodeIsNotLeader, refused.Status);
            comm.UnblockLinkBothWays(proposer.LocalEndpoint, rangeLeader.LocalEndpoint);

            // No replica reachable at all: forwarding has nowhere to go, so the local diagnosis
            // is the typed not-hosted exception consumers route around.
            comm.PartitionNode(proposer.LocalEndpoint);
            await Assert.ThrowsAsync<PartitionNotHostedException>(() =>
                proposer.ReplicateLogs(partitionId, "fwd-write", payload, cancellationToken: ct));
            comm.HealPartition(proposer.LocalEndpoint);

            // Recovery: with links healed the same forwarded write commits again — nothing in
            // the failed attempts wedged the proposer or the range.
            RaftReplicationResult recovered = await proposer.ReplicateLogs(
                partitionId, "fwd-write", payload, cancellationToken: ct);
            Assert.True(recovered.Success, $"Forwarded write after healing failed: {recovered.Status}");
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }

    // ── Replica move: evict, reclaim, re-host ────────────────────────────────

    /// <summary>
    /// Moving a range's replicas end to end: adding a third node as a learner makes it host the
    /// range and receive the range's data; promoting it brings it into the replica set as a
    /// voter; removing an original voter evacuates that host completely — it stops hosting, its
    /// WAL for the range is reclaimed, and the range keeps committing writes on the remaining
    /// replicas; re-adding the evacuated host re-hosts the range and catches it back up; and a
    /// per-range replication-factor override becomes visible cluster-wide without churning the
    /// map's generation. The untouched sibling range keeps serving throughout.
    /// </summary>
    [Fact]
    public async Task ReplicaMove_EvacuatesHostReclaimsItsWalAndRehostsOnDemand()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager[] nodes, InMemoryCommunication comm) = await AssemblePlacedCluster(basePort: 8360, ct);

        try
        {
            (int partitionId, RaftManager joiner) = FindNonHostedPair(nodes);

            DeliveryLog delivery = new();
            foreach (RaftManager n in nodes)
                delivery.Attach(n);

            byte[] firstPayload = Encoding.UTF8.GetBytes("replica-move-1");
            RaftManager initialLeader = await LeaderOf(nodes, partitionId, ct);
            RaftReplicationResult firstWrite = await initialLeader.ReplicateLogs(
                partitionId, "move-write", firstPayload, cancellationToken: ct);
            Assert.True(firstWrite.Success, $"Seed write failed: {firstWrite.Status}");

            // Add: the joining node hosts the range as a learner and receives its data. Every
            // map mutation is re-driven against the current system-partition leader — leadership
            // can move between mutations and a stale driver is refused outright.
            RaftPartitionLifecycleResult added = await (await P0Leader(nodes, ct)).AddReplicaAsync(
                partitionId, joiner.LocalEndpoint, NodeIdOf(joiner), ct);
            Assert.True(added.Success, $"AddReplica failed: {added.Status}");

            await WaitForCondition(() => joiner.HostsPartition(partitionId), ct, timeoutMs: 20_000);
            await WaitForCondition(
                () => joiner.WalAdapter.GetMaxLog(partitionId) >= firstWrite.LogIndex, ct, timeoutMs: 20_000);

            // Promote: the learner becomes a voter of the range in the committed map.
            RaftPartitionLifecycleResult promoted = await (await P0Leader(nodes, ct)).PromoteReplicaAsync(
                partitionId, joiner.LocalEndpoint, NodeIdOf(joiner), ct);
            Assert.True(promoted.Success, $"PromoteReplica failed: {promoted.Status}");

            await WaitForCondition(
                () => nodes.All(n => n.GetPartitionReplicas(partitionId)
                    .Any(r => r.Endpoint == joiner.LocalEndpoint && r.Role == RaftReplicaRole.Voter)),
                ct, timeoutMs: 20_000);

            // Remove: pick a voter that neither leads the range nor leads the system partition,
            // so neither the range nor the coordinator loses its driver mid-flight.
            RaftManager coordinator = await P0Leader(nodes, ct);
            RaftManager rangeLeader = await LeaderOf(nodes, partitionId, ct);
            RaftReplica victimReplica = nodes[0].GetPartitionReplicas(partitionId).First(r =>
                r.Role == RaftReplicaRole.Voter &&
                r.Endpoint != rangeLeader.LocalEndpoint &&
                r.Endpoint != coordinator.LocalEndpoint);
            RaftManager victim = nodes.First(n => n.LocalEndpoint == victimReplica.Endpoint);

            RaftPartitionLifecycleResult removed = await coordinator.RemoveReplicaAsync(
                partitionId, victim.LocalEndpoint, NodeIdOf(victim), ct);
            Assert.True(removed.Success, $"RemoveReplica failed: {removed.Status}");

            // The evacuated host is out of the range completely and its WAL for the range is gone.
            await WaitForCondition(() => !victim.HostsPartition(partitionId), ct, timeoutMs: 20_000);
            await WaitForCondition(() => victim.WalAdapter.GetMaxLog(partitionId) == 0, ct, timeoutMs: 20_000);

            await WaitForCondition(
                () => nodes.All(n => n.GetPartitionReplicas(partitionId).Count == 2 &&
                                    n.GetPartitionReplicas(partitionId)
                                        .All(r => r.Endpoint != victim.LocalEndpoint)),
                ct, timeoutMs: 20_000);

            // The range still commits on the remaining replicas.
            byte[] secondPayload = Encoding.UTF8.GetBytes("replica-move-2");
            RaftManager survivingLeader = await LeaderOf(nodes, partitionId, ct);
            RaftReplicationResult secondWrite = await survivingLeader.ReplicateLogs(
                partitionId, "move-write", secondPayload, cancellationToken: ct);
            Assert.True(secondWrite.Success, $"Write after evacuation failed: {secondWrite.Status}");

            await WaitForCondition(
                () => delivery.DistinctEndpoints(partitionId, Encoding.UTF8.GetString(secondPayload)) >= 2, ct);

            // Re-host: the evacuated node rejoins the replica set and catches back up.
            RaftPartitionLifecycleResult reAdded = await (await P0Leader(nodes, ct)).AddReplicaAsync(
                partitionId, victim.LocalEndpoint, NodeIdOf(victim), ct);
            Assert.True(reAdded.Success, $"Re-add replica failed: {reAdded.Status}");

            await WaitForCondition(() => victim.HostsPartition(partitionId), ct, timeoutMs: 20_000);
            await WaitForCondition(
                () => victim.WalAdapter.GetMaxLog(partitionId) >= secondWrite.LogIndex, ct, timeoutMs: 20_000);

            byte[] thirdPayload = Encoding.UTF8.GetBytes("replica-move-3");
            RaftManager finalLeader = await LeaderOf(nodes, partitionId, ct);
            RaftReplicationResult thirdWrite = await finalLeader.ReplicateLogs(
                partitionId, "move-write", thirdPayload, cancellationToken: ct);
            Assert.True(thirdWrite.Success, $"Write after re-hosting failed: {thirdWrite.Status}");

            // A per-range replication-factor override commits and is visible everywhere, without
            // bumping the range's generation — placement converges to it later, offline here.
            long generationBefore = nodes[0].GetPartitionMap()
                .Single(r => r.PartitionId == partitionId).Generation;

            RaftPartitionLifecycleResult factorSet = await (await P0Leader(nodes, ct)).SetReplicationFactorAsync(
                partitionId, 1, ct);
            Assert.True(factorSet.Success, $"SetReplicationFactor failed: {factorSet.Status}");

            await WaitForCondition(
                () => nodes.All(n => n.GetEffectiveReplicationFactor(partitionId) == 1),
                ct, timeoutMs: 20_000);
            Assert.Equal(generationBefore,
                nodes[0].GetPartitionMap().Single(r => r.PartitionId == partitionId).Generation);

            // The sibling range was never involved and still serves writes.
            RaftPartitionRange sibling = nodes[0].GetPartitionMap().Single(r => r.PartitionId != partitionId);
            RaftManager siblingLeader = await LeaderOf(nodes, sibling.PartitionId, ct);
            RaftReplicationResult siblingWrite = await siblingLeader.ReplicateLogs(
                sibling.PartitionId, "sibling-write", "sibling"u8.ToArray(), cancellationToken: ct);
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
