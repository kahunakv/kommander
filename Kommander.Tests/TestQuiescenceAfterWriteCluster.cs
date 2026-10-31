using System.Text;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Quiescence must re-engage after a write, on the production WAL and the production
/// <see cref="RaftConfiguration.QuiesceAfter"/>. The 2026-10-18 in-process diagnostic (three
/// nodes, two ranges, replication factor 2, quiescence on) saw a range that took ONE write stay
/// non-quiesced on every host for as long as it was watched, while its never-written sibling
/// range kept quiescing. The cause was the leader's quiesce gate: it required the proposal
/// registry to be empty, and a settled proposal stays registered for 30 s so its ticket remains
/// answerable — so every written range stayed awake 30 s after each write, twenty times the
/// default <c>QuiesceAfter</c>, and the idle-cluster savings quiescence exists for applied only
/// to never-written ranges.
/// <para>The test writes once on a range, then requires every host of that range (the leader
/// and each follower) to report quiesced within a few <c>QuiesceAfter</c> windows of the write;
/// then writes again and requires the same, to prove the cycle repeats. Each host is checked
/// for having been SEEN quiesced rather than for all being quiesced at one instant, because the
/// check-quorum probe wakes a quiesced leader for one round on its own cadence.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestQuiescenceAfterWriteCluster
{
    /// <summary>How many <c>QuiesceAfter</c> windows a written range gets to quiesce in. Far above
    /// the one window plus a heartbeat round the fix needs, far below the 30 s the defect took.</summary>
    private const int WindowsAllowed = 4;

    private readonly ILogger<IRaft> logger;

    public TestQuiescenceAfterWriteCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    // ── Cluster assembly ──────────────────────────────────────────────────────

    private static RaftConfiguration MakeConfiguration(int port, int nodeId) => new()
    {
        NodeName = $"node{nodeId}",
        NodeId = nodeId,
        Host = "localhost",
        Port = port,
        InitialPartitions = 2,
        ReplicationFactor = 2,
        // Rebalancer OFF: the replica sets must hold still so a host's quiesced state is only
        // about the write, never about a placement move.
        EnablePlacementRebalancer = false,
        PlacementPassInterval = TimeSpan.FromMilliseconds(250),
        // EnableQuiescence and QuiesceAfter stay at their defaults: the production path is the
        // one under test.
        HeartbeatInterval = TimeSpan.FromMilliseconds(50),
        RecentHeartbeat = TimeSpan.FromMilliseconds(25),
        VotingTimeout = TimeSpan.FromMilliseconds(250),
        CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
        UpdateNodesInterval = TimeSpan.FromSeconds(5),
        TimerInitialDelay = TimeSpan.FromMilliseconds(25),
        PingInterval = TimeSpan.FromMilliseconds(200),
        StartElectionTimeout = 500,
        EndElectionTimeout = 1000,
    };

    private RaftManager MakeNode(InMemoryCommunication communication, string walDirectory, int port, int nodeId, IEnumerable<string> peers)
    {
        RaftConfiguration config = MakeConfiguration(port, nodeId);
        Assert.True(config.EnableQuiescence, "the test exercises the production default: quiescence on");

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            // The production default WAL (Kommander.Server opens RocksDB unless told otherwise).
            new RocksDbWAL(walDirectory, $"node{nodeId}", logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private async Task<(RaftManager[] Nodes, InMemoryCommunication Comm)> AssemblePlacedCluster(
        int basePort, string walDirectory, CancellationToken ct)
    {
        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{basePort + i}";

        RaftManager[] nodes =
        {
            MakeNode(comm, walDirectory, basePort + 1, 1, [Ep(2), Ep(3)]),
            MakeNode(comm, walDirectory, basePort + 2, 2, [Ep(1), Ep(3)]),
            MakeNode(comm, walDirectory, basePort + 3, 3, [Ep(1), Ep(2)]),
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

    private static RaftManager[] HostsOf(RaftManager[] nodes, int partitionId) =>
        nodes.Where(n => n.HostsPartition(partitionId)).ToArray();

    /// <summary>
    /// Polls until every host of the range has reported quiesced at least once, or the bound of
    /// <see cref="WindowsAllowed"/> <c>QuiesceAfter</c> windows runs out. Returns the elapsed
    /// milliseconds; throws naming the hosts that were never seen quiesced.
    /// </summary>
    private static async Task<long> WaitForEveryHostSeenQuiesced(
        RaftManager[] hosts, int partitionId, TimeSpan quiesceAfter, string phase, CancellationToken ct)
    {
        int boundMs = TestTimeouts.Scale((int)(WindowsAllowed * quiesceAfter.TotalMilliseconds));
        HashSet<string> seen = [];
        ValueStopwatch sw = ValueStopwatch.StartNew();

        while (sw.GetElapsedMilliseconds() < boundMs)
        {
            ct.ThrowIfCancellationRequested();

            foreach (RaftManager host in hosts)
            {
                if (host.IsPartitionQuiesced(partitionId))
                    seen.Add(host.LocalEndpoint);
            }

            if (seen.Count == hosts.Length)
                return sw.GetElapsedMilliseconds();

            await Task.Delay(25, ct);
        }

        string missing = string.Join(", ", hosts.Select(h => h.LocalEndpoint).Where(e => !seen.Contains(e)));
        throw new TimeoutException(
            $"{phase}: partition {partitionId} hosts [{missing}] never reported quiesced within " +
            $"{boundMs} ms ({WindowsAllowed} x QuiesceAfter {quiesceAfter.TotalMilliseconds} ms)");
    }

    // ── The test ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task RangeThatTookAWrite_QuiescesAgainOnEveryHost_WithinAFewQuiesceAfterWindows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string walDirectory = Path.Combine(Path.GetTempPath(), $"kommander-quiesce-after-write-{Guid.NewGuid():N}");
        Directory.CreateDirectory(walDirectory);

        (RaftManager[] nodes, InMemoryCommunication comm) = await AssemblePlacedCluster(basePort: 8400, walDirectory, ct);

        try
        {
            TimeSpan quiesceAfter = MakeConfiguration(0, 0).QuiesceAfter;
            int[] ranges = nodes[0].GetPartitionMap().Select(r => r.PartitionId).OrderBy(id => id).ToArray();
            int written = ranges[0];
            int sibling = ranges[1];

            RaftManager[] hosts = HostsOf(nodes, written);
            Assert.Equal(2, hosts.Length);
            RaftManager leader = await LeaderOf(nodes, written, ct);

            // Before any write both ranges quiesce; this was never in doubt and anchors the bound.
            await WaitForEveryHostSeenQuiesced(hosts, written, quiesceAfter, "before the first write", ct);

            for (int cycle = 1; cycle <= 2; cycle++)
            {
                RaftReplicationResult write = await leader.ReplicateLogs(
                    written, "quiesce", Encoding.UTF8.GetBytes($"write-{cycle}"), cancellationToken: ct);
                Assert.True(write.Success, $"write {cycle} on partition {written} failed: {write.Status}");

                // The write woke every host; the leader must not re-quiesce before QuiesceAfter.
                Assert.False(leader.IsPartitionQuiesced(written), $"the leader stayed quiesced through write {cycle}");

                long elapsedMs = await WaitForEveryHostSeenQuiesced(hosts, written, quiesceAfter, $"after write {cycle}", ct);
                Assert.True(elapsedMs >= quiesceAfter.TotalMilliseconds / 2,
                    $"after write {cycle} the hosts reported quiesced {elapsedMs} ms after the write, before the idle window could have elapsed");

                // Still the same leader: a quiesced range re-elected nobody on the way.
                Assert.True(await leader.AmILeaderQuick(written), $"partition {written} changed leader during cycle {cycle}");
            }

            // The never-written sibling range quiesces throughout, as in the diagnostic.
            await WaitForEveryHostSeenQuiesced(HostsOf(nodes, sibling), sibling, quiesceAfter, "sibling range", ct);
        }
        finally
        {
            comm.HealAll();
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
            foreach (RaftManager n in nodes)
                n.Dispose();
            try { Directory.Delete(walDirectory, recursive: true); } catch { /* best-effort temp cleanup */ }
        }
    }
}
