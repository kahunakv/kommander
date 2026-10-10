
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
/// End-to-end regression for the Kahuna 2026-10-09 RF 1 placement stall: a range leader
/// restarts, a learner is then added to its range, the learner catches up at once, and the P0
/// placement controller never promotes it. The mechanism: the restarted node's load-report
/// version counter restarted at 1, the controller's report store rejected every post-restart
/// report against the pre-restart entry it still held, that entry aged past the hint TTL, and
/// the controller's learner-lag probe — which took its leader from the hint alone — returned
/// "not caught up" forever, silently.
///
/// Two independent assertions, so each layer of the fix is pinned on its own: the hint on the
/// controller must follow the restarted leader (the store now orders by incarnation), and the
/// learner must be promoted within bounded time (the pass now falls back to a read-index-confirmed
/// leader when the hint fails). A short report TTL makes the pre-fix stall show within seconds.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestPlacementLeaderRestartLearnerPromotion
{
    private readonly ILogger<IRaft> logger;

    public TestPlacementLeaderRestartLearnerPromotion(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b => b
            .AddXUnit(outputHelper)
            .SetMinimumLevel(LogLevel.Warning));
        logger = lf.CreateLogger<IRaft>();
    }

    private static RaftManager MakeNode(
        InMemoryCommunication communication,
        int port, int nodeId,
        IEnumerable<string> peers,
        IWAL wal,
        ILogger<IRaft> logger)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            // RF 1: every range has one voter, which is therefore its leader — the nightly's shape.
            ReplicationFactor = 1,
            // Rebalancer OFF: nothing plans moves, so the only thing that can resolve the
            // hand-committed Learner is the transition drive (promotion) — the path under test.
            EnablePlacementRebalancer = false,
            PlacementPassInterval = TimeSpan.FromMilliseconds(250),
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(200),
            // A short hint TTL so the dead lifetime's report ages out within the test, which is
            // what left the pre-fix controller with no leader hint for the restarted node's ranges.
            LeaderBalancerReportTtl = TimeSpan.FromSeconds(1),
            GossipInterval = TimeSpan.FromMilliseconds(200),
            EnableQuiescence = false,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            PingInterval = TimeSpan.FromMilliseconds(200),
            StartElectionTimeout = 500,
            EndElectionTimeout = 1000,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            wal,
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 20_000, string? what = null)
    {
        timeoutMs = TestTimeouts.Scale(timeoutMs);
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"Condition not satisfied within timeout: {what ?? "(unnamed)"}");
    }

    private static async Task<RaftManager> P0Leader(IReadOnlyList<RaftManager> nodes, CancellationToken ct)
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

    [Fact]
    public async Task RangeLeaderRestarts_ThenLearnerAdded_HintFollowsAndLearnerIsPromoted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();
        const int basePort = 8270;
        string Ep(int i) => $"localhost:{basePort + i}";

        // Every node keeps its WAL across the restart below, so the restarted process restores
        // its P0 log (and with it the committed map) exactly as a real restart does.
        NonDisposingWAL[] wals = [new(new InMemoryWAL(logger)), new(new InMemoryWAL(logger)), new(new InMemoryWAL(logger))];

        RaftManager[] nodes =
        [
            MakeNode(comm, basePort + 1, 1, [Ep(2), Ep(3)], wals[0], logger),
            MakeNode(comm, basePort + 2, 2, [Ep(1), Ep(3)], wals[1], logger),
            MakeNode(comm, basePort + 3, 3, [Ep(1), Ep(2)], wals[2], logger),
        ];
        RaftManager? restarted = null;

        void Register() => comm.SetNodes(new Dictionary<string, IRaft>
        {
            [Ep(1)] = nodes[0],
            [Ep(2)] = nodes[1],
            [Ep(3)] = nodes[2],
        });
        Register();

        try
        {
            foreach (RaftManager n in nodes)
                await n.UpdateNodes();

            await Task.WhenAll(nodes.Select(n => n.JoinCluster(ct)));

            // Roster seeded and the initial placement committed: 2 ranges × 1 replica over 3 nodes.
            await WaitForCondition(
                () => nodes.All(n =>
                    n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                    n.GetPartitionMap().Count == 2 &&
                    n.GetPartitionMap().All(r => r.Replicas.Count == 1)),
                ct, what: "initial placement");

            // Pick a range whose sole voter is NOT the P0 leader: the controller must reason about
            // a range it does not host, through the gossiped hint — the nightly's shape.
            RaftManager p0 = await P0Leader(nodes, ct);
            RaftPartitionRange target = p0.GetPartitionMap()
                .First(r => r.Replicas[0].Endpoint != p0.LocalEndpoint);
            int pid = target.PartitionId;
            int leaderIndex = Array.FindIndex(nodes, n => n.LocalEndpoint == target.Replicas[0].Endpoint);
            RaftManager leader = nodes[leaderIndex];
            string leaderEndpoint = leader.LocalEndpoint;
            RaftManager newcomer = nodes.First(n => n != p0 && n != leader);

            await leader.WaitForLeader(pid, ct);

            // The controller has accepted at least one pre-restart report from the leader: the
            // hint names it.
            await WaitForCondition(() => p0.GetPartitionLeaderHint(pid) == leaderEndpoint, ct, what: "pre-restart hint");

            // Model a long uptime before the kill. The version counter advances once per gossip
            // round, so the nightly's nodes (minutes of uptime) had retained versions in the
            // hundreds, and the restarted process needed as many rounds to overtake it — longer
            // than the run. A few seconds of test uptime would be overtaken within seconds and
            // hide the defect, so the retained entry is bumped to what a long uptime leaves
            // behind: same lifetime, a far higher version.
            NodeLoadReport retained = p0.SystemCoordinator.GetLoadReports().First(r => r.Endpoint == leaderEndpoint);
            p0.SystemCoordinator.Send(new RaftSystemRequest(new NodeLoadReport
            {
                Endpoint = leaderEndpoint,
                Incarnation = retained.Incarnation,
                ReportVersion = retained.ReportVersion + 1_000_000,
                Time = p0.HybridLogicalClock.SendOrLocalEvent(0),
                Zone = retained.Zone,
                Leaderships = retained.Leaderships,
            }));
            await p0.SystemCoordinator.DrainAsync();
            Assert.Equal(leaderEndpoint, p0.GetPartitionLeaderHint(pid));

            // The nightly's timeline: the node was down longer than the hint TTL (killed 11:16:05,
            // TTL expired 11:16:25, restarted 11:16:34), so the controller's hint for its ranges
            // was already empty when the node came back. Wait for the retained entry to age out
            // here so the post-restart hint assertion below can only be satisfied by a report of
            // the NEW lifetime.
            RaftManager controllerBefore = p0;
            await WaitForCondition(() => controllerBefore.GetPartitionLeaderHint(pid) is null, ct, timeoutMs: 10_000, what: "hint aged out before restart");

            // Fast restart of the range leader over the SAME WAL: no eviction, roster unchanged.
            leader.Dispose();
            restarted = MakeNode(comm, basePort + leaderIndex + 1, leaderIndex + 1,
                Enumerable.Range(1, 3).Where(i => i != leaderIndex + 1).Select(Ep), wals[leaderIndex], logger);
            nodes[leaderIndex] = restarted;
            Register();

            await restarted.UpdateNodes();
            RaftManager toJoin = restarted;
            Task joinTask = Task.Run(() => toJoin.JoinCluster(ct), ct);

            // The sole voter re-wins its range.
            await WaitForCondition(() => toJoin.AmILeaderQuick(pid).AsTask().GetAwaiter().GetResult(), ct, what: "re-election");

            // Layer 1 — the store: the dead lifetime's entry has aged past the 1 s TTL by now,
            // and the restarted process reports from version 1. The hint must name the leader
            // again. Before the fix every post-restart report lost the version check and the
            // hint stayed null for good.
            p0 = await P0Leader(nodes, ct);
            RaftManager controller = p0;
            await WaitForCondition(() => controller.GetPartitionLeaderHint(pid) == leaderEndpoint, ct, timeoutMs: 10_000, what: "post-restart hint");

            // Hand-commit AddReplica of the third node on the P0 leader. The membership fence needs
            // a quorum-confirmed leader of the range, which the restarted node now is; retry while
            // the fence settles.
            RaftOperationStatus status = RaftOperationStatus.Errored;
            ValueStopwatch sw = ValueStopwatch.StartNew();
            while (sw.GetElapsedMilliseconds() < TestTimeouts.Scale(15_000))
            {
                ct.ThrowIfCancellationRequested();
                p0 = await P0Leader(nodes, ct);
                TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);
                p0.SystemCoordinator.Send(new RaftSystemRequest(
                    RaftSystemRequestType.AddReplica, pid, newcomer.LocalEndpoint,
                    newcomer.Configuration.NodeId, tcs));
                (status, _) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
                if (status == RaftOperationStatus.Success)
                    break;
                await Task.Delay(250, ct);
            }
            Assert.Equal(RaftOperationStatus.Success, status);

            // Layer 2 — the pass: the learner catches up at once (the range is tiny) and must be
            // promoted within bounded time. Before the fix the controller's lag probe had no
            // leader to ask and returned "not caught up" on every pass, with no log line.
            await WaitForCondition(
                () => nodes.All(n =>
                {
                    RaftPartitionRange? r = n.GetPartitionMap().FirstOrDefault(x => x.PartitionId == pid);
                    return r is not null
                        && r.Replicas.Count == 2
                        && r.Replicas.All(x => x.Role == RaftReplicaRole.Voter);
                }),
                ct, what: "learner promotion");

            try { await joinTask.WaitAsync(TimeSpan.FromSeconds(10), ct); } catch (TimeoutException) { /* convergence verified above */ }
        }
        finally
        {
            foreach (RaftManager n in nodes)
                n.Dispose();
        }
    }

    /// <summary>Survives <see cref="RaftManager.Dispose"/> so a restarted node restores the same log.</summary>
    private sealed class NonDisposingWAL : IWAL
    {
        private readonly InMemoryWAL inner;

        public NonDisposingWAL(InMemoryWAL inner) => this.inner = inner;

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => inner.Write(logs);
        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
            int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
            inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() { /* survives manager restarts */ }
    }
}
