
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
/// End-to-end regression for the Jepsen <c>register/placement</c> wedge: a placement-added
/// Learner on an idle, never-written range whose leader had already <b>quiesced</b> was never
/// heartbeat, so the leader's progress table never gained an entry for it, the placement
/// promotion driver measured "never acked" forever, and the range stayed transitional until
/// every decommission drain timed out.
///
/// Every other placement cluster test runs with <c>EnableQuiescence = false</c>, which is why
/// the suite never caught it — this one runs with quiescence ON (the production default) and
/// deliberately lets the range leaders quiesce before the AddReplica commits.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestPlacementQuiescedLearnerPromotion
{
    private readonly ILogger<IRaft> logger;

    public TestPlacementQuiescedLearnerPromotion(ITestOutputHelper outputHelper)
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
        ILogger<IRaft> logger)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            ReplicationFactor = 2,
            // Rebalancer OFF: nothing plans moves, so the only thing that can resolve the
            // hand-committed Learner is the transition drive (promotion) — the exact path
            // under test. It also keeps the pass from trimming the promoted voter back out.
            EnablePlacementRebalancer = false,
            PlacementPassInterval = TimeSpan.FromMilliseconds(250),
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(200),
            // Quiescence ON with a short idle window so the never-written data ranges quiesce
            // well before the AddReplica below — the Jepsen wedge precondition.
            EnableQuiescence = true,
            QuiesceAfter = TimeSpan.FromMilliseconds(150),
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
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task WaitForCondition(Func<bool> cond, CancellationToken ct, int timeoutMs = 20_000)
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
    public async Task QuiescedIdleRange_AddReplica_LearnerIsPromotedWithinBoundedTime()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{8250 + i}";

        RaftManager[] nodes =
        [
            MakeNode(comm, 8251, 1, [Ep(2), Ep(3)], logger),
            MakeNode(comm, 8252, 2, [Ep(1), Ep(3)], logger),
            MakeNode(comm, 8253, 3, [Ep(1), Ep(2)], logger),
        ];

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

            // Roster seeded and the initial placement committed: 2 ranges × 2 replicas over 3 nodes.
            await WaitForCondition(
                () => nodes.All(n =>
                    n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                    n.GetPartitionMap().Count == 2 &&
                    n.GetPartitionMap().All(r => r.Replicas.Count == 2)),
                ct);

            // Every range must have an elected leader before the add — the wedge requires the
            // leader's peer set to be extended AFTER its election, not discovered during one.
            foreach (RaftPartitionRange range in nodes[0].GetPartitionMap())
            {
                RaftManager host = nodes.First(n =>
                    range.Replicas.Any(r => r.Endpoint == n.LocalEndpoint));
                await host.WaitForLeader(range.PartitionId, ct);
            }

            // No writes ever reach the data ranges; let their leaders sit past QuiesceAfter so
            // they quiesce (150 ms window — 1 s is comfortably beyond it plus scheduler jitter).
            await Task.Delay(1000, ct);

            // Hand-commit AddReplica of the one node the range does not name, on the P0 leader
            // (replica lifecycle mutations are leader-only commits).
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftPartitionRange target = p0Leader.GetPartitionMap()[0];
            RaftManager newcomer = nodes.First(n =>
                target.Replicas.All(r => r.Endpoint != n.LocalEndpoint));

            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            p0Leader.SystemCoordinator.Send(new RaftSystemRequest(
                RaftSystemRequestType.AddReplica, target.PartitionId, newcomer.LocalEndpoint,
                newcomer.Configuration.NodeId, tcs));
            (RaftOperationStatus status, _) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.Equal(RaftOperationStatus.Success, status);

            // The learner must be promoted to Voter within bounded time: the quiesced range
            // leader re-arms heartbeats for the never-contacted peer, its progress table gains
            // the ack, and the placement pass promotes after the stable window. Before the fix
            // this timed out — the leader stayed quiesced forever and the range stayed
            // transitional {2 Voters, 1 Learner}.
            await WaitForCondition(
                () => nodes.All(n =>
                {
                    RaftPartitionRange? r = n.GetPartitionMap()
                        .FirstOrDefault(x => x.PartitionId == target.PartitionId);
                    return r is not null
                        && r.Replicas.Count == 3
                        && r.Replicas.All(x => x.Role == RaftReplicaRole.Voter);
                }),
                ct);
        }
        finally
        {
            foreach (RaftManager n in nodes)
                await n.LeaveCluster(true, CancellationToken.None);
        }
    }
}
