
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
/// End-to-end regression for the RF-1 replica-move wedge: a range whose committed replica set is
/// one Voter plus one Learner could never win an election. The election quorum floored the
/// majority at 2, the single-node self-election fast path fired only on an empty peer set, and
/// Learner grants are (correctly) discarded by the tally — so the sole voter looped in pre-vote
/// rounds forever and the range stayed leaderless until the Learner left the map. A leaderless
/// range never replicates to its Learner, so the Learner was never caught up and never promoted,
/// and the replica move never completed.
///
/// This test builds exactly that topology (replication factor 1, a hand-committed Learner) and
/// forces the sole voter to step down: it must re-elect itself promptly (its own vote is the
/// majority of the one-voter committed set) and the Learner must then catch up and be promoted.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSoleVoterElectionWithLearner
{
    private readonly ILogger<IRaft> logger;

    public TestSoleVoterElectionWithLearner(ITestOutputHelper outputHelper)
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
            // Replication factor 1: every range has exactly one Voter, the topology where an
            // election with a transitional peer present used to deadlock.
            ReplicationFactor = 1,
            // Rebalancer OFF: nothing plans moves, so the only thing that can resolve the
            // hand-committed Learner is the transition drive (promotion) — and nothing trims
            // the promoted voter back out before the assertion reads it.
            EnablePlacementRebalancer = false,
            PlacementPassInterval = TimeSpan.FromMilliseconds(250),
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(200),
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
    public async Task SoleVoterRange_StepDownWithLearnerPresent_ReelectsAndPromotes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();
        string Ep(int i) => $"localhost:{8290 + i}";

        RaftManager[] nodes =
        [
            MakeNode(comm, 8291, 1, [Ep(2), Ep(3)], logger),
            MakeNode(comm, 8292, 2, [Ep(1), Ep(3)], logger),
            MakeNode(comm, 8293, 3, [Ep(1), Ep(2)], logger),
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

            // Roster seeded and the initial placement committed: 2 ranges × 1 replica over 3 nodes.
            await WaitForCondition(
                () => nodes.All(n =>
                    n.SystemCoordinator.GetMembership().MembershipVersion > 0 &&
                    n.GetPartitionMap().Count == 2 &&
                    n.GetPartitionMap().All(r => r.Replicas.Count == 1)),
                ct);

            // Every range must have an elected leader before the add: the wedge is an election
            // that starts AFTER the Learner is already in the peer set.
            foreach (RaftPartitionRange range in nodes[0].GetPartitionMap())
            {
                RaftManager host = nodes.First(n =>
                    range.Replicas.Any(r => r.Endpoint == n.LocalEndpoint));
                await host.WaitForLeader(range.PartitionId, ct);
            }

            // Hand-commit AddReplica of one node the range does not name, on the P0 leader
            // (replica lifecycle mutations are leader-only commits).
            RaftManager p0Leader = await P0Leader(nodes, ct);
            RaftPartitionRange target = p0Leader.GetPartitionMap()[0];
            RaftManager soleVoter = nodes.First(n =>
                target.Replicas.Any(r => r.Endpoint == n.LocalEndpoint));
            RaftManager newcomer = nodes.First(n =>
                target.Replicas.All(r => r.Endpoint != n.LocalEndpoint));

            TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            p0Leader.SystemCoordinator.Send(new RaftSystemRequest(
                RaftSystemRequestType.AddReplica, target.PartitionId, newcomer.LocalEndpoint,
                newcomer.Configuration.NodeId, tcs));
            (RaftOperationStatus status, _) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.Equal(RaftOperationStatus.Success, status);

            // The sole voter must observe the committed {1 Voter, 1 Learner} set before it steps
            // down, so its next election runs with the Learner in the peer set.
            await WaitForCondition(
                () => soleVoter.GetPartitionMap()
                    .First(r => r.PartitionId == target.PartitionId).Replicas.Count == 2,
                ct);

            // Force the election-with-Learner-present interleaving deterministically.
            RaftOperationStatus stepDown = await soleVoter.StepDownAsync(target.PartitionId, ct);
            Assert.NotEqual(RaftOperationStatus.NodeIsNotLeader, stepDown);

            // The sole voter must re-elect itself (its own vote is the majority of the one-voter
            // committed set), replicate to the Learner, and the placement pass must promote it.
            // Before the fix the voter looped in pre-vote rounds forever — the Learner's grant
            // was discarded and the quorum floor of 2 was unreachable — so this timed out with
            // the range leaderless and still transitional {1 Voter, 1 Learner}.
            await WaitForCondition(
                () => nodes.All(n =>
                {
                    RaftPartitionRange? r = n.GetPartitionMap()
                        .FirstOrDefault(x => x.PartitionId == target.PartitionId);
                    return r is not null
                        && r.Replicas.Count == 2
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
