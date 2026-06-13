
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Integration test: a 4th node contacts a seed, is added as a Learner,
/// catches up via bounded backfill, and is auto-promoted to Voter.
/// Verifies the full join RPC + JoinCluster(seeds) + promotion-driver path.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestFourNodeJoin
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    private static async Task WaitForConditionAsync(Func<bool> condition, CancellationToken ct, int timeoutMs = 30_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (condition()) return;
            await Task.Delay(50, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    [Fact]
    public async Task FourthNode_JoinClusterWithSeeds_BecomesVoter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();

        RaftManager node1 = BuildNode("node1", 1, 8201, ["localhost:8202", "localhost:8203"], communication, logger);
        RaftManager node2 = BuildNode("node2", 2, 8202, ["localhost:8201", "localhost:8203"], communication, logger);
        RaftManager node3 = BuildNode("node3", 3, 8203, ["localhost:8201", "localhost:8202"], communication, logger);
        // Node 4 has no initial partitions. Its discovery seeds include nodes 1-3 so that before
        // the committed roster populates its Nodes list it can still route CompleteAppendLogs ACKs
        // back to the P0 leader (required by InMemoryCommunication's IsNode guard).
        RaftManager node4 = BuildNode("node4", 4, 8204, ["localhost:8201", "localhost:8202", "localhost:8203"], communication, logger, initialPartitions: 0);

        // Register all 4 nodes in the in-memory routing table up-front so the P0 leader can
        // deliver AppendLogs to node4 once it is added to the roster as a Learner.
        communication.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8201"] = node1,
            ["localhost:8202"] = node2,
            ["localhost:8203"] = node3,
            ["localhost:8204"] = node4
        });

        try
        {
            // ── Step 1: start 3-node cluster ────────────────────────────────────────
            await Task.WhenAll(
                node1.JoinCluster(ct),
                node2.JoinCluster(ct),
                node3.JoinCluster(ct)
            );

            await WaitForConditionAsync(
                () => node1.IsInitialized && node2.IsInitialized && node3.IsInitialized,
                ct);

            // ── Step 2: commit entries so node4 has log to backfill ──────────────────
            RaftManager leader = await FindLeaderAsync([node1, node2, node3], ct);
            int userPartitionId = leader.Partitions.Keys.FirstOrDefault();
            if (userPartitionId != 0)
            {
                for (int i = 0; i < 10; i++)
                {
                    await leader.ReplicateLogs(userPartitionId, "test", [1, 2, 3], cancellationToken: ct);
                }
            }

            // ── Step 3: node4 joins via seeds ────────────────────────────────────────
            // JoinCluster(seeds) contacts a seed, is admitted as Learner, waits for
            // IsInitialized, then waits for promotion to Voter — all within the 60 s deadline.
            await node4.JoinCluster(["localhost:8201"], ct);

            // ── Step 4: verify node4 is now a committed Voter ─────────────────────────
            Assert.Equal(System.ClusterMemberRole.Voter, node4.LocalRole);

            // All original nodes should also see node4 as Voter in the committed roster.
            await WaitForConditionAsync(
                () => node1.SystemCoordinator.GetMembership().Members
                    .Any(m => m.Endpoint == "localhost:8204" && m.Role == System.ClusterMemberRole.Voter),
                ct);
        }
        finally
        {
            await node4.LeaveCluster(dispose: true);
            await node3.LeaveCluster(dispose: true);
            await node2.LeaveCluster(dispose: true);
            await node1.LeaveCluster(dispose: true);
        }
    }

    private static async Task<RaftManager> FindLeaderAsync(RaftManager[] nodes, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 15_000;
        while (Environment.TickCount64 < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager node in nodes)
            {
                if (await node.AmILeaderQuick(0).ConfigureAwait(false))
                    return node;
            }
            await Task.Delay(100, ct).ConfigureAwait(false);
        }
        return nodes[0];
    }

    private RaftManager BuildNode(
        string name, int id, int port,
        string[] peers,
        InMemoryCommunication communication,
        ILogger<IRaft> logger,
        int initialPartitions = 1)
    {
        RaftConfiguration config = new()
        {
            NodeName = name,
            NodeId = id,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 300,
            BackfillThreshold = 0,
            MaxBackfillEntriesPerRound = 128,
            LearnerPromotionLag = 5,
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(500),
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }
}
