using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// End-to-end contract of the term API on <see cref="IRaft"/>: <see cref="IRaft.GetPartitionTerm"/>,
/// the <c>expectedTerm</c> fence on <see cref="IRaft.ReplicateLogs(int,string,byte[],bool,long,long,CancellationToken)"/>
/// and <see cref="RaftProposalEntry.ExpectedTerm"/>, and <see cref="IRaft.OnLeadershipLost"/>. A
/// consumer reads the term while it decides a write, stamps the write with it, and is refused with a
/// definite <see cref="RaftOperationStatus.TermMismatch"/> — nothing appended — when the leadership it
/// observed no longer holds.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestTermFence
{
    private readonly ILogger<IRaft> logger;

    private const int UserPartition = 1;

    public TestTermFence()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    private static RaftConfiguration NodeConfig(string name, int id, int port) => new()
    {
        NodeName = name,
        NodeId = id,
        Host = "localhost",
        Port = port,
        InitialPartitions = 1,
        HeartbeatInterval = TimeSpan.FromMilliseconds(50),
        RecentHeartbeat = TimeSpan.FromMilliseconds(25),
        VotingTimeout = TimeSpan.FromMilliseconds(250),
        CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
        UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
        TimerInitialDelay = TimeSpan.FromMilliseconds(25),
        StartElectionTimeout = 100,
        EnableQuiescence = false,
        EndElectionTimeout = 250,
    };

    private RaftManager NewNode(InMemoryCommunication communication, string name, int id, int port, string peer) =>
        new(
            NodeConfig(name, id, port),
            new StaticDiscovery([new(peer)]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);

    private async Task<(RaftManager node1, RaftManager node2)> AssembleTwoNodeCluster(InMemoryCommunication communication)
    {
        RaftManager node1 = NewNode(communication, "node1", 1, 8601, "localhost:8602");
        RaftManager node2 = NewNode(communication, "node2", 2, 8602, "localhost:8601");

        communication.SetNodes(new()
        {
            { "localhost:8601", node1 },
            { "localhost:8602", node2 }
        });

        await node1.UpdateNodes();
        await node2.UpdateNodes();

        CancellationToken ct = TestContext.Current.CancellationToken;
        await Task.WhenAll(node1.JoinCluster(ct), node2.JoinCluster(ct));

        return (node1, node2);
    }

    private static async Task<RaftManager> GetLeaderAsync(int partitionId, RaftManager[] nodes)
    {
        for (int attempt = 0; attempt < 400; attempt++)
        {
            foreach (RaftManager node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(10).ConfigureAwait(false);
        }

        throw new InvalidOperationException($"No leader elected for partition {partitionId}");
    }

    [Fact]
    public async Task GetPartitionTerm_ReportsTheServedTerm_AndMinusOneWhenNotHosted()
    {
        InMemoryCommunication communication = new();
        (RaftManager node1, RaftManager node2) = await AssembleTwoNodeCluster(communication);
        using (node1)
        using (node2)
        {
            RaftManager leader = await GetLeaderAsync(UserPartition, [node1, node2]);

            long term = leader.GetPartitionTerm(UserPartition);
            Assert.True(term >= 1, $"leader term {term}");
            Assert.Equal(-1, leader.GetPartitionTerm(4242));
        }
    }

    [Fact]
    public async Task TermFencedProposal_StaleTermIsRefusedPreAccept_CurrentTermIsAdmitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();
        (RaftManager node1, RaftManager node2) = await AssembleTwoNodeCluster(communication);
        using (node1)
        using (node2)
        {
            RaftManager leader = await GetLeaderAsync(UserPartition, [node1, node2]);
            long term = leader.GetPartitionTerm(UserPartition);
            long maxBefore = leader.WalAdapter.GetMaxLog(UserPartition);

            // A stamp from a term this node does not serve in: refused before anything is appended.
            RaftReplicationResult stale = await leader.ReplicateLogs(
                UserPartition, "kv", [1], expectedTerm: term + 1, cancellationToken: ct);
            Assert.False(stale.Success);
            Assert.Equal(RaftOperationStatus.TermMismatch, stale.Status);
            Assert.Equal(-1, stale.LogIndex);
            Assert.Equal(maxBefore, leader.WalAdapter.GetMaxLog(UserPartition));

            // The current term is admitted.
            RaftReplicationResult current = await leader.ReplicateLogs(
                UserPartition, "kv", [2], expectedTerm: term, cancellationToken: ct);
            Assert.True(current.Success, current.Status.ToString());

            // Batches: one stale stamp refuses the whole batch; two different stamps cannot both be
            // current; a matching stamp is admitted.
            RaftBatchReplicationResult staleBatch = await leader.ReplicateEntries(
                UserPartition, [new("kv", [3], ExpectedTerm: term + 1), new("kv", [4])], cancellationToken: ct);
            Assert.False(staleBatch.Success);
            Assert.Equal(RaftOperationStatus.TermMismatch, staleBatch.Status);
            Assert.All(staleBatch.Entries, e =>
            {
                Assert.Equal(RaftOperationStatus.TermMismatch, e.Status);
                Assert.Equal(-1, e.LogIndex);
            });

            RaftBatchReplicationResult mixedBatch = await leader.ReplicateEntries(
                UserPartition, [new("kv", [5], ExpectedTerm: term), new("kv", [6], ExpectedTerm: term + 1)], cancellationToken: ct);
            Assert.Equal(RaftOperationStatus.TermMismatch, mixedBatch.Status);

            RaftBatchReplicationResult okBatch = await leader.ReplicateEntries(
                UserPartition, [new("kv", [7], ExpectedTerm: term), new("kv", [8])], cancellationToken: ct);
            Assert.True(okBatch.Success, okBatch.Status.ToString());
        }
    }

    [Fact]
    public async Task OnLeadershipLost_FiresWithTheLedTerm_WhenTheLeaderStepsDown()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication communication = new();
        (RaftManager node1, RaftManager node2) = await AssembleTwoNodeCluster(communication);
        using (node1)
        using (node2)
        {
            RaftManager leader = await GetLeaderAsync(UserPartition, [node1, node2]);
            long term = leader.GetPartitionTerm(UserPartition);

            TaskCompletionSource<long> lost = new(TaskCreationOptions.RunContinuationsAsynchronously);
            leader.OnLeadershipLost += (partitionId, lostTerm) =>
            {
                if (partitionId == UserPartition)
                    lost.TrySetResult(lostTerm);
                return Task.CompletedTask;
            };

            await leader.StepDownAsync(UserPartition, ct);

            long reported = await lost.Task.WaitAsync(TestTimeouts.Scale(TimeSpan.FromSeconds(5)), ct);
            Assert.Equal(term, reported);

            // A write still stamped with the lost term can never take effect on this node: it is
            // either not the leader, or it leads again in a newer term.
            RaftReplicationResult afterLoss = await leader.ReplicateLogs(
                UserPartition, "kv", [9], expectedTerm: term, cancellationToken: ct);
            Assert.False(afterLoss.Success);
            Assert.True(
                afterLoss.Status is RaftOperationStatus.NodeIsNotLeader or RaftOperationStatus.TermMismatch,
                afterLoss.Status.ToString());
        }
    }
}
