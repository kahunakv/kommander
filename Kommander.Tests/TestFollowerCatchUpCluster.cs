
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// End-to-end coverage for follower catch-up confirmation
/// (<see cref="IRaft.ConfirmLocalApplicationAsync"/>) on a real two-node in-memory cluster: the
/// non-leader fetches a quorum-confirmed read index from the leader over the transport
/// (<c>GetReadIndex</c>) and waits its own applied frontier up to it, so a <c>true</c> result
/// proves the local applied state covers everything committed before the call began — the gate a
/// consumer needs before acting destructively on locally-applied replicated state (e.g. pruning
/// disk from a replicated hold registry).
///
/// <list type="bullet">
///   <item>A caught-up follower confirms after replicated writes; the leader's call degenerates
///         to its leadership confirmation and succeeds too.</item>
///   <item>A transport-partitioned follower can never confirm — the read-index fetch fails
///         (or no leader is known once it starts campaigning), and the primitive fails closed.
///         Healing the partition restores confirmability.</item>
/// </list>
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public class TestFollowerCatchUpCluster
{
    private readonly ILogger<IRaft> logger;

    private const int UserPartition = 1;

    public TestFollowerCatchUpCluster()
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

    private IRaft NewNode(InMemoryCommunication communication, string name, int id, int port, string peer) =>
        new RaftManager(
            NodeConfig(name, id, port),
            new StaticDiscovery([new(peer)]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);

    private async Task<(IRaft node1, IRaft node2)> AssembleTwoNodeCluster(InMemoryCommunication communication)
    {
        IRaft node1 = NewNode(communication, "node1", 1, 8001, "localhost:8002");
        IRaft node2 = NewNode(communication, "node2", 2, 8002, "localhost:8001");

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 }
        });

        await node1.UpdateNodes();
        await node2.UpdateNodes();

        await Task.WhenAll(node1.JoinCluster(TestContext.Current.CancellationToken), node2.JoinCluster(TestContext.Current.CancellationToken));

        return (node1, node2);
    }

    private static async Task<IRaft> GetLeaderAsync(int partitionId, IRaft[] nodes)
    {
        for (int attempt = 0; attempt < 200; attempt++)
        {
            foreach (IRaft node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(10).ConfigureAwait(false);
        }

        throw new InvalidOperationException($"No leader elected for partition {partitionId}");
    }

    private static IRaft Follower(IRaft leader, IRaft node1, IRaft node2) =>
        leader.GetLocalEndpoint() == node1.GetLocalEndpoint() ? node2 : node1;

    /// <summary>Retries the primitive until it confirms — the contract puts retry cadence on the
    /// caller (no retries inside), so tests model a well-behaved consumer's retry loop.</summary>
    private static async Task<bool> ConfirmWithRetries(IRaft node, int partitionId, int attempts = 100)
    {
        for (int i = 0; i < attempts; i++)
        {
            if (await node.ConfirmLocalApplicationAsync(partitionId, TestContext.Current.CancellationToken).ConfigureAwait(false))
                return true;

            await Task.Delay(25).ConfigureAwait(false);
        }

        return false;
    }

    [Fact]
    public async Task CaughtUpFollower_Confirms_AfterReplicatedWrites()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);
        IRaft follower = Follower(leader, node1, node2);

        // Commit a few entries so the confirmed read index is a real frontier, not an empty log.
        RaftReplicationResult result = await leader.ReplicateLogs(
            UserPartition, "catchup", [new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }],
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);

        // The follower's confirmation runs the full remote path: GetReadIndex on the leader
        // (quorum ack round) + local applied-frontier wait. Retry loop because heartbeat/apply
        // delivery is asynchronous — the primitive itself never retries.
        Assert.True(await ConfirmWithRetries(follower, UserPartition));

        // On the leader the call degenerates to the leadership confirmation.
        Assert.True(await ConfirmWithRetries(leader, UserPartition));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task PartitionedFollower_FailsClosed_UntilHealed()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);
        IRaft follower = Follower(leader, node1, node2);

        // Healthy baseline: the follower can confirm.
        Assert.True(await ConfirmWithRetries(follower, UserPartition));

        // Cut the follower off. Its read-index fetch is dropped by the transport (and once it
        // starts campaigning it has no leader to ask): every call must return false — the caller
        // skips its destructive action while it cannot prove catch-up.
        communication.PartitionNode(follower.GetLocalEndpoint());
        try
        {
            for (int i = 0; i < 10; i++)
            {
                Assert.False(await follower.ConfirmLocalApplicationAsync(UserPartition, TestContext.Current.CancellationToken));
                await Task.Delay(25, TestContext.Current.CancellationToken);
            }
        }
        finally
        {
            communication.HealPartition(follower.GetLocalEndpoint());
        }

        // After healing, some node must eventually confirm catch-up again (leadership may have
        // moved while the transport was cut, so re-resolve the leader/follower pair first).
        IRaft healedLeader = await GetLeaderAsync(UserPartition, [node1, node2]);
        IRaft healedFollower = Follower(healedLeader, node1, node2);
        Assert.True(await ConfirmWithRetries(healedFollower, UserPartition, attempts: 200));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }
}
