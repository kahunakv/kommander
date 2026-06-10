
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public class TestTwoNodeCluster
{
    private readonly ILogger<IRaft> logger;

    private const int UserPartition = 1;

    public TestTwoNodeCluster()
    {
        ILoggerFactory loggerFactory1 = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Warning);
        });

        logger = loggerFactory1.CreateLogger<IRaft>();
    }

    private static IRaft GetNode1(InMemoryCommunication communication, ILogger<IRaft> logger)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
        };

        RaftManager node = new(
            config,
            new StaticDiscovery([new("localhost:8002")]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static IRaft GetNode2(InMemoryCommunication communication, ILogger<IRaft> logger)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node2",
            NodeId = 2,
            Host = "localhost",
            Port = 8002,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
        };

        RaftManager node = new(
            config,
            new StaticDiscovery([new("localhost:8001")]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static async Task<(IRaft node1, IRaft node2)> AssembleTwoNodeCluster(
        InMemoryCommunication communication,
        ILogger<IRaft> logger,
        Action<IWAL, IWAL>? seedWal = null)
    {
        IRaft node1 = GetNode1(communication, logger);
        IRaft node2 = GetNode2(communication, logger);

        seedWal?.Invoke(node1.WalAdapter, node2.WalAdapter);

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 }
        });

        await node1.UpdateNodes();
        await node2.UpdateNodes();

        await Task.WhenAll([node1.JoinCluster(TestContext.Current.CancellationToken), node2.JoinCluster(TestContext.Current.CancellationToken)]);

        return (node1, node2);
    }

    [Fact]
    public async Task TestJoinCluster()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication, logger);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task TestJoinClusterAndDecideLeader()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication, logger);

        Assert.True(node1.Joined);
        Assert.True(node2.Joined);

        await WaitForAnyLeader([node1, node2], UserPartition, TestContext.Current.CancellationToken);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task TestJoinClusterSimultAndDecideLeader()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication, logger);

        Assert.True(node1.Joined);
        Assert.True(node2.Joined);

        await WaitForAnyLeader([node1, node2], UserPartition, TestContext.Current.CancellationToken);

        IRaft? leader = await GetLeader(UserPartition, [node1, node2]);
        Assert.NotNull(leader);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task WaitForLeaderStableAsync_ReturnsStableLeaderEndpoint()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication, logger);

        string stableLeader1 = await node1.WaitForLeaderStableAsync(
            UserPartition,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);
        string stableLeader2 = await node2.WaitForLeaderStableAsync(
            UserPartition,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.False(string.IsNullOrEmpty(stableLeader1));
        Assert.Equal(stableLeader1, stableLeader2);
        Assert.True(
            stableLeader1 == node1.GetLocalEndpoint() || stableLeader1 == node2.GetLocalEndpoint(),
            $"Unexpected leader endpoint '{stableLeader1}'.");

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task GetState_PreCanceledToken_ThrowsOperationCanceledException()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication, logger);

        string stableLeader = await node1.WaitForLeaderStableAsync(
            UserPartition,
            TimeSpan.FromMilliseconds(100),
            TestContext.Current.CancellationToken);

        IRaft follower = stableLeader == node1.GetLocalEndpoint() ? node2 : node1;
        RaftPartition partition = ((RaftManager)follower).Partitions[UserPartition];

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await partition.GetState(cts.Token));

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task WaitForLeaderStableAsync_EmptyLeader_CanceledToken_ThrowsPromptly()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication, logger);

        string stableLeader = await node1.WaitForLeaderStableAsync(
            UserPartition,
            TimeSpan.FromMilliseconds(100),
            TestContext.Current.CancellationToken);

        IRaft follower = stableLeader == node1.GetLocalEndpoint() ? node2 : node1;
        RaftPartition partition = ((RaftManager)follower).Partitions[UserPartition];
        partition.Leader = "";

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await follower.WaitForLeaderStableAsync(
                UserPartition,
                TimeSpan.FromMilliseconds(100),
                cts.Token));

        Assert.True(
            stopwatch.GetElapsedMilliseconds() < 250,
            $"Cancellation took too long: {stopwatch.GetElapsedMilliseconds()}ms");

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task ForceLeaderForTestingAsync_Node2_BecomesLeader_AndReplicates()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication, logger);

        string stableLeader = await node1.WaitForLeaderStableAsync(
            UserPartition,
            TimeSpan.FromMilliseconds(100),
            TestContext.Current.CancellationToken);

        if (stableLeader == node1.GetLocalEndpoint())
        {
            RaftOperationStatus stepDownStatus = await node1.StepDownAsync(
                UserPartition,
                TestContext.Current.CancellationToken);

            Assert.Equal(RaftOperationStatus.Success, stepDownStatus);
        }

        RaftOperationStatus forceStatus = await node2.ForceLeaderForTestingAsync(
            UserPartition,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, forceStatus);

        string node2Leader = await node1.WaitForLeaderStableAsync(
            UserPartition,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(node2.GetLocalEndpoint(), node2Leader);

        RaftReplicationResult response = await node2.ReplicateLogs(
            UserPartition,
            "Greeting",
            "Hello World"u8.ToArray(),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(response.Success);
        Assert.Equal(RaftOperationStatus.Success, response.Status);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task ForceLeaderForTestingAsync_StaleNode_ReturnsReplicationFailed()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(
            communication,
            logger,
            (wal1, _) => SeedWal(
                wal1,
                UserPartition,
                [
                    new() { Id = 1, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                    new() { Id = 2, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                ]));

        string stableLeader = await node1.WaitForLeaderStableAsync(
            UserPartition,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(node1.GetLocalEndpoint(), stableLeader);

        RaftOperationStatus forceStatus = await node2.ForceLeaderForTestingAsync(
            UserPartition,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.ReplicationFailed, forceStatus);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task TestJoinClusterSimultAndDecideLeaderWithHighestWal()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(
            communication,
            logger,
            (wal1, _) => SeedWal(
                wal1,
                UserPartition,
                [
                    new() { Id = 1, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                    new() { Id = 2, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                ]));

        Assert.True(node1.Joined);
        Assert.True(node2.Joined);

        await WaitForAnyLeader([node1, node2], UserPartition, TestContext.Current.CancellationToken);

        IRaft? leader = await GetLeader(UserPartition, [node1, node2]);
        Assert.NotNull(leader);

        Assert.Equal(node1.GetLocalEndpoint(), leader.GetLocalEndpoint());

        List<IRaft> followers = await GetFollowers(UserPartition, [node1, node2]);
        Assert.NotEmpty(followers);
        Assert.Single(followers);

        long maxNode1 = node1.WalAdapter.GetMaxLog(UserPartition);
        Assert.Equal(2, maxNode1);

        // Pre-seeded WAL gives node1 election advantage; followers are not required to
        // replicate restored local state until new entries are proposed.
        long maxNode2 = node2.WalAdapter.GetMaxLog(UserPartition);
        Assert.Equal(0, maxNode2);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    [Fact]
    public async Task TestJoinClusterSimultAndDecideLeaderWithHighestTerm()
    {
        InMemoryCommunication communication = new();

        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(
            communication,
            logger,
            (_, wal2) => SeedWal(
                wal2,
                UserPartition,
                [
                    new() { Id = 1, Term = 2, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                    new() { Id = 2, Term = 2, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                ]));

        Assert.True(node1.Joined);
        Assert.True(node2.Joined);

        await WaitForLeaderEndpoint(
            [node1, node2],
            UserPartition,
            node2.GetLocalEndpoint(),
            TestContext.Current.CancellationToken);

        IRaft? leader = await GetLeader(UserPartition, [node1, node2]);
        Assert.NotNull(leader);
        Assert.Equal(node2.GetLocalEndpoint(), leader.GetLocalEndpoint());

        List<IRaft> followers = await GetFollowers(UserPartition, [node1, node2]);
        Assert.NotEmpty(followers);
        Assert.Single(followers);

        Assert.Equal(0, node1.WalAdapter.GetMaxLog(UserPartition));
        Assert.Equal(2, node2.WalAdapter.GetMaxLog(UserPartition));

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
    }

    private static async Task WaitForAnyLeader(IRaft[] nodes, int partitionId, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10_000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return;
            }

            // Actively drive the election loop so CI load cannot starve the background timer.
            foreach (IRaft node in nodes)
            {
                RaftManager manager = (RaftManager)node;
                if (manager.Partitions.TryGetValue(partitionId, out RaftPartition? partition))
                    partition.CheckLeader();
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"No leader elected for partition {partitionId} within 10 seconds.");
    }

    private static async Task WaitForLeaderEndpoint(
        IRaft[] nodes,
        int partitionId,
        string expectedLeaderEndpoint,
        CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 30_000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IRaft? leader = await GetLeader(partitionId, nodes).ConfigureAwait(false);
            if (leader?.GetLocalEndpoint() == expectedLeaderEndpoint)
                return;

            foreach (IRaft node in nodes)
                ((RaftManager)node).SystemPartition?.CheckLeader();

            foreach (IRaft node in nodes)
            {
                RaftManager manager = (RaftManager)node;
                if (manager.Partitions.TryGetValue(partitionId, out RaftPartition? partition))
                    partition.CheckLeader();
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Partition {partitionId} did not elect leader {expectedLeaderEndpoint} within 30 seconds.");
    }

    private static async Task<IRaft?> GetLeader(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                return node;
        }

        return null;
    }

    private static async Task<List<IRaft>> GetFollowers(int partitionId, IRaft[] nodes)
    {
        List<IRaft> followers = [];

        foreach (IRaft node in nodes)
        {
            if (!await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                followers.Add(node);
        }

        return followers;
    }

    private static void SeedWal(IWAL wal, int partitionId, List<RaftLog> logs) =>
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, logs)]));
}
