
using System.Collections.Concurrent;
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
public sealed class TestThreeNodeCluster
{
    private readonly ILogger<IRaft> logger;

    private int totalLeaderReceived;

    private int totalFollowersReceived;

    public TestThreeNodeCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory1 = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(LogLevel.Debug);
        });

        logger = loggerFactory1.CreateLogger<IRaft>();
    }

    [Theory]
    [InlineData("memory", 1)]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndDecideLeaderOnManyPartitions(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory]
    [InlineData("memory", 1, 25)]
    [InlineData("memory", 8, 25)]
    [InlineData("sqlite", 1, 10)]
    [InlineData("rocksdb", 1, 10)]
    public async Task TestJoinClusterAndMultiReplicateLogs(
        string walStorage,
        int partitions,
        int entries
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        leader.OnReplicationReceived += (_, _) =>
        {
            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        ConcurrentBag<long> logsReceived = [];

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, log) =>
            {
                logsReceived.Add(log.Id);

                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        byte[] data = "Hello World"u8.ToArray();

        int expectedId = 1;

        for (int i = 0; i < entries; i++)
        {
            RaftReplicationResult response = await leader.ReplicateLogs(
                1,
                "Greeting",
                data,
                cancellationToken: TestContext.Current.CancellationToken
            );

            Assert.Equal(RaftOperationStatus.Success, response.Status);
            Assert.Equal(expectedId++, response.LogIndex);

            if (expectedId % 50 == 0)
            {
                response = await leader.ReplicateCheckpoint(
                    1,
                    cancellationToken: TestContext.Current.CancellationToken
                );

                Assert.Equal(RaftOperationStatus.Success, response.Status);
                Assert.Equal(expectedId++, response.LogIndex);
            }
        }

        maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(entries + (entries / 50), maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(entries + (entries / 50), maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(entries + (entries / 50), maxId);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory]
    [InlineData("memory", 1)]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndProposeReplicateLogs(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult response = await leader.ReplicateLogs(i, "Greeting", data, false, cancellationToken: TestContext.Current.CancellationToken);

            Assert.True(response.Success);

            Assert.Equal(RaftOperationStatus.Success, response.Status);
            Assert.Equal(1, response.LogIndex);

            Assert.Equal(0, totalFollowersReceived);
            Assert.Equal(0, totalLeaderReceived);
        }

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory]
    [InlineData("memory", 1)]
    [InlineData("memory", 8)]
    public async Task TestJoinClusterAndProposeReplicateLogsRace(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Fact]
    public async Task WaitForLeaderStableAsync_LeaderChange_ResetsStabilityTimer()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft leaderNode = new[] { node1, node2, node3 }
            .Single(node => node.GetLocalEndpoint() == initialLeader);

        RaftOperationStatus status = await leaderNode.StepDownAsync(
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, status);

        string nextLeader = await node2.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.NotEqual(initialLeader, nextLeader);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Fact]
    public async Task TransferLeadershipAsync_TargetBecomesStableLeader_AndProposalSucceeds()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        string targetEndpoint = initialLeader != node2.GetLocalEndpoint()
            ? node2.GetLocalEndpoint()
            : node3.GetLocalEndpoint();

        IRaft leaderNode = GetNodeByEndpoint(nodes, initialLeader);
        IRaft targetNode = GetNodeByEndpoint(nodes, targetEndpoint);

        RaftOperationStatus transferStatus = await leaderNode.TransferLeadershipAsync(
            1,
            targetEndpoint,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, transferStatus);

        string stableLeader = await targetNode.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(targetEndpoint, stableLeader);

        RaftReplicationResult response = await targetNode.ReplicateLogs(
            1,
            "Greeting",
            "Hello World"u8.ToArray(),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(response.Success);
        Assert.Equal(RaftOperationStatus.Success, response.Status);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Fact]
    public async Task TransferLeadershipAsync_StaleTarget_ReturnsReplicationFailed()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(
            "memory",
            1,
            (wal1, _, _) => SeedWal(
                wal1,
                1,
                [
                    new() { Id = 1, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                    new() { Id = 2, Term = 1, LogData = "Hello"u8.ToArray(), Time = HLCTimestamp.Zero, Type = RaftLogType.Committed },
                ]));

        string stableLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(node1.GetLocalEndpoint(), stableLeader);

        RaftOperationStatus transferStatus = await node1.TransferLeadershipAsync(
            1,
            node2.GetLocalEndpoint(),
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.ReplicationFailed, transferStatus);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Fact]
    public async Task SuspendHeartbeatsAsync_FollowersElectNewLeader_ResumeDoesNotCreateSplitBrain()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string initialLeader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft initialLeaderNode = GetNodeByEndpoint(nodes, initialLeader);

        RaftOperationStatus suspendStatus = await initialLeaderNode.SuspendHeartbeatsAsync(
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, suspendStatus);

        string newLeader = await WaitForDifferentStableLeader(
            nodes,
            1,
            initialLeader,
            TestContext.Current.CancellationToken);

        Assert.NotEqual(initialLeader, newLeader);
        Assert.False(await initialLeaderNode.AmILeaderQuick(1));

        RaftOperationStatus resumeStatus = await initialLeaderNode.ResumeHeartbeatsAsync(
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, resumeStatus);

        string node1Leader = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);
        string node2Leader = await node2.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);
        string node3Leader = await node3.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        Assert.Equal(newLeader, node1Leader);
        Assert.Equal(newLeader, node2Leader);
        Assert.Equal(newLeader, node3Leader);
        Assert.Equal(1, await CountLeaders(nodes, 1));

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Fact]
    public async Task GetActiveNodes_LeaderPerspective_ReturnsReachableFollowers()
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string leaderEndpoint = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft leader = GetNodeByEndpoint(nodes, leaderEndpoint);
        string[] followerEndpoints = nodes
            .Where(node => node.GetLocalEndpoint() != leaderEndpoint)
            .Select(node => node.GetLocalEndpoint())
            .OrderBy(endpoint => endpoint, StringComparer.Ordinal)
            .ToArray();

        await WaitForConditionAsync(
            () =>
            {
                IReadOnlyList<string> activeNodes = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));
                return activeNodes.Count == followerEndpoints.Length &&
                    followerEndpoints.All(endpoint => activeNodes.Contains(endpoint, StringComparer.Ordinal));
            },
            TestContext.Current.CancellationToken);

        IReadOnlyList<string> active = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));

        Assert.Equal(followerEndpoints, active);

        foreach (string followerEndpoint in followerEndpoints)
            Assert.True(leader.GetLastNodeActivity(followerEndpoint) > HLCTimestamp.Zero);

        Assert.Equal(HLCTimestamp.Zero, leader.GetLastNodeActivity("localhost:9999"));

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Fact]
    public async Task GetActiveNodes_UnreachableFollower_DropsAfterWindow()
    {
        (IRaft node1, IRaft node2, IRaft node3, Dictionary<string, IRaft> network) = await AssembleThreNodeClusterWithNetwork("memory", 1);

        IRaft[] nodes = [node1, node2, node3];
        string leaderEndpoint = await node1.WaitForLeaderStableAsync(
            1,
            TimeSpan.FromMilliseconds(150),
            TestContext.Current.CancellationToken);

        IRaft leader = GetNodeByEndpoint(nodes, leaderEndpoint);
        string isolatedFollower = nodes
            .Select(node => node.GetLocalEndpoint())
            .First(endpoint => endpoint != leaderEndpoint);
        string healthyFollower = nodes
            .Select(node => node.GetLocalEndpoint())
            .First(endpoint => endpoint != leaderEndpoint && endpoint != isolatedFollower);

        await WaitForConditionAsync(
            () =>
            {
                IReadOnlyList<string> activeNodes = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));
                return activeNodes.Contains(isolatedFollower, StringComparer.Ordinal) &&
                    activeNodes.Contains(healthyFollower, StringComparer.Ordinal);
            },
            TestContext.Current.CancellationToken);

        // Cleanly remove the isolated follower from the cluster. Unlike dropping it from the
        // in-memory routing table (which leaves it able to campaign and churn leadership), a
        // graceful leave stops it participating entirely, so the remaining two nodes keep a stable
        // leader. The leader simply stops hearing from the removed node, so it must drop out of
        // GetActiveNodes once the liveness window elapses, while the healthy follower remains.
        IRaft isolatedNode = GetNodeByEndpoint(nodes, isolatedFollower);
        await isolatedNode.LeaveCluster(true);
        network.Remove(isolatedFollower);

        await WaitForConditionAsync(
            () =>
            {
                IReadOnlyList<string> activeNodes = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));
                return !activeNodes.Contains(isolatedFollower, StringComparer.Ordinal) &&
                    activeNodes.Contains(healthyFollower, StringComparer.Ordinal);
            },
            TestContext.Current.CancellationToken,
            timeoutMs: 15_000);

        IReadOnlyList<string> active = leader.GetActiveNodes(TimeSpan.FromMilliseconds(1500));

        Assert.DoesNotContain(isolatedFollower, active);
        Assert.Contains(healthyFollower, active);
        Assert.True(leader.GetLastNodeActivity(isolatedFollower) > HLCTimestamp.Zero);

        await leader.LeaveCluster(true);
        await GetNodeByEndpoint(nodes, healthyFollower).LeaveCluster(true);
    }

    [Theory]
    [Trait("Category", "Stress")]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndProposeReplicateLogsRace2(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory]
    [Trait("Category", "Stress")]
    [InlineData("memory", 8)]
    [InlineData("sqlite", 1)]
    [InlineData("rocksdb", 1)]
    public async Task TestJoinClusterAndProposeReplicateLogsRace3(
        string walStorage,
        int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);

        IRaft? leader = await GetLeader(1, [node1, node2, node3]);
        Assert.NotNull(leader);

        List<IRaft> followers = await GetFollowers(1, [node1, node2, node3]);
        Assert.NotEmpty(followers);
        Assert.Equal(2, followers.Count);

        byte[] data = "Hello World"u8.ToArray();

        leader.OnReplicationReceived += (_, log) =>
        {
            Assert.Equal("Greeting", log.LogType);
            Assert.Equal(data, log.LogData);

            Interlocked.Increment(ref totalLeaderReceived);
            return Task.FromResult(true);
        };

        foreach (IRaft follower in followers)
            follower.OnReplicationReceived += (_, _) =>
            {
                Interlocked.Increment(ref totalFollowersReceived);
                return Task.FromResult(true);
            };

        long maxId = node1.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node2.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        maxId = node3.WalAdapter.GetMaxLog(1);
        Assert.Equal(0, maxId);

        for (int i = 1; i <= partitions; i++)
        {
            leader = await GetLeader(i, [node1, node2, node3]);
            Assert.NotNull(leader);

            RaftReplicationResult[] responses = await Task.WhenAll(
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateLogs(i, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken),
                leader.ReplicateCheckpoint(i, cancellationToken: TestContext.Current.CancellationToken)
            );

            foreach (RaftReplicationResult response in responses)
            {
                if (!response.Success)
                    throw new Exception(response.Status.ToString());

                Assert.True(response.Success);
                Assert.Equal(RaftOperationStatus.Success, response.Status);
            }
        }

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    private async Task<(IRaft, IRaft, IRaft)> AssembleThreNodeCluster(
        string walStorage,
        int partitions,
        Action<IWAL, IWAL, IWAL>? seedWal = null)
    {
        (IRaft node1, IRaft node2, IRaft node3, _) = await AssembleThreNodeClusterWithNetwork(
            walStorage,
            partitions,
            seedWal);

        return (node1, node2, node3);
    }

    private async Task<(IRaft, IRaft, IRaft, Dictionary<string, IRaft>)> AssembleThreNodeClusterWithNetwork(
        string walStorage,
        int partitions,
        Action<IWAL, IWAL, IWAL>? seedWal = null)
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode1(communication, walStorage, partitions, logger);
        IRaft node2 = GetNode2(communication, walStorage, partitions, logger);
        IRaft node3 = GetNode3(communication, walStorage, partitions, logger);

        seedWal?.Invoke(node1.WalAdapter, node2.WalAdapter, node3.WalAdapter);

        Dictionary<string, IRaft> network = new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 },
            { "localhost:8003", node3 }
        };

        communication.SetNodes(network);

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        await Task.WhenAll([node1.JoinCluster(TestContext.Current.CancellationToken), node2.JoinCluster(TestContext.Current.CancellationToken), node3.JoinCluster(TestContext.Current.CancellationToken)]);

        for (int i = 1; i <= partitions; i++)
            await WaitForAnyLeader([node1, node2, node3], i, TestContext.Current.CancellationToken);

        return (node1, node2, node3, network);
    }

    private static async Task WaitForAnyLeader(IRaft[] nodes, int partitionId, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 15_000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return;
            }

            await Task.Delay(25, cancellationToken);
        }

        throw new TimeoutException($"No leader elected for partition {partitionId} within 15 seconds.");
    }

    private static async Task<string> WaitForDifferentStableLeader(
        IRaft[] nodes,
        int partitionId,
        string previousLeader,
        CancellationToken cancellationToken)
    {
        const long totalBudgetMs = 30_000;
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < totalBudgetMs)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                long remaining = totalBudgetMs - stopwatch.GetElapsedMilliseconds();
                if (remaining <= 0)
                    break;

                // Cap each per-node wait so the outer budget is always honoured.
                // WaitForLeaderStableAsync loops indefinitely if no leader is stable yet,
                // so without a bound, one slow node can exhaust the entire timeout.
                using CancellationTokenSource nodeCts =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                nodeCts.CancelAfter(TimeSpan.FromMilliseconds(Math.Min(remaining, 500)));

                try
                {
                    string leader = await node.WaitForLeaderStableAsync(
                        partitionId,
                        TimeSpan.FromMilliseconds(150),
                        nodeCts.Token);

                    if (leader != previousLeader)
                        return leader;
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    // Per-node budget expired; continue to the next node.
                }
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Partition {partitionId} did not change leaders from {previousLeader} within {totalBudgetMs / 1000} seconds.");
    }

    private static IRaft GetNodeByEndpoint(IRaft[] nodes, string endpoint) =>
        nodes.Single(node => node.GetLocalEndpoint() == endpoint);

    private static async Task<int> CountLeaders(IRaft[] nodes, int partitionId)
    {
        int leaders = 0;

        foreach (IRaft node in nodes)
        {
            if (await node.AmILeaderQuick(partitionId))
                leaders++;
        }

        return leaders;
    }

    private static async Task WaitForConditionAsync(
        Func<bool> condition,
        CancellationToken cancellationToken,
        int timeoutMs = 15_000)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < timeoutMs)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (condition())
                return;

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"Condition not satisfied within {timeoutMs}ms.");
    }

    private static IRaft GetNode1(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);

        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50,
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
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static RaftManager GetNode2(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);

        RaftConfiguration config = new()
        {
            NodeName = "node2",
            NodeId = 2,
            Host = "localhost",
            Port = 8002,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50,
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
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static RaftManager GetNode3(InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> logger)
    {
        IWAL wal = GetWAL(walStorage, logger);

        RaftConfiguration config = new()
        {
            NodeName = "node3",
            NodeId = 3,
            Host = "localhost",
            Port = 8003,
            InitialPartitions = partitions,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50,
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
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002")]),
            wal,
            communication,
            new HybridLogicalClock(),
            logger
        );

        return node;
    }

    private static IWAL GetWAL(string walStorage, ILogger<IRaft> logger)
    {
        return walStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            "sqlite" => new SqliteWAL("/tmp", Guid.NewGuid().ToString(), logger),
            "rocksdb" => new RocksDbWAL("/tmp", Guid.NewGuid().ToString(), logger),
            _ => throw new ArgumentException($"Unknown wal: {walStorage}")
        };
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
