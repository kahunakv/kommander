
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Task 11 regression: a single-node (0-peer) cluster must finish <see cref="IRaft.JoinCluster"/>
/// and flip <see cref="IRaft.IsInitialized"/>. The node elects itself immediately (the
/// <c>Nodes.Count == 0</c> fast-path), but before the fix <c>ReplicateLogs</c> rejected the
/// coordinator's initial partition-map proposal with <c>Errored</c> when there were no peers, so
/// the user partitions were never started. A 0-peer leader is its own quorum and must commit its
/// own proposals locally.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSingleNodeCluster
{
    private readonly ILogger<IRaft> logger;

    public TestSingleNodeCluster()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task SingleNode_JoinCluster_Initializes(int partitions)
    {
        IRaft node = BuildSingleNode(partitions);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        await node.JoinCluster(cts.Token);

        Assert.True(node.IsInitialized);

        await node.LeaveCluster(true);
    }

    [Fact]
    public async Task SingleNode_Leader_ReplicateLogs_CommitsLocallyWithoutPeers()
    {
        IRaft node = BuildSingleNode(1);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        await node.JoinCluster(cts.Token);
        Assert.True(node.IsInitialized);
        Assert.True(await node.AmILeaderQuick(1));

        long before = node.WalAdapter.GetMaxLog(1);

        RaftReplicationResult result = await node.ReplicateLogs(
            1,
            "Greeting",
            "Hello World"u8.ToArray(),
            cancellationToken: cts.Token);

        Assert.True(result.Success);
        Assert.Equal(RaftOperationStatus.Success, result.Status);

        // The entry committed locally (quorum = self) and is now durable in the WAL.
        Assert.True(node.WalAdapter.GetMaxLog(1) > before);
        Assert.Equal(result.LogIndex, node.WalAdapter.GetMaxLog(1));

        await node.LeaveCluster(true);
    }

    private IRaft BuildSingleNode(int partitions)
    {
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

        return new RaftManager(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(logger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            logger);
    }
}
