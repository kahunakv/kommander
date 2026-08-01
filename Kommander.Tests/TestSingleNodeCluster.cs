
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Regression: a single-node (0-peer) cluster must finish <see cref="IRaft.JoinCluster"/>
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

        await node.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Regression guard for the "signal cluster initialization" change: <see cref="IRaft.JoinCluster"/>
    /// must return as soon as the node initializes, not round up to the next tick of the old
    /// fixed 1 s poll (<c>while (!IsInitialized) await Task.Delay(1000)</c>). A single 0-peer node
    /// self-elects immediately, so real assembly here is well under 100 ms; the &lt; 500 ms bound is a
    /// deliberately generous floor-removal guard (it leaves margin for slow/loaded CI). Before the
    /// fix this measured ~1000 ms and would fail; after, it completes in tens of ms.
    /// </summary>
    [Fact]
    public async Task SingleNode_JoinCluster_ReturnsWithoutPollFloor()
    {
        IRaft node = BuildSingleNode(1);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        try
        {
            long startTimestamp = Stopwatch.GetTimestamp();
            await node.JoinCluster(cts.Token);
            TimeSpan elapsed = Stopwatch.GetElapsedTime(startTimestamp);

            Assert.True(node.IsInitialized);
            Assert.True(
                elapsed < TimeSpan.FromMilliseconds(500),
                $"JoinCluster took {elapsed.TotalMilliseconds:F0} ms — the fixed 1 s poll floor appears to have returned.");
        }
        finally
        {
            // Tear down unconditionally: if join or the latency assertion fails, the manager's timers,
            // schedulers, and partition executors would otherwise keep running and contaminate later
            // cluster tests in this process. Use a non-cancelled token — cts may already be cancelled.
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    [Fact]
    public async Task SingleNode_Leader_ReplicateLogs_CommitsLocallyWithoutPeers()
    {
        IRaft node = BuildSingleNode(1);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        await node.JoinCluster(cts.Token);
        Assert.True(node.IsInitialized);
        // JoinCluster now returns as soon as the partition map is applied (IsInitialized), which can
        // precede partition 1 electing its own leader. Previously the fixed 1 s poll-floor latency
        // masked this gap; wait for the election explicitly instead of relying on that timing.
        await node.WaitForLeader(1, cts.Token);
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

        await node.LeaveCluster(true, CancellationToken.None);
    }

    [Fact]
    public async Task SingleNode_Leader_ReplicateCheckpoint_CommitsLocallyWithoutPeers()
    {
        IRaft node = BuildSingleNode(1);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        await node.JoinCluster(cts.Token);
        Assert.True(node.IsInitialized);
        // JoinCluster now returns as soon as the partition map is applied (IsInitialized), which can
        // precede partition 1 electing its own leader. Previously the fixed 1 s poll-floor latency
        // masked this gap; wait for the election explicitly instead of relying on that timing.
        await node.WaitForLeader(1, cts.Token);
        Assert.True(await node.AmILeaderQuick(1));

        // Replicate an entry first so the checkpoint marks a non-trivial point in the log.
        RaftReplicationResult entry = await node.ReplicateLogs(
            1,
            "Greeting",
            "Hello World"u8.ToArray(),
            cancellationToken: cts.Token);
        Assert.True(entry.Success);

        // Without the single-node checkpoint path this returned Errored (no quorum), so a 0-peer
        // cluster could never checkpoint and therefore never compact.
        RaftReplicationResult checkpoint = await node.ReplicateCheckpoint(1, cts.Token);

        Assert.True(checkpoint.Success);
        Assert.Equal(RaftOperationStatus.Success, checkpoint.Status);

        // The Success reply is sent at propose-completion; the committed-checkpoint WAL write lands
        // immediately after on the executor. Poll briefly to confirm the checkpoint actually commits
        // locally (quorum = self), not merely that the propose was accepted. (InMemoryWAL does not
        // implement GetLastCheckpoint, so we assert on the persisted committed-checkpoint entry.)
        while (!node.WalAdapter.ReadLogs(1).Any(l => l.Type == RaftLogType.CommittedCheckpoint)
               && !cts.Token.IsCancellationRequested)
            await Task.Delay(25, cts.Token);

        Assert.Contains(node.WalAdapter.ReadLogs(1), l => l.Type == RaftLogType.CommittedCheckpoint);

        await node.LeaveCluster(true, CancellationToken.None);
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
            EnableQuiescence = false,
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
