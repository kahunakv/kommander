#if KOMMANDER_THREAD_FREE
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// The host-pumped scheduling mode of the thread-free build (<c>KOMMANDER_THREAD_FREE</c>), on
/// normal .NET. Compiled only in a build with <c>-p:KommanderThreadFree=true</c>; a default test run
/// does not see this class.
///
/// <para><b>What this covers and what it does not.</b> These tests prove that the pump alone makes
/// a node work: construction, join, election, commit, several partitions, and teardown, with no
/// scheduling thread in the node. They cannot prove that no code blocks a thread. Library code
/// resumes with <c>ConfigureAwait(false)</c>, so on normal .NET a blocked continuation moves to
/// another pool thread and the test still passes. The single-thread guarantee is checked by
/// <c>scripts/run-wasm-smoke.sh</c>, which runs a node on the single-threaded browser-wasm runtime,
/// where a blocking wait has no other thread to release it.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestHostPumpedScheduling
{
    private readonly ILogger<IRaft> logger;

    public TestHostPumpedScheduling()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// The thread-free build defaults to the host pump, so a host that sets nothing gets a node
    /// that runs. A regression to the threaded defaults would fail <c>Validate</c> at construction.
    /// </summary>
    [Fact]
    public void Defaults_SelectTheHostPump()
    {
        RaftConfiguration config = new();

        Assert.False(config.EnableInternalSchedulingThreads);
        Assert.True(config.EnableHostPumpedScheduling);
        Assert.True(config.EnableSharedExecutorPool);
    }

    /// <summary>
    /// The thread-free build compiled out every Thread start site, so asking for scheduling threads
    /// must fail at validation rather than on the first use of a worker that does not exist.
    /// </summary>
    [Fact]
    public void Validate_RefusesSchedulingThreads()
    {
        RaftConfiguration config = new() { EnableInternalSchedulingThreads = true };

        RaftException ex = Assert.Throws<RaftException>(config.Validate);
        Assert.Contains("thread-free", ex.Message);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task SingleNode_ElectsAndCommits_WithOnlyTheHostPump(int partitions)
    {
        RaftManager node = BuildSingleNode(partitions);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            await node.JoinCluster(cts.Token);
            Assert.True(node.IsInitialized);

            for (int partitionId = 1; partitionId <= partitions; partitionId++)
            {
                await node.WaitForLeader(partitionId, cts.Token);
                Assert.True(await node.AmILeader(partitionId, cts.Token));

                long before = node.WalAdapter.GetMaxLog(partitionId);

                for (int i = 0; i < 10; i++)
                {
                    RaftReplicationResult result = await node.ReplicateLogs(
                        partitionId,
                        "HostPump",
                        global::System.Text.Encoding.UTF8.GetBytes($"p{partitionId}-e{i}"),
                        cancellationToken: cts.Token);

                    Assert.True(result.Success, $"partition {partitionId} proposal {i}: {result.Status}");
                    Assert.Equal(RaftOperationStatus.Success, result.Status);
                }

                Assert.Equal(before + 10, node.WalAdapter.GetMaxLog(partitionId));
            }
        }
        finally
        {
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// Proposals started together, so several executor drains and write-ahead-log writes are in
    /// flight at once. This is the case the pump's concurrent drains exist for: a pump that awaited
    /// one drain at a time would still pass the sequential test above.
    /// </summary>
    [Fact]
    public async Task SingleNode_ConcurrentProposals_AllCommit()
    {
        const int partitions = 4;
        const int perPartition = 25;

        RaftManager node = BuildSingleNode(partitions);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            await node.JoinCluster(cts.Token);

            for (int partitionId = 1; partitionId <= partitions; partitionId++)
                await node.WaitForLeader(partitionId, cts.Token);

            List<Task<RaftReplicationResult>> proposals = [];

            for (int partitionId = 1; partitionId <= partitions; partitionId++)
            {
                for (int i = 0; i < perPartition; i++)
                {
                    proposals.Add(node.ReplicateLogs(
                        partitionId,
                        "HostPump",
                        global::System.Text.Encoding.UTF8.GetBytes($"p{partitionId}-e{i}"),
                        cancellationToken: cts.Token));
                }
            }

            RaftReplicationResult[] results = await Task.WhenAll(proposals);

            Assert.All(results, result => Assert.True(result.Success, result.Status.ToString()));
        }
        finally
        {
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// With the pump off and nothing else driving the node, nothing runs. This is the control for
    /// the tests above: it shows that the pump, and not a thread left behind somewhere, is what
    /// makes the node elect.
    /// </summary>
    [Fact]
    public async Task SingleNode_WithThePumpOff_DoesNotJoin()
    {
        RaftManager node = BuildSingleNode(1, enableHostPump: false);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2));

        try
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node.JoinCluster(cts.Token));
            Assert.False(node.IsInitialized);
        }
        finally
        {
            node.Dispose();
        }
    }

    private RaftManager BuildSingleNode(int partitions, bool enableHostPump = true)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = partitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
            EnableHostPumpedScheduling = enableHostPump,
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
#endif
