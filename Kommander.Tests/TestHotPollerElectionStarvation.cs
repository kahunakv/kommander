
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Adversarial regression coverage for hot-poller election starvation: a caller that polls
/// <c>AmILeader</c>/<c>GetState</c> in a delay-free loop must not prevent the leadership
/// convergence it is waiting on.
///
/// <para>Historically each poll was an <c>Ask(GetNodeState)</c> into the partition executor; a
/// delay-free loop turned that into an unbounded Ask stream whose scheduling churn crowded out
/// election and heartbeat work, so no leader emerged and the caller kept polling — a self
/// sustaining livelock (observed as a permanent 500%+-CPU hang). <c>GetState</c> now reads a
/// volatile role snapshot, so pollers cost the executor nothing.</para>
///
/// <para>Two things fail on the pre-fix code path (verified by pointing <c>GetState</c> back at the
/// executor Ask): the poll itself throws <c>RaftException</c> while the partition restore gate is
/// still closed — client-class Asks are answered with <c>RestoreInProgress</c> during assembly —
/// and, on a constrained runner, the Ask stream delays leader convergence past the bound below.</para>
///
/// <para>The cluster is deliberately configured onto a starved profile — a shared executor pool of
/// two workers driving four partitions across three nodes — because on an idle many-core dev box
/// the pre-fix code often converged anyway; the failure only reproduced on constrained runners.</para>
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestHotPollerElectionStarvation
{
    private const int Partitions = 4;

    /// <summary>Pollers per node — comfortably above the two-worker pool size.</summary>
    private const int HammersPerNode = 6;

    private readonly ILogger<IRaft> logger;

    public TestHotPollerElectionStarvation()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    [Fact]
    public async Task ZeroDelayAmILeaderHammeringDoesNotBlockClusterAssembly()
    {
        InMemoryCommunication communication = new();

        IRaft node1 = GetNode(communication, 1, logger);
        IRaft node2 = GetNode(communication, 2, logger);
        IRaft node3 = GetNode(communication, 3, logger);
        IRaft[] nodes = [node1, node2, node3];

        communication.SetNodes(new()
        {
            { "localhost:8901", node1 },
            { "localhost:8902", node2 },
            { "localhost:8903", node3 }
        });

        await node1.UpdateNodes();
        await node2.UpdateNodes();
        await node3.UpdateNodes();

        using CancellationTokenSource hammerCts = new();
        long[] polls = [0];
        List<Exception> hammerFailures = [];
        List<Thread> hammers = [];

        // Hammer from dedicated threads rather than the thread pool: the poll loop has no await
        // point, so pool tasks would simply pin their workers and starve the whole test host
        // (including xUnit's own continuations) instead of exercising the partition executors.
        foreach (IRaft node in nodes)
        {
            for (int i = 0; i < HammersPerNode; i++)
            {
                Thread hammer = new(() => HammerLoop(node, polls, hammerFailures, hammerCts.Token))
                {
                    IsBackground = true,
                    Name = $"hot-poller-{node.GetLocalEndpoint()}-{i}"
                };
                hammer.Start();
                hammers.Add(hammer);
            }
        }

        try
        {
            // Assembly runs *while* the pollers hammer — that is the scenario under test.
            await Task.WhenAll(
                node1.JoinCluster(TestContext.Current.CancellationToken),
                node2.JoinCluster(TestContext.Current.CancellationToken),
                node3.JoinCluster(TestContext.Current.CancellationToken));

            for (int partitionId = 1; partitionId <= Partitions; partitionId++)
                await WaitForAnyLeader(nodes, partitionId, TestContext.Current.CancellationToken);
        }
        finally
        {
            await hammerCts.CancelAsync();

            foreach (Thread hammer in hammers)
                hammer.Join(TimeSpan.FromSeconds(10));
        }

        lock (hammerFailures)
            Assert.Empty(hammerFailures);

        // Guard against a vacuous pass: the loops must actually have hammered throughout assembly.
        Assert.True(Interlocked.Read(ref polls[0]) > 1_000, $"pollers only completed {Interlocked.Read(ref polls[0])} polls");

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
        await node3.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// The pathological caller: <c>AmILeader</c>-style polling with no delay and no backoff, over
    /// every partition. Errors are collected rather than thrown so a poller failure surfaces as an
    /// assertion instead of an unobserved background exception.
    /// </summary>
    private static void HammerLoop(IRaft node, long[] polls, List<Exception> failures, CancellationToken cancellationToken)
    {
        RaftManager manager = (RaftManager)node;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                for (int partitionId = 1; partitionId <= Partitions; partitionId++)
                {
                    _ = manager.AmILeaderQuick(partitionId).AsTask().GetAwaiter().GetResult();

                    if (manager.Partitions.TryGetValue(partitionId, out RaftPartition? partition))
                        _ = partition.GetState().AsTask().GetAwaiter().GetResult();

                    Interlocked.Increment(ref polls[0]);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        catch (Exception e)
        {
            lock (failures)
                failures.Add(e);
        }
    }

    /// <summary>
    /// Waits for a leader purely by observation. Unlike the shared cluster helpers this never
    /// nudges <c>CheckLeader</c>: driving the election loop from the test would mask exactly the
    /// starvation this test exists to catch.
    /// </summary>
    private static async Task WaitForAnyLeader(IRaft[] nodes, int partitionId, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 20_000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (IRaft node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return;
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"No leader elected for partition {partitionId} within 20 seconds while pollers hammered AmILeader.");
    }

    private static IRaft GetNode(InMemoryCommunication communication, int nodeId, ILogger<IRaft> logger)
    {
        List<RaftNode> peers = [];

        for (int i = 1; i <= 3; i++)
        {
            if (i != nodeId)
                peers.Add(new($"localhost:{8900 + i}"));
        }

        RaftConfiguration config = new()
        {
            NodeName = $"node{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = 8900 + nodeId,
            InitialPartitions = Partitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
            // Emulate a 2-core CI runner: every partition on this node shares two pool workers.
            EnableSharedExecutorPool = true,
            PartitionExecutorPoolSize = 2,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);
    }
}
