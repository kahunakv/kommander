using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.History;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Runs real clusters under fault and checks what the clients were told, not only what the nodes
/// hold.
///
/// <para><b>Why both checks and not one.</b> The per-step invariants and the convergence check read
/// node state; the history check reads promises. Neither subsumes the other. A cluster can converge
/// perfectly on a log that is missing an entry it acknowledged, and every node will agree with every
/// other about the gap. So each scenario here ends with the invariants, the convergence check, and
/// the history check, and a run passes only if all three do.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestClientHistoryScenarios
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestClientHistoryScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// A run with no fault produces a history the log honours. This is the baseline: a checker that
    /// failed here would be wrong about something basic, and every later verdict would be suspect.
    /// </summary>
    [Fact]
    public async Task AHealthyRun_ProducesAHistoryTheLogHonours()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(20260908, cancellationToken);

        ClusterInvariantRunner invariants = new();
        ClientHistory history = new();

        for (int index = 0; index < 5; index++)
        {
            SimulationNode leader = await ElectAsync(cluster, cancellationToken);
            await history.AppendUniqueAsync(cluster, leader, PartitionId, "Greeting", cancellationToken);
        }

        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);
        await VerifyAsync(cluster, invariants, history, cancellationToken);

        Assert.Equal(5, history.AcknowledgedCount);
    }

    /// <summary>
    /// A paused follower changes nothing a client can observe. The majority commits without it, and
    /// the backlog it takes on waking must not disturb what the log already promised.
    /// </summary>
    [Fact]
    public async Task AHistoryStaysValidAcrossAPausedFollower()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(20260909, cancellationToken);

        ClusterInvariantRunner invariants = new();
        ClientHistory history = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode sleeper = cluster.Nodes.First(node => node != leader);

        await history.AppendUniqueAsync(cluster, leader, PartitionId, "Greeting", cancellationToken);

        sleeper.Pause();

        for (int index = 0; index < 3; index++)
        {
            SimulationNode current = await ElectAsync(cluster, cancellationToken);
            await history.AppendUniqueAsync(cluster, current, PartitionId, "Greeting", cancellationToken);
        }

        Assert.True(sleeper.FrozenBacklog > 0, "Nothing was stored for the paused node.");

        sleeper.Resume();

        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);
        await VerifyAsync(cluster, invariants, history, cancellationToken);

        Assert.True(history.AcknowledgedCount >= 1, "No append was acknowledged, so nothing was checked.");
    }

    /// <summary>
    /// A crash and restart in the middle of a client's workload. The restarted node loses its fsync
    /// window, so this is the scenario where a promise is most likely to go unhonoured.
    /// </summary>
    [Fact]
    public async Task AHistoryStaysValidAcrossACrashAndRestart()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(20260910, cancellationToken);

        ClusterInvariantRunner invariants = new();
        ClientHistory history = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        await history.AppendUniqueAsync(cluster, leader, PartitionId, "Greeting", cancellationToken);

        await cluster.CrashNodeAsync(victim, cancellationToken);

        SimulationNode survivor = await ElectAsync(cluster, cancellationToken);
        await history.AppendUniqueAsync(cluster, survivor, PartitionId, "Greeting", cancellationToken);

        await cluster.RestartNodeAsync(victim, cancellationToken);

        SimulationNode after = await ElectAsync(cluster, cancellationToken);
        await history.AppendUniqueAsync(cluster, after, PartitionId, "Greeting", cancellationToken);

        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);
        await VerifyAsync(cluster, invariants, history, cancellationToken);

        Assert.True(history.AcknowledgedCount >= 2, "Too few appends were acknowledged to check anything.");
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private Task<SimulationCluster> StartAsync(ulong seed, CancellationToken cancellationToken) =>
        SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = seed },
            logger,
            cancellationToken);

    /// <summary>
    /// Runs the convergence check and the history check together, against the same converged node.
    ///
    /// <para>Reading the history against one node is sound only because the convergence check ran
    /// first and established that the live nodes hold identical committed prefixes. On its own it
    /// would be one replica's opinion.</para>
    /// </summary>
    private static async Task VerifyAsync(
        SimulationCluster cluster,
        ClusterInvariantRunner invariants,
        ClientHistory history,
        CancellationToken cancellationToken)
    {
        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);

        SimulationNode reader = cluster.Nodes.First(node => node.HasLiveManager);

        ClientHistoryChecker.Check(
            history,
            reader.Wal.ReadLogsRange(PartitionId, 0),
            cluster.StepNumber);
    }

    private static async Task ConvergeAsync(
        SimulationCluster cluster,
        ClusterInvariantRunner invariants,
        long index,
        CancellationToken cancellationToken)
    {
        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    IReadOnlyList<RaftPartitionView> views =
                        await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

                    int live = cluster.Nodes.Count(node => node.HasLiveManager);

                    return views.Count == live && views.All(view => view.CommitIndex >= index);
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The cluster did not commit up to {index} on every live node.");
    }

    private static async Task<SimulationNode> ElectAsync(
        SimulationCluster cluster,
        CancellationToken cancellationToken)
    {
        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                IReadOnlyList<RaftPartitionView> views =
                    await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

                return views.Count(view => view.Role == RaftNodeState.Leader) == 1;
            },
            stepCount: 300,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No single leader was elected within the step budget.");

        foreach (SimulationNode node in cluster.Nodes)
        {
            if (!node.HasLiveManager)
                continue;

            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No running leader is present.");
    }
}
