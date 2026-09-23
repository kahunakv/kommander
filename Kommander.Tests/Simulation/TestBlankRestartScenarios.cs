using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// A member restarted with an empty store under the same endpoint, against a real three-node
/// cluster.
///
/// <para><b>Why these exist.</b> DST-19 item 1, and the Kahuna GA flake behind <c>e618064e</c>,
/// fixed in <c>3552ab9</c>. The roster still names the node, so no membership change resets what the
/// leader recorded about its previous life. Before the fix, the leader anchored every hole repair
/// at the stale presence frontier of the old incarnation, the blank node rejected each batch, and
/// the loop ran until SWIM evicted the dead incarnation minutes later. The random search could not
/// reach this: its restart always reused the store the crash left.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestBlankRestartScenarios
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestBlankRestartScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// A follower that comes back with an empty store, and then receives live writes above its empty
    /// log, is caught up by the leader without a snapshot and without any later write.
    ///
    /// <para>No write after the two that open the hole: a further live write can re-supply a short
    /// follower through the ordinary path and would hide a repair loop that never converges on its
    /// own. That is the lesson of the idle-convergence check (DST-16, result 2).</para>
    /// </summary>
    [Fact]
    public async Task AFollowerRestartedWithAnEmptyStore_IsCaughtUpAfterALiveWriteOpensAHole()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 20260923 },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        await ProposeAsync(leader, count: 6, cancellationToken);
        long target = await CommitIndexAsync(leader, cancellationToken);
        await ConvergeAsync(cluster, invariants, target, cancellationToken);

        await cluster.CrashNodeAsync(victim, cancellationToken);
        await cluster.RestartNodeBlankAsync(victim, cancellationToken);

        Assert.Equal(1, victim.SimulatedWal!.Wipes);

        // Two live writes after the restart. The live broadcast is not anchored, so it lands on the
        // blank node above an empty log: a hole below the tail. Repairing that hole is the path
        // the stale record broke — the leader anchored the repair at the old incarnation's presence
        // frontier, above anything the blank node holds. Nothing is written after these two.
        await ProposeAsync(leader, count: 2, cancellationToken);
        target = await CommitIndexAsync(leader, cancellationToken);

        bool caughtUp = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return await CommitIndexAsync(victim, cancellationToken) >= target;
            },
            stepCount: 300,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            caughtUp,
            $"The blank node never caught up: it is at {await CommitIndexAsync(victim, cancellationToken)} " +
            $"against the leader's {target}. Leader belief: " +
            $"{leader.Manager.GetFollowerProgress(PartitionId, victim.Endpoint)}.");

        Assert.Equal(0, cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed));

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static long FirstRetained(SimulationNode node) =>
        node.SimulatedWal?.Snapshot().Partition(PartitionId)?.FirstLogId ?? -1;

    private static async Task<long> CommitIndexAsync(SimulationNode node, CancellationToken cancellationToken)
    {
        RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
        return view?.CommitIndex ?? -1;
    }

    private static async Task ProposeAsync(SimulationNode leader, int count, CancellationToken cancellationToken)
    {
        for (int index = 0; index < count; index++)
        {
            RaftReplicationResult result = await leader.Manager.ReplicateLogs(
                PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }
    }

    /// <summary>
    /// Writes a checkpoint, started and then stepped: a checkpoint needs a quorum, and a plain await
    /// would stop the harness stepping.
    /// </summary>
    private static async Task CheckpointAsync(
        SimulationCluster cluster,
        SimulationNode leader,
        CancellationToken cancellationToken)
    {
        Task<RaftReplicationResult> checkpoint = leader.Manager.ReplicateCheckpoint(PartitionId, cancellationToken);

        await cluster.RunUntilAsync(
            () => Task.FromResult(checkpoint.IsCompleted),
            stepCount: 40,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, (await checkpoint).Status);
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

                    return views.Count == cluster.Nodes.Count(node => node.HasLiveManager)
                           && views.All(view => view.CommitIndex >= index);
                },
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The running nodes did not all commit up to {index}.");
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
            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No running leader is present.");
    }
}
