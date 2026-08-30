using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Smoke scenarios for the deterministic cluster harness: three real nodes, one partition,
/// simulated time, and the per-step invariant set.
///
/// <para>These run on every pull request, so they are bounded by step count rather than by a
/// wall-clock deadline. A step advances simulated time, ticks the nodes, delivers the wire, and
/// waits on the executor drain barrier.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestSimulationClusterSmoke
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestSimulationClusterSmoke(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// Three nodes elect exactly one leader under simulated time, and every invariant holds at
    /// every settled state along the way.
    /// </summary>
    [Fact]
    public async Task ThreeNodes_ElectOneLeader_UnderSimulatedTime()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 20260829 },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return await HasSingleLeaderAsync(cluster, cancellationToken);
            },
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No single leader was elected within the step budget.");
        Assert.True(invariants.ChecksRun > 0);

        // The consensus path really consulted the simulated clock rather than the process clock.
        Assert.True(cluster.Clock.ReadCount > 0);
    }

    /// <summary>
    /// The election timeout is a function of simulated time only.
    ///
    /// <para>This is the behavioral proof that the tick-source seam is closed. The cluster is
    /// built, then the harness ticks it many times without advancing the clock. Real elapsed time
    /// passes during those ticks — far more than the 100 ms election timeout. If any gate on the
    /// campaign path still read the process clock, a node would time out and campaign. The
    /// assertions are that the term stays where it started and that leadership does not move.</para>
    ///
    /// <para>Then the clock advances past the timeout and the same ticks do elect a leader, which
    /// shows the cluster is not simply wedged.</para>
    /// </summary>
    [Fact]
    public async Task FrozenSimulatedTime_SuppressesElection_ThenAdvancingTimeAllowsIt()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 7,
                StartElectionTimeoutMs = 100,
                EndElectionTimeoutMs = 250,
            },
            logger,
            cancellationToken);

        long frozenAt = cluster.Clock.LogicalMilliseconds;
        long termBefore = await MaxTermAsync(cluster, cancellationToken);
        int leadersBefore = await LeaderCountAsync(cluster, cancellationToken);

        for (int step = 0; step < 40; step++)
            await cluster.StepAsync(advanceMilliseconds: 0, cancellationToken);

        Assert.Equal(frozenAt, cluster.Clock.LogicalMilliseconds);

        // No campaign started and no leadership changed hands, although far more than the
        // 100 ms election timeout of real time passed during those forty steps.
        Assert.Equal(termBefore, await MaxTermAsync(cluster, cancellationToken));
        Assert.Equal(leadersBefore, await LeaderCountAsync(cluster, cancellationToken));

        bool elected = await cluster.RunUntilAsync(
            () => HasSingleLeaderAsync(cluster, cancellationToken),
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "Advancing simulated time did not produce a leader.");
    }

    /// <summary>
    /// A client proposal commits on the elected leader, and the followers converge to the same
    /// committed frontier once the held append responses are released.
    /// </summary>
    [Fact]
    public async Task ClientProposal_CommitsAndFollowersConverge()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 42 },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        bool elected = await cluster.RunUntilAsync(
            () => HasSingleLeaderAsync(cluster, cancellationToken),
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No leader was elected within the step budget.");

        SimulationNode leader = await GetLeaderAsync(cluster, cancellationToken);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId,
            "Greeting",
            "Hello World"u8.ToArray(),
            cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);
        Assert.Equal(1, result.LogIndex);

        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return cluster.Nodes.All(node => node.Wal.GetMaxLog(PartitionId) >= 1);
            },
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(converged, "Followers did not converge on the committed entry.");

        // The run-level check. Every fault is over and the frontiers have had time to meet, so the
        // three nodes must now hold the same committed prefix, entry for entry.
        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// Held append responses delay convergence without breaking any invariant, and releasing them
    /// lets the cluster converge. This exercises the hold queue that the extension scenario
    /// families build on.
    /// </summary>
    [Fact]
    public async Task HeldMessages_DelayConvergence_AndReleasingThemRestoresIt()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 99 },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        bool elected = await cluster.RunUntilAsync(
            () => HasSingleLeaderAsync(cluster, cancellationToken),
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No leader was elected within the step budget.");

        // Hold the wire, then run steps that do not deliver. Nothing may break while the network
        // is silent; the cluster is allowed to make no progress.
        cluster.Transport.HoldMessages = true;

        for (int step = 0; step < 10; step++)
        {
            await cluster.StepAsync(advanceMilliseconds: 20, cancellationToken, deliverMessages: false);
            await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
        }

        Assert.True(cluster.Transport.PendingCount > 0, "No message was held while the wire was closed.");

        cluster.Transport.HoldMessages = false;
        await cluster.Transport.DeliverAll();

        bool recovered = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return await HasSingleLeaderAsync(cluster, cancellationToken);
            },
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(recovered, "The cluster did not recover a leader after the wire reopened.");

        // The wire is open again and the cluster has settled, so convergence is now owed.
        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A one-way partition must not produce two leaders.
    ///
    /// <para>The leader keeps hearing its followers but cannot reach them. That asymmetry is what
    /// makes the case worth its own test: the leader believes it still leads, while the followers
    /// see silence and campaign. Election safety must hold through it, and the cluster must settle
    /// on exactly one leader once the link is restored.</para>
    /// </summary>
    [Fact]
    public async Task OneWayPartition_DoesNotProduceTwoLeaders()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 314 },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        Assert.True(
            await cluster.RunUntilAsync(
                () => HasSingleLeaderAsync(cluster, cancellationToken),
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "No leader was elected within the step budget.");

        SimulationNode leader = await GetLeaderAsync(cluster, cancellationToken);

        // Cut the leader's outbound links only. It still receives.
        foreach (SimulationNode node in cluster.Nodes)
        {
            if (node.NodeIndex != leader.NodeIndex)
                cluster.Transport.BlockLink(leader.Endpoint, node.Endpoint);
        }

        for (int step = 0; step < 60; step++)
        {
            await cluster.StepAsync(advanceMilliseconds: 50, cancellationToken);
            await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
        }

        Assert.True(cluster.Transport.DroppedCount > 0, "The blocked link dropped nothing.");

        cluster.Transport.ClearLinkFaults();

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return await HasSingleLeaderAsync(cluster, cancellationToken);
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "The cluster did not settle on one leader after the link was restored.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A duplicating link changes nothing.
    ///
    /// <para>Raft claims its RPCs are idempotent. This makes every message arrive three times and
    /// checks that claim against the invariants: a duplicate that were counted twice would show up
    /// as a second vote in one term, or as an entry committed at an index that already held one.</para>
    /// </summary>
    [Fact]
    public async Task DuplicatedMessages_ChangeNothing()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 2718 },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        foreach (SimulationNode from in cluster.Nodes)
        {
            foreach (SimulationNode to in cluster.Nodes)
            {
                if (from.NodeIndex != to.NodeIndex)
                    cluster.Transport.SetLinkDuplication(from.Endpoint, to.Endpoint, copies: 3);
            }
        }

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return await HasSingleLeaderAsync(cluster, cancellationToken);
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "No leader was elected while every message was duplicated.");

        Assert.True(cluster.Transport.DuplicatedCount > 0, "No message was duplicated.");

        SimulationNode leader = await GetLeaderAsync(cluster, cancellationToken);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId,
            "Greeting",
            "Hello World"u8.ToArray(),
            cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);

        // One proposal, one index, however many copies of each message crossed the wire.
        Assert.Equal(1, result.LogIndex);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return cluster.Nodes.All(node => node.Wal.GetMaxLog(PartitionId) >= 1);
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "Followers did not converge while every message was duplicated.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    private static async Task<bool> HasSingleLeaderAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

        return views.Count(view => view.Role == RaftNodeState.Leader) == 1;
    }

    private static async Task<int> LeaderCountAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

        return views.Count(view => view.Role == RaftNodeState.Leader);
    }

    private static async Task<long> MaxTermAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

        return views.Count == 0 ? 0 : views.Max(view => view.Term);
    }

    private static async Task<SimulationNode> GetLeaderAsync(
        SimulationCluster cluster,
        CancellationToken cancellationToken)
    {
        foreach (SimulationNode node in cluster.Nodes)
        {
            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No leader is present.");
    }
}
