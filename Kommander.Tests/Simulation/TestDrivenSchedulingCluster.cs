using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// A cluster whose nodes own no scheduling threads at all.
///
/// <para><b>What this mode is for.</b> With the threads gone, the executors, the write-ahead logs,
/// and the outbound transports advance only when the harness advances them. That is the last thing
/// standing between the harness and an exactly replayable run: today the thread pool decides when
/// each of those makes progress, and two runs of one seed can diverge on that alone.</para>
///
/// <para><b>The shape that makes it work.</b> The driver starts each node's executor drain and
/// keeps it in flight rather than awaiting it. A drain can be waiting for another node, and a
/// driver that awaited one would be parked inside the very node it must leave in order to service
/// the other. While the drains run, the driver keeps flushing every node's outbound transport and
/// delivering the wire — the two things a parked drain is waiting for.</para>
///
/// <para>These tests are the acceptance for that mode. They are separate from
/// <see cref="TestSimulationClusterSmoke"/>, which still runs on the threaded path, so a failure
/// here says the driven mode is wrong rather than that the cluster is.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestDrivenSchedulingCluster
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestDrivenSchedulingCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// Three nodes join and elect a leader with no scheduling threads of their own.
    ///
    /// <para>The join is the hard part and the reason this test exists. It covers a
    /// system-partition election and a partition-map commit, and the commit is driven from the
    /// system coordinator, which awaits a partition proposal from inside its own loop. That is
    /// exactly the cross-node wait that a sequential driver cannot resolve.</para>
    /// </summary>
    [Fact]
    public async Task ThreeNodes_JoinAndElect_WithNoSchedulingThreads()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            DrivenOptions(seed: 1234),
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

        Assert.True(elected, "No leader was elected with the scheduling threads switched off.");
        Assert.True(invariants.ChecksRun > 0);
        Assert.True(cluster.Clock.ReadCount > 0);

        // A whole run costs tens of driving rounds, not thousands. If this ever climbs, the run is
        // waiting on something rather than driving it.
        Assert.True(cluster.PumpRounds < 2_000, $"The run took {cluster.PumpRounds} driving rounds.");
    }

    /// <summary>
    /// A client proposal commits and every node converges, still with no scheduling threads.
    /// </summary>
    [Fact]
    public async Task ClientProposal_CommitsAndConverges_WithNoSchedulingThreads()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            DrivenOptions(seed: 5678),
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

        // Driven, like every other call in this mode: the proposal waits for a replication round
        // trip, and this thread is the only one that can produce it.
        RaftReplicationResult result = await cluster.DriveAsync(
            () => leader.Manager.ReplicateLogs(
                PartitionId,
                "Greeting",
                "Hello World"u8.ToArray(),
                cancellationToken: cancellationToken),
            cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);
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
            "The cluster did not converge with the scheduling threads switched off.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// The clock still governs, with the threads gone.
    ///
    /// <para>This is the frozen-clock proof again, run on the driven path. It matters twice over
    /// here: it shows the driven mode did not accidentally hand progress back to real time, which
    /// is the failure this whole mode exists to prevent.</para>
    /// </summary>
    [Fact]
    public async Task FrozenSimulatedTime_StillSuppressesElection_WithNoSchedulingThreads()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            DrivenOptions(seed: 4242),
            logger,
            cancellationToken);

        long frozenAt = cluster.Clock.LogicalMilliseconds;
        long termBefore = await MaxTermAsync(cluster, cancellationToken);
        int leadersBefore = await LeaderCountAsync(cluster, cancellationToken);

        for (int step = 0; step < 40; step++)
            await cluster.StepAsync(advanceMilliseconds: 0, cancellationToken);

        Assert.Equal(frozenAt, cluster.Clock.LogicalMilliseconds);
        Assert.Equal(termBefore, await MaxTermAsync(cluster, cancellationToken));
        Assert.Equal(leadersBefore, await LeaderCountAsync(cluster, cancellationToken));

        Assert.True(
            await cluster.RunUntilAsync(
                () => HasSingleLeaderAsync(cluster, cancellationToken),
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "Advancing simulated time did not produce a leader.");
    }

    /// <summary>
    /// Two runs of one seed reach the same state at every step.
    ///
    /// <para><b>This is what the whole driven mode is for.</b> With the nodes' threads gone, the
    /// only remaining sources of order are the seed and the driver, and both are fixed. So the
    /// sequence of states must be a function of the seed alone. The snapshot hash covers each
    /// node's lifecycle, term, known leader, and committed frontier, plus the pending wire — so
    /// two runs that agreed by luck on the leader but differed anywhere else would still fail.</para>
    ///
    /// <para><b>What it took.</b> An earlier attempt agreed for 34 steps and then diverged. Two
    /// things were still deciding order on their own: each node's <c>HybridLogicalClock</c> read
    /// the wall clock, and the system coordinator started its loop on <c>Task.Run</c>. Both now
    /// follow the simulation — the clock reads simulated milliseconds, and the loop starts inline
    /// when the node's scheduling threads are off.</para>
    ///
    /// <para>Do not weaken this test. One asserting that the runs agree "for a while" would pass
    /// forever and measure nothing.</para>
    /// </summary>
    [Theory]
    [InlineData(20260830UL)]
    [InlineData(4242UL)]
    public async Task SameSeed_ReachesTheSameStateAtEveryStep(ulong seed)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        IReadOnlyList<string> first = await RecordStateHashesAsync(seed, cancellationToken);
        IReadOnlyList<string> second = await RecordStateHashesAsync(seed, cancellationToken);

        Assert.Equal(first.Count, second.Count);
        Assert.True(first.Count > 5, "The run was too short to be evidence.");

        for (int step = 0; step < first.Count; step++)
        {
            Assert.True(
                string.Equals(first[step], second[step], StringComparison.Ordinal),
                $"Seed {seed}: two runs diverged at step {step} of {first.Count}. Something outside " +
                "the seed and the driver is deciding order; that is a determinism leak, not a flake.");
        }
    }

    /// <summary>
    /// Two different seeds reach different states. Without this, a harness that ignored the seed
    /// entirely would pass the check above and look correct while exploring one schedule forever.
    /// </summary>
    [Fact]
    public async Task DifferentSeeds_ReachDifferentStates()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        IReadOnlyList<string> first = await RecordStateHashesAsync(seed: 11, cancellationToken);
        IReadOnlyList<string> second = await RecordStateHashesAsync(seed: 2222, cancellationToken);

        Assert.True(
            !first.SequenceEqual(second, StringComparer.Ordinal),
            "Two different seeds produced the same run, so the seed does not steer the search.");
    }

    /// <summary>
    /// Runs a driven cluster for a fixed number of steps and returns the snapshot hash after each.
    /// </summary>
    private async Task<IReadOnlyList<string>> RecordStateHashesAsync(ulong seed, CancellationToken cancellationToken)
    {
        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            DrivenOptions(seed),
            logger,
            cancellationToken);

        List<string> hashes = [];

        // Two seeds of 25 steps rather than more of either. The property is all-or-nothing: a
        // determinism leak shows within a few steps of the first message it touches, so a longer
        // run mostly re-confirms what the early steps already proved, and this category runs on
        // every pull request.
        for (int step = 0; step < 25; step++)
        {
            await cluster.StepAsync(advanceMilliseconds: 50, cancellationToken);

            SimulationSnapshot snapshot =
                await cluster.CaptureSnapshotAsync(PartitionId, cancellationToken);

            hashes.Add(snapshot.ComputeContentHash());
        }

        return hashes;
    }

    private static SimulationClusterOptions DrivenOptions(ulong seed) =>
        new()
        {
            NodeCount = 3,
            PartitionCount = 1,
            Seed = seed,
            DrivenScheduling = true,
        };

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

    /// <summary>
    /// Finds the leader through the cluster's own view read.
    ///
    /// <para>Asking a node directly would hang here. A view read posts a request to that node's
    /// executor, and in driven mode this thread is the only one that runs executors — so it would
    /// be waiting for work only it could do.</para>
    /// </summary>
    private static async Task<SimulationNode> GetLeaderAsync(
        SimulationCluster cluster,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

        foreach (RaftPartitionView view in views)
        {
            if (view.Role != RaftNodeState.Leader)
                continue;

            SimulationNode? leader = cluster.Nodes.FirstOrDefault(
                node => string.Equals(node.Endpoint, view.Endpoint, StringComparison.Ordinal));

            if (leader is not null)
                return leader;
        }

        throw new InvalidOperationException("No leader is present.");
    }
}
