using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Crash, restart, pause and resume against a real three-node cluster.
///
/// <para><b>Why these are separate from the storage faults.</b> A storage fault damages what a node
/// holds. These damage the node itself, and the two produce different failures: a crash loses the
/// fsync window and every in-memory belief at once, and a pause loses nothing at all but delivers
/// a burst of stale traffic when it ends. Most of the defects in this project's history lived in one
/// of those two shapes rather than in the protocol core.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestNodeLifecycleScenarios
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestNodeLifecycleScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    // ── Crash and restart ─────────────────────────────────────────────────

    /// <summary>
    /// A crashed follower rejoins over its own store and catches up on what it missed.
    ///
    /// <para>The entries written while it was gone are the interesting part: they exist only on the
    /// other two, so the rejoining node must be told about them rather than discovering them in its
    /// own log.</para>
    /// </summary>
    [Fact]
    public async Task ACrashedFollower_RejoinsAndCatchesUp()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(20260904, cancellationToken);

        ClusterInvariantRunner invariants = new();
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        await ProposeAsync(cluster, cancellationToken);
        await ConvergeAsync(cluster, invariants, index: 1, cancellationToken);

        await cluster.CrashNodeAsync(victim, cancellationToken);

        Assert.Equal(SimulationNodeLifecycleStatus.Crashed, victim.LifecycleStatus);
        Assert.Equal(1, victim.CrashCount);

        // The surviving two are a majority and must keep committing without it.
        await ProposeAsync(cluster, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    return cluster.Nodes
                        .Where(node => node != victim)
                        .All(node => node.Wal.GetMaxLog(PartitionId) >= 2);
                },
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            "The surviving majority did not commit while one node was down.");

        // The node really is behind before it restarts. Without this the test could pass on a
        // cluster where nothing happened during the outage, and prove nothing about catching up.
        //
        // How far behind is not fixed, and asserting a single number was wrong: the crash may keep
        // the first entry, revert it to its proposed form, or take it outright, depending on
        // whether any fsync had carried it. All three are the durability window working. What must
        // hold is that the entry written during the outage is not there.
        Assert.True(
            victim.Wal.GetMaxLog(PartitionId) < 2,
            $"The crashed node already holds log {victim.Wal.GetMaxLog(PartitionId)}, so it missed nothing.");

        await cluster.RestartNodeAsync(victim, cancellationToken);

        Assert.Equal(SimulationNodeLifecycleStatus.Running, victim.LifecycleStatus);

        // One live write, and it is the contract rather than a convenience. A leader does not push
        // merely-restored committed state at a voter (the LiveCommitFloor confinement), and a gap
        // of one entry is far under BackfillThreshold, so a restarted node that is a short way
        // behind an idle cluster stays behind until something is written. Measured: without this
        // line the node sits at commit index 1 against the cluster's 2 for the full step budget.
        await ProposeAsync(cluster, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    RaftPartitionView? view =
                        await victim.GetPartitionViewAsync(PartitionId, cancellationToken);

                    return view is not null && view.CommitIndex >= 3;
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            "The restarted node never caught up on what it missed.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);

        // The monotonic-commit rule was reset for the crashed node, exactly once. Without the
        // reset this scenario reports a violation that is really the durability model working;
        // without this assertion the reset could stop happening and nothing would notice.
        Assert.Equal(1, invariants.CrashResets);
    }

    /// <summary>
    /// A crash keeps what reached the disk and loses only what was still inside the fsync window.
    ///
    /// <para>The window is opened deliberately with a write latency. Without one the store is
    /// durable the instant it is written, and a crash test would pass while proving nothing about
    /// durability at all.</para>
    /// </summary>
    [Fact]
    public async Task ACrash_LosesOnlyWhatWasNotYetDurable()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260905,

                // Wide enough that a write is still in flight when the crash lands, and short
                // enough that the run reaches it.
                WalWriteLatencyMilliseconds = 5_000,
            },
            logger,
            cancellationToken);

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        await ProposeAsync(cluster, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                () => Task.FromResult(victim.Wal.GetMaxLog(PartitionId) >= 1),
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            "The victim never received the entry to lose.");

        int inWindow = victim.SimulatedWal!.Snapshot().NonDurableEntryCount;
        Assert.True(inWindow > 0, "Nothing was inside the fsync window, so the crash would prove nothing.");

        await cluster.CrashNodeAsync(victim, cancellationToken);

        Assert.Equal(inWindow, victim.SimulatedWal.Counters.EntriesLostOnCrash);
        Assert.Equal(0, victim.SimulatedWal.Snapshot().NonDurableEntryCount);
    }

    // ── Pause and resume ──────────────────────────────────────────────────

    /// <summary>
    /// A paused follower stores the traffic sent to it and takes the whole backlog when it wakes.
    ///
    /// <para>This is the property that separates a stopped process from a cut link. A cut link has
    /// already lost the messages; a stopped one still has every one of them, and the burst on
    /// resume is where the defects are.</para>
    /// </summary>
    [Fact]
    public async Task APausedFollower_StoresItsTraffic_AndTakesItInABurst()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(20260906, cancellationToken);

        ClusterInvariantRunner invariants = new();
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode sleeper = cluster.Nodes.First(node => node != leader);

        sleeper.Pause();
        Assert.Equal(SimulationNodeLifecycleStatus.Paused, sleeper.LifecycleStatus);

        await ProposeAsync(cluster, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return sleeper.FrozenBacklog > 0;
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "Nothing was stored for the paused node, so it was not really asleep.");

        Assert.Equal(0, sleeper.Wal.GetMaxLog(PartitionId));

        sleeper.Resume();

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    RaftPartitionView? view =
                        await sleeper.GetPartitionViewAsync(PartitionId, cancellationToken);

                    return view is not null && view.CommitIndex >= 1;
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            "The woken node never caught up on its backlog.");

        Assert.Equal(0, sleeper.FrozenBacklog);
        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// Pausing the leader elects another one, and waking the old leader does not produce a second.
    ///
    /// <para>This is the corpus shape in which a paused leader wakes into a term that moved without
    /// it. Election safety is checked at every settled state, so a run in which both the old and the
    /// new leader believe they hold the term fails here rather than at the end.</para>
    /// </summary>
    [Fact]
    public async Task APausedLeader_IsReplaced_AndDoesNotComeBackAsASecondLeader()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(20260907, cancellationToken);

        ClusterInvariantRunner invariants = new();
        SimulationNode original = await ElectAsync(cluster, cancellationToken);

        original.Pause();

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    IReadOnlyList<RaftPartitionView> views =
                        await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

                    return views.Any(view =>
                        view.Role == RaftNodeState.Leader && view.Endpoint != original.Endpoint);
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            "No replacement leader was elected while the original slept.");

        original.Resume();

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    IReadOnlyList<RaftPartitionView> views =
                        await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

                    return views.Count(view => view.Role == RaftNodeState.Leader) == 1;
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            "The cluster did not settle on exactly one leader after the old one woke.");

        await ProposeAsync(cluster, cancellationToken);
        await ConvergeAsync(cluster, invariants, index: 1, cancellationToken);
        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private Task<SimulationCluster> StartAsync(ulong seed, CancellationToken cancellationToken) =>
        SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = seed },
            logger,
            cancellationToken);

    private static async Task ProposeAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);
    }

    /// <summary>
    /// Waits until every node with a live manager has committed up to <paramref name="index"/>.
    ///
    /// <para>The committed frontier is the condition, not the stored log. A node can hold an entry
    /// over a hole below it, which satisfies a max-log test while its frontier honestly stays where
    /// the hole starts — the exact state DST FINDING 1 described. Waiting on the log would let a
    /// scenario declare convergence and then fail its own convergence check.</para>
    /// </summary>
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
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The running nodes did not all commit up to {index}.");
    }

    /// <summary>
    /// Waits for exactly one leader among the running nodes and returns it. Paused and crashed
    /// nodes are skipped: a paused node still believes it leads, and asking it would return a
    /// leader the rest of the cluster has already replaced.
    /// </summary>
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
            if (node.LifecycleStatus != SimulationNodeLifecycleStatus.Running)
                continue;

            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No running leader is present.");
    }
}
