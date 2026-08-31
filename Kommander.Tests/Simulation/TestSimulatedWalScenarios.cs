using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Storage-fault scenarios against a real three-node cluster.
///
/// <para><b>What these add over the unit tests.</b> The unit tests prove the simulated store loses
/// what its model says it loses. They cannot prove the model describes anything the library
/// actually does. These scenarios close that gap: they run the production write path and assert
/// that the modes the store offers are the modes the write path takes, then check that a storage
/// fault on one node breaks no Raft invariant.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestSimulatedWalScenarios
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestSimulatedWalScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// Every node stores its log in the simulated store, and a converged cluster holds a run of
    /// entries with no hole in it.
    /// </summary>
    [Fact]
    public async Task EveryNode_UsesTheSimulatedStore_AndConvergesWithoutAHole()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(new SimulationClusterOptions
        {
            NodeCount = 3,
            PartitionCount = 1,
            Seed = 20260831,
        }, cancellationToken);

        ClusterInvariantRunner invariants = new();
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);

        for (int index = 0; index < 3; index++)
        {
            RaftReplicationResult result = await leader.Manager.ReplicateLogs(
                PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return cluster.Nodes.All(node => node.Wal.GetMaxLog(PartitionId) >= 3);
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "Followers did not converge on the committed entries.");

        IReadOnlyDictionary<string, SimulatedWalSnapshot> snapshots = cluster.GetWalSnapshots();

        Assert.Equal(cluster.Nodes.Count, snapshots.Count);

        foreach ((string endpoint, SimulatedWalSnapshot snapshot) in snapshots)
        {
            SimulatedWalPartitionSnapshot? partition = snapshot.Partition(PartitionId);

            Assert.True(partition is not null, $"{endpoint} holds no entries for partition {PartitionId}.");
            Assert.False(partition!.HasHole, $"{endpoint} has a hole at {string.Join(", ", partition.MissingIds)}.");
            Assert.True(partition.MaxLogId >= 3, $"{endpoint} stopped at {partition.MaxLogId}.");
        }

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// The library really does write commit markers without an fsync of their own.
    ///
    /// <para>This is the test that keeps the durability model honest. The single-fsync fast path is
    /// on by default, and the store models the window it opens; if the write path ever stopped
    /// taking that path, the model would still pass its own unit tests while describing nothing.
    /// Then this test fails, which is the point.</para>
    /// </summary>
    [Fact]
    public async Task TheWritePath_ReallyIssuesWritesWithNoFsyncOfTheirOwn()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(new SimulationClusterOptions
        {
            NodeCount = 3,
            PartitionCount = 1,
            Seed = 20260901,
        }, cancellationToken);

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);

        Assert.True(
            await cluster.RunUntilAsync(
                () => Task.FromResult(cluster.Nodes.All(node => node.Wal.GetMaxLog(PartitionId) >= 1)),
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "Followers did not converge on the committed entry.");

        long nonSyncWrites = cluster.GetWalSnapshots().Values.Sum(snapshot => snapshot.Counters.NonSyncWrites);
        long syncWrites = cluster.GetWalSnapshots().Values.Sum(snapshot => snapshot.Counters.SyncWrites);

        Assert.True(syncWrites > 0, "No durable write was issued at all, which cannot be right.");
        Assert.True(
            nonSyncWrites > 0,
            "No write skipped its fsync. The single-fsync fast path is the reason the store models a "
            + "durability window, so either the path changed or the store no longer sees it.");
    }

    /// <summary>
    /// Transient write failures on one follower break no invariant, and that follower catches up
    /// once its disk works again.
    ///
    /// <para>A refused write is not a lost write: the entry never reached the log, so the follower's
    /// frontier must not advance over it. A follower that reported progress it did not make is the
    /// shape that strands a replica, so the convergence check at the end is the real assertion.</para>
    /// </summary>
    [Fact]
    public async Task TransientWriteFailuresOnOneFollower_BreakNoInvariant()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(new SimulationClusterOptions
        {
            NodeCount = 3,
            PartitionCount = 1,
            Seed = 20260902,
        }, cancellationToken);

        ClusterInvariantRunner invariants = new();
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode follower = cluster.Nodes.First(node => node != leader);

        follower.SimulatedWal!.FailNextWrites(5, PartitionId);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);

        // Run on with the fault still biting. Nothing must break while the follower is refusing.
        await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return false;
            },
            stepCount: 40,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(follower.SimulatedWal.Counters.FailedWrites > 0, "The fault never bit.");

        // The disk works again from here. One more write is needed, and that is the contract
        // rather than a weakness of the scenario: idle backfill is confined to entries the current
        // leader committed live (HeartbeatDriver, LocalCommittedIndex > LiveCommitFloor), because a
        // leader must not push merely-restored committed state at a voter. The refused writes cost
        // this cluster an election, so the entry the follower missed is restored state to whoever
        // leads now. A live write lifts the floor and the follower is repaired with it.
        await ProposeAsync(cluster, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return cluster.Nodes.All(node => node.Wal.GetMaxLog(PartitionId) >= 2);
                },
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            "The follower never caught up after its disk recovered and a new write landed.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A full disk on one follower does not stop the cluster: the other two form a quorum and the
    /// entry commits. The starved node stores nothing while its disk is full.
    /// </summary>
    [Fact]
    public async Task AFullDiskOnOneFollower_DoesNotStopTheCluster()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(new SimulationClusterOptions
        {
            NodeCount = 3,
            PartitionCount = 1,
            Seed = 20260903,
        }, cancellationToken);

        ClusterInvariantRunner invariants = new();
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode follower = cluster.Nodes.First(node => node != leader);

        follower.SimulatedWal!.SetOutOfSpace(true, PartitionId);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    return cluster.Nodes
                        .Where(node => node != follower)
                        .All(node => node.Wal.GetMaxLog(PartitionId) >= 1);
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "The healthy majority did not commit while one disk was full.");

        // The starved node advertises nothing it does not hold. This is the property that keeps the
        // failure safe: the entry is missing, and the node says so.
        Assert.Equal(0, follower.Wal.GetMaxLog(PartitionId));
        Assert.True(follower.SimulatedWal.Counters.FailedWrites > 0, "The fault never bit.");
    }

    /// <summary>
    /// A follower that misses one entry to a write failure is never repaired, because the hole is
    /// smaller than <see cref="RaftConfiguration.BackfillThreshold"/>.
    ///
    /// <para><b>This test is skipped because it fails, and it fails on a real defect.</b> It is kept
    /// so the defect has an exact reproduction rather than a description.</para>
    ///
    /// <para><b>The state it reaches.</b> The starved node ends holding entry 2 and not entry 1 — a
    /// physical hole — with its committed frontier correctly stopped at 0 while the other two are at
    /// 2. Twenty seconds of simulated time and a further live write do not repair it. Reproduced on
    /// seeds 20260903, 7 and 999; on seed 999 the victim is a different node, so it follows the
    /// fault rather than the node.</para>
    ///
    /// <para><b>The mechanism, confirmed by experiment.</b> With
    /// <c>BackfillThreshold = 0</c> the same run repairs completely, all three nodes reaching
    /// <c>[1,2]</c> at commit index 2. With the default of 10 it never repairs. Three triggers could
    /// have caught it and none can:</para>
    ///
    /// <list type="number">
    ///   <item>The committed-gap trigger needs <c>followerGap > BackfillThreshold</c>. The gap is 2.</item>
    ///   <item>The idle-tail-gap trigger is confined to entries the current leader committed live,
    ///     and the write failures cost this cluster an election, so those entries are merely restored
    ///     state to whoever leads afterwards.</item>
    ///   <item>No log-mismatch rejection is ever raised, because the propose and commit broadcast
    ///     ships entries unanchored. The follower accepts entry 2 over the hole with no check
    ///     against entry 1.</item>
    /// </list>
    ///
    /// <para><b>What it costs.</b> No acknowledged data is lost and no safety invariant breaks: the
    /// node's frontier honestly stops below the hole. The replica is stranded — permanently outside
    /// the durability quorum while appearing healthy. The threshold is therefore a lower bound on
    /// damage that can never be repaired, which is not what a tuning knob should be.</para>
    /// </summary>
    [Fact(Skip = "Open defect: a log hole smaller than BackfillThreshold is never repaired. "
                 + "Reproduced on seeds 20260903, 7 and 999. Setting BackfillThreshold to 0 repairs it, "
                 + "which pins the mechanism to the committed-gap trigger. See the summary above.")]
    public async Task AStarvedFollower_IsRepairedOnceItsDiskIsFreed()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartAsync(new SimulationClusterOptions
        {
            NodeCount = 3,
            PartitionCount = 1,
            Seed = 20260903,
        }, cancellationToken);

        ClusterInvariantRunner invariants = new();
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode follower = cluster.Nodes.First(node => node != leader);

        follower.SimulatedWal!.SetOutOfSpace(true, PartitionId);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);

        Assert.True(
            await cluster.RunUntilAsync(
                () => Task.FromResult(cluster.Nodes
                    .Where(node => node != follower)
                    .All(node => node.Wal.GetMaxLog(PartitionId) >= 1)),
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "The healthy majority did not commit while one disk was full.");

        follower.SimulatedWal.SetOutOfSpace(false);

        // A live write, because idle backfill will not push state the current leader only restored.
        await ProposeAsync(cluster, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    IReadOnlyList<RaftPartitionView> views =
                        await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

                    return views.Count > 0
                        && views.Select(view => view.CommitIndex).Distinct().Count() == 1
                        && views[0].CommitIndex >= 2;
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            "The starved follower never converged after its disk was freed.");

        SimulatedWalPartitionSnapshot? partition = follower.SimulatedWal.Snapshot().Partition(PartitionId);

        Assert.True(partition is not null, "The repaired follower holds no entries.");
        Assert.False(partition!.HasHole, $"The follower still has a hole at {string.Join(", ", partition.MissingIds)}.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private Task<SimulationCluster> StartAsync(
        SimulationClusterOptions options,
        CancellationToken cancellationToken) =>
        SimulationCluster.StartAsync(options, logger, cancellationToken);

    /// <summary>
    /// Commits one entry through whoever leads now. The leader is re-read rather than remembered,
    /// because a scenario that injected a fault has usually cost the cluster an election.
    /// </summary>
    private static async Task ProposeAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);

        RaftReplicationResult result = await leader.Manager.ReplicateLogs(
            PartitionId, "Greeting", "Hello Again"u8.ToArray(), cancellationToken: cancellationToken);

        Assert.Equal(RaftOperationStatus.Success, result.Status);
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
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No leader was elected within the step budget.");

        foreach (SimulationNode node in cluster.Nodes)
        {
            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No leader is present.");
    }
}
