using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// A node killed under load and restarted over its own store, against a real three-node cluster
/// whose leader has a compaction floor.
///
/// <para><b>Why these exist.</b> The Caraxes bank-leader-kill run (vorpal <c>b24b3891</c>). A
/// restarted node answers AppendEntries while Phase 1 of its restore is still reading the log, and
/// every frontier those acks read is at its init. The restarted leader reported a contiguous
/// frontier of 0 with a log that covered the leader's floor by millions of entries; the leader
/// anchored a backfill at 1, could not serve it below its floor, and re-seeded the node with a
/// 38-second snapshot three seconds before the restore finished. Below the floor a snapshot is the
/// only repair, and every existing rescue scenario puts its follower there on purpose — so nothing
/// asserted that a node ABOVE the floor is repaired by backfill, and the run reported success.</para>
///
/// <para><b>What makes a pass mean something.</b> The scenario asserts that the window was
/// reached — the leader's traffic really arrived before the restore landed — and that the node's
/// store really covered the floor. A restart that misses the window proves only that backfill
/// works, which other scenarios already prove.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestRestartUnderLoadScenarios
{
    private const int PartitionId = 1;

    /// <summary>The compaction cadence of the frequent-compaction sweep, so a short scenario compacts.</summary>
    private const int CompactEveryOperations = 8;

    /// <summary>Entries the leader retains below its checkpoint for a lagging follower it counts as alive.</summary>
    private const long LiveReplicaLagBudget = 4;

    /// <summary>
    /// The budget for the silent-peer scenario: wide enough that an outage spanning a whole
    /// compaction cadence (two batches of <see cref="CompactEveryOperations"/> plus a checkpoint)
    /// stays inside it, so the hold — not the budget — decides whether the node is compacted past.
    /// </summary>
    private const long SilentPeerLagBudget = 32;

    private readonly ILogger<IRaft> logger;

    public TestRestartUnderLoadScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// A killed follower whose store still covers the leader's compaction floor rejoins by backfill,
    /// not by snapshot, even though the leader reaches it before its restore has landed.
    ///
    /// <para><b>The shape.</b> The leader writes and checkpoints until it has compacted a prefix,
    /// with every node caught up, so every store covers the floor. A follower is killed and the
    /// survivors commit a few entries without a checkpoint, so the floor does not move. The follower
    /// restarts with its restore read held open, into a leader that is heartbeating and re-supplying
    /// at it: the acks it sends before its restore completes must carry no position, and the leader
    /// must wait for the first real one rather than anchor a backfill at 1.</para>
    ///
    /// <para><b>Two defects, one shape.</b> The pre-restore ack above is the Caraxes mechanism. The
    /// same scenario also found the restart-initialisation form the report first suspected: the
    /// crash reverts the last checkpoint's commit marker inside the fsync window, the compaction
    /// that trusted it has already removed every earlier checkpoint, and the restore — reading only
    /// the rows — rebuilt a frontier of 0 for a log the node held through the floor. The restore now
    /// seeds its scan at the compaction floor, and the no-unnecessary-snapshot invariant is what
    /// caught it.</para>
    /// </summary>
    [Fact]
    public async Task AKilledFollowerAboveTheFloor_RejoinsByBackfill_WhenTheLeaderReachesItBeforeItsRestoreLands()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260915,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;
                    configuration.CompactionLiveReplicaLagBudget = LiveReplicaLagBudget;
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        // Build a compaction floor while everyone is caught up. A loop, because compaction runs on
        // its own cadence and the retention hold paces it behind the slowest live follower.
        for (int round = 0; round < 12 && FirstRetained(leader) <= 1; round++)
        {
            await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);
            await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);
        }

        long floor = FirstRetained(leader);
        Assert.True(floor > 1, "The leader never compacted, so there is no floor for a restarted node to be judged against.");

        // Slack above the floor, so the fsync window a crash takes cannot drop the victim below it.
        await ProposeAsync(leader, count: 3, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);

        await cluster.CrashNodeAsync(victim, cancellationToken);

        // The entries the victim misses. No checkpoint, so the floor stays where it is and backfill
        // remains the right repair.
        await ProposeAsync(leader, count: 2, cancellationToken);
        long duringOutage = await CommitIndexAsync(leader, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return cluster.Nodes.Where(node => node != victim)
                        .All(node => node.Wal.GetMaxLog(PartitionId) >= duringOutage);
                },
                stepCount: 300,
                advanceMilliseconds: 50,
                cancellationToken),
            "The surviving majority did not commit while one node was down.");

        // The preconditions, asserted rather than assumed: the floor did not move, and the victim's
        // own store covers it, so a backfill anchored at its prefix + 1 can be served.
        Assert.Equal(floor, FirstRetained(leader));
        long heldThrough = HeldThrough(victim);
        Assert.True(
            heldThrough >= floor - 1,
            $"The crashed node holds a resolved prefix only through {heldThrough} against a floor of {floor}: " +
            "it is below the floor, and a snapshot would be the correct repair. The scenario did not reach its shape.");

        // The window is the point of the scenario, so it is held open rather than raced for: the
        // partition's restore read waits a quarter of a simulated second, and the leader heartbeats and
        // re-supplies the node from the moment its endpoint is back — every one of those is
        // answered before the restore has read the store.
        victim.SimulatedWal!.HoldRestoreReads(PartitionId, forMilliseconds: 250);

        await cluster.RestartNodeAsync(victim, cancellationToken);

        await ProposeAsync(leader, count: 1, cancellationToken);
        long target = await CommitIndexAsync(leader, cancellationToken);

        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return await CommitIndexAsync(victim, cancellationToken) >= target;
            },
            stepCount: 400,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            converged,
            $"The restarted node never caught up: it is at {await CommitIndexAsync(victim, cancellationToken)} " +
            $"against the leader's {target}; the leader's log starts at {FirstRetained(leader)}.");

        // Read after the repair: the join returns when the partition map is applied, which is
        // before the held restore read and before the leader's first batch reaches the partition.
        long answeredBeforeRestore = victim.AppendsAnsweredBeforeRestore(PartitionId);
        Assert.True(
            answeredBeforeRestore > 0,
            "The leader's traffic never reached the node before its restore landed, so the pre-restore " +
            "window was not exercised and this run proves only that backfill works.");

        // Repaired by backfill. A snapshot here is the Caraxes shape: a node re-seeded for a
        // position it never had.
        Assert.Equal(0, cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed));
        Assert.Empty(cluster.UnnecessarySnapshotImports);

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A killed follower whose range the leader compacts past DURING the outage is still repaired
    /// by backfill: the leader holds retention for a silent peer for the silent-peer window, so a
    /// restart inside it is served from the log. The Caraxes bank-leader-kill residue: the floor
    /// ran past the restarting node twice inside one 30-second outage and it was re-seeded by a
    /// full snapshot each time, then ran at half speed while its replica recovered.
    ///
    /// <para>Two runs of one shape. With the window on, no snapshot is served; with it off, the
    /// same outage puts the node below the floor and the (then legitimate) snapshot is served —
    /// which proves the hold is what makes the difference, and that the writes during the outage
    /// really did compact past the node.</para>
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AKilledFollowerCompactedPastDuringItsOutage_IsHeldForTheWindow_AndRejoinsByBackfill(bool windowEnabled)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260916,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;
                    configuration.CompactionLiveReplicaLagBudget = SilentPeerLagBudget;
                    configuration.CompactionSilentPeerRetentionWindow = windowEnabled ? TimeSpan.FromMinutes(2) : TimeSpan.Zero;
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        for (int round = 0; round < 12 && FirstRetained(leader) <= 1; round++)
        {
            await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);
            await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);
        }

        Assert.True(FirstRetained(leader) > 1, "The leader never compacted, so there is no floor to run past.");

        await ProposeAsync(leader, count: 3, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);

        await cluster.CrashNodeAsync(victim, cancellationToken);
        long victimHeld = HeldThrough(victim);

        // A whole compaction cadence during the outage — a batch, a checkpoint, and the batch that
        // triggers the pass against it — inside the lag budget, so the hold alone decides whether
        // the node's range survives.
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
        await CheckpointAsync(cluster, leader, cancellationToken);
        await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);

        long floorTarget = await CommitIndexAsync(leader, cancellationToken);
        await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return !windowEnabled && FirstRetained(leader) > victimHeld + 1;
            },
            stepCount: 60,
            advanceMilliseconds: 50,
            cancellationToken);

        long firstRetained = FirstRetained(leader);
        if (windowEnabled)
            Assert.True(
                firstRetained <= victimHeld + 1,
                $"The leader compacted through {firstRetained - 1} past the silent node's prefix {victimHeld} inside the window.");
        else
            Assert.True(
                firstRetained > victimHeld + 1,
                $"The leader's log starts at {firstRetained} while the crashed node holds through {victimHeld}: " +
                "the outage never compacted past it, so this run tests nothing.");

        await cluster.RestartNodeAsync(victim, cancellationToken);

        await ProposeAsync(leader, count: 1, cancellationToken);
        long target = await CommitIndexAsync(leader, cancellationToken);

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                    return await CommitIndexAsync(victim, cancellationToken) >= target;
                },
                stepCount: 400,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The restarted node never caught up: it is at {await CommitIndexAsync(victim, cancellationToken)} " +
            $"against the leader's {target}; the leader's log starts at {FirstRetained(leader)} (floor target {floorTarget}).");

        Assert.Equal(windowEnabled ? 0 : 1, cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed));
        Assert.Empty(cluster.UnnecessarySnapshotImports);

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static long FirstRetained(SimulationNode node) =>
        node.SimulatedWal?.Snapshot().Partition(PartitionId)?.FirstLogId ?? -1;

    /// <summary>
    /// Highest id the node holds resolved with no hole below it, above whatever it is no longer
    /// required to hold — the position backfill would anchor above.
    /// </summary>
    private static long HeldThrough(SimulationNode node)
    {
        SimulatedWalPartitionSnapshot? store = node.SimulatedWal?.Snapshot().Partition(PartitionId);
        if (store is null)
            return -1;

        long held = store.CoveredThrough;
        foreach (RaftLog log in node.Wal.ReadLogsRange(PartitionId, held + 1))
        {
            if (log.Id != held + 1 || log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                break;
            held = log.Id;
        }

        return held;
    }

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
