using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Snapshot rescue against a real three-node cluster: a follower below the leader's compaction floor
/// must come back, whatever the application's export does on the first attempt.
///
/// <para><b>Why these exist.</b> Family 12 of the simulation plan. Below the floor, backfill cannot
/// repair a follower, because the entries it needs are gone, so a snapshot install is the only path
/// back. That path has its own failure habitat — the transfer is a background task that waits on the
/// application and on the wire — and a wedge there looks exactly like a healthy cluster with one
/// quiet replica.</para>
///
/// <para><b>What makes a pass mean something.</b> Each scenario asserts that the fault really
/// happened: the export really hung, and a second export really ran. A rescue scenario that passes
/// without the hang proves only that a healthy transfer works, which a unit test already proves.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestSnapshotRescueScenarios
{
    private const int PartitionId = 1;

    /// <summary>
    /// The compaction cadence of the frequent-compaction sweep. At the production default a short
    /// scenario never compacts, and a follower can never fall below a floor that does not exist.
    /// </summary>
    private const int CompactEveryOperations = 8;

    /// <summary>Entries the leader retains below its checkpoint for a lagging follower it counts as alive.</summary>
    private const long LiveReplicaLagBudget = 4;

    private readonly ILogger<IRaft> logger;

    public TestSnapshotRescueScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// A hung application export does not strand a follower below the compaction floor.
    ///
    /// <para><b>The defect this pins.</b> The Caraxes anchor-1 wedge, round 2 (vorpal
    /// <c>d11fd5f9</c>). The leader escalated correctly and started a transfer. The application's
    /// export then never returned, and it ignored cancellation. The transfer task parked forever,
    /// its in-flight guard never released, and every later escalation for that follower was vetoed
    /// in silence. The cluster stayed wedged with one replica below the floor.
    /// <c>SnapshotTransferStepTimeout</c> is the fix: a step that stops moving is abandoned and
    /// recorded as a failure, and the backoff paces a retry.</para>
    ///
    /// <para><b>Why this is a cluster-level control and not a unit test.</b> A unit test already
    /// asserts that a hung export times out. This scenario asserts the consequence a user sees: the
    /// follower converges. The simulation plan requires that distinction for a corpus control — a
    /// control that observes a deleted line instead of a behaviour proves nothing about a running
    /// cluster.</para>
    ///
    /// <para><b>The shape.</b> The leader's next export is armed to hang, and the victim crashes after
    /// it holds a committed prefix. The survivors write and checkpoint until the leader has compacted
    /// past everything the victim holds, and the victim restarts. No client writes after the
    /// restart: the victim reported a committed prefix and the gap is above the backfill threshold,
    /// so the heartbeat path owns the repair.</para>
    /// </summary>
    [Fact]
    public async Task AHungExport_DoesNotStrandAFollowerBelowTheFloor()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260913,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;

                    // The live-replica hold keeps up to this many entries for a follower the leader
                    // still counts as alive. A crashed simulated node can stay alive in the
                    // leader's view for the whole scenario, and at the production budget of
                    // 100,000 the leader then never compacts past it. Production reaches the same
                    // state with a follower that lags beyond the budget; a small budget reaches it
                    // with a short log.
                    configuration.CompactionLiveReplicaLagBudget = LiveReplicaLagBudget;
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode victim = cluster.Nodes.First(node => node != leader);

        await ProposeAsync(cluster, leader, count: 2, cancellationToken);
        await ConvergeAsync(cluster, invariants, index: 2, cancellationToken);

        // Armed before the crash, so the first export the leader ever makes is the hung one. The
        // leader starts escalating as soon as it compacts past the crashed victim — long before the
        // restart — and an export served then is cached and reused by later attempts. A hang armed
        // after that point can go unused, and a run that never hung proves nothing. With the step
        // timeout removed, a hang at any point holds the in-flight guard forever, so the victim is
        // never rescued whether the hang lands before or after the restart.
        leader.StateTransfer.HangNextExports(1);

        await cluster.CrashNodeAsync(victim, cancellationToken);

        long victimMaxLog = victim.Wal.GetMaxLog(PartitionId);

        // Write and checkpoint until the leader's log starts above everything the victim holds.
        // A loop rather than a fixed count, because compaction runs on its own cadence and the
        // number of entries it takes is a property of that cadence, not of this scenario.
        long compactedThrough = -1;

        for (int round = 0; round < 12 && compactedThrough <= victimMaxLog + 1; round++)
        {
            await ProposeAsync(cluster, leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);

            await cluster.RunUntilAsync(
                () => Task.FromResult(CompactedThrough(leader) > victimMaxLog + 1),
                stepCount: 20,
                advanceMilliseconds: 50,
                cancellationToken);

            compactedThrough = CompactedThrough(leader);
        }

        // The victim really is below the floor. Without this the scenario could pass through an
        // ordinary backfill and never ask the leader for a snapshot at all.
        Assert.True(
            compactedThrough > victimMaxLog + 1,
            $"The leader compacted only through {compactedThrough}, and the victim holds up to " +
            $"{victimMaxLog}, so the victim is not below the floor and no rescue is needed.");

        await cluster.RestartNodeAsync(victim, cancellationToken);

        long target = await CommitIndexAsync(leader, cancellationToken);

        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                RaftPartitionView? view = await victim.GetPartitionViewAsync(PartitionId, cancellationToken);

                return view is not null && view.CommitIndex >= target;
            },
            stepCount: 600,
            advanceMilliseconds: 50,
            cancellationToken);

        // The hang was really hit, and a second export really ran. Checked before the convergence
        // verdict, because a run that never hung says nothing about the defect either way.
        Assert.Equal(1, leader.StateTransfer.ExportsHung);

        Assert.True(
            converged,
            $"The follower below the floor was never rescued. Victim holds up to " +
            $"{victim.Wal.GetMaxLog(PartitionId)} against the leader's commit index {target}. " +
            $"Exports hung={leader.StateTransfer.ExportsHung} served={leader.StateTransfer.ExportsServed}. " +
            $"Snapshot rescue: {DescribeSnapshotRescue(cluster)}");

        Assert.True(
            leader.StateTransfer.ExportsServed >= 1,
            "The follower converged without a second export, so the rescue did not go through the " +
            "retry this scenario exists to test.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static long CompactedThrough(SimulationNode node) =>
        node.SimulatedWal?.Snapshot().Partition(PartitionId)?.CompactedThrough ?? -1;

    private static async Task<long> CommitIndexAsync(SimulationNode node, CancellationToken cancellationToken)
    {
        RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
        return view?.CommitIndex ?? -1;
    }

    /// <summary>
    /// Writes <paramref name="count"/> entries through <paramref name="leader"/>, driving the cluster
    /// while each write is in flight so the quorum it needs keeps running.
    /// </summary>
    private static async Task ProposeAsync(
        SimulationCluster cluster,
        SimulationNode leader,
        int count,
        CancellationToken cancellationToken)
    {
        for (int index = 0; index < count; index++)
        {
            RaftReplicationResult result = await cluster.DriveAsync(
                () => leader.Manager.ReplicateLogs(
                    PartitionId, "Greeting", "Hello World"u8.ToArray(), cancellationToken: cancellationToken),
                cancellationToken);

            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }
    }

    /// <summary>
    /// Writes a checkpoint through <paramref name="leader"/>, which lets every node compact.
    ///
    /// <para>Started, then stepped, the way the random runner does it. A checkpoint needs a quorum,
    /// and a plain await stops the harness stepping, so the call waits out its timeout on the wall
    /// clock instead of committing.</para>
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

        RaftReplicationResult result = await checkpoint;

        Assert.Equal(RaftOperationStatus.Success, result.Status);
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
            if (node.LifecycleStatus != SimulationNodeLifecycleStatus.Running)
                continue;

            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No running leader is present.");
    }

    private static string DescribeSnapshotRescue(SimulationCluster cluster)
    {
        List<string> rescues = [];

        foreach (SimulationNode node in cluster.Nodes.Where(candidate => candidate.HasLiveManager))
        {
            foreach (RaftSnapshotStatus status in node.Manager.GetSnapshotStatuses(PartitionId))
            {
                rescues.Add(
                    $"{node.Endpoint}->{status.FollowerEndpoint} inFlight={status.InFlight} " +
                    $"failedAttempts={status.FailedAttempts} lastError={status.LastError ?? "none"}");
            }
        }

        return rescues.Count > 0 ? string.Join(" | ", rescues) : "none recorded.";
    }
}
