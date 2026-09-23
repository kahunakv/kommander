using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Backfill at the leader's compaction floor, against a real three-node cluster.
///
/// <para><b>Why these exist.</b> Family 11 of the simulation plan, and DST FINDING 6 (vorpal
/// <c>8176b8e2</c>). A follower whose committed frontier is exactly one below the leader's first
/// retained entry is not below the floor: the leader still holds every entry the follower needs, so
/// ordinary backfill must repair it and no snapshot is involved. But the entry the batch is anchored
/// on — the follower's frontier — is the one entry the leader compacted. The leader cannot read its
/// term, and the batch carries a previous term of -1.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestCompactionFloorBackfillScenarios
{
    private const int PartitionId = 1;

    /// <summary>The compaction cadence of the frequent-compaction sweep, so a short scenario compacts.</summary>
    private const int CompactEveryOperations = 8;

    private readonly ILogger<IRaft> logger;

    public TestCompactionFloorBackfillScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// A live follower held by the retention floor is repaired by backfill once its disk recovers,
    /// with no client write.
    ///
    /// <para><b>The defect this pins (FINDING 6).</b> The live-replica retention hold keeps entries
    /// from the follower's position + 1, so compaction deletes the entry at the position itself.
    /// The backfill batch starts at position + 1 and is anchored on the position, whose term the
    /// leader can no longer read, so it ships a previous term of -1. The follower holds that entry
    /// in its committed prefix, compared its real term with -1, called it divergence, and rejected
    /// the batch. The batch read itself succeeded, so the leader recorded no refusal and started no
    /// snapshot, and it shipped the same batch on every heartbeat, forever.</para>
    ///
    /// <para><b>The shape.</b> The follower commits two entries and a checkpoint, then its disk
    /// refuses every write. It stays alive and keeps answering heartbeats, so the leader keeps
    /// counting it and publishes a retention floor one above the follower's reported durable
    /// frontier. The checkpoint is what makes that report equal to the follower's commit index; see
    /// the comment at the checkpoint. The leader writes and checkpoints until it has compacted through that frontier.
    /// The disk is then freed, and nobody writes. A disk fault, not a crash, because a crash can
    /// revert the follower's last commit marker, and a follower whose frontier drops below the
    /// anchor is below the floor — a different path, repaired by a snapshot.</para>
    /// </summary>
    [Fact]
    public async Task AFollowerAtTheCompactionFloor_IsRepairedByBackfill()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260914,
                ConfigureNode = configuration => configuration.CompactEveryOperations = CompactEveryOperations,
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        SimulationNode follower = cluster.Nodes.First(node => node != leader);

        await ProposeAsync(leader, count: 2, cancellationToken);

        // A checkpoint last, so the follower's newest resolution is on disk. Its commit row is a
        // CommittedCheckpoint, which the scheduler always writes with an fsync, and that fsync also
        // carries the markers before it. Without it the last Committed marker can still ride
        // sync-off, the follower then reports one entry less than it has committed (DST FINDING 7),
        // the leader keeps that entry, and the backfill is anchored on a retained entry instead of
        // a compacted one — the shape below is never reached.
        await CheckpointAsync(cluster, leader, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(leader, cancellationToken), cancellationToken);

        follower.SimulatedWal!.SetOutOfSpace(true, PartitionId);

        // The anchor is what the follower last REPORTED as durable, read from the leader's record,
        // because that is the value the retention hold follows. The checkpoint above makes it equal
        // to the follower's commit index, which is asserted below.
        await cluster.RunUntilAsync(() => Task.FromResult(false), stepCount: 4, advanceMilliseconds: 50, cancellationToken);

        RaftFollowerProgress? progress = leader.Manager.GetFollowerProgress(PartitionId, follower.Endpoint);
        Assert.NotNull(progress);
        long anchor = progress.DurableFrontier;
        Assert.True(anchor >= 1, $"The follower reported no durable frontier ({anchor}), so there is no anchor to compact.");

        // Write and checkpoint until the leader has compacted exactly through the follower's
        // frontier. The retention hold stops compaction there, which is the state under test; a
        // loop, because compaction runs on its own cadence.
        for (int round = 0; round < 12 && FirstRetained(leader) <= anchor; round++)
        {
            await ProposeAsync(leader, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, leader, cancellationToken);

            await cluster.RunUntilAsync(
                () => Task.FromResult(FirstRetained(leader) > anchor),
                stepCount: 20,
                advanceMilliseconds: 50,
                cancellationToken);
        }

        // The precondition, asserted rather than assumed. The leader's log must start exactly one
        // above the follower's frontier: lower, and the anchor entry still exists and nothing is
        // tested; higher, and the follower is below the floor and a snapshot repairs it instead.
        Assert.Equal(anchor + 1, FirstRetained(leader));
        Assert.Equal(anchor, await CommitIndexAsync(follower, cancellationToken));

        follower.SimulatedWal.SetOutOfSpace(false);

        long target = await CommitIndexAsync(leader, cancellationToken);

        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                return await CommitIndexAsync(follower, cancellationToken) >= target;
            },
            stepCount: 300,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            converged,
            $"The follower at the compaction floor was never repaired. It is at " +
            $"{await CommitIndexAsync(follower, cancellationToken)} against the leader's {target}; " +
            $"the leader's log starts at {FirstRetained(leader)}.");

        // Repaired by backfill, as the precondition promised. A snapshot here would mean the
        // scenario reached a different state from the one it describes.
        Assert.Equal(0, leader.StateTransfer.ExportsServed);

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
    }

    /// <summary>
    /// A deposed leader that holds an uncommitted entry at an index the new leader has compacted is
    /// repaired, and does not leave the new leader re-sending a refused batch forever.
    ///
    /// <para><b>The state.</b> The old leader writes an entry at index N and is cut off before it
    /// commits it. The new leader commits a different entry at N and compacts past N while the old
    /// leader is away. After the heal, the old leader holds N in its old term, above its commit
    /// frontier of N - 1. The new leader's first retained entry is above N, so its backfill is
    /// anchored on N with a previous term of -1, and the FINDING 6 rule accepts such an anchor only
    /// inside the follower's committed prefix. N is not in it, so the follower refuses.</para>
    ///
    /// <para><b>Why this is the FINDING 7 liveness backstop.</b> FINDING 7 was one way to reach a
    /// refused -1 anchor; its fix removed that producer. This is another one, and it needs no
    /// defect at all: a deposed leader's uncommitted tail is normal Raft. What must not happen is
    /// the leader's answer to it — re-sending the same batch on every heartbeat, with no snapshot,
    /// because the batch read itself succeeded.</para>
    /// </summary>
    [Fact]
    public async Task ADeposedLeaderHoldingAnUncommittedEntryAtACompactedIndex_IsRepaired()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = 20260923,
                ConfigureNode = configuration =>
                {
                    configuration.CompactEveryOperations = CompactEveryOperations;

                    // The deposed leader is silent while it is cut off. With no silent-peer window
                    // and a small lag budget, the new leader compacts past it, which is the state.
                    configuration.CompactionLiveReplicaLagBudget = 4;
                    configuration.CompactionSilentPeerRetentionWindow = TimeSpan.Zero;
                },
            },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new();

        SimulationNode oldLeader = await ElectAsync(cluster, cancellationToken);

        await ProposeAsync(oldLeader, count: 2, cancellationToken);
        await ConvergeAsync(cluster, invariants, await CommitIndexAsync(oldLeader, cancellationToken), cancellationToken);

        long committedBefore = await CommitIndexAsync(oldLeader, cancellationToken);

        cluster.Transport.PartitionNode(oldLeader.Endpoint);

        // Written on the old leader only: it cannot reach a quorum, so the entry stays uncommitted.
        Task<RaftReplicationResult> stranded = oldLeader.Manager.ReplicateLogs(
            PartitionId, "Greeting", "Stranded"u8.ToArray(), cancellationToken: cancellationToken);

        _ = stranded.ContinueWith(
            static task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        long strandedIndex = -1;

        Assert.True(
            await cluster.RunUntilAsync(
                () =>
                {
                    strandedIndex = oldLeader.Wal.GetMaxLog(PartitionId);
                    return Task.FromResult(strandedIndex > committedBefore);
                },
                stepCount: 20,
                advanceMilliseconds: 50,
                cancellationToken),
            "The cut-off leader never wrote its entry.");

        SimulationNode? newLeader = null;

        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    foreach (SimulationNode node in cluster.Nodes.Where(node => node != oldLeader))
                    {
                        RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
                        if (view?.Role == RaftNodeState.Leader)
                        {
                            newLeader = node;
                            return true;
                        }
                    }

                    return false;
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "The other two nodes never elected a new leader.");

        for (int round = 0; round < 12 && FirstRetained(newLeader!) <= strandedIndex + 1; round++)
        {
            await ProposeAsync(newLeader!, count: CompactEveryOperations, cancellationToken);
            await CheckpointAsync(cluster, newLeader!, cancellationToken);

            await cluster.RunUntilAsync(
                () => Task.FromResult(FirstRetained(newLeader!) > strandedIndex + 1),
                stepCount: 20,
                advanceMilliseconds: 50,
                cancellationToken);
        }

        Assert.True(
            FirstRetained(newLeader!) > strandedIndex + 1,
            $"The new leader's log starts at {FirstRetained(newLeader!)}, not above the stranded index {strandedIndex}.");

        cluster.Transport.HealPartition(oldLeader.Endpoint);

        long target = await CommitIndexAsync(newLeader!, cancellationToken);

        bool repaired = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                return await CommitIndexAsync(oldLeader, cancellationToken) >= target;
            },
            stepCount: 400,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            repaired,
            $"The deposed leader was never repaired: it is at {await CommitIndexAsync(oldLeader, cancellationToken)} " +
            $"against {target}; it holds up to {oldLeader.Wal.GetMaxLog(PartitionId)}, the stranded entry was at " +
            $"{strandedIndex}, and the new leader's log starts at {FirstRetained(newLeader!)}. " +
            $"Exports served: {newLeader!.StateTransfer.ExportsServed}. " +
            $"Backfill refusals: {string.Join(" | ", newLeader.Manager.GetBackfillStatuses(PartitionId))}. " +
            $"Snapshot statuses: {string.Join(" | ", newLeader.Manager.GetSnapshotStatuses(PartitionId))}.");

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);
        await AppliedStateRule.CheckAsync(cluster, PartitionId, advanceMilliseconds: 50, cancellationToken);
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
