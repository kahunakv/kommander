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
    /// <para><b>The shape.</b> The follower commits two entries, then its disk refuses every write.
    /// It stays alive and keeps answering heartbeats, so the leader keeps counting it and publishes
    /// a retention floor of 3. The leader writes and checkpoints until it has compacted through 2.
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
        await ConvergeAsync(cluster, invariants, index: 2, cancellationToken);

        long anchor = await CommitIndexAsync(follower, cancellationToken);

        follower.SimulatedWal!.SetOutOfSpace(true, PartitionId);

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
