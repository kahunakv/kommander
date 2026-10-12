using Kommander.System;

namespace Kommander.Tests;

/// <summary>
/// The partition-lifecycle façade's precondition contract. Every operation validates
/// initialization, refuses to mutate the system partition, and requires leadership — all before
/// the system coordinator is contacted. Each guard is therefore asserted with a coordinator that
/// is never reached (null) and delegate stubs that answer only the precondition question, which
/// fails the test the moment a guard stops rejecting up front.
/// </summary>
public sealed class TestPartitionLifecyclePreconditions
{
    private const int DataPartition = 3;

    private const int OtherDataPartition = 4;

    private static PartitionLifecycleService Create(
        bool initialized = true,
        Func<int, bool>? leaderOf = null)
    {
        return new PartitionLifecycleService(
            systemCoordinator: null!,
            isInitialized: () => initialized,
            amILeader: (partitionId, _) => ValueTask.FromResult(leaderOf?.Invoke(partitionId) ?? false));
    }

    // ── initialization gate ────────────────────────────────────────────────────

    [Fact]
    public async Task NotInitialized_EveryOperation_ThrowsBeforeTouchingTheCoordinator()
    {
        PartitionLifecycleService lifecycle = Create(initialized: false);

        await Assert.ThrowsAsync<RaftException>(() => lifecycle.SplitPartitionAsync(DataPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.MergePartitionsAsync(DataPartition, OtherDataPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.SetReplicationFactorAsync(DataPartition, 3, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(
            () => lifecycle.ChangeReplicaAsync(RaftSystemRequestType.AddReplica, DataPartition, "10.0.0.1:7000", 11, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.CreatePartitionAsync(DataPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.RemovePartitionAsync(DataPartition, ct: TestContext.Current.CancellationToken));
    }

    // ── system partition is not a lifecycle target ─────────────────────────────

    [Fact]
    public async Task SystemPartition_CannotBeSplitMergedCreatedRemovedOrRetargeted()
    {
        PartitionLifecycleService lifecycle = Create(leaderOf: _ => true);

        await Assert.ThrowsAsync<RaftException>(() => lifecycle.SplitPartitionAsync(RaftSystemConfig.SystemPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(
            () => lifecycle.MergePartitionsAsync(DataPartition, RaftSystemConfig.SystemPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(
            () => lifecycle.MergePartitionsAsync(RaftSystemConfig.SystemPartition, DataPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(
            () => lifecycle.SetReplicationFactorAsync(RaftSystemConfig.SystemPartition, 3, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(
            () => lifecycle.ChangeReplicaAsync(
                RaftSystemRequestType.RemoveReplica, RaftSystemConfig.SystemPartition, "10.0.0.1:7000", 11, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.CreatePartitionAsync(RaftSystemConfig.SystemPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.RemovePartitionAsync(RaftSystemConfig.SystemPartition, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Merge_WithSourceEqualToSurvivor_IsRefused()
    {
        PartitionLifecycleService lifecycle = Create(leaderOf: _ => true);

        await Assert.ThrowsAsync<RaftException>(() => lifecycle.MergePartitionsAsync(DataPartition, DataPartition, ct: TestContext.Current.CancellationToken));
    }

    // ── leadership gate ────────────────────────────────────────────────────────

    [Fact]
    public async Task FollowerInitiation_EveryOperation_Throws()
    {
        PartitionLifecycleService lifecycle = Create(leaderOf: _ => false);

        await Assert.ThrowsAsync<RaftException>(() => lifecycle.SplitPartitionAsync(DataPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.MergePartitionsAsync(DataPartition, OtherDataPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.SetReplicationFactorAsync(DataPartition, 3, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(
            () => lifecycle.ChangeReplicaAsync(RaftSystemRequestType.PromoteReplica, DataPartition, "10.0.0.1:7000", 11, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.CreatePartitionAsync(DataPartition, ct: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RaftException>(() => lifecycle.RemovePartitionAsync(DataPartition, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Merge_RequiresLeadershipOfBothSurvivorAndSource()
    {
        // Split/merge/change-replica leadership is scoped per partition: a caller who leads the
        // system partition but neither side of the merge, or only the survivor, must both be
        // refused — the two checks have distinct messages, so assert each landing spot.
        PartitionLifecycleService leadsNeither = Create(leaderOf: partitionId => partitionId == RaftSystemConfig.SystemPartition);

        RaftException survivorRejection = await Assert.ThrowsAsync<RaftException>(
            () => leadsNeither.MergePartitionsAsync(DataPartition, OtherDataPartition, ct: TestContext.Current.CancellationToken));
        Assert.Contains("survivor", survivorRejection.Message);

        PartitionLifecycleService leadsSurvivorOnly = Create(
            leaderOf: partitionId => partitionId == DataPartition);

        RaftException sourceRejection = await Assert.ThrowsAsync<RaftException>(
            () => leadsSurvivorOnly.MergePartitionsAsync(DataPartition, OtherDataPartition, ct: TestContext.Current.CancellationToken));
        Assert.Contains("source", sourceRejection.Message);
    }

    [Fact]
    public async Task Merge_ChecksSurvivorLeadershipBeforeSourceLeadership()
    {
        // A caller that leads neither side must hear about the survivor first, which is what
        // makes the two rejections in the test above distinguishable.
        PartitionLifecycleService lifecycle = Create(leaderOf: _ => false);

        RaftException rejection = await Assert.ThrowsAsync<RaftException>(
            () => lifecycle.MergePartitionsAsync(DataPartition, OtherDataPartition, ct: TestContext.Current.CancellationToken));

        Assert.Contains("survivor", rejection.Message);
    }
}
