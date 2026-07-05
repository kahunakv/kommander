
using Kommander.Data;
using Kommander.System;

namespace Kommander;

/// <summary>
/// Implements the four user-initiated partition lifecycle operations —
/// <see cref="SplitPartitionAsync"/>, <see cref="MergePartitionsAsync"/>,
/// <see cref="CreatePartitionAsync"/>, and <see cref="RemovePartitionAsync"/> — on behalf
/// of <see cref="RaftManager"/>. Each operation validates preconditions (initialized,
/// leader) and then posts a <see cref="RaftSystemRequest"/> to the coordinator channel,
/// awaiting the resulting <see cref="TaskCompletionSource{T}"/> for the outcome.
/// This class owns no mutable state; it is a pure delegation layer over the coordinator.
/// </summary>
internal sealed class PartitionLifecycleService
{
    private readonly RaftSystemCoordinator systemCoordinator;
    private readonly Func<bool> isInitialized;
    private readonly Func<int, CancellationToken, ValueTask<bool>> amILeader;

    internal PartitionLifecycleService(
        RaftSystemCoordinator systemCoordinator,
        Func<bool> isInitialized,
        Func<int, CancellationToken, ValueTask<bool>> amILeader)
    {
        this.systemCoordinator = systemCoordinator;
        this.isInitialized = isInitialized;
        this.amILeader = amILeader;
    }

    internal async Task<RaftPartitionLifecycleResult> SplitPartitionAsync(
        int sourcePartitionId,
        int targetPartitionId = 0,
        RaftSplitPlan? plan = null,
        CancellationToken ct = default)
    {
        if (!isInitialized())
            throw new RaftException("System is not initialized");

        if (sourcePartitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be split");

        if (!await amILeader(sourcePartitionId, ct).ConfigureAwait(false))
            throw new RaftException("Split cannot be initiated by follower");

        RaftSplitPlan effectivePlan = new()
        {
            TargetPartitionId = targetPartitionId > 0 ? targetPartitionId : (plan?.TargetPartitionId ?? 0),
            TargetRoutingMode = plan?.TargetRoutingMode ?? RaftRoutingMode.HashRange,
            HashBoundary      = plan?.HashBoundary,
            AutoCommit        = true
        };

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new RaftSystemRequest(sourcePartitionId, effectivePlan, tcs));

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success    = status == RaftOperationStatus.Success,
            Status     = status,
            Generation = generation
        };
    }

    internal async Task<RaftPartitionLifecycleResult> MergePartitionsAsync(
        int survivorPartitionId,
        int sourcePartitionId,
        RaftMergePlan? plan = null,
        CancellationToken ct = default)
    {
        if (!isInitialized())
            throw new RaftException("System is not initialized");

        if (sourcePartitionId == RaftSystemConfig.SystemPartition || survivorPartitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be merged");

        if (sourcePartitionId == survivorPartitionId)
            throw new RaftException("Source and survivor partition must be different");

        if (!await amILeader(survivorPartitionId, ct).ConfigureAwait(false))
            throw new RaftException("Merge cannot be initiated by follower: not leader of survivor partition");

        if (!await amILeader(sourcePartitionId, ct).ConfigureAwait(false))
            throw new RaftException("Merge cannot be initiated by follower: not leader of source partition");

        RaftMergePlan effectivePlan = plan ?? new()
        {
            SurvivorPartitionId = survivorPartitionId,
            SourcePartitionId   = sourcePartitionId
        };

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new RaftSystemRequest(effectivePlan, tcs));

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success    = status == RaftOperationStatus.Success,
            Status     = status,
            Generation = generation
        };
    }

    internal async Task<RaftPartitionLifecycleResult> CreatePartitionAsync(
        int partitionId,
        RaftRoutingMode mode = RaftRoutingMode.Unrouted,
        (int start, int end)? hashRange = null,
        CancellationToken ct = default)
    {
        if (!isInitialized())
            throw new RaftException("System is not initialized");

        if (partitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be created");

        if (!await amILeader(RaftSystemConfig.SystemPartition, ct).ConfigureAwait(false))
            throw new RaftException("CreatePartition cannot be initiated by a follower");

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new RaftSystemRequest(
            partitionId, mode, hashRange?.start, hashRange?.end, tcs));

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success    = status == RaftOperationStatus.Success,
            Status     = status,
            Generation = generation
        };
    }

    internal async Task<RaftPartitionLifecycleResult> RemovePartitionAsync(
        int partitionId,
        CancellationToken ct = default)
    {
        if (!isInitialized())
            throw new RaftException("System is not initialized");

        if (partitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be removed");

        if (!await amILeader(RaftSystemConfig.SystemPartition, ct).ConfigureAwait(false))
            throw new RaftException("RemovePartition cannot be initiated by a follower");

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(
            new RaftSystemRequest(RaftSystemRequestType.RemovePartition, partitionId)
            {
                Completion = tcs
            });

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success    = status == RaftOperationStatus.Success,
            Status     = status,
            Generation = generation
        };
    }
}
