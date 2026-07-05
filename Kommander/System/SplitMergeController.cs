
using System.Text.Json;
using Kommander.Data;
using Kommander.Logging;
using Kommander.System.Protos;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.System;

/// <summary>Tracks one in-progress split: the target partition id and the optional caller TCS.</summary>
internal sealed record SplitInProgress(
    int TargetPartitionId,
    TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? Completion);

/// <summary>Tracks one in-progress merge: the survivor partition id and the optional caller TCS.</summary>
internal sealed record MergeInProgress(
    int SurvivorPartitionId,
    TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? Completion);

/// <summary>
/// Owns the two-phase split and merge state machines for <see cref="RaftSystemCoordinator"/>.
/// Runs exclusively on the coordinator's single-reader channel loop — no locking is needed
/// because only one <see cref="Receive"/> call executes at a time.
/// <para>
/// <see cref="PendingSplits"/> and <see cref="PendingMerges"/> are exposed so that
/// <see cref="PartitionMapService.InitializePartitions"/> can seed crash-recovery entries
/// without taking any additional ownership of the split/merge lifecycle.
/// </para>
/// </summary>
internal sealed class SplitMergeController
{
    private readonly Dictionary<int, SplitInProgress> _pendingSplits = new();
    private readonly Dictionary<int, MergeInProgress> _pendingMerges = new();

    private readonly Dictionary<string, string> systemConfiguration;
    private readonly Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate;
    private readonly Func<int, CancellationToken, Task<RaftReplicationResult>> replicateCheckpoint;
    private readonly Action<RaftSystemRequest> send;
    private readonly Action<List<RaftPartitionRange>> startPartitions;
    private readonly Func<IRaftStateMachineTransfer?> getStateMachineTransfer;
    private readonly Func<int, long> getMaxLog;
    private readonly Func<int, RaftPartition?> getPartition;
    private readonly Action<int> removePartition;
    private readonly Action<int> deletePartitionWAL;
    private readonly Func<TimeSpan> getRetryDelay;
    private readonly int maxRetries;
    private readonly ILogger<IRaft> logger;

    internal Dictionary<int, SplitInProgress> PendingSplits => _pendingSplits;
    internal Dictionary<int, MergeInProgress> PendingMerges => _pendingMerges;

    internal SplitMergeController(
        Dictionary<string, string> systemConfiguration,
        Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate,
        Func<int, CancellationToken, Task<RaftReplicationResult>> replicateCheckpoint,
        Action<RaftSystemRequest> send,
        Action<List<RaftPartitionRange>> startPartitions,
        Func<IRaftStateMachineTransfer?> getStateMachineTransfer,
        Func<int, long> getMaxLog,
        Func<int, RaftPartition?> getPartition,
        Action<int> removePartition,
        Action<int> deletePartitionWAL,
        Func<TimeSpan> getRetryDelay,
        int maxRetries,
        ILogger<IRaft> logger)
    {
        this.systemConfiguration = systemConfiguration;
        this.replicate = replicate;
        this.replicateCheckpoint = replicateCheckpoint;
        this.send = send;
        this.startPartitions = startPartitions;
        this.getStateMachineTransfer = getStateMachineTransfer;
        this.getMaxLog = getMaxLog;
        this.getPartition = getPartition;
        this.removePartition = removePartition;
        this.deletePartitionWAL = deletePartitionWAL;
        this.getRetryDelay = getRetryDelay;
        this.maxRetries = maxRetries;
        this.logger = logger;
    }

    /// <summary>
    /// Phase 1 of the two-phase split: marks both source and new-target as
    /// <see cref="RaftPartitionState.Splitting"/>, replicates the map, then
    /// either auto-enqueues Phase 2 (when <see cref="RaftSplitPlan.AutoCommit"/>
    /// is true) or leaves the split pending for the caller to commit explicitly.
    /// </summary>
    internal async Task TrySplitPartition(
        int partitionId,
        RaftSplitPlan? plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TrySplitPartition: Failed to get partitions from system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TrySplitPartition: Failed to parse partition map {Partitions}", partitions);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        List<RaftPartitionRange> ranges = map.Partitions;

        RaftPartitionRange? partitionRange = ranges.FirstOrDefault(r => r.PartitionId == partitionId);
        if (partitionRange is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find partition range {Partition}", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (partitionRange.State == RaftPartitionState.Splitting)
        {
            logger.LogWarning(
                "TrySplitPartition: Partition {Id} is already in Splitting state; rejecting duplicate request",
                partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
            return;
        }

        if (partitionRange.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TrySplitPartition: Partition {Id} cannot be split (State={State})",
                partitionId, partitionRange.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
            return;
        }

        RaftPartitionRange? maxPartition = ranges.MaxBy(r => r.PartitionId);
        if (maxPartition is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find next partition");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        int targetPartitionId = (plan?.TargetPartitionId > 0)
            ? plan.TargetPartitionId
            : maxPartition.PartitionId + 1;

        if (ranges.Any(r => r.PartitionId == targetPartitionId))
        {
            logger.LogError(
                "TrySplitPartition: Target partition id {Id} already exists in map",
                targetPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
            return;
        }

        RaftRoutingMode targetRoutingMode = plan?.TargetRoutingMode ?? RaftRoutingMode.HashRange;

        int originalEnd = partitionRange.EndRange;

        int splitBoundary;
        if (targetRoutingMode == RaftRoutingMode.HashRange)
        {
            splitBoundary = plan?.HashBoundary
                ?? (partitionRange.StartRange + (partitionRange.EndRange - partitionRange.StartRange) / 2);

            if (splitBoundary <= partitionRange.StartRange || splitBoundary > partitionRange.EndRange)
            {
                logger.LogError(
                    "TrySplitPartition: HashBoundary {B} is outside ({S},{E}]",
                    splitBoundary, partitionRange.StartRange, partitionRange.EndRange);
                completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
                return;
            }

            partitionRange.EndRange = splitBoundary - 1;
        }
        else
        {
            splitBoundary = 0;
        }

        partitionRange.Generation++;
        partitionRange.State = RaftPartitionState.Splitting;

        RaftPartitionRange newRange = new()
        {
            PartitionId = targetPartitionId,
            StartRange  = targetRoutingMode == RaftRoutingMode.HashRange ? splitBoundary : 0,
            EndRange    = targetRoutingMode == RaftRoutingMode.HashRange ? originalEnd   : 0,
            Generation  = 1,
            State       = RaftPartitionState.Splitting,
            RoutingMode = targetRoutingMode,
        };

        ranges.Add(newRange);
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySplitPartition aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await replicate(
                RaftSystemConfig.RaftLogType,
                RaftSystemCoordinator.Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TrySplitPartition: Phase 1 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TrySplitPartition delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInfoSplitPartitionPhase1Committed(partitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        startPartitions(ranges);

        if (getStateMachineTransfer() is { } transfer)
        {
            RaftSplitPlan transferPlan = new()
            {
                TargetPartitionId = targetPartitionId,
                TargetRoutingMode = targetRoutingMode,
                HashBoundary      = targetRoutingMode == RaftRoutingMode.HashRange ? splitBoundary : null,
                AutoCommit        = false,
            };
            await RunSnapshotTransferAsync(partitionId, targetPartitionId, transferPlan, transfer, cancellationToken).ConfigureAwait(false);
        }

        _pendingSplits[partitionId] = new SplitInProgress(targetPartitionId, completion);

        if (plan?.AutoCommit == true)
            send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, partitionId));
    }

    /// <summary>
    /// Phase 2 of the two-phase split: transitions both source and target from
    /// <see cref="RaftPartitionState.Splitting"/> to <see cref="RaftPartitionState.Active"/>.
    /// </summary>
    internal async Task TrySplitPartitionCommit(int sourcePartitionId, CancellationToken cancellationToken)
    {
        if (!_pendingSplits.TryGetValue(sourcePartitionId, out SplitInProgress? split))
        {
            logger.LogError(
                "TrySplitPartitionCommit: No pending split for partition {Partition}", sourcePartitionId);
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TrySplitPartitionCommit: Failed to get partitions from system configuration");
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TrySplitPartitionCommit: Failed to parse partition map");
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? sourceRange = map.Partitions.FirstOrDefault(r => r.PartitionId == sourcePartitionId);
        if (sourceRange is null)
        {
            logger.LogError(
                "TrySplitPartitionCommit: Source partition {Id} not found", sourcePartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? targetRange = map.Partitions.FirstOrDefault(r => r.PartitionId == split.TargetPartitionId);
        if (targetRange is null)
        {
            logger.LogError(
                "TrySplitPartitionCommit: Target partition {Id} not found", split.TargetPartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (sourceRange.State != RaftPartitionState.Splitting || targetRange.State != RaftPartitionState.Splitting)
        {
            logger.LogWarning(
                "TrySplitPartitionCommit: Unexpected states (source={SrcState}, target={TgtState}); skipping duplicate Phase 2",
                sourceRange.State, targetRange.State);
            _pendingSplits.Remove(sourcePartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Success, targetRange.Generation));
            return;
        }

        sourceRange.Generation++;
        sourceRange.State = RaftPartitionState.Active;
        targetRange.Generation++;
        targetRange.State = RaftPartitionState.Active;
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySplitPartitionCommit aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await replicate(
                RaftSystemConfig.RaftLogType,
                RaftSystemCoordinator.Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TrySplitPartitionCommit: Phase 2 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TrySplitPartitionCommit delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                split.Completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInfoSplitPartitionCommitPhase2Committed(sourcePartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        _pendingSplits.Remove(sourcePartitionId);
        startPartitions(map.Partitions);
        split.Completion?.TrySetResult((RaftOperationStatus.Success, targetRange.Generation));
    }

    /// <summary>
    /// Phase 1 of the two-phase merge: marks the source partition as
    /// <see cref="RaftPartitionState.Draining"/> and replicates the updated map.
    /// Phase 2 is auto-enqueued via <see cref="RaftSystemRequestType.MergePartitionCommit"/>.
    /// </summary>
    internal async Task TryMergePartitions(
        RaftMergePlan plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryMergePartitions: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryMergePartitions: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? src = map.Partitions.FirstOrDefault(r => r.PartitionId == plan.SourcePartitionId);
        if (src is null)
        {
            logger.LogError("TryMergePartitions: Source partition {Id} not found", plan.SourcePartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? surv = map.Partitions.FirstOrDefault(r => r.PartitionId == plan.SurvivorPartitionId);
        if (surv is null)
        {
            logger.LogError("TryMergePartitions: Survivor partition {Id} not found", plan.SurvivorPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (src.State == RaftPartitionState.Draining)
        {
            logger.LogWarning(
                "TryMergePartitions: Source partition {Id} is already Draining; rejecting duplicate merge request",
                plan.SourcePartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, src.Generation));
            return;
        }

        if (src.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TryMergePartitions: Source partition {Id} is not Active (State={State})",
                plan.SourcePartitionId, src.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, src.Generation));
            return;
        }

        if (surv.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TryMergePartitions: Survivor partition {Id} is not Active (State={State})",
                plan.SurvivorPartitionId, surv.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, surv.Generation));
            return;
        }

        bool adjacent =
            (src.RoutingMode == RaftRoutingMode.Unrouted && surv.RoutingMode == RaftRoutingMode.Unrouted)
            || (src.RoutingMode == RaftRoutingMode.HashRange && surv.RoutingMode == RaftRoutingMode.HashRange
                && (src.EndRange + 1 == surv.StartRange || surv.EndRange + 1 == src.StartRange));

        if (!adjacent)
        {
            logger.LogError(
                "TryMergePartitions: Partitions {Src} and {Surv} are not adjacent",
                plan.SourcePartitionId, plan.SurvivorPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        src.State = RaftPartitionState.Draining;
        src.Generation++;
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryMergePartitions aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await replicate(
                RaftSystemConfig.RaftLogType, RaftSystemCoordinator.Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryMergePartitions: Phase 1 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryMergePartitions delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8) continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInfoMergePartitionsPhase1Committed(plan.SourcePartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        startPartitions(map.Partitions);

        _pendingMerges[plan.SourcePartitionId] = new MergeInProgress(plan.SurvivorPartitionId, completion);
        send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, plan.SourcePartitionId));
    }

    /// <summary>
    /// Phase 2 of the two-phase merge: absorbs the source range into the survivor,
    /// marks the source as <see cref="RaftPartitionState.Removed"/>, replicates,
    /// then stops the source partition and reclaims its WAL.
    /// </summary>
    internal async Task TryMergePartitionCommit(int sourcePartitionId, CancellationToken cancellationToken)
    {
        if (!_pendingMerges.TryGetValue(sourcePartitionId, out MergeInProgress? merge))
        {
            logger.LogError(
                "TryMergePartitionCommit: No pending merge for source partition {Id}", sourcePartitionId);
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryMergePartitionCommit: No partition map in system configuration");
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryMergePartitionCommit: Failed to parse partition map");
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? src = map.Partitions.FirstOrDefault(r => r.PartitionId == sourcePartitionId);
        if (src is null)
        {
            logger.LogError("TryMergePartitionCommit: Source partition {Id} not found", sourcePartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? surv = map.Partitions.FirstOrDefault(r => r.PartitionId == merge.SurvivorPartitionId);
        if (surv is null)
        {
            logger.LogError(
                "TryMergePartitionCommit: Survivor partition {Id} not found", merge.SurvivorPartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (src.State != RaftPartitionState.Draining)
        {
            logger.LogWarning(
                "TryMergePartitionCommit: Source partition {Id} is not Draining (State={State}); skipping duplicate Phase 2",
                sourcePartitionId, src.State);
            _pendingMerges.Remove(sourcePartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Success, surv.Generation));
            return;
        }

        if (surv.RoutingMode == RaftRoutingMode.HashRange)
        {
            surv.StartRange = Math.Min(surv.StartRange, src.StartRange);
            surv.EndRange   = Math.Max(surv.EndRange,   src.EndRange);
        }

        surv.Generation++;
        surv.State = RaftPartitionState.Active;

        src.State = RaftPartitionState.Removed;
        src.Generation++;

        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryMergePartitionCommit aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await replicate(
                RaftSystemConfig.RaftLogType, RaftSystemCoordinator.Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryMergePartitionCommit: Phase 2 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryMergePartitionCommit delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8) continue;
                }

                merge.Completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInfoMergePartitionCommitPhase2Committed(sourcePartitionId, merge.SurvivorPartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        _pendingMerges.Remove(sourcePartitionId);

        if (getPartition(sourcePartitionId) is { } livePartition)
        {
            await livePartition.DrainAsync(cancellationToken).ConfigureAwait(false);
            livePartition.Stop();
            removePartition(sourcePartitionId);
        }

        deletePartitionWAL(sourcePartitionId);

        startPartitions(map.Partitions);

        merge.Completion?.TrySetResult((RaftOperationStatus.Success, surv.Generation));
    }

    /// <summary>
    /// Executes the snapshot-transfer step of a split:
    /// exports a point-in-time state snapshot from the source partition, imports it into
    /// the target partition, then replicates a checkpoint into the target partition's log
    /// so all target replicas converge on the imported state.
    /// <para>
    /// This is an optimization: on error the split remains valid and continues via the
    /// log-shipping fallback. The caller is responsible for data movement before Phase 2.
    /// </para>
    /// </summary>
    private async Task RunSnapshotTransferAsync(
        int sourcePartitionId,
        int targetPartitionId,
        RaftSplitPlan plan,
        IRaftStateMachineTransfer transfer,
        CancellationToken cancellationToken)
    {
        long snapshotIndex = getMaxLog(sourcePartitionId);

        Stream snapshot;
        try
        {
            snapshot = await transfer.ExportRange(plan, snapshotIndex, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "TrySplitPartition: ExportRange source={Src} → target={Tgt} failed: {Message}; falling back to log-shipping",
                sourcePartitionId, targetPartitionId, ex.Message);
            return;
        }

        try
        {
            await using (snapshot.ConfigureAwait(false))
                await transfer.ImportRange(targetPartitionId, snapshot, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "TrySplitPartition: ImportRange target={Tgt} failed: {Message}; falling back to log-shipping",
                targetPartitionId, ex.Message);
            return;
        }

        RaftReplicationResult checkpointResult =
            await replicateCheckpoint(targetPartitionId, cancellationToken).ConfigureAwait(false);

        if (checkpointResult.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning(
                "TrySplitPartition: ReplicateCheckpoint target={Tgt} failed ({Status}); retrying once",
                targetPartitionId, checkpointResult.Status);

            checkpointResult = await replicateCheckpoint(targetPartitionId, cancellationToken).ConfigureAwait(false);
        }

        if (checkpointResult.Status != RaftOperationStatus.Success)
            logger.LogError(
                "TrySplitPartition: ReplicateCheckpoint target={Tgt} failed after retry ({Status}); " +
                "snapshot was imported locally but followers are diverged — manual intervention required",
                targetPartitionId, checkpointResult.Status);
        else
            logger.LogInfoSplitPartitionSnapshotTransferComplete(sourcePartitionId, snapshotIndex, targetPartitionId);
    }
}
