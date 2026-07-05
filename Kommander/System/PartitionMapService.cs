
using System.Text.Json;
using Kommander.Data;
using Kommander.Logging;
using Kommander.System.Protos;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.System;

/// <summary>
/// Manages the partition map lifecycle for <see cref="RaftSystemCoordinator"/>: bootstrap
/// (<see cref="TrySetInitialPartitions"/>), map propagation (<see cref="InitializePartitions"/>),
/// and the atomic create/remove operations (<see cref="TryCreatePartition"/>,
/// <see cref="TryRemovePartition"/>).
/// <para>
/// Runs exclusively on the coordinator's single-reader channel loop — no locking is needed
/// because only one <see cref="Receive"/> call executes at a time.
/// </para>
/// <para>
/// <see cref="InitializePartitions"/> also seeds crash-recovery entries into
/// <see cref="SplitMergeController.PendingSplits"/> and
/// <see cref="SplitMergeController.PendingMerges"/> because both actions (map boot and
/// phase-2 re-enqueue) must observe the same decoded <see cref="RaftPartitionMap"/> atomically.
/// Those dictionaries are owned by <see cref="SplitMergeController"/> and passed in here
/// by reference; no locking is required because both services share the channel thread.
/// </para>
/// </summary>
internal sealed class PartitionMapService
{
    private readonly Dictionary<string, string> systemConfiguration;
    private readonly Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate;
    private readonly Action<RaftSystemRequest> send;
    private readonly Action<List<RaftPartitionRange>> startPartitions;
    private readonly Action<int> deletePartitionWAL;
    private readonly Func<int, RaftPartition?> getPartition;
    private readonly Action<int> removePartition;
    private readonly int initialPartitions;
    private readonly Func<CancellationToken, Task> seedInitialMembership;
    private readonly Dictionary<int, SplitInProgress> pendingSplits;
    private readonly Dictionary<int, MergeInProgress> pendingMerges;
    private readonly Func<TimeSpan> getRetryDelay;
    private readonly int maxRetries;
    private readonly ILogger<IRaft> logger;

    internal PartitionMapService(
        Dictionary<string, string> systemConfiguration,
        Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate,
        Action<RaftSystemRequest> send,
        Action<List<RaftPartitionRange>> startPartitions,
        Action<int> deletePartitionWAL,
        Func<int, RaftPartition?> getPartition,
        Action<int> removePartition,
        int initialPartitions,
        Func<CancellationToken, Task> seedInitialMembership,
        Dictionary<int, SplitInProgress> pendingSplits,
        Dictionary<int, MergeInProgress> pendingMerges,
        Func<TimeSpan> getRetryDelay,
        int maxRetries,
        ILogger<IRaft> logger)
    {
        this.systemConfiguration = systemConfiguration;
        this.replicate = replicate;
        this.send = send;
        this.startPartitions = startPartitions;
        this.deletePartitionWAL = deletePartitionWAL;
        this.getPartition = getPartition;
        this.removePartition = removePartition;
        this.initialPartitions = initialPartitions;
        this.seedInitialMembership = seedInitialMembership;
        this.pendingSplits = pendingSplits;
        this.pendingMerges = pendingMerges;
        this.getRetryDelay = getRetryDelay;
        this.maxRetries = maxRetries;
        this.logger = logger;
    }

    /// <param name="crashRecovery">
    /// When <see langword="true"/>, re-enqueues Phase 2 for any Splitting or Draining
    /// partition found in the map (crash-recovery path). Must only be set when this node
    /// is becoming the leader (<c>LeaderChanged</c>) or completing WAL restore
    /// (<c>RestoreCompleted</c>). Must be <see langword="false"/> for live replication
    /// events (<c>ConfigReplicated</c>) because followers must never drive Phase 2 commits,
    /// and leaders already track in-progress operations via <c>_pendingSplits</c> /
    /// <c>_pendingMerges</c>.
    /// </param>
    internal void InitializePartitions(bool crashRecovery = false)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogDebug("InitializePartitions: partition map not yet available in system configuration; awaiting replication");
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("InitializePartitions: Failed to parse partition map: {Partitions}", partitions);
            return;
        }

        startPartitions(map.Partitions);

        foreach (RaftPartitionRange range in map.Partitions)
        {
            if (range.State == RaftPartitionState.Removed)
                deletePartitionWAL(range.PartitionId);
        }

        if (!crashRecovery)
            return;

        List<RaftPartitionRange> splitting = map.Partitions
            .Where(r => r.State == RaftPartitionState.Splitting)
            .ToList();

        foreach (RaftPartitionRange target in splitting.Where(r => r.Generation == 1))
        {
            RaftPartitionRange? source = target.RoutingMode == RaftRoutingMode.HashRange
                ? splitting.FirstOrDefault(r =>
                    r.PartitionId != target.PartitionId &&
                    r.EndRange + 1 == target.StartRange)
                : splitting.FirstOrDefault(r =>
                    r.PartitionId != target.PartitionId &&
                    r.RoutingMode == RaftRoutingMode.Unrouted);

            if (source is not null && !pendingSplits.ContainsKey(source.PartitionId))
            {
                pendingSplits[source.PartitionId] = new SplitInProgress(target.PartitionId, null);
                send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, source.PartitionId));
            }
        }

        foreach (RaftPartitionRange src in map.Partitions.Where(r => r.State == RaftPartitionState.Draining))
        {
            if (pendingMerges.ContainsKey(src.PartitionId))
                continue;

            RaftPartitionRange? survivor = src.RoutingMode == RaftRoutingMode.HashRange
                ? map.Partitions.FirstOrDefault(r =>
                    r.State == RaftPartitionState.Active &&
                    r.RoutingMode == RaftRoutingMode.HashRange &&
                    (src.EndRange + 1 == r.StartRange || r.EndRange + 1 == src.StartRange))
                : map.Partitions.FirstOrDefault(r =>
                    r.State == RaftPartitionState.Active &&
                    r.RoutingMode == RaftRoutingMode.Unrouted &&
                    r.PartitionId != src.PartitionId);

            if (survivor is null)
            {
                logger.LogWarning(
                    "InitializePartitions: Draining partition {Id} has no adjacent Active survivor; merge cannot be resumed",
                    src.PartitionId);
                continue;
            }

            logger.LogInfoReEnqueuingMergePartitionCommit(src.PartitionId, survivor.PartitionId);

            pendingMerges[src.PartitionId] = new MergeInProgress(survivor.PartitionId, null);
            send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, src.PartitionId));
        }
    }

    internal async Task TrySetInitialPartitions(CancellationToken cancellationToken)
    {
        if (systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            RaftPartitionMap? existingMap = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
            if (existingMap is not null)
            {
                RaftSystemMessage reassert = new()
                {
                    Key = RaftSystemConfigKeys.Partitions,
                    Value = partitions
                };

                try
                {
                    RaftReplicationResult result = await replicate(
                        RaftSystemConfig.RaftLogType,
                        RaftSystemCoordinator.Serialize(reassert),
                        true,
                        cancellationToken
                    ).ConfigureAwait(false);

                    if (result.Status != RaftOperationStatus.Success)
                        logger.LogWarning(
                            "[RaftSystemCoordinator] Failed to re-assert existing partition map to followers: {Status}",
                            result.Status);
                }
                catch (OperationCanceledException)
                {
                    logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions re-assert aborted on shutdown");
                }

                startPartitions(existingMap.Partitions);
                await seedInitialMembership(cancellationToken).ConfigureAwait(false);
                return;
            }
        }

        List<RaftPartitionRange> initialRanges = RaftSystemCoordinator.DivideIntoRanges(initialPartitions);

        RaftPartitionMap newMap = new() { MapVersion = 1, Partitions = initialRanges };

        RaftSystemMessage message = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(newMap)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions aborted on shutdown");
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
                    "Failed to replicate initial partitions {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status == RaftOperationStatus.NodeIsNotLeader)
                    return;

                try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException)
                {
                    logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions delay aborted on shutdown");
                    return;
                }
                if (i <= 8)
                    continue;

                logger.LogError(
                    "Cannot continue without initial partitions {Status} {LogIndex}",
                    result.Status, result.LogIndex);
                Environment.Exit(1);
                return;
            }

            logger.LogInfoSuccessfullyReplicatedInitialPartitions(result.Status, result.LogIndex);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        startPartitions(initialRanges);
        await seedInitialMembership(cancellationToken).ConfigureAwait(false);
    }

    internal async Task TryCreatePartition(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (message.PartitionId == RaftSystemConfig.SystemPartition)
        {
            logger.LogWarning("TryCreatePartition: System partition (id=0) cannot be created");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryCreatePartition: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryCreatePartition: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? existing = map.Partitions.FirstOrDefault(r => r.PartitionId == message.PartitionId);
        if (existing is not null)
        {
            if (existing.State == RaftPartitionState.Active)
            {
                logger.LogInfoCreatePartitionAlreadyActive(message.PartitionId, existing.Generation);
                completion?.TrySetResult((RaftOperationStatus.Success, existing.Generation));
                return;
            }

            logger.LogError(
                "TryCreatePartition: Partition {Id} exists with non-active state {State}",
                message.PartitionId, existing.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        int newStart = 0, newEnd = 0;
        if (message.RoutingMode == RaftRoutingMode.HashRange)
        {
            if (message.HashRangeStart is null || message.HashRangeEnd is null)
            {
                logger.LogError("TryCreatePartition: HashRange mode requires start and end");
                completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                return;
            }

            newStart = message.HashRangeStart.Value;
            newEnd = message.HashRangeEnd.Value;

            if (newStart > newEnd)
            {
                logger.LogError(
                    "TryCreatePartition: Invalid HashRange [{Start},{End}]",
                    newStart, newEnd);
                completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                return;
            }

            foreach (RaftPartitionRange range in map.Partitions)
            {
                if (range.RoutingMode != RaftRoutingMode.HashRange)
                    continue;
                if (newStart <= range.EndRange && newEnd >= range.StartRange)
                {
                    logger.LogError(
                        "TryCreatePartition: [{Start},{End}] overlaps partition {Id} [{RStart},{REnd}]",
                        newStart, newEnd, range.PartitionId, range.StartRange, range.EndRange);
                    completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                    return;
                }
            }
        }

        map.Partitions.Add(new RaftPartitionRange
        {
            PartitionId = message.PartitionId,
            StartRange = newStart,
            EndRange = newEnd,
            Generation = 1,
            State = RaftPartitionState.Active,
            RoutingMode = message.RoutingMode
        });
        map.MapVersion++;

        RaftSystemMessage sysMessage = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryCreatePartition aborted on shutdown");
                completion?.TrySetCanceled(cancellationToken);
                return;
            }

            RaftReplicationResult result = await replicate(
                RaftSystemConfig.RaftLogType,
                RaftSystemCoordinator.Serialize(sysMessage),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryCreatePartition: Replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryCreatePartition delay aborted on shutdown");
                        completion?.TrySetCanceled(cancellationToken);
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = sysMessage.Value;
        startPartitions(map.Partitions);
        completion?.TrySetResult((RaftOperationStatus.Success, 1));
    }

    internal async Task TryRemovePartition(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;
        int partitionId = message.PartitionId;

        if (partitionId == RaftSystemConfig.SystemPartition)
        {
            logger.LogWarning("TryRemovePartition: System partition (id=0) cannot be removed");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryRemovePartition: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryRemovePartition: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? entry = map.Partitions.FirstOrDefault(r => r.PartitionId == partitionId);
        if (entry is null)
        {
            logger.LogError("TryRemovePartition: Partition {Id} not found in partition map", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (entry.State == RaftPartitionState.Removed)
        {
            logger.LogInfoRemovePartitionAlreadyRemoved(partitionId);
            completion?.TrySetResult((RaftOperationStatus.Success, entry.Generation));
            deletePartitionWAL(partitionId);
            return;
        }

        if (entry.State is RaftPartitionState.Splitting or RaftPartitionState.Draining)
        {
            logger.LogError(
                "TryRemovePartition: Partition {Id} is in {State} state; complete or abort the pending phase first",
                partitionId, entry.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        entry.State = RaftPartitionState.Removed;
        entry.Generation++;
        map.MapVersion++;

        RaftSystemMessage sysMessage = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryRemovePartition aborted on shutdown");
                completion?.TrySetCanceled(cancellationToken);
                return;
            }

            RaftReplicationResult result = await replicate(
                RaftSystemConfig.RaftLogType,
                RaftSystemCoordinator.Serialize(sysMessage),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryRemovePartition: Replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryRemovePartition delay aborted on shutdown");
                        completion?.TrySetCanceled(cancellationToken);
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = sysMessage.Value;

        if (getPartition(partitionId) is { } livePartition)
        {
            await livePartition.DrainAsync(cancellationToken).ConfigureAwait(false);
            livePartition.Stop();
            removePartition(partitionId);
        }

        deletePartitionWAL(partitionId);

        logger.LogInfoRemovePartitionReclaimedWal(partitionId);

        startPartitions(map.Partitions);

        completion?.TrySetResult((RaftOperationStatus.Success, entry.Generation));
    }
}
