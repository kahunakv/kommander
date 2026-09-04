
using System.Collections.Concurrent;
using System.Text.Json;
using Kommander.Data;
using Kommander.Gossip;
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
    private readonly ConcurrentDictionary<string, string> systemConfiguration;
    private readonly Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate;
    private readonly Action<RaftSystemRequest> send;
    private readonly Action<List<RaftPartitionRange>> startPartitions;
    private readonly Action<int> deletePartitionWAL;
    private readonly Func<int, RaftPartition?> getPartition;
    private readonly Action<int> removePartition;
    private readonly int initialPartitions;
    private readonly RaftConfiguration configuration;
    private readonly Func<List<RaftNode>> getDiscoveredNodes;
    private readonly string localEndpoint;
    private readonly int localNodeId;
    private readonly Func<CancellationToken, Task> seedInitialMembership;
    private readonly Func<ClusterMembership> getMembership;
    private readonly Func<string, string?> getNodeZone;
    private readonly Func<string, MemberLivenessState> getNodeLiveness;
    private readonly Dictionary<int, SplitInProgress> pendingSplits;
    private readonly Dictionary<int, MergeInProgress> pendingMerges;
    private readonly Func<TimeSpan> getRetryDelay;
    private readonly int maxRetries;
    private readonly ILogger<IRaft> logger;

    internal PartitionMapService(
        ConcurrentDictionary<string, string> systemConfiguration,
        Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate,
        Action<RaftSystemRequest> send,
        Action<List<RaftPartitionRange>> startPartitions,
        Action<int> deletePartitionWAL,
        Func<int, RaftPartition?> getPartition,
        Action<int> removePartition,
        int initialPartitions,
        RaftConfiguration configuration,
        Func<List<RaftNode>> getDiscoveredNodes,
        string localEndpoint,
        int localNodeId,
        Func<CancellationToken, Task> seedInitialMembership,
        Func<ClusterMembership> getMembership,
        Func<string, string?> getNodeZone,
        Func<string, MemberLivenessState> getNodeLiveness,
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
        this.configuration = configuration;
        this.getDiscoveredNodes = getDiscoveredNodes;
        this.localEndpoint = localEndpoint;
        this.localNodeId = localNodeId;
        this.seedInitialMembership = seedInitialMembership;
        this.getMembership = getMembership;
        this.getNodeZone = getNodeZone;
        this.getNodeLiveness = getNodeLiveness;
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

        AssignInitialPlacement(initialRanges);

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

                // NodeIsNotLeader: someone else leads — stop, they own this. ProposalOutcomeUnknown:
                // this node lost authority with the proposal in flight — the entry may still commit
                // via the new leader's §5.4.2 inherited commit, so a blind re-propose risks a
                // duplicate; stop for the same reason.
                if (result.Status is RaftOperationStatus.NodeIsNotLeader or RaftOperationStatus.ProposalOutcomeUnknown)
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

    /// <summary>
    /// When <see cref="RaftConfiguration.ReplicationFactor"/> &gt; 0, assigns each initial range
    /// an RF-sized replica set (all Voter — the founding configuration needs no catch-up) spread
    /// round-robin across the nodes visible at bootstrap (discovery peers plus self, the same
    /// set the membership seed captures). A cluster with fewer nodes than the factor leaves the
    /// replica sets empty — the RF floor degrades to legacy full replication with no loss of
    /// safety, and the placement controller expands placement as voters join.
    /// </summary>
    private void AssignInitialPlacement(List<RaftPartitionRange> ranges)
    {
        if (configuration.ReplicationFactor <= 0)
            return;

        List<Placement.CandidateNode> nodes =
        [
            new() { Endpoint = localEndpoint, NodeId = localNodeId, Zone = configuration.Zone },
            ..getDiscoveredNodes()
                .Where(n => n.Endpoint != localEndpoint)
                .DistinctBy(n => n.Endpoint)
                // Remote zones are best-effort at bootstrap: they come from gossiped load
                // reports, which may not have arrived yet during assembly. Nodes with no known
                // zone still get assigned; the rebalancer improves the spread later as reports
                // flow.
                .Select(n => new Placement.CandidateNode { Endpoint = n.Endpoint, Zone = getNodeZone(n.Endpoint) })
        ];

        Dictionary<int, List<Placement.CandidateNode>> assignment = Placement.PlacementPlanner.AssignInitial(
            ranges.Select(r => r.PartitionId).ToList(),
            nodes,
            configuration.ReplicationFactor);

        if (assignment.Count == 0)
        {
            logger.LogWarning(
                "AssignInitialPlacement: cluster has {Nodes} node(s), replication factor {Rf} — degrading to full replication",
                nodes.Count, configuration.ReplicationFactor);
            return;
        }

        foreach (RaftPartitionRange range in ranges)
        {
            if (!assignment.TryGetValue(range.PartitionId, out List<Placement.CandidateNode>? replicas))
                continue;

            range.Replicas = replicas.Select(n => new RaftReplica
            {
                Endpoint = n.Endpoint,
                NodeId = n.NodeId,
                Role = RaftReplicaRole.Voter,
                SinceGeneration = range.Generation
            }).ToList();
        }
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

        RaftPartitionRange newRange = new()
        {
            PartitionId = message.PartitionId,
            StartRange = newStart,
            EndRange = newEnd,
            Generation = 1,
            State = RaftPartitionState.Active,
            RoutingMode = message.RoutingMode,
            Replicas = PickReplicasForNewRange(map)
        };
        map.Partitions.Add(newRange);
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

    /// <summary>
    /// Chooses the replica set for a dynamically-created partition when
    /// <see cref="RaftConfiguration.ReplicationFactor"/> &gt; 0: the RF least-loaded committed
    /// roster voters (replica load counted from the current map), founding configuration all
    /// Voter. Returns an empty list — legacy full replication — when RF is 0 or the voter count
    /// does not exceed RF.
    /// <para>
    /// Voters whose SWIM liveness is not <see cref="MemberLivenessState.Alive"/> sort behind
    /// every live voter (the local endpoint always counts as alive, mirroring
    /// <see cref="ReplicaPlacementService.RunPlacementPassAsync"/>). Without this gate a down
    /// node's frozen load count made it the least-loaded voter over time, so every fresh
    /// partition was founded with a dead member deterministically first in its replica set.
    /// When fewer than RF voters are alive the set is backfilled with non-alive voters rather
    /// than short-placed or degraded: the founding group stays RF-sized, quorum arithmetic is
    /// unchanged from the pre-gate behavior, and a transient liveness dip cannot flip creation
    /// into full replication. Liveness is gossip-fed and can lag a failure by seconds, so the
    /// resulting set is best-effort — the placement rebalancer repairs any stale pick later.
    /// </para>
    /// </summary>
    private List<RaftReplica> PickReplicasForNewRange(RaftPartitionMap map)
    {
        int rf = configuration.ReplicationFactor;
        if (rf <= 0)
            return [];

        List<ClusterMember> voters = getMembership().Members
            .Where(m => m.Role == ClusterMemberRole.Voter)
            .ToList();

        if (voters.Count <= rf)
            return [];

        Dictionary<string, int> load = voters.ToDictionary(v => v.Endpoint, _ => 0, StringComparer.Ordinal);
        foreach (RaftPartitionRange range in map.Partitions)
        {
            if (range.State == RaftPartitionState.Removed)
                continue;

            foreach (RaftReplica replica in range.Replicas)
            {
                if (load.TryGetValue(replica.Endpoint, out int count))
                    load[replica.Endpoint] = count + 1;
            }
        }

        Dictionary<string, bool> alive = voters.ToDictionary(
            v => v.Endpoint,
            v => v.Endpoint == localEndpoint || getNodeLiveness(v.Endpoint) == MemberLivenessState.Alive,
            StringComparer.Ordinal);

        List<ClusterMember> picked = voters
            .OrderByDescending(v => alive[v.Endpoint])
            .ThenBy(v => load[v.Endpoint])
            .ThenBy(v => v.Endpoint, StringComparer.Ordinal)
            .Take(rf)
            .ToList();

        List<string> backfilled = picked.Where(v => !alive[v.Endpoint]).Select(v => v.Endpoint).ToList();
        if (backfilled.Count > 0)
            logger.LogWarning(
                "PickReplicasForNewRange: only {Live} of {Rf} required voters are alive; backfilled replica set with non-alive voters [{Endpoints}]",
                rf - backfilled.Count, rf, string.Join(",", backfilled));

        return picked
            .Select(v => new RaftReplica
            {
                Endpoint = v.Endpoint,
                NodeId = v.NodeId,
                Role = RaftReplicaRole.Voter,
                SinceGeneration = 1
            })
            .ToList();
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
