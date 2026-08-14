
using System.Collections.Concurrent;
using System.Text.Json;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.System.Placement;
using Kommander.System.Protos;
using Kommander.Logging;
using Microsoft.Extensions.Logging;

namespace Kommander.System;

/// <summary>
/// Owns per-partition replica placement for <see cref="RaftSystemCoordinator"/>: the committed
/// replica-lifecycle mutations (<see cref="TryAddReplica"/>, <see cref="TryPromoteReplica"/>,
/// <see cref="TryRemoveReplica"/>, <see cref="TrySetReplicationFactor"/>) and the P0 placement
/// controller pass (<see cref="RunPlacementPassAsync"/>).
/// <para>
/// Runs exclusively on the coordinator's single-reader channel loop, which is what enforces the
/// two safety disciplines placement depends on: <b>single mover per range</b> (a range never has
/// two transitional replicas, so successive committed configurations of the range always overlap
/// by a quorum — Raft §6 applied per group) and serialization against roster and split/merge
/// changes (they share the loop, so they compose without racing).
/// </para>
/// <para>
/// The controller holds only advisory in-memory state (learner catch-up timestamps); after a P0
/// leader change the new leader re-derives everything from the committed map — a Learner replica
/// resumes promotion tracking, a Removing replica is re-driven to its final drop.
/// </para>
/// </summary>
internal sealed class ReplicaPlacementService
{
    private readonly ConcurrentDictionary<string, string> systemConfiguration;
    private readonly Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate;
    private readonly Action<RaftSystemRequest> send;
    private readonly Action<List<RaftPartitionRange>> startPartitions;
    private readonly Func<ClusterMembership> getMembership;
    private readonly Func<int, ValueTask<bool>> amILeaderQuick;
    private readonly Func<int, string?> getPartitionLeader;
    private readonly Func<int, string, ValueTask<long?>> getFollowerCommitted;
    private readonly Func<RaftNode, int, string, Task<long?>> getRemoteFollowerLag;
    private readonly Func<string, MemberLivenessState> getNodeLiveness;
    private readonly Func<string, string?> getNodeZone;
    private readonly Func<int, string, CancellationToken, Task> transferLeadership;
    private readonly RaftConfiguration configuration;
    private readonly string localEndpoint;
    private readonly Func<TimeSpan> getRetryDelay;
    private readonly int maxRetries;
    private readonly ILogger<IRaft> logger;

    // Keyed by (partitionId, endpoint): when the learner replica first appeared caught up.
    // Advisory only — cleared on P0 leadership loss and rebuilt from observation.
    private readonly Dictionary<(int PartitionId, string Endpoint), DateTimeOffset> _replicaCaughtUpSince = new();

    internal ReplicaPlacementService(
        ConcurrentDictionary<string, string> systemConfiguration,
        Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>> replicate,
        Action<RaftSystemRequest> send,
        Action<List<RaftPartitionRange>> startPartitions,
        Func<ClusterMembership> getMembership,
        Func<int, ValueTask<bool>> amILeaderQuick,
        Func<int, string?> getPartitionLeader,
        Func<int, string, ValueTask<long?>> getFollowerCommitted,
        Func<RaftNode, int, string, Task<long?>> getRemoteFollowerLag,
        Func<string, MemberLivenessState> getNodeLiveness,
        Func<string, string?> getNodeZone,
        Func<int, string, CancellationToken, Task> transferLeadership,
        RaftConfiguration configuration,
        string localEndpoint,
        Func<TimeSpan> getRetryDelay,
        int maxRetries,
        ILogger<IRaft> logger)
    {
        this.systemConfiguration = systemConfiguration;
        this.replicate = replicate;
        this.send = send;
        this.startPartitions = startPartitions;
        this.getMembership = getMembership;
        this.amILeaderQuick = amILeaderQuick;
        this.getPartitionLeader = getPartitionLeader;
        this.getFollowerCommitted = getFollowerCommitted;
        this.getRemoteFollowerLag = getRemoteFollowerLag;
        this.getNodeLiveness = getNodeLiveness;
        this.getNodeZone = getNodeZone;
        this.transferLeadership = transferLeadership;
        this.configuration = configuration;
        this.localEndpoint = localEndpoint;
        this.getRetryDelay = getRetryDelay;
        this.maxRetries = maxRetries;
        this.logger = logger;
    }

    // ── Map helpers ────────────────────────────────────────────────────────

    private RaftPartitionMap? LoadMap()
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
            return null;

        return JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
    }

    /// <summary>
    /// Replicates a mutated map with the shared retry/backoff discipline, then applies it
    /// locally and re-materializes partitions. Returns false when replication ultimately failed
    /// (the completion, if any, has already been resolved with the failure).
    /// </summary>
    private async Task<bool> ReplicateMapAsync(
        RaftPartitionMap map,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        RaftSystemMessage sysMessage = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < maxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                completion?.TrySetCanceled(cancellationToken);
                return false;
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
                    "ReplicaPlacementService: map replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(getRetryDelay(), cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        completion?.TrySetCanceled(cancellationToken);
                        return false;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return false;
            }

            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = sysMessage.Value;
        startPartitions(map.Partitions);
        return true;
    }

    /// <summary>
    /// Returns the range's transitional replica (Learner or Removing), or null. At most one may
    /// exist at a time — enforced at every mutation entry point.
    /// </summary>
    private static RaftReplica? GetTransitionalReplica(RaftPartitionRange range) =>
        range.Replicas.FirstOrDefault(r => r.Role != RaftReplicaRole.Voter);

    private static (RaftPartitionRange? Range, RaftOperationStatus Status) ValidateRange(
        RaftPartitionMap map, int partitionId, ILogger<IRaft> logger, string operation)
    {
        RaftPartitionRange? range = map.Partitions.FirstOrDefault(r => r.PartitionId == partitionId);
        if (range is null)
        {
            logger.LogError("{Operation}: Partition {Id} not found in partition map", operation, partitionId);
            return (null, RaftOperationStatus.Errored);
        }

        if (range.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "{Operation}: Partition {Id} is in state {State}; replica changes require Active",
                operation, partitionId, range.State);
            return (null, RaftOperationStatus.Errored);
        }

        return (range, RaftOperationStatus.Success);
    }

    // ── Replica lifecycle mutations ────────────────────────────────────────

    /// <summary>
    /// Adds <c>message.MemberEndpoint</c> as a Learner replica of the range: one committed map
    /// mutation that bumps the range's <c>Generation</c>. The target node materializes the
    /// partition on map application and catches up via the existing backfill/snapshot path;
    /// it enters quorum only at the later <c>PromoteReplica</c> commit. Idempotent when the
    /// endpoint is already a replica.
    /// </summary>
    internal async Task TryAddReplica(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;
        string endpoint = message.MemberEndpoint ?? "";

        RaftPartitionMap? map = LoadMap();
        if (map is null || string.IsNullOrEmpty(endpoint))
        {
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        (RaftPartitionRange? range, RaftOperationStatus status) = ValidateRange(map, message.PartitionId, logger, "TryAddReplica");
        if (range is null)
        {
            completion?.TrySetResult((status, 0));
            return;
        }

        if (range.Replicas.Count == 0)
        {
            // Legacy full replication: there is no committed replica set to extend. Assigning one
            // implicitly here would silently un-host every other node, so reject.
            logger.LogError(
                "TryAddReplica: Partition {Id} uses legacy full replication (empty replica set); assign an initial placement first",
                message.PartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftReplica? existing = range.Replicas.FirstOrDefault(r => r.Endpoint == endpoint);
        if (existing is not null)
        {
            // Idempotent: a previous AddReplica committed but the caller never saw the response.
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
            return;
        }

        if (GetTransitionalReplica(range) is { } transitional)
        {
            logger.LogWarning(
                "TryAddReplica: Partition {Id} already has transitional replica {Endpoint} ({Role}); single mover per range",
                message.PartitionId, transitional.Endpoint, transitional.Role);
            completion?.TrySetResult((RaftOperationStatus.ConcurrentMembershipChange, range.Generation));
            return;
        }

        ClusterMembership roster = getMembership();
        if (roster.MembershipVersion > 0 && roster.Members.All(m => m.Endpoint != endpoint))
        {
            logger.LogError(
                "TryAddReplica: Endpoint {Endpoint} is not a committed roster member", endpoint);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        range.Generation++;
        range.Replicas.Add(new RaftReplica
        {
            Endpoint = endpoint,
            NodeId = message.MemberNodeId,
            Role = RaftReplicaRole.Learner,
            SinceGeneration = range.Generation
        });
        map.MapVersion++;

        logger.LogInfoAddReplica(localEndpoint, message.PartitionId, endpoint, range.Generation);

        if (await ReplicateMapAsync(map, completion, cancellationToken).ConfigureAwait(false))
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
    }

    /// <summary>
    /// Promotes a Learner replica of the range to Voter — the commit point at which the node
    /// enters the range's quorum. Idempotent when the replica is already a Voter.
    /// </summary>
    internal async Task TryPromoteReplica(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;
        string endpoint = message.MemberEndpoint ?? "";

        RaftPartitionMap? map = LoadMap();
        if (map is null || string.IsNullOrEmpty(endpoint))
        {
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        (RaftPartitionRange? range, RaftOperationStatus status) = ValidateRange(map, message.PartitionId, logger, "TryPromoteReplica");
        if (range is null)
        {
            completion?.TrySetResult((status, 0));
            return;
        }

        RaftReplica? replica = range.Replicas.FirstOrDefault(r => r.Endpoint == endpoint);
        if (replica is null)
        {
            logger.LogError(
                "TryPromoteReplica: Endpoint {Endpoint} is not a replica of partition {Id}",
                endpoint, message.PartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (replica.Role == RaftReplicaRole.Voter)
        {
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
            return;
        }

        if (replica.Role != RaftReplicaRole.Learner)
        {
            logger.LogError(
                "TryPromoteReplica: Replica {Endpoint} of partition {Id} is {Role}, not Learner",
                endpoint, message.PartitionId, replica.Role);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        range.Generation++;
        replica.Role = RaftReplicaRole.Voter;
        replica.SinceGeneration = range.Generation;
        map.MapVersion++;

        logger.LogInfoPromoteReplica(localEndpoint, message.PartitionId, endpoint, range.Generation);

        if (await ReplicateMapAsync(map, completion, cancellationToken).ConfigureAwait(false))
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
    }

    /// <summary>
    /// Removes a replica from the range with the two-commit discipline of the spec: first the
    /// replica is marked <see cref="RaftReplicaRole.Removing"/> (its vote leaves the quorum
    /// denominator while it still serves), then a second commit drops it from the set entirely —
    /// the departing node observes that final map, drains the partition, and reclaims its WAL.
    /// Both commits run inside this handler call; a crash between them leaves a Removing replica
    /// that <see cref="RunPlacementPassAsync"/> re-drives idempotently. When the victim currently
    /// leads the range, leadership is transferred away (best-effort) before the first commit.
    /// </summary>
    internal async Task TryRemoveReplica(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;
        string endpoint = message.MemberEndpoint ?? "";

        RaftPartitionMap? map = LoadMap();
        if (map is null || string.IsNullOrEmpty(endpoint))
        {
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        (RaftPartitionRange? range, RaftOperationStatus status) = ValidateRange(map, message.PartitionId, logger, "TryRemoveReplica");
        if (range is null)
        {
            completion?.TrySetResult((status, 0));
            return;
        }

        RaftReplica? replica = range.Replicas.FirstOrDefault(r => r.Endpoint == endpoint);
        if (replica is null)
        {
            // Idempotent: already gone.
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
            return;
        }

        if (replica.Role != RaftReplicaRole.Removing && GetTransitionalReplica(range) is { } transitional)
        {
            logger.LogWarning(
                "TryRemoveReplica: Partition {Id} already has transitional replica {Endpoint} ({Role}); single mover per range",
                message.PartitionId, transitional.Endpoint, transitional.Role);
            completion?.TrySetResult((RaftOperationStatus.ConcurrentMembershipChange, range.Generation));
            return;
        }

        if (replica.Role == RaftReplicaRole.Voter)
        {
            int remainingVoters = range.Replicas.Count(r => r.Role == RaftReplicaRole.Voter && r.Endpoint != endpoint);
            if (remainingVoters < 1)
            {
                logger.LogError(
                    "TryRemoveReplica: Refusing to remove {Endpoint} from partition {Id} — would leave zero voter replicas",
                    endpoint, message.PartitionId);
                completion?.TrySetResult((RaftOperationStatus.InsufficientVoters, 0));
                return;
            }
        }

        // Phase 1: mark Removing (skipped when re-driving an interrupted removal).
        if (replica.Role != RaftReplicaRole.Removing)
        {
            // A leader replica must hand off leadership before its vote is discounted, otherwise
            // the range is led by a node outside its own quorum denominator. Best-effort: the
            // election path recovers on failure, this just avoids an availability blip.
            if (string.Equals(getPartitionLeader(message.PartitionId), endpoint, StringComparison.Ordinal))
            {
                string? successor = range.Replicas
                    .Where(r => r.Role == RaftReplicaRole.Voter && r.Endpoint != endpoint)
                    .Select(r => r.Endpoint)
                    .FirstOrDefault();

                if (successor is not null)
                {
                    try
                    {
                        await transferLeadership(message.PartitionId, successor, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(
                            "TryRemoveReplica: leadership transfer of partition {Id} away from {Endpoint} failed: {Message}",
                            message.PartitionId, endpoint, ex.Message);
                    }
                }
            }

            range.Generation++;
            replica.Role = RaftReplicaRole.Removing;
            replica.SinceGeneration = range.Generation;
            map.MapVersion++;

            logger.LogInfoRemoveReplicaMarking(localEndpoint, message.PartitionId, endpoint, range.Generation);

            if (!await ReplicateMapAsync(map, completion, cancellationToken).ConfigureAwait(false))
                return;
        }

        // Phase 2: drop the replica entirely; the departing node drains and reclaims its WAL
        // when it applies this map.
        range.Generation++;
        range.Replicas.RemoveAll(r => r.Endpoint == endpoint);
        map.MapVersion++;

        logger.LogInfoRemoveReplicaDropped(localEndpoint, message.PartitionId, endpoint, range.Generation);

        if (await ReplicateMapAsync(map, completion, cancellationToken).ConfigureAwait(false))
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
    }

    /// <summary>
    /// Sets the per-range replication-factor override. Bumps <c>MapVersion</c> only — placement
    /// and routing are unchanged until the controller acts on the new target, so the range's
    /// <c>Generation</c> (the consumer fence) is deliberately not invalidated.
    /// </summary>
    internal async Task TrySetReplicationFactor(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (message.ReplicationFactorValue < 0)
        {
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = LoadMap();
        if (map is null)
        {
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        (RaftPartitionRange? range, RaftOperationStatus status) = ValidateRange(map, message.PartitionId, logger, "TrySetReplicationFactor");
        if (range is null)
        {
            completion?.TrySetResult((status, 0));
            return;
        }

        if (range.ReplicationFactor == message.ReplicationFactorValue)
        {
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
            return;
        }

        range.ReplicationFactor = message.ReplicationFactorValue;
        map.MapVersion++;

        if (await ReplicateMapAsync(map, completion, cancellationToken).ConfigureAwait(false))
            completion?.TrySetResult((RaftOperationStatus.Success, range.Generation));
    }

    // ── Placement controller ───────────────────────────────────────────────

    /// <summary>
    /// One P0 placement-controller pass. Always drives in-flight transitions to completion —
    /// re-issues the final drop for Removing replicas and promotes Learner replicas that have
    /// stayed within <see cref="RaftConfiguration.LearnerPromotionLag"/> for the stable window —
    /// so a crash mid-move converges regardless of configuration. Rebalancing moves (repair
    /// under-replication, trim over-replication, spread skew) are planned by
    /// <see cref="PlacementPlanner"/> and dispatched only when
    /// <see cref="RaftConfiguration.EnablePlacementRebalancer"/> is on, bounded by the move and
    /// transfer caps.
    /// </summary>
    internal async Task RunPlacementPassAsync(CancellationToken cancellationToken)
    {
        if (!await amILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false))
        {
            _replicaCaughtUpSince.Clear();
            return;
        }

        RaftPartitionMap? map = LoadMap();
        if (map is null)
            return;

        List<RaftPartitionRange> placed = map.Partitions
            .Where(r => r.State == RaftPartitionState.Active && r.Replicas.Count > 0)
            .ToList();

        if (placed.Count == 0)
            return;

        int transitionalCount = 0;

        // ── Drive in-flight transitions ────────────────────────────────────
        foreach (RaftPartitionRange range in placed)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            RaftReplica? transitional = GetTransitionalReplica(range);
            if (transitional is null)
                continue;

            transitionalCount++;

            if (transitional.Role == RaftReplicaRole.Removing)
            {
                // Crash-recovery re-drive: finish the interrupted two-commit removal.
                send(new RaftSystemRequest(
                    RaftSystemRequestType.RemoveReplica, range.PartitionId, transitional.Endpoint, transitional.NodeId));
                continue;
            }

            // Learner: promote once caught up for the stable window.
            if (await IsLearnerCaughtUp(range.PartitionId, transitional.Endpoint).ConfigureAwait(false))
            {
                (int, string) key = (range.PartitionId, transitional.Endpoint);
                DateTimeOffset now = DateTimeOffset.UtcNow;

                if (!_replicaCaughtUpSince.TryGetValue(key, out DateTimeOffset since))
                    _replicaCaughtUpSince[key] = now;
                else if (now - since >= configuration.LearnerPromotionStableWindow)
                {
                    _replicaCaughtUpSince.Remove(key);
                    send(new RaftSystemRequest(
                        RaftSystemRequestType.PromoteReplica, range.PartitionId, transitional.Endpoint, transitional.NodeId));
                }
            }
            else
                _replicaCaughtUpSince.Remove((range.PartitionId, transitional.Endpoint));
        }

        // ── Plan rebalancing moves ─────────────────────────────────────────
        if (!configuration.EnablePlacementRebalancer)
            return;

        ClusterMembership roster = getMembership();
        if (roster.MembershipVersion == 0)
            return;

        List<CandidateNode> candidates = roster.Members
            .Where(m => m.Role == ClusterMemberRole.Voter)
            .Select(m => new CandidateNode
            {
                Endpoint = m.Endpoint,
                NodeId = m.NodeId,
                Alive = m.Endpoint == localEndpoint || getNodeLiveness(m.Endpoint) == MemberLivenessState.Alive,
                // Remote zones come from each node's gossiped load report (the committed roster
                // carries no zone); best-effort — a node that never reported has a null zone and
                // simply doesn't participate in the zone-spread tiebreak.
                Zone = m.Endpoint == localEndpoint ? configuration.Zone : getNodeZone(m.Endpoint)
            })
            .ToList();

        PlacementView view = new()
        {
            Ranges = placed.Select(r => new RangePlacement
            {
                PartitionId = r.PartitionId,
                ReplicationFactor = r.ReplicationFactor > 0 ? r.ReplicationFactor : configuration.ReplicationFactor,
                VoterEndpoints = r.Replicas.Where(x => x.Role == RaftReplicaRole.Voter).Select(x => x.Endpoint).ToList(),
                LearnerEndpoints = r.Replicas.Where(x => x.Role == RaftReplicaRole.Learner).Select(x => x.Endpoint).ToList(),
                HasTransitionalReplica = GetTransitionalReplica(r) is not null,
                LeaderEndpoint = getPartitionLeader(r.PartitionId)
            }).ToList(),
            Nodes = candidates,
            ReplicaCountDeadband = configuration.ReplicaCountDeadband,
            MaxMoves = configuration.MaxReplicaMovesPerPass,
            TransferBudget = Math.Max(0, configuration.MaxConcurrentReplicaTransfers - transitionalCount)
        };

        foreach (PlacementMove move in PlacementPlanner.Plan(view))
        {
            int nodeId = roster.Members.FirstOrDefault(m => m.Endpoint == move.Endpoint)?.NodeId ?? 0;

            logger.LogInfoPlacementMove(localEndpoint, move.Kind, move.PartitionId, move.Endpoint);

            send(new RaftSystemRequest(
                move.Kind == PlacementMoveKind.AddReplica
                    ? RaftSystemRequestType.AddReplica
                    : RaftSystemRequestType.RemoveReplica,
                move.PartitionId, move.Endpoint, nodeId));
        }
    }

    /// <summary>
    /// Measures the learner replica's commit lag on its range, from the range leader's
    /// follower-progress table (directly when this node leads the range, via
    /// <c>GetRemoteFollowerLag</c> otherwise). Unlike the roster promotion driver, a null
    /// learner index counts as <b>not caught up</b>: under per-partition placement the learner
    /// is explicitly expected to ack this range, so "never acked" means replication has not
    /// reached it yet — the expected-partition-set distinction the join-all model couldn't make.
    /// </summary>
    private async Task<bool> IsLearnerCaughtUp(int partitionId, string endpoint)
    {
        long leaderCommitted;
        long? learnerCommitted;

        if (await amILeaderQuick(partitionId).ConfigureAwait(false))
        {
            long? own = await getFollowerCommitted(partitionId, localEndpoint).ConfigureAwait(false);
            if (own is null or < 0)
                return false;

            leaderCommitted = own.Value;
            learnerCommitted = await getFollowerCommitted(partitionId, endpoint).ConfigureAwait(false);
        }
        else
        {
            string? leaderEndpoint = getPartitionLeader(partitionId);
            if (string.IsNullOrEmpty(leaderEndpoint))
                return false;

            RaftNode leaderNode = new(leaderEndpoint);

            long? remoteLeaderCommitted = await getRemoteFollowerLag(leaderNode, partitionId, leaderEndpoint).ConfigureAwait(false);
            if (remoteLeaderCommitted is null or < 0)
                return false;

            leaderCommitted = remoteLeaderCommitted.Value;
            learnerCommitted = await getRemoteFollowerLag(leaderNode, partitionId, endpoint).ConfigureAwait(false);
        }

        if (learnerCommitted is null)
            return false;

        return leaderCommitted - learnerCommitted.Value <= configuration.LearnerPromotionLag;
    }
}
