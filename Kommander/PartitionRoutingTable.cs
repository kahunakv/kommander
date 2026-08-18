
using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.System;

namespace Kommander;

/// <summary>
/// Owns the node's view of the committed partition map and answers every routing question derived
/// from it: which partition a key belongs to, which peers a partition replicates to, who counts
/// toward its quorum, and which ids are still free.
/// <para>
/// This is deliberately separate from the partition <i>registry</i> (the materialized
/// <see cref="RaftPartition"/> instances, which stay with <see cref="RaftManager"/>). Under
/// per-partition placement the two differ: a node routes for every committed range but hosts only
/// the ones whose replica set names it. Reading routing off the hosted set would silently
/// mis-route on any non-replica node, so the committed map is the primary source here and the
/// hosted set is only a fallback for hosts that never applied a map (unit-test harnesses that
/// populate the registry directly).
/// </para>
/// <para>
/// Concurrency: the committed map is replaced wholesale on every application and readers take the
/// reference once, so each reader iterates its own immutable snapshot and never observes a
/// half-applied map. The placement dictionary is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// updated entry-by-entry in the same pass; its entries are immutable once published.
/// </para>
/// </summary>
internal sealed class PartitionRoutingTable
{
    /// <summary>
    /// Immutable per-range placement view consumed by the quorum seam
    /// (<see cref="GetPartitionPeers"/> / <see cref="IsPartitionVoter"/>).
    /// </summary>
    private sealed class PartitionPlacement
    {
        internal required IReadOnlyList<RaftNode> Peers { get; init; }
        internal required HashSet<string> VoterEndpoints { get; init; }
        internal required IReadOnlyList<RaftReplica> Replicas { get; init; }
    }

    /// <summary>
    /// Snapshot of the last committed partition map, including ranges this node does <b>not</b>
    /// host. Replaced wholesale on every application; readers take the reference once and iterate
    /// their own immutable snapshot.
    /// </summary>
    private volatile List<RaftPartitionRange> committedRanges = [];

    /// <summary>
    /// Per-partition placement resolution derived from <see cref="committedRanges"/>: the range's
    /// peer set (replicas minus self) and voter endpoints. Only populated for ranges with a
    /// non-empty replica set — absence means legacy full replication and the whole-cluster
    /// fallback applies. Rebuilt on every map application.
    /// </summary>
    private readonly ConcurrentDictionary<int, PartitionPlacement> partitionPlacements = new();

    private readonly IPartitionProvider partitionProvider;
    private readonly Func<List<RaftNode>> getNodes;
    private readonly Func<ClusterMembership> getMembership;
    private readonly RaftConfiguration configuration;
    private readonly string localEndpoint;

    internal PartitionRoutingTable(
        IPartitionProvider partitionProvider,
        Func<List<RaftNode>> getNodes,
        Func<ClusterMembership> getMembership,
        RaftConfiguration configuration,
        string localEndpoint)
    {
        this.partitionProvider = partitionProvider;
        this.getNodes = getNodes;
        this.getMembership = getMembership;
        this.configuration = configuration;
        this.localEndpoint = localEndpoint;
    }

    /// <summary>
    /// Adopts a newly committed partition map: publishes the routing snapshot, then rebuilds the
    /// derived placement entries. Tombstoned (<see cref="RaftPartitionState.Removed"/>) ranges and
    /// ranges with an empty replica set carry no placement — the first are gone, the second are
    /// legacy full replication where the whole-cluster fallback applies.
    /// </summary>
    internal void ApplyCommittedMap(List<RaftPartitionRange> ranges)
    {
        committedRanges = ranges;

        foreach (RaftPartitionRange range in ranges)
        {
            if (range.Replicas.Count > 0 && range.State != RaftPartitionState.Removed)
            {
                List<RaftNode> peers = new(range.Replicas.Count);
                HashSet<string> voters = new(StringComparer.Ordinal);
                foreach (RaftReplica replica in range.Replicas)
                {
                    if (replica.Endpoint != localEndpoint)
                        peers.Add(new RaftNode(replica.Endpoint));
                    if (replica.Role == RaftReplicaRole.Voter)
                        voters.Add(replica.Endpoint);
                }

                partitionPlacements[range.PartitionId] = new PartitionPlacement
                {
                    Peers = peers,
                    VoterEndpoints = voters,
                    Replicas = range.Replicas.ToList()
                };
            }
            else
                partitionPlacements.TryRemove(range.PartitionId, out _);
        }
    }

    /// <summary>
    /// Returns the peer set for one partition: the range's replica set (minus self) when the
    /// committed map assigns it one, otherwise the whole-cluster node list (legacy full
    /// replication and the system partition, which always replicates everywhere). This is the
    /// seam that makes quorum per-partition — <see cref="RaftPartitionStateMachine"/> computes
    /// every quorum from <c>host.Nodes</c> filtered by <c>host.IsVoter</c>.
    /// </summary>
    internal IReadOnlyList<RaftNode> GetPartitionPeers(int partitionId)
    {
        if (partitionId != RaftSystemConfig.SystemPartition &&
            partitionPlacements.TryGetValue(partitionId, out PartitionPlacement? placement))
            return placement.Peers;

        return getNodes();
    }

    /// <summary>
    /// Returns true when <paramref name="endpoint"/> counts toward <paramref name="partitionId"/>'s
    /// quorum. For a range with an assigned replica set this is membership in the range's
    /// <see cref="RaftReplicaRole.Voter"/> replicas — Learner and Removing replicas are
    /// peers but excluded from the quorum denominator. For legacy ranges and the system partition
    /// it falls back to the committed roster's voter set (pre-seed: everyone is a voter).
    /// </summary>
    internal bool IsPartitionVoter(int partitionId, string endpoint)
    {
        if (partitionId != RaftSystemConfig.SystemPartition &&
            partitionPlacements.TryGetValue(partitionId, out PartitionPlacement? placement))
            return placement.VoterEndpoints.Contains(endpoint);

        ClusterMembership roster = getMembership();
        if (roster.MembershipVersion == 0)
            return true; // pre-seed: treat all known peers as voters (backward compat)

        // Plain loop, not LINQ Any: this runs per peer per check-quorum tick and per propose
        // broadcast on P0/legacy ranges — the capturing lambda would allocate on every call.
        foreach (ClusterMember member in roster.Members)
        {
            if (member.Role == ClusterMemberRole.Voter &&
                string.Equals(member.Endpoint, endpoint, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Returns the committed replica set of <paramref name="partitionId"/>, or an empty list for
    /// legacy full replication (every roster voter hosts the range) and unknown partitions.
    /// The returned list is a snapshot; it never mutates after being returned.
    /// </summary>
    internal IReadOnlyList<RaftReplica> GetPartitionReplicas(int partitionId)
    {
        if (partitionPlacements.TryGetValue(partitionId, out PartitionPlacement? placement))
            return placement.Replicas;

        return [];
    }

    /// <summary>
    /// Like <see cref="GetPartitionReplicas"/> but distinguishes "no placement assigned" from
    /// "placement with no replicas". Forwarding needs that distinction: a range under legacy full
    /// replication has nowhere to forward to and must fall through to the local diagnosis, which
    /// an empty list would hide.
    /// </summary>
    internal IReadOnlyList<RaftReplica>? TryGetPlacementReplicas(int partitionId)
    {
        if (partitionPlacements.TryGetValue(partitionId, out PartitionPlacement? placement))
            return placement.Replicas;

        return null;
    }

    /// <summary>
    /// Returns the effective replication factor for <paramref name="partitionId"/>: the range's
    /// override when set, otherwise <see cref="RaftConfiguration.ReplicationFactor"/>.
    /// 0 means full replication.
    /// </summary>
    internal int GetEffectiveReplicationFactor(int partitionId)
    {
        List<RaftPartitionRange> ranges = committedRanges;
        foreach (RaftPartitionRange range in ranges)
        {
            if (range.PartitionId == partitionId && range.ReplicationFactor > 0)
                return range.ReplicationFactor;
        }

        return configuration.ReplicationFactor;
    }

    /// <summary>
    /// The committed partition map as an independent snapshot, with tombstones filtered out.
    /// Falls back to the hosted set for hosts that never applied a committed map.
    /// </summary>
    internal IReadOnlyList<RaftPartitionRange> GetPartitionMap()
    {
        // Prefer the committed map: under per-partition placement the local partition registry
        // only holds hosted ranges, but routing must see every range.
        List<RaftPartitionRange> ranges = committedRanges;
        if (ranges.Count > 0)
        {
            List<RaftPartitionRange> mapSnapshot = new(ranges.Count);
            foreach (RaftPartitionRange range in ranges)
            {
                if (range.State == RaftPartitionState.Removed)
                    continue;

                mapSnapshot.Add(new RaftPartitionRange
                {
                    PartitionId = range.PartitionId,
                    StartRange = range.StartRange,
                    EndRange = range.EndRange,
                    RoutingMode = range.RoutingMode,
                    Generation = range.Generation,
                    State = range.State,
                    ReplicationFactor = range.ReplicationFactor,
                    Replicas = range.Replicas.Select(r => new RaftReplica
                    {
                        Endpoint = r.Endpoint,
                        NodeId = r.NodeId,
                        Role = r.Role,
                        SinceGeneration = r.SinceGeneration
                    }).ToList()
                });
            }

            return mapSnapshot;
        }

        // Fallback for hosts that never applied a committed map (unit-test harnesses that
        // populate the partition registry directly).
        List<RaftPartitionRange> snapshot = new(partitionProvider.DataPartitionCount);

        foreach (RaftPartition p in partitionProvider.DataPartitions)
        {
            snapshot.Add(new RaftPartitionRange
            {
                PartitionId  = p.PartitionId,
                StartRange   = p.StartRange,
                EndRange     = p.EndRange,
                RoutingMode  = p.RoutingMode,
                Generation   = p.Generation,
                State        = p.State,
            });
        }

        return snapshot;
    }

    /// <summary>
    /// Returns the lowest partition id no committed range has ever claimed.
    /// </summary>
    internal int GetNextAvailablePartitionId()
    {
        // Unlike GetPartitionMap this keeps Removed entries: a tombstoned id can never be recreated,
        // so it is spent and the allocator has to step past it.
        List<RaftPartitionRange> ranges = committedRanges;
        if (ranges.Count > 0)
            return RaftPartitionMap.NextAvailablePartitionId(ranges);

        // Fallback for hosts that never applied a committed map (unit-test harnesses that
        // populate the partition registry directly). No tombstones exist there — a removal takes
        // the partition out of the registry — so this is the best the local view can do.
        int maxPartitionId = RaftSystemConfig.SystemPartition;

        foreach (RaftPartition partition in partitionProvider.DataPartitions)
        {
            if (partition.PartitionId > maxPartitionId)
                maxPartitionId = partition.PartitionId;
        }

        return maxPartitionId + 1;
    }

    /// <summary>
    /// Returns the number of the partition for the given partition key
    /// </summary>
    internal int GetPartitionKey(string partitionKey)
    {
        int rangeId = (int)HashUtils.InversePrefixedStaticHash(partitionKey, '/');
        if (rangeId < 0)
            rangeId = -rangeId;

        int? partitionId = ResolveRangeId(rangeId);
        if (partitionId is not null)
            return partitionId.Value;

        throw new RaftException("Couldn't find partition range for: " + partitionKey + " " + rangeId);
    }

    /// <summary>
    /// Returns the number of the partition for the given prefix partition key
    /// </summary>
    internal int GetPrefixPartitionKey(string prefixPartitionKey)
    {
        int rangeId = (int)HashUtils.SimpleHash(prefixPartitionKey);
        if (rangeId < 0)
            rangeId = -rangeId;

        int? partitionId = ResolveRangeId(rangeId);
        if (partitionId is not null)
            return partitionId.Value;

        throw new RaftException("Couldn't find partition range for: " + prefixPartitionKey + " " + rangeId);
    }

    /// <summary>
    /// Resolves a hash range id to its partition id using the committed map when available —
    /// required under per-partition placement, where a non-replica node must still route any
    /// key even though it hosts no <see cref="RaftPartition"/> for the range — falling back to
    /// the hosted set for hosts that never applied a committed map.
    /// </summary>
    private int? ResolveRangeId(int rangeId)
    {
        List<RaftPartitionRange> ranges = committedRanges;
        if (ranges.Count > 0)
        {
            foreach (RaftPartitionRange range in ranges)
            {
                if (range.State != RaftPartitionState.Removed &&
                    range.RoutingMode == RaftRoutingMode.HashRange &&
                    range.StartRange <= rangeId && range.EndRange >= rangeId)
                    return range.PartitionId;
            }

            return null;
        }

        foreach (RaftPartition partition in partitionProvider.DataPartitions)
        {
            if (partition.RoutingMode == RaftRoutingMode.HashRange &&
                partition.StartRange <= rangeId && partition.EndRange >= rangeId)
                return partition.PartitionId;
        }

        return null;
    }

    /// <summary>
    /// True when the committed map still lists <paramref name="partitionId"/> as a live (non
    /// tombstoned) range. Used to classify a local registry miss: a range that exists in the map
    /// but is not materialized here is a routing condition some other node can serve, whereas an
    /// id absent from the map is hosted nowhere and retrying cannot help.
    /// </summary>
    internal bool IsLiveMappedRange(int partitionId)
    {
        List<RaftPartitionRange> ranges = committedRanges;
        foreach (RaftPartitionRange range in ranges)
        {
            if (range.PartitionId == partitionId && range.State != RaftPartitionState.Removed)
                return true;
        }

        return false;
    }

    /// <summary>
    /// The committed generation of <paramref name="partitionId"/>, or 0 when the range is unknown
    /// or tombstoned. Non-hosted ranges still expose it so callers on non-replica nodes can build
    /// a correctly-fenced forwarded proposal; tombstones report 0 because no proposal can target
    /// them, and leaking the tombstone's generation would let one look valid.
    /// </summary>
    internal long GetCommittedGeneration(int partitionId)
    {
        List<RaftPartitionRange> ranges = committedRanges;
        foreach (RaftPartitionRange range in ranges)
        {
            if (range.PartitionId == partitionId)
                return range.State == RaftPartitionState.Removed ? 0 : range.Generation;
        }

        return 0;
    }

    /// <summary>
    /// True when any committed range's replica set names <paramref name="endpoint"/> (in any
    /// replica role). Removed ranges are ignored — their replica sets are historical.
    /// </summary>
    internal bool CommittedMapNamesEndpoint(string endpoint)
    {
        List<RaftPartitionRange> ranges = committedRanges;

        foreach (RaftPartitionRange range in ranges)
        {
            if (range.State == RaftPartitionState.Removed)
                continue;

            foreach (RaftReplica replica in range.Replicas)
            {
                if (string.Equals(replica.Endpoint, endpoint, StringComparison.Ordinal))
                    return true;
            }
        }

        return false;
    }
}
