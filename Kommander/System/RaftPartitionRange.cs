
namespace Kommander.System;

/// <summary>
/// Represents a Raft partition range with a unique identifier, start, and end range values.
/// </summary>
public sealed class RaftPartitionRange
{
    public int PartitionId { get; set; }

    public int StartRange { get; set; }

    public int EndRange { get; set; }

    /// <summary>Monotonically increasing counter; bumped on every map mutation touching this entry.</summary>
    public long Generation { get; set; }

    /// <summary>Lifecycle state of this partition entry.</summary>
    public RaftPartitionState State { get; set; }

    /// <summary>Routing mode; Unrouted entries are excluded from GetPartitionKey hash routing.</summary>
    public RaftRoutingMode RoutingMode { get; set; }

    /// <summary>
    /// The nodes hosting this range and their per-range roles. An <b>empty list means legacy
    /// full replication</b> — every roster voter hosts the range — so pre-existing maps keep
    /// working until a placement is assigned (ADR 0006). Quorum for the range is computed over
    /// its <see cref="RaftReplicaRole.Voter"/> replicas only.
    /// </summary>
    public List<RaftReplica> Replicas { get; set; } = [];

    /// <summary>
    /// Desired number of voter replicas for this range; 0 inherits
    /// <see cref="RaftConfiguration.ReplicationFactor"/>.
    /// </summary>
    public int ReplicationFactor { get; set; }
}