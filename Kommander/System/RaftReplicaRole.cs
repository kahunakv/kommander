
namespace Kommander.System;

/// <summary>
/// Role of a single replica of one partition range. The per-partition analogue of
/// <see cref="ClusterMemberRole"/>: quorum for a range is computed over its
/// <see cref="Voter"/> replicas only, and at most one replica of a range may be in a
/// transitional role (<see cref="Learner"/> or <see cref="Removing"/>) at a time —
/// the single-mover rule that keeps successive committed configurations of the range
/// overlapping by a quorum (Raft §6, applied per group).
/// </summary>
public enum RaftReplicaRole
{
    /// <summary>Full voting replica; counts toward the range's quorum.</summary>
    Voter,

    /// <summary>
    /// Catch-up replica: hosts the range and receives replication but is excluded from the
    /// quorum denominator and never campaigns. Promoted to <see cref="Voter"/> once its lag
    /// is within <c>LearnerPromotionLag</c> for the stable window.
    /// </summary>
    Learner,

    /// <summary>
    /// Marked for removal: still hosts and serves the range but no longer counts toward
    /// quorum. A subsequent commit drops the replica from the set entirely, at which point
    /// the node drains the partition and reclaims its WAL.
    /// </summary>
    Removing
}
