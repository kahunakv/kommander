
namespace Kommander.System;

public enum ClusterMemberRole
{
    Learner,
    Voter,
    /// <summary>
    /// A committed roster state for a node whose graceful decommission is draining: the
    /// placement pass evacuates its replicas onto survivors before the final <c>RemoveMember</c>.
    /// A <c>Leaving</c> member no longer counts toward roster-level quorums or placement
    /// candidacy, but it stays in every peer list (heartbeats and replication keep flowing —
    /// evacuating learners catch up from it) and it keeps counting toward the quorum of any
    /// range whose committed replica set still names it. The transition is reversible: a drain
    /// that times out or is refused rolls the role back to <see cref="Voter"/>, because the
    /// campaign gates suppress elections for any node whose roster role is not Voter.
    /// <para>
    /// <see cref="RaftManager.LocalRole"/> also returns this value locally (without a committed
    /// role change) once teardown has begun, so election suppression kicks in immediately on
    /// shutdown.
    /// </para>
    /// </summary>
    Leaving,
    /// <summary>
    /// The local node is not present in the committed roster at all.
    /// Used only as a return value from <see cref="RaftManager.LocalRole"/>;
    /// never stored in a <see cref="ClusterMember"/> entry.
    /// </summary>
    NotMember
}
