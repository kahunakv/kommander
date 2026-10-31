namespace Kommander.System;

/// <summary>
/// Why a committed roster change removed a member. Recorded by the P0 leader in
/// <see cref="ClusterMembership.Departures"/> at the single commit point for removals
/// (<c>TryRemoveMember</c>), because only the leader knows which request it is applying, and
/// the removed node is the one that needs the answer: a node that finds itself absent from a
/// roster while its own leave request is in force cannot tell, from the member list alone, an
/// eviction it must recover from apart from the departure it asked for.
/// </summary>
public enum ClusterDepartureReason
{
    /// <summary>
    /// The removal was asked for: the node's own <c>RemoveMember(self)</c> (graceful leave or
    /// shutdown), the placement pass completing a decommission drain, the withdrawal of a failed
    /// join, or an operator removing the member. The departed node must not re-admit itself.
    /// </summary>
    Requested = 0,

    /// <summary>
    /// The P0 leader evicted the member because its SWIM liveness state had been Dead for longer
    /// than <see cref="RaftConfiguration.DeadMemberEvictionGrace"/>. Nobody asked for this
    /// departure, and nothing the node itself had in flight can land after it (a role change for
    /// an absent member is refused, a removal of an absent member is a no-op), so a leave request
    /// that was in force is void and the node must auto-rejoin like any other evicted member.
    /// </summary>
    Evicted = 1
}

/// <summary>
/// One recent removal from the committed roster: which endpoint left, at which roster version,
/// and <see cref="Reason">why</see>. Kept in <see cref="ClusterMembership.Departures"/> until the
/// endpoint is admitted again (or the list is full), so the record survives roster versions the
/// departed node never sees: an evicted node is partitioned while it is evicted and typically
/// learns the roster through a gossip push that skips straight to the newest version.
/// </summary>
public sealed class ClusterDeparture
{
    /// <summary>host:port key of the removed member.</summary>
    public string Endpoint { get; set; } = "";

    /// <summary>The <see cref="ClusterMembership.MembershipVersion"/> whose commit removed the endpoint.</summary>
    public long Version { get; set; }

    public ClusterDepartureReason Reason { get; set; }
}
