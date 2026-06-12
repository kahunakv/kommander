
namespace Kommander.System;

/// <summary>
/// A single node entry in the committed cluster roster.
/// Liveness state (Alive/Suspect/Dead) lives in the gossip layer, not here,
/// so it never churns the Raft log.
/// </summary>
public sealed class ClusterMember
{
    /// <summary>host:port key matching the <c>RaftNode</c> endpoint.</summary>
    public string Endpoint { get; set; } = "";

    public int NodeId { get; set; }

    public ClusterMemberRole Role { get; set; }

    /// <summary><see cref="ClusterMembership.MembershipVersion"/> at which this node was first added as a Learner.</summary>
    public long JoinedVersion { get; set; }
}
