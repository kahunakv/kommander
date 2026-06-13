
namespace Kommander.Data;

/// <summary>
/// Returned by <see cref="ICommunication.SendJoin"/> after a join attempt.
/// <para>
/// When <see cref="Success"/> is <c>true</c> the node has been committed to the roster
/// as a <see cref="ClusterMemberRole.Learner"/> at <see cref="MembershipVersion"/>.
/// When <see cref="Success"/> is <c>false</c> the contacted node is not the P0 leader
/// and <see cref="LeaderHint"/> carries the current leader endpoint to retry against.
/// </para>
/// </summary>
public sealed record JoinResponse(bool Success, string? LeaderHint = null, long MembershipVersion = 0);
