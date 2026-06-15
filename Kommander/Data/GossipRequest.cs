
namespace Kommander.Data;

/// <summary>
/// Wire envelope sent by a gossip initiator to a peer over gRPC or REST.
/// <see cref="RosterJson"/> carries the sender's current <c>ClusterMembership</c>
/// serialized as JSON; null or empty means the sender is not including its roster.
/// </summary>
public sealed record GossipRequest(
    string SenderEndpoint,
    long MembershipVersion,
    string? RosterJson);
