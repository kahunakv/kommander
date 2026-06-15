
namespace Kommander.Data;

/// <summary>
/// Wire response returned to the gossip initiator by the peer over gRPC or REST.
/// <see cref="RosterJson"/> is non-null only when the responder holds a strictly
/// newer roster than the sender, completing a push-pull exchange in one round trip.
/// </summary>
public sealed record GossipResponse(long MembershipVersion, string? RosterJson);
