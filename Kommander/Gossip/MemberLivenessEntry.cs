
namespace Kommander.Gossip;

/// <summary>
/// A single node's liveness state piggybacked on gossip messages.
///
/// <para>
/// <see cref="Incarnation"/> is the SWIM incarnation counter for the member.  Higher
/// incarnations always win: a node that suspects itself can refute the suspicion by
/// bumping its incarnation in the next gossip round, overwriting any stale
/// <see cref="MemberLivenessState.Suspect"/> entries already in circulation.
/// </para>
/// </summary>
public sealed record MemberLivenessEntry(
    string Endpoint,
    MemberLivenessState State,
    long Incarnation,
    DateTimeOffset StateChangedAt
);
