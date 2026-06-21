
using Kommander.System;

namespace Kommander.Gossip;


/// <summary>
/// Full-state membership push sent to a random peer each gossip round.
/// Carries the sender's committed roster so the receiver can apply it when
/// its local version is stale, and the receiver responds symmetrically with
/// its own roster when it holds newer data (push-pull in one round trip).
///
/// <para>
/// <b>Design note — full-state push vs. digest/pull:</b>
/// A lightweight digest <c>{MembershipVersion, rosterHash}</c> followed by a
/// conditional pull of the full roster only when the receiver's version is lower
/// is the obvious bandwidth-saving alternative.  This implementation instead ships
/// the full <see cref="ClusterMembership"/> in every message.  For the cluster
/// sizes Kommander targets (3–9 nodes) the roster is a small blob (≈ 1 KB), so the
/// bandwidth cost of full-state push is negligible.  The two-round-trip digest/pull
/// design would add latency and complexity for no practical benefit at this scale.
/// If per-member liveness state is later added to the gossip envelope, a hash+pull
/// optimisation can be introduced at that point should roster sizes grow.
/// </para>
///
/// <para>
/// Gossip is read-only with respect to committed truth — it only converges
/// local caches; it never commits new membership entries to the Raft log.
/// </para>
/// </summary>
public sealed record GossipMessage(
    string SenderEndpoint,
    long MembershipVersion,
    ClusterMembership? Roster)
{
    /// <summary>
    /// SWIM liveness updates piggybacked on this gossip message.
    /// May be <c>null</c> when the sender has no tracked liveness state yet.
    /// The receiver applies updates using the SWIM merge rule
    /// (higher incarnation wins; Dead &gt; Suspect &gt; Alive at the same incarnation).
    /// </summary>
    public IReadOnlyList<MemberLivenessEntry>? LivenessUpdates { get; init; }

    /// <summary>
    /// Advisory load report for the sender node, piggybacked on the gossip round.
    /// Present only when <c>EnableLeaderBalancer</c> is true on the sender.
    /// The receiver forwards it to the system coordinator, which retains the
    /// highest <see cref="NodeLoadReport.ReportVersion"/> per endpoint for planning.
    /// </summary>
    public NodeLoadReport? LoadReport { get; init; }
};

/// <summary>
/// Response returned by the gossip receiver to complete a push-pull exchange.
/// The receiver's current membership version is always included.  The roster
/// is only included when the receiver holds a strictly newer version than the
/// sender, allowing the sender to catch up in the same network round trip.
/// </summary>
public sealed record GossipAck(
    long MembershipVersion,
    ClusterMembership? Roster
);
