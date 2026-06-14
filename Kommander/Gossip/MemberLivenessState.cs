
namespace Kommander.Gossip;

/// <summary>
/// SWIM failure-detector liveness state for a single cluster member.
///
/// <para>
/// State transitions (per SWIM §4):
/// <c>Alive → Suspect</c> when direct + indirect probes both timeout.
/// <c>Suspect → Dead</c> when suspicion age exceeds <c>SuspicionTimeout</c>.
/// <c>Suspect → Alive</c> when a successful ping is received or a higher-incarnation
/// refutation arrives via gossip.
/// </para>
///
/// <para>
/// Dead is treated as terminal locally.  Once a member is Dead the P0 leader commits
/// <c>RemoveMember</c> after <c>DeadMemberEvictionGrace</c>; the eviction removes the
/// endpoint from the roster entirely.
/// </para>
/// </summary>
public enum MemberLivenessState
{
    /// <summary>Responding to pings; known good.</summary>
    Alive = 0,

    /// <summary>
    /// Direct probe timed out and all indirect probes also timed out.
    /// The node may still be alive — incarnation refutation can clear this.
    /// </summary>
    Suspect = 1,

    /// <summary>
    /// Suspicion age exceeded <c>SuspicionTimeout</c>.  The node is presumed failed.
    /// The P0 leader will commit <c>RemoveMember</c> after the eviction grace period.
    /// </summary>
    Dead = 2,
}
