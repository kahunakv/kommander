
namespace Kommander.Data;

/// <summary>
/// Tracks which phase of the two-step (Raft §9.6) election a node is currently in.
/// PreVote is a side-effect-free probe that gates whether a real election (and the
/// term bump that comes with it) is allowed to start, so a stale/partitioned node
/// can never disrupt a healthy leader by campaigning with an inflated term.
/// </summary>
public enum RaftElectionPhase
{
    /// <summary>No election activity; the node is following or leading normally.</summary>
    None,

    /// <summary>A side-effect-free pre-vote round is open for <c>currentTerm + 1</c>.</summary>
    PreVote,

    /// <summary>A real election is in progress (term bumped, self-vote cast).</summary>
    Election
}
