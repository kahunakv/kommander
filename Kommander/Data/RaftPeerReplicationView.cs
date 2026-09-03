namespace Kommander.Data;

/// <summary>
/// What a leader believes about one peer's position in this partition's log.
///
/// <para><b>Why this is exposed at all.</b> The leader's repair decisions are computed from these
/// values and from nothing else. When a follower is never caught up, the question is always which of
/// them was wrong — and until now the only way to see them was a <c>Debug</c> trace. That is not a
/// usable answer: the traces share one category, so reaching one turns on every other trace in the
/// build, and the extra formatting work per operation is enough to change the timing of the very
/// states worth investigating. A simulation run measured a state at roughly one in ten, then failed
/// to reproduce it in forty runs with that logging enabled.</para>
///
/// <para>So this is read-only, costs nothing when nobody asks, and reports the same values the
/// decision reads. It is a diagnostic view, not a contract: nothing in the protocol depends on it.
/// </para>
/// </summary>
/// <param name="Endpoint">The peer.</param>
/// <param name="IsVoter">Whether this peer counts towards quorum. Non-voters are exempt from some
/// of the confinement rules, so the flag changes how the other values should be read.</param>
/// <param name="FrontierKnown">
/// Whether the leader has ever recorded a committed frontier for this peer.
/// <para>The value that most often explains a follower nobody repairs. A frontier is recorded from
/// success acknowledgements only, so a peer whose acknowledgements are refusals has none — and an
/// unknown frontier is deliberately treated as a gap of zero, which means no repair is attempted
/// and nothing reports why.</para>
/// </param>
/// <param name="CommitFrontier">The recorded frontier, or -1 when none is known.</param>
/// <param name="StartCommitIndex">
/// Where the leader last saw this peer's log start, from its handshake or vote. The fallback the
/// gap is derived from when no frontier is known.
/// </param>
public sealed record RaftPeerReplicationView(
    string Endpoint,
    bool IsVoter,
    bool FrontierKnown,
    long CommitFrontier,
    long StartCommitIndex)
{
    public override string ToString() =>
        $"{Endpoint}{(IsVoter ? "" : "/learner")} frontierKnown={FrontierKnown} " +
        $"frontier={CommitFrontier} start={StartCommitIndex}";
}
