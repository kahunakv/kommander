namespace Kommander.Consensus;

/// <summary>
/// The Raft §5.4.1 log-freshness comparison, on its own so it can be checked as a function.
///
/// <para><b>Why this type exists.</b> Election safety rests on one rule: a voter only grants its
/// vote to a candidate whose log is at least as up to date as its own, where "up to date" compares
/// the last entry's term first and its index second. Every recent election bug in this code base
/// sat around that rule rather than inside it — which position to feed it, when to consult it —
/// and the rule itself was a private method with no test of its own. A comparison is exactly the
/// kind of thing a bounded exhaustive test can settle completely, so it lives here where such a
/// test can reach it.</para>
///
/// <para><b>Pure by construction.</b> No state, no clock, no I/O. The caller decides which log
/// position to advertise — see <c>ElectionCoordinator.GetFreshnessLogPositionAsync</c>, which uses
/// the contiguous-presence frontier rather than the raw max id, because the §5.4.1 proof assumes
/// contiguous logs.</para>
/// </summary>
internal static class ElectionFreshness
{
    /// <summary>
    /// <see langword="true"/> when the remote candidate's <c>(lastLogTerm, lastLogIndex)</c> is
    /// strictly less up to date than the local pair, which is the condition to deny a vote.
    ///
    /// <para><b>The missing-term fallback is symmetric on purpose.</b> A term of <c>0</c> or
    /// <c>-1</c> means "no usable last-log term here", not "an ancient term", so a lexicographic
    /// comparison against it is meaningless in EITHER direction. Testing only the remote side left
    /// a hole: a voter whose own term degraded read <c>remoteTerm != localTerm</c>, returned
    /// <c>remoteTerm &lt; localTerm</c> — false for any real term against 0 — and granted the vote
    /// with the index never examined at all. That voter is not necessarily an empty node whose vote
    /// costs nothing: the degraded pair is <b>a high index with no term</b>, reachable whenever the
    /// presence frontier lands on a compacted checkpoint boundary (its stored term reads -1, and
    /// both restore and the post-truncation re-read clamp that to 0). Such a voter holds a long
    /// committed log and still granted to a candidate missing an arbitrary range of it; the
    /// candidate wins and overwrites the range.</para>
    ///
    /// <para>With either side missing a term the comparison falls back to index-only, which is the
    /// legacy ordering and is strictly stricter than the old behaviour — it can only deny where the
    /// old code granted. A genuinely empty voter still grants, because its own index is 0 and no
    /// candidate is behind that.</para>
    /// </summary>
    /// <param name="remoteLastLogTerm">Term of the candidate's last log entry, or 0 when unknown.</param>
    /// <param name="remoteMaxLogId">Index the candidate advertises.</param>
    /// <param name="localLastLogTerm">Term of this node's last log entry, or 0 when unknown.</param>
    /// <param name="localMaxId">Index this node advertises.</param>
    public static bool CandidateLogIsBehind(
        long remoteLastLogTerm,
        long remoteMaxLogId,
        long localLastLogTerm,
        long localMaxId)
    {
        // Legacy peer / empty candidate log / boundary term the local side cannot read → index-only.
        if (remoteLastLogTerm <= 0 || localLastLogTerm <= 0)
            return remoteMaxLogId < localMaxId;

        if (remoteLastLogTerm != localLastLogTerm)
            return remoteLastLogTerm < localLastLogTerm;

        return remoteMaxLogId < localMaxId;
    }
}
