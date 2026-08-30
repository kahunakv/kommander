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
    /// <see langword="true"/> when the remote candidate's log is strictly less up to date than the
    /// local log, so the local node must deny its vote.
    ///
    /// <para>A <paramref name="remoteLastLogTerm"/> at or below zero means the peer sent no term:
    /// either a legacy peer that predates the field, or a candidate whose log is empty. The
    /// comparison then falls back to index only, which preserves the ordering those peers were
    /// built against.</para>
    /// </summary>
    /// <param name="remoteLastLogTerm">Term of the candidate's last log entry, or 0 when unknown.</param>
    /// <param name="remoteMaxLogId">Index the candidate advertises.</param>
    /// <param name="localLastLogTerm">Term of this node's last log entry.</param>
    /// <param name="localMaxId">Index this node advertises.</param>
    public static bool CandidateLogIsBehind(
        long remoteLastLogTerm,
        long remoteMaxLogId,
        long localLastLogTerm,
        long localMaxId)
    {
        if (remoteLastLogTerm <= 0)
            return remoteMaxLogId < localMaxId; // legacy peer / empty candidate log → index-only

        if (remoteLastLogTerm != localLastLogTerm)
            return remoteLastLogTerm < localLastLogTerm;

        return remoteMaxLogId < localMaxId;
    }
}
