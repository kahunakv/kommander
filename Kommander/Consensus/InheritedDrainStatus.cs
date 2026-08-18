namespace Kommander.Consensus;

/// <summary>
/// Result of <see cref="DrainInheritedAppliesAsync"/>. The three outcomes demand different
/// caller responses, which is why this is not a bool: <see cref="Hole"/> is retryable (a write
/// may still be queued behind the read) and disqualifying if it persists, while
/// <see cref="BlockedByInFlight"/> is neither — retrying cannot resolve an in-flight proposal
/// (its own completion does), and treating it as disqualifying would step the leader down on
/// ordinary pipelined load.
/// </summary>
internal enum InheritedDrainStatus
{
    /// <summary>The cursor covers the requested range; the caller's batch can be applied.</summary>
    Covered,

    /// <summary>An id in the range is absent above the snapshot floor — a genuine WAL hole or a
    /// write still queued in the WAL scheduler; indistinguishable from the read side.</summary>
    Hole,

    /// <summary>The drain reached a current-term Proposed entry: a pipelined proposal still in
    /// flight. The cursor was NOT advanced over it; the caller must defer its batch via
    /// <see cref="DeferLeaderApplies"/> until the in-flight entry resolves.</summary>
    BlockedByInFlight,
}
