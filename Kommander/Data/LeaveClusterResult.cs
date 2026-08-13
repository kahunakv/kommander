
namespace Kommander.Data;

/// <summary>
/// Why a graceful-leave request ended the way it did.
/// </summary>
public enum LeaveClusterOutcome
{
    /// <summary>
    /// The <c>RemoveMember(self)</c> entry was committed on the system partition (or the node was
    /// already absent from the roster and the removal is therefore a no-op). The node is out of
    /// the committed roster and surviving members no longer count it towards quorum.
    /// </summary>
    Committed = 0,

    /// <summary>
    /// The local endpoint is not present in the committed roster — it already left, or was evicted
    /// while it was down. Nothing was committed; the request is a success for idempotency purposes.
    /// </summary>
    NotAMember = 1,

    /// <summary>
    /// Removing the local node would leave the cluster with zero voters, so the leader refused.
    /// This is permanent: retrying can never succeed while the roster is unchanged. The node keeps
    /// its role and continues to serve.
    /// </summary>
    RefusedInsufficientVoters = 2,

    /// <summary>
    /// The node has not finished cluster initialization (no system partition, or no committed
    /// roster yet), so there is no membership entry to remove. Nothing was committed.
    /// </summary>
    NotInitialized = 3,

    /// <summary>
    /// The system-partition leader could not be resolved (election in progress, or the node is
    /// isolated). Nothing was committed; the caller may retry.
    /// </summary>
    NoLeader = 4,

    /// <summary>
    /// The leader was contacted but the removal was not confirmed before the deadline expired, or
    /// the caller cancelled the request. The removal may or may not have committed — the caller
    /// must re-read the roster before deciding.
    /// </summary>
    Timeout = 5
}

/// <summary>
/// Outcome of <see cref="IRaft.RequestLeaveAsync"/>.
/// </summary>
/// <param name="Outcome">What happened to the removal request.</param>
/// <param name="MembershipVersion">
/// The roster version the caller should observe as a result. For <see cref="LeaveClusterOutcome.Committed"/>
/// this is the version that no longer contains the local endpoint; for every other outcome it is the
/// last roster version known locally (0 when none is known).
/// </param>
public readonly record struct LeaveClusterResult(LeaveClusterOutcome Outcome, long MembershipVersion)
{
    /// <summary>
    /// True when the node is out of the committed roster — either because the removal committed or
    /// because it was never (or no longer) in it.
    /// </summary>
    public bool Left => Outcome is LeaveClusterOutcome.Committed or LeaveClusterOutcome.NotAMember;

    /// <summary>
    /// True when retrying can never change the answer, so the caller must stop rather than back off.
    /// </summary>
    public bool Terminal => Outcome is LeaveClusterOutcome.RefusedInsufficientVoters;
}
