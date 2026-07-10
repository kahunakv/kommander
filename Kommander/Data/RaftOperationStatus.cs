
namespace Kommander.Data;

public enum RaftOperationStatus
{
    Success = 0,
    Errored = 1,
    NodeIsNotLeader = 2,
    LeaderInOldTerm = 3,
    LeaderAlreadyElected = 4,
    LogsFromAnotherLeader = 5,
    ActiveProposal = 6,
    ProposalNotFound = 7,
    ProposalTimeout = 8,
    ReplicationFailed = 9,
    Pending = 10,

    /// <summary>
    /// The per-partition client proposal queue is full.
    /// The caller should back off and retry after a delay.
    /// </summary>
    ProposalQueueFull = 11,

    /// <summary>
    /// The partition is still restoring state from the WAL.
    /// Client proposals are not accepted until restore completes.
    /// The caller should back off and retry after a short delay.
    /// </summary>
    RestoreInProgress = 12,

    /// <summary>
    /// The partition has moved to a new generation. The caller should refresh
    /// the partition map and retry the request on the new owner.
    /// </summary>
    PartitionMoved = 13,

    /// <summary>
    /// The membership change was computed against an older <c>MembershipVersion</c>.
    /// The caller should re-read the current roster and retry with the correct version.
    /// </summary>
    StaleMembership = 14,

    /// <summary>
    /// Another membership change is already in flight. Only one single-server change
    /// may be pending at a time. The caller should retry after the in-flight change commits.
    /// </summary>
    ConcurrentMembershipChange = 15,

    /// <summary>
    /// The requested removal would leave fewer voters than needed to form a majority,
    /// making the cluster permanently unavailable.  The caller must not retry.
    /// </summary>
    InsufficientVoters = 16,

    /// <summary>
    /// The follower's log does not contain the expected entry at <c>PrevLogIndex</c> with
    /// term <c>PrevLogTerm</c> (Log Matching Property violation).  The leader must backtrack
    /// <c>nextIndex</c> for this peer and retry with an earlier prefix anchor.
    /// </summary>
    LogMismatch = 17,

    /// <summary>
    /// The entry the leader needs to send has been compacted below the follower's
    /// <c>nextIndex</c>.  The leader cannot backfill via normal AppendEntries and must
    /// instead install a snapshot.  The leader must not keep decrementing
    /// <c>nextIndex</c> for this peer.
    /// </summary>
    SnapshotRequired = 18,

    /// <summary>
    /// The caller's <see cref="System.Threading.CancellationToken"/> fired before the
    /// operation could complete.  The caller should back off and retry the same ticket
    /// after a delay — the queued work may still apply, and re-issuing the same ticket
    /// is a safe idempotent no-op once the executor drains.
    /// </summary>
    OperationCancelled = 19,
}