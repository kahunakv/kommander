
namespace Kommander.Data;

public enum RaftRequestType
{
    CheckLeader,
    ForceLeaderForTesting,

    /// <summary>
    /// Test-only: injects quiesced state directly into the state machine via
    /// <see cref="RaftPartitionStateMachine.SetQuiescedForTesting"/>, triggering the
    /// quiesce callback so the hot-set bookkeeping in <see cref="RaftManager"/> is
    /// updated under the single-owner guarantee.
    /// The <see cref="RaftRequest.Quiesce"/> field carries the target value.
    /// </summary>
    SetQuiescedForTesting,
    StepDown,
    TransferLeadership,
    SuspendHeartbeats,
    ResumeHeartbeats,
    ReceiveStepDownNotice,
    ReceiveTransferLeadership,
    ReceiveHandshake,
    RequestVote,
    ReceiveVote,
    AppendLogs,
    CompleteAppendLogs,
    ReplicateLogs,
    ReplicateCheckpoint,
    CommitLogs,
    RollbackLogs,
    GetNodeState,
    GetTicketState,
    WriteOperationCompleted,
    DrainBarrier,

    /// <summary>
    /// Posted by the async restore task back to the executor once the WAL logs
    /// have been loaded from storage.  The executor processes this on its own
    /// worker thread so that log replay callbacks run under the single-owner
    /// guarantee, satisfying correctness rule 1.
    /// </summary>
    RestoreLogsLoaded,

    /// <summary>
    /// Returns the last commit index reported by a specific follower endpoint via
    /// <c>CompleteAppendLogs</c> acknowledgements.  Used by the promotion driver to
    /// measure learner lag without reading WAL storage.
    /// </summary>
    GetFollowerCommittedIndex,

    /// <summary>
    /// Returns the event-driven completion task for an active proposal ticket.
    /// The returned <see cref="System.Threading.Tasks.Task{T}"/> completes when the
    /// proposal reaches a terminal state (committed, rolled-back, or invalidated by
    /// leader loss), eliminating the need to poll <see cref="GetTicketState"/>.
    /// </summary>
    GetTicketWaiterTask,

    /// <summary>
    /// Posted by the background snapshot-transfer task back to the partition executor once the
    /// target follower has confirmed the snapshot was installed.  The executor processes this on
    /// its worker thread to safely advance <c>lastCommitIndexes[endpoint]</c> to the snapshot
    /// index, allowing normal backfill to resume from the next entry.
    /// </summary>
    SnapshotInstalled,
}
