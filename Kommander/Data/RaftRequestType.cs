
namespace Kommander.Data;

public enum RaftRequestType
{
    CheckLeader,
    ForceLeaderForTesting,
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
}
