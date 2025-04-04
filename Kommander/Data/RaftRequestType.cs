﻿
namespace Kommander.Data;

public enum RaftRequestType
{
    CheckLeader,
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
    GetTicketState
}
