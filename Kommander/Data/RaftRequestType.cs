
namespace Kommander.Data;

public enum RaftRequestType
{
    CheckLeader,
    AppendLogs,
    CompleteAppendLogs,
    RequestVote,
    ReceiveVote,
    ReplicateLogs,
    ReplicateCheckpoint,
    GetNodeState,
    GetTicketState
}
