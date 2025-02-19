
namespace Kommander.Data;

public enum RaftRequestType
{
    CheckLeader = 0,
    AppendLogs = 1,
    RequestVote = 2,
    ReceiveVote = 3,
    ReplicateLogs = 4,
    ReplicateCheckpoint = 5,
    GetState = 6
}
