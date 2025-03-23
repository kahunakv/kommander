
namespace Kommander.Data;

public enum RaftResponderRequestType
{
    Handshake,
    Vote,
    RequestVotes,
    AppendLogs,
    CompleteAppendLogs,
    TryBatch
}