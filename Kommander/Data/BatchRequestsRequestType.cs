
namespace Kommander.Data;

public enum BatchRequestsRequestType
{
    Ping,
    Handshake,
    Vote,
    RequestVote,
    AppendLogs,
    CompleteAppendLogs
}