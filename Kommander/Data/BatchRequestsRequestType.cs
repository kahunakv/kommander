namespace Kommander.Data;

public enum BatchRequestsRequestType
{
    Handshake,
    Vote,
    RequestVote,
    AppendLogs,
    CompleteAppendLogs
}