
namespace Kommander.Data;

public enum BatchRequestsRequestType
{
    Ping = 0,
    Handshake = 1,
    Vote = 2,
    RequestVote = 3,
    AppendLogs = 4,
    CompleteAppendLogs = 5,
    StepDownNotice = 6,
    TransferLeadership = 7
}
