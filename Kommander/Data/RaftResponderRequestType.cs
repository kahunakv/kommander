
namespace Kommander.Data;

public enum RaftResponderRequestType
{
    Vote,
    RequestVotes,
    AppendLogs,
    CompleteAppendLogs,
    RequestAcknowledgment,
    Acknowledgment
}