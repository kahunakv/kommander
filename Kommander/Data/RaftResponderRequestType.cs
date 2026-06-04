
namespace Kommander.Data;

public enum RaftResponderRequestType
{
    Handshake,
    Vote,
    RequestVotes,
    StepDownNotice,
    TransferLeadership,
    AppendLogs,
    CompleteAppendLogs,
    TryBatch
}
