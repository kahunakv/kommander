
using Kommander.Data;
using Kommander.Time;

namespace Kommander;

public sealed class RaftReplicationResult
{
    public bool Success { get; }
    
    public RaftOperationStatus Status { get; }
    
    public HLCTimestamp TicketId { get; }

    public long LogIndex { get; }
    
    public RaftReplicationResult(bool success, RaftOperationStatus status, HLCTimestamp ticketId, long logIndex)
    {
        Success = success;
        Status = status;
        TicketId = ticketId;
        LogIndex = logIndex;
    }
}