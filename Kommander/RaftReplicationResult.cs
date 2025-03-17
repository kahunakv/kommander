using Kommander.Data;
using Kommander.Time;

namespace Kommander;

public readonly struct RaftReplicationResult
{
    public bool Success { get; }
    
    public RaftOperationStatus Status { get; }
    
    public HLCTimestamp TicketId { get; }

    public long CommitLogId { get; }
    
    public RaftReplicationResult(bool success, RaftOperationStatus status, HLCTimestamp ticketId, long commitLogId)
    {
        Success = success;
        Status = status;
        TicketId = ticketId;
        CommitLogId = commitLogId;
    }
}