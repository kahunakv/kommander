
using Kommander.Data;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Represents the result of a Raft log replication operation in a distributed system.
/// </summary>
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