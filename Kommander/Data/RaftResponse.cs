
using Kommander.Time;

namespace Kommander.Data;

public sealed class RaftResponse
{
    public RaftResponseType Type { get; }
    
    public RaftOperationStatus Status { get; } = RaftOperationStatus.Success;

    public RaftNodeState NodeState { get; } = RaftNodeState.Follower;
    
    public RaftTicketState TicketState { get; } = RaftTicketState.NotFound;
    
    public long LogIndex { get; }
    
    public HLCTimestamp TicketId { get; } = HLCTimestamp.Zero;
    
    public RaftResponse(RaftResponseType type, RaftNodeState nodeState)
    {
        Type = type;
        NodeState = nodeState;
    }
    
    public RaftResponse(RaftResponseType type, RaftTicketState ticketState, long logIndex)
    {
        Type = type;
        TicketState = ticketState;
        LogIndex = logIndex;
    }
    
    public RaftResponse(RaftResponseType type)
    {
        Type = type;
    }
    
    public RaftResponse(RaftResponseType type, RaftOperationStatus status, long logIndex)
    {
        Type = type;
        Status = status;
        LogIndex = logIndex;
    }
    
    public RaftResponse(RaftResponseType type, RaftOperationStatus status, HLCTimestamp ticketId)
    {
        Type = type;
        Status = status;
        TicketId = ticketId;
    }
}