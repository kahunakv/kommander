
using Kommander.Time;

namespace Kommander.Data;

public readonly struct RaftResponse
{
    public RaftResponseType Type { get; }
    
    public RaftOperationStatus Status { get; } = RaftOperationStatus.Success;

    public RaftNodeState NodeState { get; } = RaftNodeState.Follower;
    
    public RaftTicketState TicketState { get; } = RaftTicketState.NotFound;
    
    public long CommitIndex { get; } = 0;
    
    public HLCTimestamp TicketId { get; } = HLCTimestamp.Zero;
    
    public RaftResponse(RaftResponseType type, RaftNodeState nodeState)
    {
        Type = type;
        NodeState = nodeState;
    }
    
    public RaftResponse(RaftResponseType type, RaftTicketState ticketState, long commitIndex)
    {
        Type = type;
        TicketState = ticketState;
        CommitIndex = commitIndex;
    }
    
    public RaftResponse(RaftResponseType type)
    {
        Type = type;
    }
    
    public RaftResponse(RaftResponseType type, RaftOperationStatus status, long commitIndex)
    {
        Type = type;
        Status = status;
        CommitIndex = commitIndex;
    }
    
    public RaftResponse(RaftResponseType type, RaftOperationStatus status, HLCTimestamp ticketId)
    {
        Type = type;
        Status = status;
        TicketId = ticketId;
    }
}