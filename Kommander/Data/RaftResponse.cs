
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// Represents a response in the Raft state actor.
/// Encapsulates various details about the state and results of Raft operations,
/// such as operation status, node state, log index, ticket state, and more.
/// </summary>
public sealed class RaftResponse
{
    public RaftResponseType Type { get; }
    
    public RaftOperationStatus Status { get; } = RaftOperationStatus.Success;

    public RaftNodeState NodeState { get; } = RaftNodeState.Follower;
    
    public RaftProposalTicketState ProposalTicketState { get; } = RaftProposalTicketState.NotFound;
    
    public long LogIndex { get; }
    
    public HLCTimestamp TicketId { get; } = HLCTimestamp.Zero;
    
    public RaftResponse(RaftResponseType type, RaftNodeState nodeState)
    {
        Type = type;
        NodeState = nodeState;
    }
    
    public RaftResponse(RaftResponseType type, RaftProposalTicketState proposalTicketState, long logIndex)
    {
        Type = type;
        ProposalTicketState = proposalTicketState;
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