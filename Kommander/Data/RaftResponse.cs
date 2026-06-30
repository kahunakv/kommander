
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

    /// <summary>
    /// Carries the event-driven completion task for a proposal ticket when
    /// <see cref="RaftResponseType"/> is <see cref="RaftResponseType.TicketWaiterTask"/>.
    /// The task completes when the proposal reaches a terminal state (committed, rolled-back,
    /// or invalidated) so callers can await it instead of polling <c>GetTicketState</c>.
    /// </summary>
    public Task<(RaftProposalTicketState, long)>? WaiterTask { get; }
    
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

    /// <summary>
    /// Constructs a <see cref="RaftResponseType.TicketWaiterTask"/> response that carries
    /// the event-driven completion task for an active proposal ticket.
    /// </summary>
    public RaftResponse(RaftResponseType type, Task<(RaftProposalTicketState, long)>? waiterTask)
    {
        Type = type;
        WaiterTask = waiterTask;
    }
}