
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
    /// The event-driven completion task of a proposal ticket. The task completes when the proposal
    /// reaches a terminal state (committed, rolled-back, or invalidated), so callers can await it
    /// instead of polling <c>GetTicketState</c>.
    ///
    /// <para>It arrives two ways. The answer to an accepted proposal carries it together with
    /// <see cref="TicketId"/>, so the proposer needs no second request. A
    /// <see cref="RaftResponseType.TicketWaiterTask"/> response carries it for a caller that only
    /// has the ticket.</para>
    /// </summary>
    public Task<(RaftProposalTicketState, long)>? WaiterTask { get; }

    /// <summary>
    /// Carries the immutable partition snapshot when <see cref="Type"/> is
    /// <see cref="RaftResponseType.None"/> and the request was
    /// <see cref="RaftRequestType.GetPartitionView"/> (test-only harness accessor).
    /// </summary>
    public RaftPartitionView? PartitionView { get; }

    /// <summary>
    /// Carries the receiver-side result of a <see cref="RaftRequestType.InstallSnapshot"/> so the
    /// transport can tell an import from an idempotent skip. <see cref="Status"/> is
    /// <see cref="RaftOperationStatus.Success"/> for every outcome but
    /// <see cref="SnapshotInstallOutcome.Rejected"/>.
    /// </summary>
    public SnapshotInstallOutcome SnapshotOutcome { get; }

    public RaftResponse(SnapshotInstallOutcome snapshotOutcome, long logIndex)
    {
        Type = RaftResponseType.None;
        SnapshotOutcome = snapshotOutcome;
        Status = snapshotOutcome == SnapshotInstallOutcome.Rejected ? RaftOperationStatus.Errored : RaftOperationStatus.Success;
        LogIndex = logIndex;
    }

    public RaftResponse(RaftResponseType type, RaftPartitionView partitionView)
    {
        Type = type;
        PartitionView = partitionView;
    }

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
    /// Constructs the answer to an accepted proposal: its ticket and the task that completes when
    /// the ticket is released.
    /// </summary>
    public RaftResponse(RaftResponseType type, RaftOperationStatus status, HLCTimestamp ticketId, Task<(RaftProposalTicketState, long)>? waiterTask)
    {
        Type = type;
        Status = status;
        TicketId = ticketId;
        WaiterTask = waiterTask;
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