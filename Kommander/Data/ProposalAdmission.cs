
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// The partition's answer to a proposal: whether it was accepted, and if so its ticket and the task
/// that completes when the ticket is released.
///
/// <para><b>Why the waiter is part of the answer.</b> The proposer needs it to learn the outcome, and
/// fetching it by ticket (<see cref="RaftPartition.GetTicketWaiterTaskAsync"/>) is a second request
/// to the partition executor, which waits behind whatever the executor does after it accepted the
/// proposal. The propose completion already holds the proposal, so it hands the waiter over with
/// the ticket.</para>
///
/// <para><see cref="Waiter"/> is null when the proposal was refused, and can be null on an accepted
/// one that was answered by a path which does not take the waiter. A caller then falls back to the
/// fetch by ticket.</para>
/// </summary>
internal readonly record struct ProposalAdmission(
    bool Success,
    RaftOperationStatus Status,
    HLCTimestamp TicketId,
    Task<(RaftProposalTicketState, long)>? Waiter);
