
using Kommander.Time;

namespace Kommander.Scheduling;

/// <summary>
/// Tracks metadata for an in-flight WAL operation inside the partition state machine.
/// Reply correlation is handled externally via <see cref="IRaftOperationReplySink"/>.
/// </summary>
public sealed class RaftPendingWalOperation
{
    /// <summary>Optional reply correlation id assigned by the actor adapter.</summary>
    public ulong? ReplyCorrelationId { get; init; }

    public RaftProposalQuorum? Proposal { get; init; }

    public HLCTimestamp TicketId { get; init; }
}
