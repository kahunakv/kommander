
using Kommander.Data;
using Kommander.Time;

namespace Kommander.Scheduling;

/// <summary>
/// Tracks metadata for an in-flight WAL operation inside the partition state machine.
///
/// <para>Fields are populated at enqueue time so the completion handler never needs to
/// reach back into the original <see cref="WAL.Data.WALWriteOperation"/>.</para>
/// </summary>
public sealed class RaftPendingWalOperation
{
    /// <summary>Optional reply correlation id assigned by the actor adapter.</summary>
    public ulong? ReplyCorrelationId { get; init; }

    public RaftProposalQuorum? Proposal { get; init; }

    public HLCTimestamp TicketId { get; init; }

    /// <summary>
    /// Log entries associated with the operation.
    /// Set for <c>LeaderPropose</c> and <c>FollowerAppend</c> operations.
    /// </summary>
    public List<RaftLog>? Logs { get; init; }

    /// <summary>
    /// Whether the proposal should be committed automatically after reaching quorum.
    /// Set for <c>LeaderPropose</c> operations.
    /// </summary>
    public bool AutoCommit { get; init; }

    /// <summary>
    /// Endpoint of the leader that sent the append request.
    /// Set for <c>FollowerAppend</c> operations so the completion can reply.
    /// </summary>
    public string? Endpoint { get; init; }

    /// <summary>
    /// Timestamp carried by the append request.
    /// Set for <c>FollowerAppend</c> operations and used in the reply message.
    /// </summary>
    public HLCTimestamp Timestamp { get; init; }
}
