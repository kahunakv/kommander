
using Kommander.Data;
using Kommander.Time;

namespace Kommander.Scheduling;

/// <summary>
/// Tracks metadata for an in-flight WAL operation inside the partition state machine.
///
/// <para>Fields are populated at enqueue time so the completion handler never needs to
/// reach back into the original <see cref="WAL.Data.WALWriteOperation"/>.</para>
///
/// <para><b>Pooling:</b> one of these is created per accepted WAL op and discarded once its
/// completion is handled — a high-frequency churn on a busy leader. The state machine pools instances
/// (rent on insert, return after the completion drains it); a benchmark showed pooling cuts this path's
/// allocation to ~0.25× vs. a fresh object per op, and that a <c>readonly struct</c> alternative
/// actually regressed (its ~90-byte value enlarges every dictionary entry). Properties are therefore
/// settable and <see cref="Reset"/> clears every field so a rented instance never carries stale state.
/// The state machine is single-threaded per partition, so the pool needs no synchronization.</para>
/// </summary>
public sealed class RaftPendingWalOperation
{
    /// <summary>Optional reply correlation id assigned by the actor adapter.</summary>
    public ulong? ReplyCorrelationId { get; set; }

    public RaftProposalQuorum? Proposal { get; set; }

    public HLCTimestamp TicketId { get; set; }

    /// <summary>
    /// Log entries associated with the operation.
    /// Set for <c>LeaderPropose</c> and <c>FollowerAppend</c> operations.
    /// </summary>
    public List<RaftLog>? Logs { get; set; }

    /// <summary>
    /// Whether the proposal should be committed automatically after reaching quorum.
    /// Set for <c>LeaderPropose</c> operations.
    /// </summary>
    public bool AutoCommit { get; set; }

    /// <summary>
    /// Endpoint of the leader that sent the append request.
    /// Set for <c>FollowerAppend</c> operations so the completion can reply.
    /// </summary>
    public string? Endpoint { get; set; }

    /// <summary>
    /// Timestamp carried by the append request.
    /// Set for <c>FollowerAppend</c> operations and used in the reply message.
    /// </summary>
    public HLCTimestamp Timestamp { get; set; }

    /// <summary>Clears every field before the instance is returned to the pool.</summary>
    internal void Reset()
    {
        ReplyCorrelationId = null;
        Proposal = null;
        TicketId = default;
        Logs = null;
        AutoCommit = false;
        Endpoint = null;
        Timestamp = default;
    }
}
