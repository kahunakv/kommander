using Kommander.Data;

namespace Kommander.WAL.Data;

/// <summary>
/// Carries the completion metadata for a finished WAL write operation.
/// Produced by <see cref="Kommander.WAL.IO.IRaftWalScheduler"/> and delivered back to the
/// owning partition executor (or, during the transition period, routed through
/// <see cref="RaftStateActor"/> via an actor message).
/// </summary>
public sealed record RaftWalCompletion(
    /// <summary>Partition that owns this WAL operation.</summary>
    int PartitionId,

    /// <summary>Monotonically increasing sequence number assigned by <see cref="RaftWriteAhead"/>.</summary>
    long OperationId,

    /// <summary>Raft term at the time the operation was submitted.</summary>
    long Term,

    /// <summary>Lowest Raft log index covered by this write (inclusive). -1 when not applicable.</summary>
    long MinLogIndex,

    /// <summary>Highest Raft log index covered by this write (inclusive). -1 when not applicable.</summary>
    long MaxLogIndex,

    /// <summary>The kind of WAL work that was performed.</summary>
    WALWriteOperationType OperationType,

    /// <summary>Whether the WAL write succeeded or failed.</summary>
    RaftOperationStatus Status,

    /// <summary>
    /// Reference to the original <see cref="WALWriteOperation"/>.
    /// Retained for the transition period while completion is still routed through
    /// <see cref="RaftStateActor"/> actor messages.  Will be removed once
    /// <see cref="RaftPartitionStateMachine"/> accepts <see cref="RaftWalCompletion"/> directly.
    /// </summary>
    WALWriteOperation? Operation = null
);
