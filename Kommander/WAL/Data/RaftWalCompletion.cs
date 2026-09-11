using Kommander.Data;

namespace Kommander.WAL.Data;

/// <summary>
/// Carries the fencing metadata for a finished WAL write operation.
/// Produced by <see cref="Kommander.WAL.IO.IRaftWalScheduler"/> and delivered back to the
/// owning partition executor via the <see cref="WALWriteOperation.OnComplete"/> callback.
///
/// <para>All data required to drive state-machine transitions after a WAL write is
/// stored either here (immutable, set at write time) or in the matching
/// <see cref="Kommander.Scheduling.RaftPendingWalOperation"/> entry registered before
/// the operation was enqueued.  The completion carries no reference to the original
/// <see cref="WALWriteOperation"/>.</para>
/// </summary>
public sealed record RaftWalCompletion(
    /// <summary>Partition that owns this WAL operation.</summary>
    int PartitionId,

    /// <summary>Monotonically increasing sequence number assigned by <see cref="RaftWriteAhead"/>.</summary>
    long OperationId,

    /// <summary>Raft term at the time the operation was submitted.  -1 means "not set".</summary>
    long Term,

    /// <summary>Lowest Raft log index covered by this write (inclusive). -1 when not applicable.</summary>
    long MinLogIndex,

    /// <summary>
    /// Highest Raft log index covered by this write (inclusive). -1 when not applicable.
    /// For commit/rollback operations this carries the last committed/rolled-back log index.
    /// </summary>
    long MaxLogIndex,

    /// <summary>The kind of WAL work that was performed.</summary>
    WALWriteOperationType OperationType,

    /// <summary>Whether the WAL write succeeded or failed.</summary>
    RaftOperationStatus Status,

    /// <summary>
    /// Highest log id this write actually carried, or -1 when it carried none. Distinct from
    /// <see cref="MaxLogIndex"/>, whose meaning depends on the operation type: the propose path
    /// stores the allocator's NEXT id there and the commit path the last newly committed id. The
    /// durable presence frontier needs the ids that reached the disk, so it reads this field and
    /// <see cref="SparseLogIds"/>, never <see cref="MaxLogIndex"/>.
    /// </summary>
    long WrittenMaxLogIndex = -1,

    /// <summary>
    /// The distinct ascending ids this write carried, when they do NOT fill
    /// [<see cref="MinLogIndex"/>, <see cref="WrittenMaxLogIndex"/>] contiguously; null when they
    /// do, which is the common case and allocates nothing. A batch with a hole in its own span is
    /// rare (a resolution batch that skips an id resolved earlier) but real, and a durability
    /// frontier that certified the skipped id from the span alone would certify an entry whose own
    /// write may have failed.
    /// </summary>
    long[]? SparseLogIds = null
);
