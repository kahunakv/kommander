using Kommander.Data;
using Kommander.Time;

namespace Kommander.WAL.Data;

/// <summary>
/// Represents a single synchronous WAL write command submitted to
/// <see cref="Kommander.WAL.IO.IRaftWalScheduler"/>.
///
/// <para>Holds the partition-tagged log data and a monotonic operation id so that
/// the scheduler can maintain per-partition ordering and deliver exactly-once
/// completions via <see cref="OnComplete"/>.</para>
/// </summary>
public sealed class WALWriteOperation
{
    /// <summary>
    /// Invoked exactly once by the scheduler after the underlying synchronous
    /// WAL write (or error) has been observed.  The callback must not block.
    /// </summary>
    public Action<RaftWalCompletion> OnComplete { get; }

    public long OperationId { get; }

    public WALWriteOperationType Type { get; }

    public (int PartitionId, List<RaftLog> Logs) Logs { get; }

    public HLCTimestamp Timestamp { get; }

    public string? Endpoint { get; }

    public long Term { get; }

    public bool AutoCommit { get; }

    public long LogIndex { get; }

    /// <summary>
    /// Highest log id the enqueuing partition had accepted (contiguous presence frontier plus any
    /// ids buffered over a gap, plus this batch) at enqueue time. The scheduler's proposed-tail
    /// truncation must never delete a row at or below this floor: every such id was durably
    /// accepted by this process and may back a quorum ack, so deleting it silently loses
    /// acknowledged data while the in-memory frontiers keep certifying it (the "hole below the
    /// advertised frontier" class). <c>-1</c> means "no floor known" (non-append operations and
    /// legacy callers), which leaves the truncation cutoff unclamped.
    /// </summary>
    public long TruncateFloor { get; }

    /// <summary>
    /// True when this batch is a FIRST-durability write for at least one of its rows even though
    /// the row's type says otherwise. The scheduler's single-fsync fast path classifies a batch as
    /// lazy commit markers by log type alone (every row <c>Committed</c>), and a marker may ride the
    /// next fsync because the row it marks is already durable from its own propose write. A
    /// follower can receive a row typed <c>Committed</c> that it has never held: a propose broadcast
    /// that arrived after the leader committed it, a backfill of a committed range, a re-ship to a
    /// node that missed the propose. Such a row has no earlier durable version, so a sync-off write
    /// of it followed by a crash loses the row itself while the follower has already reported it
    /// as durably present — the leader's retention floor then compacts past what the node really
    /// holds and a restart that should be backfilled needs a snapshot. Only the enqueuing partition
    /// knows what it holds, so it sets this and the scheduler forces the fsync.
    /// </summary>
    public bool RequiresSync { get; }

    /// <summary>
    /// Monotonic tick count stamped by <see cref="Kommander.WAL.IO.FairWalScheduler"/>
    /// at the moment the operation enters the per-partition queue. Used to compute
    /// the enqueue-to-durable latency once the write batch completes.
    /// </summary>
    internal long EnqueueTicks;

    /// <summary>The vote recorded by a <see cref="WALWriteOperationType.HardState"/> operation (null = no vote); <see cref="Term"/> is the term.</summary>
    public string? VotedFor { get; }

    /// <summary>The value carried by a <see cref="WALWriteOperationType.HlcFloor"/> operation (the floor to persist); -1 otherwise.</summary>
    public long MetadataValue { get; }

    /// <summary>Outcome of a metadata write (<see cref="WALWriteOperationType.HardState"/>, <see cref="WALWriteOperationType.HlcFloor"/>), set by the worker that ran it; unused for other types.</summary>
    internal RaftOperationStatus MetadataStatus = RaftOperationStatus.Success;

    public WALWriteOperation(
        Action<RaftWalCompletion> onComplete,
        long operationId,
        WALWriteOperationType type,
        (int, List<RaftLog>) logs,
        HLCTimestamp timestamp = default,
        string? endpoint = null,
        long term = -1,
        bool autoCommit = false,
        long logIndex = -1,
        long truncateFloor = -1,
        string? votedFor = null,
        long metadataValue = -1,
        bool requiresSync = false
    )
    {
        RequiresSync = requiresSync;
        VotedFor = votedFor;
        MetadataValue = metadataValue;
        OnComplete = onComplete;
        OperationId = operationId;
        Type = type;
        Logs = logs;
        Timestamp = timestamp;
        Endpoint = endpoint;
        Term = term;
        AutoCommit = autoCommit;
        LogIndex = logIndex;
        TruncateFloor = truncateFloor;
    }
}
