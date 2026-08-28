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
    /// Monotonic tick count stamped by <see cref="Kommander.WAL.IO.FairWalScheduler"/>
    /// at the moment the operation enters the per-partition queue. Used to compute
    /// the enqueue-to-durable latency once the write batch completes.
    /// </summary>
    internal long EnqueueTicks;

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
        long truncateFloor = -1
    )
    {
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
