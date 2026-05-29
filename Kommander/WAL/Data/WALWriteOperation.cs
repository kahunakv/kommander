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

    public WALWriteOperation(
        Action<RaftWalCompletion> onComplete,
        long operationId,
        WALWriteOperationType type,
        (int, List<RaftLog>) logs,
        HLCTimestamp timestamp = default,
        string? endpoint = null,
        long term = -1,
        bool autoCommit = false,
        long logIndex = -1
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
    }
}
