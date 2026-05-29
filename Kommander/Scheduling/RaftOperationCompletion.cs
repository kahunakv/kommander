
using Kommander.Data;

namespace Kommander.Scheduling;

/// <summary>
/// Fencing metadata returned to the partition executor when a WAL I/O operation
/// completes (successfully or with an error).
///
/// <para>The executor must validate all fence fields before acting on the completion.
/// A completion whose <see cref="Term"/> or <see cref="OperationId"/> no longer matches
/// the partition's current term or expected sequence must be silently dropped (rule 5 of
/// the non-negotiable correctness rules).</para>
/// </summary>
/// <param name="PartitionId">Partition that owns this completion.</param>
/// <param name="OperationId">
/// Unique identifier of the WAL operation that finished; must match the id that was
/// submitted to the I/O scheduler.
/// </param>
/// <param name="Term">Raft term in effect when the WAL operation was submitted.</param>
/// <param name="LogIndexStart">First log index written by the operation, or -1 if not applicable.</param>
/// <param name="LogIndexEnd">Last log index written by the operation, or -1 if not applicable.</param>
/// <param name="OperationKind">
/// Kind of the original partition operation (used for metrics and stale-drop logging).
/// </param>
/// <param name="Status">Whether the underlying I/O succeeded or failed.</param>
public sealed record RaftOperationCompletion(
    int PartitionId,
    long OperationId,
    long Term,
    long LogIndexStart,
    long LogIndexEnd,
    RaftOperationKind OperationKind,
    RaftOperationStatus Status
);
