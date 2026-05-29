
using Kommander.Data;

namespace Kommander.Scheduling;

/// <summary>
/// A unit of work targeted at a single Raft partition executor.
///
/// <para>Each operation carries the <see cref="RaftRequest"/> that was dispatched by
/// the current actor layer together with the resolved scheduling metadata so that the
/// executor can apply weighted-fair drain without re-inspecting every request field.</para>
///
/// <para>This type is intentionally a record so that it is cheap to create, pattern-match,
/// and compare during testing.</para>
/// </summary>
/// <param name="PartitionId">The partition this operation is destined for.</param>
/// <param name="Request">The underlying Raft request payload.</param>
/// <param name="Kind">Logical class of the operation (control, replication, client, maintenance).</param>
/// <param name="Priority">Weighted drain priority that corresponds to <paramref name="Kind"/>.</param>
/// <param name="SequenceNumber">
/// Monotonically increasing number assigned by the submitter to preserve within-partition
/// ordering for operations of the same kind when needed.
/// </param>
public sealed record RaftPartitionOperation(
    int PartitionId,
    RaftRequest Request,
    RaftOperationKind Kind,
    RaftOperationPriority Priority,
    long SequenceNumber
);
