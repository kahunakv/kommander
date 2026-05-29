
using Kommander.Data;

namespace Kommander.Scheduling;

/// <summary>
/// Maps <see cref="RaftRequestType"/> values to the scheduling metadata required by the
/// <c>RaftPartitionExecutor</c>.
///
/// <para>This class is intentionally static and allocation-free so it can be called on
/// every message dispatch without overhead.</para>
///
/// <para>The mapping intentionally mirrors the priority groups already used in
/// <c>RaftStateActor.Receive</c>, expressed in the new vocabulary so that future tasks
/// can eliminate the actor without reconsidering the classification.</para>
/// </summary>
public static class RaftOperationMapper
{
    /// <summary>
    /// Returns the <see cref="RaftOperationKind"/> that corresponds to the given request type.
    /// </summary>
    public static RaftOperationKind GetKind(RaftRequestType requestType) =>
        requestType switch
        {
            // ── Control ──────────────────────────────────────────────────────────────
            // Leader liveness checks, handshakes, and all election traffic must never
            // be starved by replication or client work.
            RaftRequestType.CheckLeader       => RaftOperationKind.Control,
            RaftRequestType.ReceiveHandshake  => RaftOperationKind.Control,
            RaftRequestType.RequestVote       => RaftOperationKind.Control,
            RaftRequestType.ReceiveVote       => RaftOperationKind.Control,

            // ── Replication ───────────────────────────────────────────────────────────
            // Cross-node log exchange and WAL completion callbacks.
            RaftRequestType.AppendLogs           => RaftOperationKind.Replication,
            RaftRequestType.CompleteAppendLogs   => RaftOperationKind.Replication,
            RaftRequestType.WriteOperationCompleted => RaftOperationKind.Replication,

            // ── Client ────────────────────────────────────────────────────────────────
            // Proposals, commits, rollbacks, checkpoints, and state/ticket reads all
            // originate from (or on behalf of) external callers.
            RaftRequestType.ReplicateLogs        => RaftOperationKind.Client,
            RaftRequestType.ReplicateCheckpoint  => RaftOperationKind.Client,
            RaftRequestType.CommitLogs           => RaftOperationKind.Client,
            RaftRequestType.RollbackLogs         => RaftOperationKind.Client,
            RaftRequestType.GetNodeState         => RaftOperationKind.Client,
            RaftRequestType.GetTicketState       => RaftOperationKind.Client,

            _ => throw new ArgumentOutOfRangeException(nameof(requestType), requestType, "Unrecognised RaftRequestType"),
        };

    /// <summary>
    /// Returns the <see cref="RaftOperationPriority"/> weight that corresponds to the
    /// given request type.  The weight is derived from the kind so the two values are
    /// always consistent.
    /// </summary>
    public static RaftOperationPriority GetPriority(RaftRequestType requestType) =>
        GetKind(requestType) switch
        {
            RaftOperationKind.Control     => RaftOperationPriority.Control,
            RaftOperationKind.Replication => RaftOperationPriority.Replication,
            RaftOperationKind.Client      => RaftOperationPriority.Client,
            RaftOperationKind.Maintenance => RaftOperationPriority.Maintenance,
            var k => throw new ArgumentOutOfRangeException(nameof(k), k, "Unrecognised RaftOperationKind"),
        };

    /// <summary>
    /// Constructs a <see cref="RaftPartitionOperation"/> from an existing
    /// <see cref="RaftRequest"/>, assigning scheduling metadata automatically.
    /// </summary>
    /// <param name="partitionId">The partition the operation belongs to.</param>
    /// <param name="request">The original request.</param>
    /// <param name="sequenceNumber">
    /// Caller-assigned monotonic sequence number for within-partition ordering.
    /// </param>
    public static RaftPartitionOperation CreateOperation(
        int partitionId,
        RaftRequest request,
        long sequenceNumber) =>
        new(
            partitionId,
            request,
            GetKind(request.Type),
            GetPriority(request.Type),
            sequenceNumber);

    /// <summary>
    /// Maps a request type to the legacy <see cref="RaftStatePriority"/> used by
    /// <c>RaftStateActor</c> during the actor-to-executor migration.
    ///
    /// <para>This preserves existing batch-drain behavior exactly.  It is a temporary
    /// bridge and will be removed when <c>RaftPartitionExecutor</c> replaces the actor
    /// priority buckets.</para>
    /// </summary>
    public static RaftStatePriority GetLegacyStatePriority(RaftRequestType requestType) =>
        requestType switch
        {
            RaftRequestType.CheckLeader or
            RaftRequestType.ReceiveHandshake or
            RaftRequestType.RequestVote or
            RaftRequestType.ReceiveVote => RaftStatePriority.High,

            RaftRequestType.GetNodeState or
            RaftRequestType.GetTicketState => RaftStatePriority.Low,

            RaftRequestType.AppendLogs or
            RaftRequestType.CompleteAppendLogs or
            RaftRequestType.ReplicateLogs or
            RaftRequestType.ReplicateCheckpoint or
            RaftRequestType.CommitLogs or
            RaftRequestType.RollbackLogs or
            RaftRequestType.WriteOperationCompleted => RaftStatePriority.Mid,

            _ => throw new ArgumentOutOfRangeException(nameof(requestType), requestType, "Unrecognised RaftRequestType"),
        };

    /// <summary>
    /// Returns whether the request type uses the actor's single-slot priority bucket
    /// (<see cref="RaftStatePriority.High"/> with deduplication) rather than the
    /// multi-message bucket.
    /// </summary>
    public static bool UsesLegacySinglePrioritySlot(RaftRequestType requestType) =>
        requestType == RaftRequestType.CheckLeader;
}
