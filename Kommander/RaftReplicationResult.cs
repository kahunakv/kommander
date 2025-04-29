
using Kommander.Data;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Represents the result of a Raft log replication operation in a Raft cluster
/// </summary>
public sealed class RaftReplicationResult
{
    /// <summary>
    /// Gets a value indicating whether the Raft log replication operation was successful.
    /// </summary>
    /// <remarks>
    /// This property returns true if the log replication operation completed successfully across the cluster,
    /// otherwise false. It indicates the overall success or failure of the operation.
    /// </remarks>
    public bool Success { get; }

    /// <summary>
    /// Gets the current status of the Raft log replication operation.
    /// </summary>
    /// <remarks>
    /// This property indicates the specific outcome or state of a Raft log replication attempt.
    /// The possible values are defined in the <see cref="RaftOperationStatus"/> enumeration,
    /// including states such as success, errors, leadership conflicts, or replication failures.
    /// </remarks>
    public RaftOperationStatus Status { get; }

    /// <summary>
    /// Gets the timestamp associated with the Raft log replication operation.
    /// </summary>
    /// <remarks>
    /// This property represents the point in time when the log replication operation was initiated or processed,
    /// using a Hybrid Logical Clock (HLC) for precise and unique timestamping across the system.
    ///
    /// It can be used to track the status of the operation within the active proposals.
    /// </remarks>
    public HLCTimestamp TicketId { get; }

    /// <summary>
    /// Gets the index of the log entry in the Raft log that was replicated as part of the operation.
    /// </summary>
    /// <remarks>
    /// This property represents the log index assigned to the replicated log entry within the Raft cluster.
    /// It can be used to identify the specific position of the log entry in the Raft log sequence.
    /// </remarks>
    public long LogIndex { get; }

    /// <summary>
    /// Represents the result of a Raft replication operation.
    /// </summary>
    public RaftReplicationResult(bool success, RaftOperationStatus status, HLCTimestamp ticketId, long logIndex)
    {
        Success = success;
        Status = status;
        TicketId = ticketId;
        LogIndex = logIndex;
    }
}