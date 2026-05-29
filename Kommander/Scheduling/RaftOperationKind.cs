
namespace Kommander.Scheduling;

/// <summary>
/// Classifies a partition operation by its role in the Raft protocol.
/// Used by the <see cref="RaftPartitionExecutor"/> to assign weighted-fair priority
/// across the four operation queues.
/// </summary>
public enum RaftOperationKind
{
    /// <summary>
    /// Control-plane traffic: leader checks, heartbeats, vote requests, vote responses,
    /// handshakes, and term changes.  Must not be starved by replication or client work.
    /// </summary>
    Control,

    /// <summary>
    /// Replication-plane traffic: append-logs, complete-append-logs, and WAL-write
    /// completion callbacks.
    /// </summary>
    Replication,

    /// <summary>
    /// Client-originated work: proposals, checkpoint replications, commits, rollbacks,
    /// and state/ticket reads.
    /// </summary>
    Client,

    /// <summary>
    /// Background maintenance: restore, compaction, and discovery updates.
    /// </summary>
    Maintenance,
}
