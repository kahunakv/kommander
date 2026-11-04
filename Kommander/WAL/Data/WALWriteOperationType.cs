namespace Kommander.WAL.Data;

public enum WALWriteOperationType
{
    LeaderPropose,
    LeaderCommit,
    LeaderRollback,
    FollowerAppend,
    Compaction,

    /// <summary>
    /// A Raft hard-state write (currentTerm, votedFor) routed through the scheduler so a vote grant or a
    /// leader adoption never blocks the partition executor on the storage engine: the metadata write runs
    /// on a WAL worker and its completion, delivered like any other, carries the reply or the bookkeeping
    /// that must wait for durability.
    /// </summary>
    HardState,

    /// <summary>
    /// The durable HLC high-water mark (<see cref="IWAL.PersistHlcFloor"/>), routed through the scheduler
    /// for the same reason as <see cref="HardState"/>: written inline it held the partition executor for
    /// the length of a device stall once per slack window (the 15 s <c>ReplicateLogs</c> and 16 s
    /// <c>AppendLogs</c> dispatches of the CamusDB slow-disk runs). Fire-and-forget; a failed write only
    /// regresses the in-memory bound so the next observation retries.
    /// </summary>
    HlcFloor,

    /// <summary>
    /// A correction of the partition's acknowledged-presence watermark (see
    /// <see cref="IWAL.Write(List{ValueTuple{int, List{RaftLog}}}, bool, IReadOnlyDictionary{int, long})"/>),
    /// queued by the Raft layer after a failed write regressed its presence frontier. Every
    /// log-carrying operation records the frontier it will establish; an operation queued between
    /// the failed write and the regression recorded a value that counts the failed range, so this
    /// operation follows it in the same per-partition FIFO and assigns the regressed value. Carries
    /// no rows; <see cref="WALWriteOperation.MetadataValue"/> is the value. Folded by the scheduler
    /// in submission order like the per-operation values, never run ahead of the batch like the
    /// metadata operations above — ordering is the whole point.
    /// </summary>
    AcknowledgedPresence
}
