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
    HlcFloor
}
