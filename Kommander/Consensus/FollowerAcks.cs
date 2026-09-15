
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;

namespace Kommander.Consensus;

/// <summary>
/// Builds the acknowledgement a follower sends for one AppendEntries — Success or any rejection,
/// heartbeat or entry-carrying — so every ack carries the two facts about this node that the leader
/// must never infer from the protocol frontier:
/// <list type="bullet">
///   <item><see cref="CompleteAppendLogsRequest.DurableIndex"/>: the durable contiguous commit
///   frontier (<see cref="IRaftWalFacade.GetDurableCommitFrontier"/>). The leader holds WAL retention
///   on it. The Success ack's <see cref="CompleteAppendLogsRequest.CommitIndex"/> is the protocol
///   frontier, which advances when an append is queued: while this node's disk is stalled it keeps
///   rising with every entry the leader ships, and a floor computed from it compacted exactly the
///   range the follower needed once its disk answered (CamusDB slow-disk run sd8).</item>
///   <item><see cref="CompleteAppendLogsRequest.WalStallMs"/>: how long the oldest write handed to
///   the storage engine has gone unanswered. A leader that sees it above its stall threshold stops
///   shipping entry-carrying backfill and snapshots to this node until the disk answers — neither
///   can land, and buffering a snapshot is what OOM-killed a follower six seconds after its heal.</item>
/// </list>
/// Both are read on the partition executor, the single writer of the frontiers.
/// </summary>
internal static class FollowerAcks
{
    /// <summary>
    /// An ack that carries NO position: commit index and durable index both -1, no stall age. Sent
    /// while this partition's WAL restore has not completed. Every frontier the other overload
    /// reads is still at its pre-restore init then (a contiguous frontier of 0 on a node whose
    /// disk holds millions of entries), and a leader that took 0 as evidence anchored a backfill
    /// at 1 — below its compaction floor — and escalated straight to a snapshot transfer the node
    /// did not need (the Caraxes bank-leader-kill restart). The leader treats -1 as "no report":
    /// it leaves matchIndex, nextIndex, the retention floor and the backfill triggers untouched.
    /// </summary>
    public static CompleteAppendLogsRequest BuildWithoutPosition(
        IRaftPartitionHost host,
        long term,
        HLCTimestamp timestamp,
        RaftOperationStatus status) =>
        new(
            host.PartitionId,
            term,
            timestamp,
            host.LocalEndpoint,
            status,
            -1,
            durableIndex: -1,
            walStallMs: 0);

    public static CompleteAppendLogsRequest Build(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        long term,
        HLCTimestamp timestamp,
        RaftOperationStatus status,
        long index) =>
        new(
            host.PartitionId,
            term,
            timestamp,
            host.LocalEndpoint,
            status,
            index,
            durableIndex: wal.GetDurableCommitFrontier(),
            walStallMs: (long)wal.GetOldestPendingWriteAgeMs());
}
