namespace Kommander;

/// <summary>
/// What a partition does when its WAL restore finds the log shorter than what this node had
/// acknowledged as durably held — the persisted acknowledged-presence watermark above the
/// contiguous log on disk (a synced tail truncated or lost after the leader may have counted it).
/// See <see cref="Data.RaftPartitionStorageState.LostAcknowledgedEntries"/>.
/// </summary>
public enum LostAcknowledgedEntriesPolicy
{
    /// <summary>
    /// The safe default: the partition neither votes nor campaigns until the leader has replicated
    /// past the watermark again or a snapshot install covers it, so a log that lost committed
    /// entries can never elect a candidate that lacks them (Alagappan et al., FAST 2018). It keeps
    /// acknowledging what it really holds, so with a replication factor above one the repair is
    /// automatic. A single-voter partition has no peer to repair from and stays leaderless with a
    /// Critical log until the operator restores the data or switches to <see cref="AcceptAndLog"/>.
    /// </summary>
    Quarantine,

    /// <summary>
    /// The operator accepts the loss: the restore logs the lost range at Critical, rewrites the
    /// watermark to what the disk holds so the next restart does not re-flag it, and the partition
    /// runs normally. The acknowledged entries above the surviving log are gone for good on this
    /// node; with a replication factor of one they are gone for good, full stop. Audited as a
    /// disabled safety fence at startup.
    /// </summary>
    AcceptAndLog,
}
