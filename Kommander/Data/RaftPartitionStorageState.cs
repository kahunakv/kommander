namespace Kommander.Data;

/// <summary>
/// What a node knows about the integrity of one partition's storage on this node — the
/// storage-corruption and lost-write verdicts a restore or a running engine can reach, and the
/// consensus consequence each one carries. Exposed through
/// <see cref="IRaft.GetPartitionStorageHealth"/> so a host can put it in its readiness probe; a
/// partition in any state but <see cref="Healthy"/> or <see cref="Restoring"/> is one the host
/// should report as not ready.
/// </summary>
public enum RaftPartitionStorageState
{
    /// <summary>Storage is intact as far as the node can tell. The partition votes, leads and acknowledges normally.</summary>
    Healthy,

    /// <summary>The WAL restore has not completed; no verdict yet. Votes are dropped and client operations refused until it does.</summary>
    Restoring,

    /// <summary>
    /// The restore found that this node may have lost entries it acknowledged as durably held, in one
    /// of two ways: the persisted acknowledged-presence watermark sits above the contiguous log on
    /// disk (rows removed under a surviving watermark), or the storage engine's sync ledger attested
    /// writes its log no longer holds (<see cref="WAL.IWAL.GetLostSyncedWritesReason"/>: a log file
    /// truncated or lost after its fsync, which cuts the watermark together with the rows, so every
    /// partition on that engine is suspect and its surviving log may be a stale prefix). The
    /// partition neither votes nor campaigns until a leader has repaired it — replicated past the
    /// watermark or the surviving frontier, appended an entry from a newer term, or confirmed the
    /// frontier as its own; it keeps acknowledging the entries it really holds, which is truthful and
    /// safe. Self-healing with a replication factor above one; a single-voter partition stays
    /// leaderless until the operator acts (see
    /// <see cref="RaftConfiguration.LostAcknowledgedEntriesPolicy"/>). The health reason names which
    /// of the two verdicts it is.
    /// </summary>
    LostAcknowledgedEntries,

    /// <summary>
    /// The application's durable floor (<see cref="IApplicationDurabilityProvider"/>) sits below
    /// the WAL's compaction floor: the entries the application still needs were compacted, so
    /// replaying what remains would apply a suffix onto a store missing its prefix. Consumer
    /// replay and live applies are held, the partition does not campaign, and it asks the leader
    /// for a whole-partition snapshot (the only repair). It keeps voting: its Raft log is complete.
    /// </summary>
    ApplicationFloorBelowCompaction,

    /// <summary>
    /// The storage engine fail-stopped on a <c>Corruption</c> status (a damaged SST or log file)
    /// and refuses every write. The partition stepped down if it led, and neither votes, campaigns
    /// nor acknowledges; reads of intact blocks still work. Permanent for this process: the
    /// operator moves the damaged directory aside and lets the node rebuild from its peers.
    /// </summary>
    EngineFailed,

    /// <summary>The partition's WAL restore threw and the partition stopped (a malformed entry, a read that hit damage). Nothing runs for it.</summary>
    RestoreFailed,
}
