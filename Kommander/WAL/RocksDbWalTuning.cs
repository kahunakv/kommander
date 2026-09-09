
namespace Kommander.WAL;

/// <summary>
/// Sizing knobs for the shard column families of <see cref="RocksDbWAL"/> — the CFs that hold the
/// append-then-die Raft log rows. Exists so a write-probe harness (and hosts with unusual log
/// rates or memory budgets) can tune the flush unit without a new package; the defaults implement
/// the CF-sizing lever of the write-amplification fix.
///
/// <para><b>What the sizing is for.</b> A log entry dies when the compaction pass covers it with a
/// range tombstone (the durability floor passed it). An entry and its tombstone that co-reside in
/// the same flush unit are dropped AT FLUSH — zero bytes reach L0 for them. An entry flushed
/// before its tombstone arrives survives to L0 and must be rewritten by compaction before it can
/// die (the 3.7x compaction residue of probe w2). The flush unit should therefore span the log's
/// typical entry lifetime.</para>
///
/// <para><b>Why merge count, not memtable size.</b> The defaults double the flush-unit span by
/// merging two 64 MB memtables per flush (<see cref="ShardMinWriteBufferNumberToMerge"/> = 2)
/// instead of doubling <see cref="ShardWriteBufferSizeBytes"/>. A memtable's arena grows with the
/// data either way, but a second small memtable also halves the WAL-replay unit on restart and
/// keeps the per-allocation granularity the shared WriteBufferManager accounts in.</para>
///
/// <para><b>Memory envelope.</b> Worst case per shard CF is
/// <c>ShardWriteBufferSizeBytes × ShardMaxWriteBufferNumber</c> — 256 MB with the defaults, up
/// from 128 MB on the RocksDB defaults. Only CFs that actually receive writes materialize
/// memtables (a typical deployment writes 1–2 of the 8 shards), and arenas grow with data rather
/// than being preallocated. Under a shared <see cref="RocksDbSharedResources"/> WriteBufferManager
/// the budget caps real usage: over budget, RocksDB flushes early — which safely degrades this
/// lever back toward the old behavior instead of growing memory. Hosts that saw the 1.4 GiB abort
/// (run Q) should size the WBM budget, not disable the merge.</para>
///
/// <para><b>Stall-lock guard.</b> <see cref="ShardMaxWriteBufferNumber"/> must be at least
/// <see cref="ShardMinWriteBufferNumberToMerge"/> + 1: a flush waits for the merge quorum of
/// immutable memtables, and the writer needs one mutable memtable above that quorum or every
/// rotation write-stalls until the flush finishes. <see cref="RocksDbWAL"/> validates this.</para>
/// </summary>
public sealed record RocksDbWalTuning
{
    /// <summary>The tuning <see cref="RocksDbWAL"/> uses when the caller passes none.</summary>
    public static RocksDbWalTuning Default { get; } = new();

    /// <summary>
    /// Size of one shard-CF memtable in bytes. Kept at the RocksDB default (64 MB): the flush-unit
    /// span is widened via <see cref="ShardMinWriteBufferNumberToMerge"/> instead (see the type
    /// summary for why).
    /// </summary>
    public long ShardWriteBufferSizeBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// Immutable memtables merged into one flush. 2 doubles the window in which an entry and its
    /// range tombstone co-reside and die at flush; the merge also deduplicates across both
    /// memtables. 1 restores the pre-tuning behavior.
    /// </summary>
    public int ShardMinWriteBufferNumberToMerge { get; init; } = 2;

    /// <summary>
    /// Maximum memtables (mutable + immutable) per shard CF. Must exceed
    /// <see cref="ShardMinWriteBufferNumberToMerge"/> — see the stall-lock guard in the type
    /// summary. 4 leaves one flush in flight plus one mutable memtable of headroom.
    /// </summary>
    public int ShardMaxWriteBufferNumber { get; init; } = 4;

    /// <summary>
    /// L0 file count that triggers compaction into the base level. Raised from the RocksDB default
    /// of 4 so short-lived rows meet their range tombstone in L0 instead of being rewritten down
    /// the levels first (measured on the 2026-09-09 write probe).
    /// </summary>
    public int ShardLevel0FileNumCompactionTrigger { get; init; } = 8;

    /// <summary>L0 file count at which RocksDB slows writers. Headroom above the compaction trigger.</summary>
    public int ShardLevel0SlowdownWritesTrigger { get; init; } = 28;

    /// <summary>L0 file count at which RocksDB stops writers entirely.</summary>
    public int ShardLevel0StopWritesTrigger { get; init; } = 44;

    // ── Layout experiment (probe-driven; see the write-amp feature's w3 analysis) ────────────────
    //
    // The flush-unit lever above kills entries that die within one flush. Entries the durability
    // floor holds longer still cascade L0→L5→L6 before their range tombstone catches them, and
    // DeleteFilesInRange cannot reclaim them while they sit in L0/L5 (it acts on whole files in the
    // lower levels only). The knobs below let a probe make the log NOT need a bottom level, so a
    // range tombstone annihilates its entries in at most one rewrite (W-Amp near 2) instead of the
    // multi-level cascade. Defaults keep the shipped leveled layout, so they are inert until set.

    /// <summary>
    /// <c>max_bytes_for_level_base</c> for the shard CFs, or 0 to leave the RocksDB default
    /// (256 MB). Sizing this at or above the retained-log window keeps the whole live log in the
    /// base level, where a range tombstone meets its entries in one compaction rather than after an
    /// L5→L6 push. 0 = unchanged.
    /// </summary>
    public long ShardMaxBytesForLevelBase { get; init; }

    /// <summary>
    /// When true, the shard CFs use universal compaction instead of leveled. Universal keeps the
    /// log in few large sorted runs and lets a range tombstone drop covered data without the
    /// leveled L0→Lmax cascade — the spec's alternative when whole-file drops alone underperform.
    /// The retention bound stays the durability floor (compaction handles it); this only changes
    /// the physical layout. Default false (leveled, as shipped).
    /// </summary>
    public bool ShardUniversalCompaction { get; init; }
}
