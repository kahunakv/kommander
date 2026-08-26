
namespace Kommander;

/// <summary>
/// Outcome of one bounded backfill batch attempt to a follower
/// (<c>RaftPartitionStateMachine.TrySendBackfillBatchAsync</c>). The non-<see cref="Sent"/> causes
/// used to collapse into a single <see langword="false"/>, which made them indistinguishable at the
/// heartbeat call site — and the snapshot fallback there would fire for all of them, so a merely
/// saturated follower could be shipped a full snapshot transfer. The caller must fall back to a
/// snapshot only for the causes that actually mean "log shipping cannot help".
/// </summary>
internal enum BackfillSendResult
{
    /// <summary>At least one entry was shipped; the follower is progressing via log backfill.</summary>
    Sent,

    /// <summary>
    /// The follower rejected a recent batch with a saturated WAL queue and is inside its drain
    /// window. Nothing is wrong with the log — do NOT fall back to a snapshot; retry after the
    /// pause expires.
    /// </summary>
    SaturationPaused,

    /// <summary>
    /// The committed-range read at the anchor came back empty: the leader has compacted past the
    /// follower's position. Log shipping cannot help — snapshot install is the only recovery.
    /// </summary>
    CompactionFloor,

    /// <summary>
    /// Committed entries exist above the anchor but the run at the anchor is uncommitted
    /// (an inherited range whose re-commit has not landed), so an anchored batch would land over
    /// the follower's gap. Routing to the snapshot fallback is deliberate here — see the
    /// anchor-contiguity guard's comment — while the inherited-tail re-commit repairs the range.
    /// </summary>
    NonContiguous,

    /// <summary>
    /// Recent batches to this peer shipped without its reported commit frontier advancing, and the
    /// no-progress pause for the current fruitless streak has not elapsed. The batch (and its WAL
    /// range read) is skipped: re-sending what the follower already acknowledged cannot advance it,
    /// and unpaced it becomes a network-speed read loop on the shared scheduler. Nothing is wrong
    /// with the log — do NOT fall back to a snapshot; retry after the pause expires or when the
    /// frontier moves.
    /// </summary>
    NoProgressPaused,
}
