namespace Kommander.Data;

/// <summary>
/// What the WAL restore concluded about this partition's storage integrity, handed from
/// <c>RaftWriteAhead</c> to the state machine once Phase 2 has reconstructed the frontiers. The
/// restore cannot act on its own verdict — withholding votes, holding applies and asking for a
/// snapshot are consensus decisions — so it reports and the state machine decides.
/// </summary>
/// <param name="State">The storage state the restore reached; <see cref="RaftPartitionStorageState.Healthy"/> when it found nothing wrong.</param>
/// <param name="AcknowledgedThrough">The persisted acknowledged-presence watermark, or -1 when the backend keeps none.</param>
/// <param name="PresentThrough">The contiguous presence frontier the restore reconstructed from the disk.</param>
/// <param name="DurablyApplied">The application's durable floor as the provider reported it at restore, or -1.</param>
/// <param name="CompactionFloor">The backend's compaction floor at restore (0 when nothing was compacted).</param>
/// <param name="ApplicationReplayRefused">
/// True when consumer replay was refused because <paramref name="DurablyApplied"/> sits below
/// <paramref name="CompactionFloor"/>. Carried separately from <paramref name="State"/> because both
/// verdicts can hold at once and the lost-entries one takes the state: the state machine must still
/// hold applies and ask for a snapshot when this is set, whatever the state says.
/// </param>
/// <param name="LogTermBar">
/// Set (≥ 0) when the lost-entries state comes from the backend's <b>lost-synced-writes</b> verdict
/// (<see cref="WAL.IWAL.GetLostSyncedWritesReason"/>) rather than from the watermark: the term of the
/// entry at <paramref name="PresentThrough"/>. The surviving log may be a stale prefix whose watermark
/// went with the lost tail, so there is no watermark to reach; the quarantine lifts once a leader has
/// replicated past <paramref name="PresentThrough"/>, appended an entry from a term above this bar, or
/// confirmed the frontier as its own through an empty heartbeat (each proves the log is that leader's
/// prefix), and the partition then records the reconciliation in the backend. -1 for the watermark verdict.
/// </param>
/// <param name="Reason">The backend's reason text for a lost-synced-writes verdict; null otherwise.</param>
public readonly record struct RaftRestoreStorageVerdict(
    RaftPartitionStorageState State,
    long AcknowledgedThrough,
    long PresentThrough,
    long DurablyApplied,
    long CompactionFloor,
    bool ApplicationReplayRefused = false,
    long LogTermBar = -1,
    string? Reason = null)
{
    /// <summary>The verdict of a restore that found nothing wrong, and of facades that do not track storage integrity.</summary>
    public static RaftRestoreStorageVerdict None { get; } = new(RaftPartitionStorageState.Healthy, -1, -1, -1, 0);
}
