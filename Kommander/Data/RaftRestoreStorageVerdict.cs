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
public readonly record struct RaftRestoreStorageVerdict(
    RaftPartitionStorageState State,
    long AcknowledgedThrough,
    long PresentThrough,
    long DurablyApplied,
    long CompactionFloor,
    bool ApplicationReplayRefused = false)
{
    /// <summary>The verdict of a restore that found nothing wrong, and of facades that do not track storage integrity.</summary>
    public static RaftRestoreStorageVerdict None { get; } = new(RaftPartitionStorageState.Healthy, -1, -1, -1, 0);
}
