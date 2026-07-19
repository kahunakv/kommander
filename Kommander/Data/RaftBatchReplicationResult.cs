
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// The result of an <see cref="IRaft.ReplicateEntries"/> batch: an overall status plus one
/// <see cref="RaftEntryResult"/> per input entry, index-aligned to the input list.
/// <para>
/// Two failure shapes are distinguished so <see cref="Entries"/> stays meaningful:
/// <list type="bullet">
/// <item><b>Batch-level failure</b> — a shape violation (a manual entry in a slice-1 batch, or mixed non-zero
/// generations) or a whole-batch fence miss. <see cref="Success"/> is <see langword="false"/>, and every
/// entry in <see cref="Entries"/> carries the same failure status with <c>LogIndex = -1</c>. Nothing was
/// appended.</item>
/// <item><b>Per-entry failure</b> — only the affected entry's slot carries a failure status (e.g.
/// <see cref="RaftOperationStatus.PartitionMoved"/>) while its siblings succeed and <see cref="Success"/>
/// remains <see langword="true"/>. (Reserved for the deferred per-entry-fence slice.)</item>
/// </list>
/// </para>
/// <para>
/// On success a batch may legitimately carry mixed <i>statuses</i> that are not failures: auto-commit entries
/// report <see cref="RaftOperationStatus.Success"/> while a trailing manual group reports
/// <see cref="RaftOperationStatus.Pending"/> with the shared manual ticket. <see cref="TicketId"/> then holds
/// that manual ticket (the actionable one); for an auto-commit-only batch it holds the auto proposal's ticket.
/// </para>
/// </summary>
public sealed class RaftBatchReplicationResult
{
    /// <summary>
    /// Whether the batch was admitted and at least one entry was appended. This is <see langword="false"/> only
    /// on a batch-level rejection (bad shape) or when <b>every</b> entry was fenced out by the per-entry
    /// generation check (nothing landed). A successful batch may still carry individual
    /// <see cref="RaftOperationStatus.PartitionMoved"/> entries in <see cref="Entries"/> — inspect per entry.
    /// </summary>
    public bool Success { get; }

    /// <summary>
    /// The overall status: <see cref="RaftOperationStatus.Success"/> when at least one entry was admitted,
    /// the rejection reason on a batch-level rejection, or <see cref="RaftOperationStatus.PartitionMoved"/>
    /// when every entry was fenced out.
    /// </summary>
    public RaftOperationStatus Status { get; }

    /// <summary>
    /// The shared ticket of the batch's single proposal, primarily for diagnostics/correlation.
    /// <see cref="HLCTimestamp.Zero"/> when the batch was rejected before any append.
    /// </summary>
    public HLCTimestamp TicketId { get; }

    /// <summary>Per-entry outcomes, index-aligned to the input entry list.</summary>
    public IReadOnlyList<RaftEntryResult> Entries { get; }

    public RaftBatchReplicationResult(bool success, RaftOperationStatus status, HLCTimestamp ticketId, IReadOnlyList<RaftEntryResult> entries)
    {
        Success = success;
        Status = status;
        TicketId = ticketId;
        Entries = entries;
    }
}
