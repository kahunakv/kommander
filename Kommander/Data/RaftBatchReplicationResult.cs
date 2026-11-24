
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// The result of an <see cref="IRaft.ReplicateEntries"/> batch: an overall status plus one
/// <see cref="RaftEntryResult"/> per input entry, index-aligned to the input list.
/// <para>
/// Three failure shapes are distinguished so <see cref="Entries"/> stays meaningful:
/// <list type="bullet">
/// <item><b>Batch rejection</b> — an auto-commit entry after a manual entry
/// (<see cref="RaftOperationStatus.Errored"/>), or an <see cref="RaftProposalEntry.ExpectedTerm"/> that is stale
/// or differs from another non-zero one (<see cref="RaftOperationStatus.TermMismatch"/>). <see cref="Success"/>
/// is <see langword="false"/>, and every slot carries that status with <c>LogIndex = -1</c>. Nothing was
/// appended.</item>
/// <item><b>Proposal failure</b> — the proposal of the auto or the manual group was refused (for example
/// <see cref="RaftOperationStatus.NodeIsNotLeader"/>), or it was accepted and then failed or did not reach quorum
/// in time. <see cref="Success"/> is <see langword="false"/>, and the slots of that group carry the failure status
/// with <c>LogIndex = -1</c>. That index does <b>not</b> prove that they were not appended: after
/// <see cref="RaftOperationStatus.ProposalOutcomeUnknown"/> or <see cref="RaftOperationStatus.ProposalTimeout"/>
/// the entries can be in the log and can commit later. When the auto group fails, the manual group is not proposed
/// and carries the same status. When only the manual group fails, the auto slots keep
/// <see cref="RaftOperationStatus.Success"/> and are committed, so a caller must not retry the whole batch
/// blindly.</item>
/// <item><b>Per-entry fence</b> — only the affected slot carries
/// <see cref="RaftOperationStatus.PartitionMoved"/> while its siblings proceed and <see cref="Success"/> remains
/// <see langword="true"/>. When every entry is fenced, <see cref="Success"/> is <see langword="false"/> and
/// nothing was appended.</item>
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
    /// Whether every admitted group reached quorum. <see langword="false"/> on a batch rejection, when
    /// <b>every</b> entry was fenced out, or when the proposal of either group was refused or failed. Only the first
    /// two mean that nothing landed: after a failed manual group, the auto entries are committed. A successful
    /// batch may still carry individual <see cref="RaftOperationStatus.PartitionMoved"/> entries in
    /// <see cref="Entries"/> — inspect per entry. An empty batch succeeds with no entries.
    /// </summary>
    public bool Success { get; }

    /// <summary>
    /// The overall status: <see cref="RaftOperationStatus.Success"/> when <see cref="Success"/> is
    /// <see langword="true"/>, the reason of a batch rejection or of a failed proposal, or
    /// <see cref="RaftOperationStatus.PartitionMoved"/> when every entry was fenced out.
    /// </summary>
    public RaftOperationStatus Status { get; }

    /// <summary>
    /// The ticket of the manual group when the batch has one, else the ticket of the auto group.
    /// <see cref="HLCTimestamp.Zero"/> whenever <see cref="Success"/> is <see langword="false"/> — also when the
    /// auto group committed and the manual group failed — and for an empty batch.
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
