
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// The per-entry outcome of an <see cref="IRaft.ReplicateEntries"/> batch. Results are
/// <b>index-aligned to the input list</b>: the result at position <c>i</c> describes the input entry at
/// position <c>i</c>, regardless of any internal reordering of the underlying log.
/// </summary>
/// <param name="Status">
/// The outcome for this entry.
/// <list type="bullet">
/// <item><see cref="RaftOperationStatus.Success"/> — an auto-commit entry that was appended and committed with
/// the batch; <see cref="LogIndex"/> is its assigned log index and <see cref="Ticket"/> is
/// <see cref="HLCTimestamp.Zero"/> (auto-commit entries carry no caller-held ticket).</item>
/// <item><see cref="RaftOperationStatus.Pending"/> — a trailing manual entry that was appended and is durable
/// but not yet committed; <see cref="LogIndex"/> is its assigned log index and <see cref="Ticket"/> is the
/// shared manual ticket the caller commits or rolls back later via <c>CommitLogs</c>/<c>RollbackLogs</c>.</item>
/// <item><see cref="RaftOperationStatus.PartitionMoved"/> — the entry was fenced out and <b>not</b> appended
/// by the per-entry fence; <see cref="LogIndex"/> is <c>-1</c>. The same status is also the failure status of a
/// proposal that a split or a merge fenced after classification, and that proposal was not appended either.</item>
/// <item>Any other status — a batch rejection, or a proposal that failed in flight; <see cref="LogIndex"/> is
/// <c>-1</c>. A rejected batch was not appended. After <see cref="RaftOperationStatus.ProposalOutcomeUnknown"/> or
/// <see cref="RaftOperationStatus.ProposalTimeout"/> the entry can be in the log and can commit later, so do not
/// read <c>-1</c> as proof that it was not appended.</item>
/// </list>
/// </param>
/// <param name="LogIndex">The entry's assigned log index, or <c>-1</c> when <see cref="Status"/> is a failure.</param>
/// <param name="Ticket">
/// The shared manual-commit ticket for a trailing manual (<c>AutoCommit == false</c>) entry; always
/// <see cref="HLCTimestamp.Zero"/> for auto-commit entries.
/// </param>
public readonly record struct RaftEntryResult(
    RaftOperationStatus Status,
    long LogIndex,
    HLCTimestamp Ticket
);
