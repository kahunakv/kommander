
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// One typed entry in a heterogeneous <see cref="IRaft.ReplicateEntries"/> batch.
/// <para>
/// Unlike the single-type <c>ReplicateLogs</c> surface — which forces one <see cref="Type"/> and one
/// <see cref="AutoCommit"/> flag across every payload — each entry in a batch carries its own type, so a
/// consumer can coalesce unrelated writes (key/value records, lock records, receipts, metadata) into one
/// proposal, one AppendEntries round trip, and one group-committed WAL flush.
/// </para>
/// <para>
/// Scope (see the heterogeneous-write-coalescing design): a batch is a leading auto-commit group plus an
/// optional single <b>trailing</b> manual (<see cref="AutoCommit"/> == <see langword="false"/>) group. Once a
/// manual entry appears, no later entry may be auto-commit — an auto-commit entry after a manual entry is a
/// batch-level rejection, because the manual entries must form one contiguous truncatable suffix.
/// <see cref="ExpectedGeneration"/> is a <b>per-entry</b> fence: each entry is fenced independently against the
/// partition's committed generation, so a batch may freely mix hash-routed (generation-0) entries with fenced
/// key-range entries and different non-zero generations. A fenced-out entry is reported
/// <see cref="RaftOperationStatus.PartitionMoved"/> and dropped while its siblings proceed.
/// </para>
/// </summary>
/// <param name="Type">The consumer log type for this entry (maps to <see cref="RaftLog.LogType"/>).</param>
/// <param name="Data">The opaque payload for this entry (maps to <see cref="RaftLog.LogData"/>).</param>
/// <param name="AutoCommit">
/// Whether this entry commits with the batch (<see langword="true"/>) or belongs to the trailing manual group
/// whose ticket the caller commits or rolls back later (<see langword="false"/>). Any manual entries must sort
/// after every auto-commit entry.
/// </param>
/// <param name="ExpectedGeneration">
/// When non-zero, this entry is fenced against the partition's committed generation and dropped with
/// <see cref="RaftOperationStatus.PartitionMoved"/> on mismatch (its siblings are unaffected). Zero disables
/// the fence for this entry.
/// </param>
public readonly record struct RaftProposalEntry(
    string Type,
    byte[] Data,
    bool AutoCommit = true,
    long ExpectedGeneration = 0
);
