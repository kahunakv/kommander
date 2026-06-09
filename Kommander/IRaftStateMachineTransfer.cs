using Kommander.System;

namespace Kommander;

/// <summary>
/// Optional callback interface an application registers to enable snapshot-based partition
/// transfer during split and merge operations.
///
/// When an implementation is registered with <see cref="IRaft"/>, the split coordinator uses
/// the snapshot primary path (ExportRange → stream → ImportRange) instead of waiting for the
/// caller to ship individual log entries. If no implementation is registered the coordinator
/// falls back to the log-shipping path.
///
/// Both methods are called on the coordinator's internal thread. Implementations must be
/// thread-safe if the same instance is shared across partitions.
/// </summary>
public interface IRaftStateMachineTransfer
{
    /// <summary>
    /// Export state for the key range described by <paramref name="plan"/> up to and including
    /// the committed log index <paramref name="upToIndex"/>.
    /// The returned <see cref="Stream"/> is consumed by the coordinator and forwarded to the
    /// target partition's leader via <see cref="ImportRange"/>. The stream must be readable and
    /// may be a <see cref="MemoryStream"/> or a file-backed stream; the coordinator disposes it
    /// after the transfer completes.
    /// </summary>
    /// <param name="plan">Describes which partition is splitting and how.</param>
    /// <param name="upToIndex">
    /// Snapshot must reflect exactly the state after this log index is applied.
    /// Entries with higher indices have not yet been applied and must not appear in the snapshot.
    /// </param>
    /// <param name="ct">Cancellation token; honour it promptly to avoid blocking the coordinator.</param>
    Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct);

    /// <summary>
    /// Install an exported snapshot into this partition's state machine.
    /// Called on the target partition's leader after it receives the stream produced by
    /// <see cref="ExportRange"/>.
    /// <para>
    /// <b>Atomicity:</b> the implementation must apply the snapshot atomically — a partial
    /// apply followed by a crash must leave the state machine in its pre-import state, not
    /// in a partially-applied state.
    /// </para>
    /// <para>
    /// <b>Checkpoint retry:</b> after a successful import the coordinator replicates a
    /// checkpoint into the target partition's log; if that checkpoint replication fails,
    /// the coordinator retries the checkpoint once. No re-import is attempted — the same
    /// local state is used for both checkpoint attempts.
    /// </para>
    /// The coordinator disposes the stream after this call returns.
    /// </summary>
    /// <param name="targetPartitionId">The partition id that will own the imported state.</param>
    /// <param name="snapshot">Readable stream produced by <see cref="ExportRange"/>; disposed by the coordinator after this call returns.</param>
    /// <param name="ct">Cancellation token.</param>
    Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct);
}
