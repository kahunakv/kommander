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
/// <para><b>Legacy catch-up fallback:</b> when a follower falls below the leader's WAL compaction
/// floor and no <see cref="IRaftPartitionStateTransfer"/> is registered, the leader also calls
/// <see cref="ExportRange"/> with a <em>boundless</em> plan (only
/// <see cref="RaftSplitPlan.TargetPartitionId"/> set, meaning "the entire partition") and the
/// follower installs it via <see cref="ImportRange"/>. Applications whose range transfer cannot
/// serve whole-partition exports should register <see cref="IRaftPartitionStateTransfer"/>
/// instead — the catch-up path prefers it and never touches this interface then.</para>
///
/// Both methods are called on the coordinator's internal thread. Implementations must be
/// thread-safe if the same instance is shared across partitions.
/// </summary>
public interface IRaftStateMachineTransfer
{
    /// <summary>
    /// Export state for the key range described by <paramref name="plan"/> covering the committed
    /// log index <paramref name="upToIndex"/>.
    /// The returned <see cref="Stream"/> is consumed by the coordinator and forwarded to the
    /// target partition's leader via <see cref="ImportRange"/>. The stream must be readable and
    /// may be a <see cref="MemoryStream"/> or a file-backed stream; the coordinator disposes it
    /// after the transfer completes.
    /// </summary>
    /// <param name="plan">Describes which partition is splitting and how.</param>
    /// <param name="upToIndex">
    /// The snapshot must reflect <b>at least</b> the state after this log index is applied; it
    /// may reflect newer committed state (e.g. an MVCC store snapshotting by timestamp after
    /// later entries applied). The receiver seeds its boundary at this index and replays any
    /// retained log entries above it onto the imported state, so applying an entry already
    /// reflected in the snapshot must be a no-op (idempotent apply). A snapshot reflecting
    /// <em>less</em> than this index would lose data and must never be produced.
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
    /// <b>Idempotent retry:</b> the import can run more than once for the same snapshot. On the
    /// split path, after a successful import the coordinator replicates a checkpoint into the
    /// target partition's log; if that replication fails the coordinator retries the checkpoint
    /// only — <c>ImportRange</c> is not re-invoked there. On the catch-up path, however, the
    /// sender retries the <em>whole snapshot</em> when the durable WAL boundary write fails after
    /// a successful import, so a repeated <c>ImportRange</c> for the same
    /// (partition, snapshot index, term) must be idempotent. If both checkpoint attempts fail on
    /// the split path the coordinator logs an error; the snapshot is present on the leader but
    /// followers have not yet received it via the log, so manual intervention may be required.
    /// </para>
    /// The coordinator disposes the stream after this call returns.
    /// </summary>
    /// <param name="targetPartitionId">The partition id that will own the imported state.</param>
    /// <param name="snapshot">Readable stream produced by <see cref="ExportRange"/>; disposed by the coordinator after this call returns.</param>
    /// <param name="ct">Cancellation token.</param>
    Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct);
}
