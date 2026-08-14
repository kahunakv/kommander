
namespace Kommander;

/// <summary>
/// Optional callback an application registers to snapshot the <em>whole application state</em> of a
/// user data partition, so a follower that has fallen below the leader's WAL compaction floor can
/// be seeded without shipping individual log entries. This is the user-partition analogue of
/// <see cref="IRaftSystemStateTransfer"/> (which covers the system partition, id 0) and is
/// deliberately distinct from <see cref="IRaftStateMachineTransfer"/>: that interface transfers a
/// bounded <em>key sub-range to a different partition</em> during split/merge, while this one
/// transfers <em>an entire partition's state to a different node</em> — different operations, so an
/// application can correctly implement either without being forced to fake the other.
/// <para>
/// When registered, the leader-side catch-up path prefers this interface and stamps the transfer
/// <see cref="Data.SnapshotKind.PartitionState"/>; without it the leader falls back to calling
/// <see cref="IRaftStateMachineTransfer.ExportRange"/> with a boundless plan (legacy behaviour,
/// kept for applications whose range transfer can serve whole-partition exports).
/// </para>
/// Both methods run off the executor thread on background transfer tasks; implementations must be
/// thread-safe if the same instance is shared across partitions.
/// </summary>
public interface IRaftPartitionStateTransfer
{
    /// <summary>
    /// Export the complete application state of <paramref name="partitionId"/> reflecting
    /// <b>at least</b> everything applied at <paramref name="upToIndex"/>. The export may reflect
    /// state <em>newer</em> than that index — an MVCC store may snapshot by timestamp after later
    /// entries have applied — because the receiver installs its WAL boundary at
    /// <paramref name="upToIndex"/> and replays any retained entries above it onto the imported
    /// state; applying an entry already reflected in the snapshot must therefore be a no-op
    /// (idempotent apply). An export reflecting <em>less</em> than <paramref name="upToIndex"/>
    /// would lose data and must never be produced.
    /// The returned stream is read in chunks by Kommander, then disposed after the transfer
    /// completes or is aborted.
    /// </summary>
    Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct);

    /// <summary>
    /// Atomically replace this node's application state for <paramref name="partitionId"/> with
    /// the exported blob received in <paramref name="snapshot"/>. A crash mid-import must leave
    /// the prior state intact so the follower can retry on the next snapshot delivery. May be
    /// invoked more than once for the same snapshot identity — the sender retries the whole
    /// transfer when the durable WAL boundary write fails after a successful import — so a
    /// repeated import must be idempotent.
    /// </summary>
    Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct);
}
