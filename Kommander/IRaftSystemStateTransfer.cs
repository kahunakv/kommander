
namespace Kommander;

/// <summary>
/// Optional callback an application registers to snapshot the <em>whole application state</em> of
/// a partition (specifically the system partition, id 0) at a committed index, so a node that has
/// fallen below the WAL compaction floor can be repaired without shipping individual delta log
/// entries.
/// <para>
/// Distinct from <see cref="IRaftStateMachineTransfer"/>, which exports a key-range slice for
/// split/merge operations.  A node may register both without conflict; the leader chooses the
/// correct transfer based on the lagging follower's context.
/// </para>
/// <para>
/// Registering this transfer for the system partition is also the signal that its logs are used as
/// application deltas — see <c>RegisterSystemStateTransfer</c> in <see cref="IRaft"/>.
/// </para>
/// Both methods run on the coordinator/executor thread; implementations must be thread-safe if the
/// same instance is shared across partitions.
/// </summary>
public interface IRaftSystemStateTransfer
{
    /// <summary>
    /// Export the complete application state committed to <paramref name="partitionId"/> as of
    /// <paramref name="upToIndex"/> (state after that index is applied; nothing above it is
    /// included). The returned stream is read in chunks by Kommander, then disposed after the
    /// transfer completes or is aborted.
    /// </summary>
    Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct);

    /// <summary>
    /// Atomically replace this node's application state for <paramref name="partitionId"/> with
    /// the exported blob received in <paramref name="snapshot"/>. A crash mid-import must leave
    /// the prior state intact so the follower can retry on the next snapshot delivery.
    /// </summary>
    Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct);
}
