
namespace Kommander.Data;

/// <summary>
/// Carries one chunk of an application-state snapshot from the partition leader to a lagging
/// follower whose last acknowledged log index is below the leader's compaction floor.
///
/// <para>Large snapshots are split into bounded chunks (each ≤ <c>SnapshotChunkSize</c> bytes)
/// so no single message exceeds gRPC's default 4 MB receive limit.  All chunks for one transfer
/// share a <see cref="SessionId"/>; the receiver accumulates them and calls
/// <see cref="IRaftStateMachineTransfer.ImportRange"/> only when <see cref="IsLast"/> is true.
/// After importing, the follower's WAL is seeded with a <c>CommittedCheckpoint</c> entry at
/// <see cref="SnapshotIndex"/> so normal backfill can resume from there.</para>
/// </summary>
public sealed class SnapshotRequest
{
    /// <summary>Unique identifier for this transfer session; all chunks of one transfer share it.</summary>
    public string SessionId { get; init; } = "";

    public int PartitionId { get; init; }

    /// <summary>Log index the snapshot reflects; all entries up to and including this index are captured.</summary>
    public long SnapshotIndex { get; init; }

    /// <summary>Endpoint of the follower that should install the snapshot.</summary>
    public string FollowerEndpoint { get; init; } = "";

    /// <summary>Zero-based position of this chunk within the transfer session.</summary>
    public int ChunkIndex { get; init; }

    /// <summary>True on the final chunk; the receiver applies <c>ImportRange</c> when this is set.</summary>
    public bool IsLast { get; init; }

    /// <summary>Raw bytes for this chunk; empty on the final chunk when the stream length is an exact multiple of the chunk size.</summary>
    public byte[] Data { get; init; } = [];
}
