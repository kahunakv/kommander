
namespace Kommander.Data;

/// <summary>
/// Discriminates which transfer interface the follower should invoke when it receives a snapshot.
/// The value is carried on the wire (gRPC field 8, REST JSON field "kind") so peers that pre-date
/// this field receive the default <see cref="Range"/> value and keep today's behaviour unchanged.
/// </summary>
public enum SnapshotKind
{
    /// <summary>
    /// Key-range snapshot produced by a split/merge operation.
    /// The follower dispatches to <see cref="IRaftStateMachineTransfer.ImportRange"/>.
    /// Default (0) so existing peers and serializers that omit the field keep today's behaviour.
    /// </summary>
    Range = 0,

    /// <summary>
    /// Whole-partition application-state snapshot for the system partition.
    /// The follower dispatches to <see cref="IRaftSystemStateTransfer.ImportPartitionState"/>.
    /// </summary>
    SystemState = 1,
}

/// <summary>
/// Carries one chunk of an application-state snapshot from the partition leader to a lagging
/// follower whose last acknowledged log index is below the leader's compaction floor.
///
/// <para>Large snapshots are split into bounded chunks (each ≤ <c>SnapshotChunkSize</c> bytes)
/// so no single message exceeds gRPC's default 4 MB receive limit.  All chunks for one transfer
/// share a <see cref="SessionId"/>; the receiver accumulates them and calls the appropriate
/// import method (determined by <see cref="Kind"/>) only when <see cref="IsLast"/> is true.
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

    /// <summary>
    /// Raw bytes for this chunk; empty on the final chunk when the stream length is an exact multiple
    /// of the chunk size.
    /// <para>
    /// A <see cref="ReadOnlyMemory{T}"/> rather than a <c>byte[]</c> so the sender can hand off a
    /// zero-copy view over its reused (pooled) 3 MiB read buffer — <c>buffer.AsMemory(0, bytesRead)</c> —
    /// instead of allocating a fresh array per chunk, and the gRPC receiver can expose the incoming
    /// <c>ByteString.Memory</c> without a <c>ToByteArray</c> copy. <b>Lifetime:</b> the view is only
    /// valid until the send completes — every transport consumes it synchronously (gRPC serializes via
    /// <c>UnsafeByteOperations.UnsafeWrap</c> before the awaited unary call; REST base64-encodes it
    /// during <c>JsonSerializer.Serialize</c>; the in-memory/​receiver path copies it into the receive
    /// <c>MemoryStream</c> under the session lock) — and the sender awaits each send before overwriting
    /// the buffer, so the view never outlives its backing bytes. A <c>byte[]</c> assigned to this
    /// property (e.g. in tests) converts implicitly and owns its own storage.
    /// </para>
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; init; } = ReadOnlyMemory<byte>.Empty;

    /// <summary>
    /// Which transfer interface the follower should invoke on the final chunk.
    /// Defaults to <see cref="SnapshotKind.Range"/> so requests with no kind field
    /// (older senders, REST bodies that omit the property) keep today's behaviour.
    /// </summary>
    public SnapshotKind Kind { get; init; } = SnapshotKind.Range;
}
