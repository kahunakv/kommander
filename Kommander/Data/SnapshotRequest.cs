
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

    /// <summary>
    /// Whole-partition application-state snapshot for a user data partition (below-floor
    /// follower catch-up). The follower dispatches to
    /// <see cref="IRaftPartitionStateTransfer.ImportPartitionState"/>.
    /// <para>Only produced when the sender has an <see cref="IRaftPartitionStateTransfer"/>
    /// registered. A receiver that pre-dates this value falls into its Range arm and hands the
    /// blob to <see cref="IRaftStateMachineTransfer.ImportRange"/> — an application-level
    /// mismatch, so in a mixed-version cluster do not register the partition-state transfer
    /// until every node understands this kind.</para>
    /// </summary>
    PartitionState = 2,
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

    /// <summary>
    /// The sender's Raft <c>currentTerm</c> at the moment the transfer session was created.
    /// Carried so the receiver can apply the same leader-RPC term rules used by AppendEntries/Vote:
    /// a snapshot from a stale leader (<c>LeaderTerm &lt; currentTerm</c>) is rejected, and one from a
    /// higher term drives the normal durable step-down. <b>Distinct from <see cref="LastIncludedTerm"/></b>
    /// — this is the leader's <em>current</em> term, not the term of the entry at <see cref="SnapshotIndex"/>.
    /// <para>Zero denotes a legacy sender that pre-dates this field; such requests are only honoured when
    /// the receiver is configured to allow legacy senders (compatibility window).</para>
    /// </summary>
    public long LeaderTerm { get; init; }

    /// <summary>
    /// The claimed sending endpoint (the leader that owns the transfer). Part of the receive-session
    /// identity key <c>(leaderEndpoint, partitionId, sessionId)</c> so two leaders cannot alias one
    /// session id, and used to reject a chunk whose sender disagrees with the accepted leader for the term.
    /// Empty denotes a legacy sender that pre-dates this field.
    /// </summary>
    public string LeaderEndpoint { get; init; } = "";

    /// <summary>
    /// The Raft term of the log entry at <see cref="SnapshotIndex"/> (the snapshot's last-included term).
    /// Used for log matching against a retained local entry at that index and stamped onto the installed
    /// <c>CommittedCheckpoint</c>. <b>Not</b> the same as <see cref="LeaderTerm"/>; conflating the two
    /// corrupts the checkpoint term and the log-match decision. Zero denotes a legacy sender.
    /// </summary>
    public long LastIncludedTerm { get; init; }

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

    /// <summary>
    /// Uppercase hex SHA-256 over the concatenated bytes of the whole snapshot, set on the
    /// <see cref="IsLast"/> chunk only. Empty on every other chunk, and empty throughout from a
    /// legacy sender that pre-dates this field.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The transfer is otherwise validated only structurally — term, membership fence, session
    /// metadata, chunk-index monotonicity — none of which constrains the payload's <em>content</em>.
    /// A snapshot install writes arbitrary bytes into the application state machine and seeds a WAL
    /// checkpoint at the snapshot index, so it is the highest-leverage message in the protocol and
    /// the one most worth binding to a digest.
    /// </para>
    /// <para>
    /// Carried on the terminal chunk rather than the opener so neither side needs a pre-pass: the
    /// sender hashes each chunk as it streams and knows the total only when the stream ends, and the
    /// receiver hashes incrementally as chunks arrive. Requiring it up front would force the sender
    /// to read the whole snapshot twice, or to buffer it.
    /// </para>
    /// <para>
    /// Whether an empty value on the terminal chunk is tolerated is governed by
    /// <c>RaftConfiguration.AllowLegacySnapshotSenders</c>, the same compatibility switch that
    /// governs the other post-hoc session fields.
    /// </para>
    /// </remarks>
    public string SnapshotChecksum { get; init; } = "";
}
