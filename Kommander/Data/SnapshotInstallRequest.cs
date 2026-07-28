
namespace Kommander.Data;

/// <summary>
/// The staged, fully-received snapshot handed to the partition executor for a follower-side install.
///
/// <para>Built by <see cref="SnapshotReceiver"/> once the terminal chunk of a session has been buffered.
/// It carries the session metadata needed for Raft "Rule 7" validation plus the accumulated snapshot
/// <see cref="Snapshot"/> stream that the application importer consumes. Unlike the wire
/// <see cref="SnapshotRequest"/> (one chunk), this represents the whole snapshot ready to install, and it
/// executes on the partition's single-writer thread so validation, import, and WAL mutation are
/// serialized against all other partition operations.</para>
///
/// <para>Ownership: the receiver owns and disposes <see cref="Snapshot"/> after the install completes;
/// the executor reads it but must not dispose it.</para>
/// </summary>
public sealed class SnapshotInstallRequest
{
    /// <summary>The partition this snapshot installs into.</summary>
    public required int PartitionId { get; init; }

    /// <summary>Log index the snapshot reflects; the installed <c>CommittedCheckpoint</c> boundary index.</summary>
    public required long SnapshotIndex { get; init; }

    /// <summary>Term of the entry at <see cref="SnapshotIndex"/>; stamped on the installed checkpoint.</summary>
    public required long LastIncludedTerm { get; init; }

    /// <summary>The sending leader's Raft term at session creation; drives the leader-RPC term rules.</summary>
    public required long LeaderTerm { get; init; }

    /// <summary>The claimed sending endpoint (the leader). Adopted as the current leader on a valid term.</summary>
    public required string LeaderEndpoint { get; init; }

    /// <summary>Which application transfer interface imports the snapshot (range vs whole-partition state).</summary>
    public required SnapshotKind Kind { get; init; }

    /// <summary>The fully accumulated snapshot bytes, positioned at 0, ready for the application importer.</summary>
    public required Stream Snapshot { get; init; }
}
