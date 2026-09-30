
namespace Kommander.Data;

/// <summary>
/// What the receiver actually did with a snapshot chunk, carried on <see cref="SnapshotResponse"/>.
/// A single <c>Success</c> bit could not tell "imported the state" from "already covered, imported
/// nothing", and the sender logged "the follower is seeded" for both — which is how a follower
/// that acknowledged an install it never imported went unnoticed until its acknowledged writes
/// were lost. The sender advances its replication cursors on <see cref="Installed"/> and
/// <see cref="SkippedAlreadyCovered"/> (both mean the receiver's boundary covers the index) but
/// logs which one happened.
/// <para><see cref="Rejected"/> is deliberately the zero value: a peer that does not populate the
/// field (an older binary) reads as a rejection, never as a silent success.</para>
/// </summary>
public enum SnapshotInstallOutcome
{
    /// <summary>The chunk (or the whole install) was refused; the sender records a failure and retries on its backoff.</summary>
    Rejected = 0,

    /// <summary>A non-terminal chunk was staged in order. Carries no statement about installation.</summary>
    ChunkAccepted = 1,

    /// <summary>The terminal chunk completed the session: the application import ran and the durable WAL boundary was installed.</summary>
    Installed = 2,

    /// <summary>
    /// The receiver already holds an installed snapshot boundary at or above the index with a
    /// compatible identity, so it imported nothing and wrote nothing. The sender's cursors may
    /// advance, but nothing on the receiver changed.
    /// </summary>
    SkippedAlreadyCovered = 3,

    /// <summary>
    /// An install of this partition is queued or running on the receiver and its outcome is not known
    /// yet. Answered to a sender that set <see cref="SnapshotRequest.InstallPolling"/>: on the terminal
    /// chunk (the snapshot is staged and verified, and the install was handed to the partition
    /// executor), on the opener of a session the receiver refused because another install of the
    /// partition is running, and on a <see cref="SnapshotRequest.StatusQuery"/>. The response names the
    /// install (<see cref="SnapshotResponse.InstallSessionId"/>, <see cref="SnapshotResponse.InstallIndex"/>)
    /// and the sender polls until the receiver reports what became of it. The install can take far
    /// longer than one chunk acknowledgement may, so its outcome is a step of its own; holding the
    /// terminal chunk's call open for it made every slow install read as a rejected chunk.
    /// </summary>
    InstallPending = 4,

    /// <summary>
    /// Answer to a <see cref="SnapshotRequest.StatusQuery"/> only: no install of the partition is queued
    /// or running on the receiver, and it holds no outcome for the session the query named (it never
    /// saw that session's terminal chunk, or it restarted since). A sender that was waiting for an
    /// install starts the transfer again.
    /// </summary>
    NoInstall = 5,
}
