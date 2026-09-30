
namespace Kommander.Data;

/// <summary>
/// Result returned by a follower after receiving one <see cref="SnapshotRequest"/> chunk.
/// <see cref="Outcome"/> says what the receiver did; <see cref="Success"/> is derived from it and
/// exists for the transports and the chunk loop, which only need "keep sending or stop".
/// </summary>
public sealed class SnapshotResponse
{
    /// <summary>What the receiver did with the chunk. See <see cref="SnapshotInstallOutcome"/>.</summary>
    public SnapshotInstallOutcome Outcome { get; init; }

    /// <summary>
    /// <see langword="true"/> when the receiver staged the chunk or holds the snapshot's state
    /// (<see cref="SnapshotInstallOutcome.ChunkAccepted"/>, <see cref="SnapshotInstallOutcome.Installed"/>,
    /// <see cref="SnapshotInstallOutcome.SkippedAlreadyCovered"/>). Serialized as well so a REST peer
    /// can read the bit without knowing the enum. <see cref="SnapshotInstallOutcome.InstallPending"/>
    /// and <see cref="SnapshotInstallOutcome.NoInstall"/> read <see langword="false"/>: neither says
    /// that anything is installed, and only a sender that reads <see cref="Outcome"/> is ever sent them.
    /// </summary>
    public bool Success
    {
        get => Outcome is SnapshotInstallOutcome.ChunkAccepted
            or SnapshotInstallOutcome.Installed
            or SnapshotInstallOutcome.SkippedAlreadyCovered;
        init
        {
            // JSON round-trip: a payload that carries only the legacy bit maps true to Installed
            // (the pre-outcome meaning of a terminal success) and false to Rejected. A payload
            // that also carries Outcome sets it after this and wins.
            if (Outcome == SnapshotInstallOutcome.Rejected && value)
                Outcome = SnapshotInstallOutcome.Installed;
        }
    }

    /// <summary>
    /// Session id of the install this answer describes, or empty when it describes none. Set with
    /// <see cref="SnapshotInstallOutcome.InstallPending"/>, and with the outcome of an install the
    /// receiver ran (<see cref="SnapshotInstallOutcome.Installed"/>,
    /// <see cref="SnapshotInstallOutcome.SkippedAlreadyCovered"/>, or
    /// <see cref="SnapshotInstallOutcome.Rejected"/> when the install itself failed). The install can
    /// be another sender's: a receiver runs one install per partition and tells every sender about it.
    /// </summary>
    public string InstallSessionId { get; init; } = "";

    /// <summary>
    /// Snapshot index of the install this answer describes; 0 when it describes none. A
    /// <see cref="SnapshotInstallOutcome.Rejected"/> answer that carries an index says that install
    /// failed on the receiver; one that carries none is a refused chunk or a call that was never
    /// answered, and says nothing about an install.
    /// </summary>
    public long InstallIndex { get; init; }

    /// <summary>Leader term of the sender whose install this answer describes; 0 when it describes none.</summary>
    public long InstallLeaderTerm { get; init; }

    /// <summary>Endpoint of the sender whose install this answer describes; empty when it describes none.</summary>
    public string InstallLeaderEndpoint { get; init; } = "";

    /// <summary>
    /// How far the described install has read into its staged snapshot, in bytes. Zero while it waits
    /// for the partition executor. The sender treats any change as progress and bounds its wait by
    /// the time since the last change, not by how long the install has run.
    /// </summary>
    public long InstallProgress { get; init; }

    public SnapshotResponse() { }

    public SnapshotResponse(SnapshotInstallOutcome outcome) => Outcome = outcome;

    /// <summary>
    /// Legacy/test convenience: <see langword="true"/> is <see cref="SnapshotInstallOutcome.Installed"/>,
    /// <see langword="false"/> is <see cref="SnapshotInstallOutcome.Rejected"/>. Production
    /// receivers answer with the typed outcome.
    /// </summary>
    public SnapshotResponse(bool success) =>
        Outcome = success ? SnapshotInstallOutcome.Installed : SnapshotInstallOutcome.Rejected;
}
