
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
    /// <see langword="true"/> for every outcome except <see cref="SnapshotInstallOutcome.Rejected"/>.
    /// Serialized as well so a REST peer can read the bit without knowing the enum.
    /// </summary>
    public bool Success
    {
        get => Outcome != SnapshotInstallOutcome.Rejected;
        init
        {
            // JSON round-trip: a payload that carries only the legacy bit maps true to Installed
            // (the pre-outcome meaning of a terminal success) and false to Rejected. A payload
            // that also carries Outcome sets it after this and wins.
            if (Outcome == SnapshotInstallOutcome.Rejected && value)
                Outcome = SnapshotInstallOutcome.Installed;
        }
    }

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
