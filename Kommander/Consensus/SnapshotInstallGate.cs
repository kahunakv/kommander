using Kommander.Data;

namespace Kommander.Consensus;

/// <summary>
/// One registered snapshot-install gate: the phase it suspends at, and the callback awaited there.
///
/// <para>Immutable and published as a single reference so the installer reads phase and callback
/// together — a two-field registration could otherwise be observed half-updated on the executor
/// turn and fire the wrong callback for the wrong phase.</para>
/// </summary>
internal sealed class SnapshotInstallGate
{
    public SnapshotInstallGate(SnapshotInstallPhase phase, Func<SnapshotInstallSignal, ValueTask> gate)
    {
        Phase = phase;
        Gate = gate;
    }

    public SnapshotInstallPhase Phase { get; }

    public Func<SnapshotInstallSignal, ValueTask> Gate { get; }
}
