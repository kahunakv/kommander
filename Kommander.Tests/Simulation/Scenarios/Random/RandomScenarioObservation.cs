namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// What the generator is allowed to know about the cluster before it chooses.
///
/// <para><b>Why a snapshot and not the cluster itself.</b> The generator must be a pure function of
/// its seed and this reading. A generator that could reach into the cluster could also read
/// something that varies between two runs of one seed, and the run would stop being reproducible
/// without anybody noticing. A record of plain values cannot do that, and it can be built by hand
/// in a unit test.</para>
/// </summary>
public sealed record RandomScenarioObservation
{
    /// <summary>Endpoints of nodes that are running normally, in a stable order.</summary>
    public required IReadOnlyList<string> Running { get; init; }

    /// <summary>Endpoints of nodes that have crashed and not yet restarted.</summary>
    public required IReadOnlyList<string> Crashed { get; init; }

    /// <summary>Endpoints of nodes that are frozen.</summary>
    public required IReadOnlyList<string> Paused { get; init; }

    /// <summary>
    /// The endpoint that reported itself leader of the partition under test, or null when the
    /// cluster is between leaders. Null is a normal reading, not a fault: a run that refused to
    /// proceed without a leader would skip every election it created.
    /// </summary>
    public string? Leader { get; init; }
}
