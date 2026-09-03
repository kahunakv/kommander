using System.Globalization;

namespace Kommander.Tests.Simulation.Diagnostics;

/// <summary>
/// What one run cost.
///
/// <para><b>Why this is worth its own type.</b> Twice now a run of this suite has been read wrongly
/// for want of these numbers. A category run took an hour where it usually takes two minutes, and
/// that looked like a regression from the change under review until a control run said otherwise —
/// the machine was loaded. A metric would have said so at the time, in the run's own output.</para>
///
/// <para><b>What each number is for.</b> Wall time and steps per second separate a wedged run from a
/// slow machine: a wedge stops making steps, a loaded machine makes them slowly. The invariant share
/// is the one that tunes the search — when most of a run is spent checking, the budget is going to
/// oracles rather than to exploration, and either the checks or the step count is wrong.</para>
/// </summary>
public sealed record SimulationMetrics
{
    /// <summary>Simulation steps the run performed.</summary>
    public required int Steps { get; init; }

    /// <summary>Actions the plan applied, heals included.</summary>
    public required int Actions { get; init; }

    /// <summary>Settled states the invariant checker examined.</summary>
    public required int InvariantChecks { get; init; }

    /// <summary>Real time the run took, end to end.</summary>
    public required TimeSpan Elapsed { get; init; }

    /// <summary>Real time spent inside the invariant checks.</summary>
    public required TimeSpan InvariantTime { get; init; }

    /// <summary>
    /// Managed memory held at the end of the run, in bytes.
    ///
    /// <para>Read without forcing a collection. A run that allocates heavily is worth noticing, and
    /// a number that made the run slower to produce would defeat the point of measuring speed in the
    /// same pass.</para>
    /// </summary>
    public required long ManagedBytes { get; init; }

    /// <summary>Steps per real second. Zero when the run took no measurable time.</summary>
    public double StepsPerSecond =>
        Elapsed.TotalSeconds > 0 ? Steps / Elapsed.TotalSeconds : 0;

    /// <summary>
    /// The part of the run spent checking invariants, from zero to one.
    ///
    /// <para>The number to watch when a search stops finding things. Checks are not free, and a run
    /// that spends most of its time on them explores less for the same money.</para>
    /// </summary>
    public double InvariantShare =>
        Elapsed.TotalSeconds > 0 ? InvariantTime.TotalSeconds / Elapsed.TotalSeconds : 0;

    /// <summary>
    /// The measurements as header pairs, one per entry.
    ///
    /// <para>One pair per line, because that is the shape the plan artifact's header already has and
    /// the shape its parser reads. A single line holding every pair would load as one key whose
    /// value is the rest of the line.</para>
    /// </summary>
    public IReadOnlyList<(string Key, string Value)> Pairs()
    {
        // Invariant culture throughout. A machine whose decimal separator is a comma would write
        // "invariantShare=0,42", and the header parser these lines are read back by would take the
        // comma as part of the value. The artifact has to mean the same thing wherever it is read.
        CultureInfo culture = CultureInfo.InvariantCulture;

        return
        [
            ("metricSteps", Steps.ToString(culture)),
            ("metricActions", Actions.ToString(culture)),
            ("metricInvariantChecks", InvariantChecks.ToString(culture)),
            ("metricElapsedMs", Elapsed.TotalMilliseconds.ToString("F0", culture)),
            ("metricStepsPerSecond", StepsPerSecond.ToString("F1", culture)),
            ("metricInvariantMs", InvariantTime.TotalMilliseconds.ToString("F0", culture)),
            ("metricInvariantShare", InvariantShare.ToString("F2", culture)),
            ("metricManagedMb", (ManagedBytes / (1024.0 * 1024.0)).ToString("F1", culture)),
        ];
    }

    /// <summary>One line, for a reader scanning a log.</summary>
    public string Describe() =>
        string.Join(' ', Pairs().Select(pair => $"{pair.Key}={pair.Value}"));
}
