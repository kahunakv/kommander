using System.Text;
using Kommander.Tests.Simulation.Scenarios.Random;

namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// What a shrink produced, and what it cost.
///
/// <para>The cost is reported beside the plan because a shrink that halved a plan for sixty cluster
/// runs and one that halved it for six are different results, and only one of them belongs in a
/// nightly job. Without the counters a reader cannot tell which one they got.</para>
/// </summary>
public sealed record ShrinkResult
{
    /// <summary>The plan the shrink started from.</summary>
    public required IReadOnlyList<RandomScenarioAction> Original { get; init; }

    /// <summary>The smallest plan confirmed to still fail for <see cref="Signature"/>.</summary>
    public required IReadOnlyList<RandomScenarioAction> Shrunk { get; init; }

    /// <summary>The failure the shrink held on to, in <see cref="FailureSignature"/> form.</summary>
    public required string Signature { get; init; }

    /// <summary>Candidate plans run, including the ones that were rejected.</summary>
    public required int CandidatesRun { get; init; }

    /// <summary>Chunk removals the oracle confirmed.</summary>
    public required int RemovalsAccepted { get; init; }

    /// <summary>Numeric parameters the oracle confirmed at a smaller value.</summary>
    public required int ParametersReduced { get; init; }

    /// <summary>
    /// Whether the shrink stopped because it ran out of budget rather than out of reductions.
    ///
    /// <para>Reported rather than thrown. The plan is still a real reproduction and still smaller
    /// than the original; the flag only says a larger budget would probably have gone further.</para>
    /// </summary>
    public required bool BudgetExhausted { get; init; }

    /// <summary>
    /// The run's own header — its seed and the bounds it was drawn under — written above the shrink
    /// counters.
    ///
    /// <para>Carried through so a shrunk plan is promotable as it stands. A regression corpus
    /// replays a plan under the bounds the failing run used, and a file that lost them would be a
    /// test of something nobody recorded. Empty is allowed: a shrink driven from a hand-built plan
    /// has no run behind it.</para>
    /// </summary>
    public IReadOnlyDictionary<string, string> Header { get; init; } =
        new Dictionary<string, string>();

    /// <summary>Actions removed, counted from the original.</summary>
    public int ActionsRemoved => Original.Count - Shrunk.Count;

    /// <summary>
    /// The shrunk plan as text, in the same shape a run's plan artifact uses, so the same reader
    /// and the same parser handle both.
    /// </summary>
    public string Describe()
    {
        StringBuilder text = new();

        foreach ((string key, string value) in Header)
            text.AppendLine($"{key}={value}");

        text.AppendLine($"signature={Signature}");
        text.AppendLine($"originalActions={Original.Count}");
        text.AppendLine($"shrunkActions={Shrunk.Count}");
        text.AppendLine($"candidatesRun={CandidatesRun}");
        text.AppendLine($"removalsAccepted={RemovalsAccepted}");
        text.AppendLine($"parametersReduced={ParametersReduced}");
        text.AppendLine($"budgetExhausted={BudgetExhausted}");
        text.AppendLine();

        foreach (RandomScenarioAction action in Shrunk)
            text.AppendLine(action.Describe());

        return text.ToString();
    }

    /// <summary>Writes the shrunk plan beside the test binary and returns the path.</summary>
    public string WriteArtifact(string directory, string name)
    {
        Directory.CreateDirectory(directory);

        string path = Path.Combine(directory, $"{name}.shrunk.plan.txt");
        File.WriteAllText(path, Describe());

        return path;
    }
}
