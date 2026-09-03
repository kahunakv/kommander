using System.Text;
using Kommander.Tests.Simulation.Diagnostics;
using Kommander.Tests.Simulation.History;

namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// What a random run did, in a form a reader can act on and a continuous-integration job can keep.
/// </summary>
public sealed record RandomScenarioReport
{
    /// <summary>Seed the run was drawn from. Re-running this seed reproduces the plan exactly.</summary>
    public required ulong Seed { get; init; }

    /// <summary>Bounds and weights the plan was drawn under.</summary>
    public required RandomScenarioOptions Options { get; init; }

    /// <summary>Every action, in order, including the heals the age bound forced.</summary>
    public required IReadOnlyList<RandomScenarioAction> Actions { get; init; }

    /// <summary>Simulation steps the run took, including the recovery phase.</summary>
    public required int StepsRun { get; init; }

    /// <summary>What the clients were told.</summary>
    public required ClientHistory History { get; init; }

    /// <summary>
    /// Highest committed index any live node reported at the end, or -1 when the run stopped
    /// before it could read one.
    /// </summary>
    public required long FinalCommitIndex { get; init; }

    /// <summary>Per-step invariant checks the run performed.</summary>
    public required int InvariantChecks { get; init; }

    /// <summary>
    /// Entries compaction removed across every node.
    ///
    /// <para>Reported so a run can prove it exercised compaction rather than assume it. Zero here
    /// means the two rules about compaction described nothing that happened.</para>
    /// </summary>
    public long EntriesCompacted { get; init; }

    /// <summary>
    /// What the run cost.
    ///
    /// <para>Reported beside the plan because a run that is slow and a run that is wedged look
    /// identical from a pass or a failure, and the difference is the whole question when a
    /// continuous-integration job reports something unusual.</para>
    /// </summary>
    public SimulationMetrics? Metrics { get; init; }

    /// <summary>Actions of one kind. Used by the tests that prove the vocabulary is reachable.</summary>
    public int CountOf(RandomScenarioActionKind kind) => Actions.Count(action => action.Kind == kind);

    /// <summary>
    /// The plan as text: a header naming the seed and the bounds, then one line per action.
    ///
    /// <para>The header is not decoration. A plan read without its bounds says what happened but
    /// not what the run was allowed to do, and the two together are what a reader needs to decide
    /// whether a failure is a defect or a bound set wrongly.</para>
    /// </summary>
    public string Describe()
    {
        StringBuilder text = new();

        text.AppendLine($"seed={Seed}");

        foreach ((string key, string value) in Options.ToParameters())
            text.AppendLine($"{key}={value}");

        text.AppendLine($"stepsRun={StepsRun}");
        text.AppendLine($"finalCommitIndex={FinalCommitIndex}");
        text.AppendLine($"invariantChecks={InvariantChecks}");
        text.AppendLine($"entriesCompacted={EntriesCompacted}");
        text.AppendLine($"appendsAcknowledged={History.AcknowledgedCount}");
        text.AppendLine($"appendsUnknown={History.UnknownCount}");

        if (Metrics is not null)
        {
            foreach ((string key, string value) in Metrics.Pairs())
                text.AppendLine($"{key}={value}");
        }
        text.AppendLine();

        foreach (RandomScenarioAction action in Actions)
            text.AppendLine(action.Describe());

        return text.ToString();
    }

    /// <summary>
    /// Writes the plan beside the test binary and returns the path.
    ///
    /// <para>Written only when a run fails. A passing run's plan is reproducible from its seed, and
    /// writing one file per passing seed would fill a build agent with files nobody reads.</para>
    /// </summary>
    public string WriteArtifact(string directory, string name)
    {
        Directory.CreateDirectory(directory);

        string path = Path.Combine(directory, $"{name}-seed-{Seed}.plan.txt");
        File.WriteAllText(path, Describe());

        return path;
    }
}
