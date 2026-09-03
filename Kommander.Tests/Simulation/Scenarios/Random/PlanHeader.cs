namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// Builds the header a plan artifact carries.
///
/// <para><b>Why this is one place and not two.</b> A plan file's header is what makes the file
/// replayable: the seed, and the bounds the run was drawn under. Two call sites building it
/// separately would drift, and the drift would not show up as a broken build — it would show up as a
/// promoted regression quietly replaying under the wrong bounds.</para>
///
/// <para><b>What is deliberately left out.</b> A run's own outcome: the steps it took, the entries it
/// compacted, what it measured. Those belong in the artifact of the run that produced them. Copied
/// onto a <em>shrunk</em> plan they would read as that plan's numbers, and they are not: a reduced
/// plan of three actions has nothing to do with the four hundred steps the original took.</para>
/// </summary>
public static class PlanHeader
{
    /// <summary>The seed and the bounds, in the order a plan artifact writes them.</summary>
    public static Dictionary<string, string> For(ulong seed, RandomScenarioOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        Dictionary<string, string> header = new() { ["seed"] = seed.ToString() };

        foreach ((string key, string value) in options.ToParameters())
            header[key] = value;

        return header;
    }
}
