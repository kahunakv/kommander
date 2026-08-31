namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// Identity and parameters of a random cluster run, in the shape the replay log's header wants.
///
/// <para><b>Why a scenario class for a run that is not modelled.</b> A random run drives a real
/// cluster, not the model runtime, so it has no event list to configure. It still needs the seed,
/// the name, and the parameters written at the top of every replay record, and
/// <see cref="Replay.ReplayLogWriter"/> takes those from a scenario. Reusing the existing header
/// keeps one replay format rather than two.</para>
/// </summary>
public sealed class RandomClusterScenario : SimulationScenario
{
    private readonly IReadOnlyDictionary<string, string> parameters;

    public RandomClusterScenario(string name, ulong seed, IReadOnlyDictionary<string, string> parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(parameters);

        Name = name;
        Seed = seed;
        this.parameters = parameters;
    }

    public override string Name { get; }

    public override IReadOnlyDictionary<string, string> Parameters => parameters;

    /// <summary>
    /// Not supported, and deliberately loud rather than empty.
    ///
    /// <para>This scenario describes a run against a real cluster. Handing it to
    /// <see cref="SimulationRuntime"/> would configure nothing and then report a passing run that
    /// exercised an empty model, which is the worst failure a test harness can have.</para>
    /// </summary>
    public override void Configure(SimulationRuntime runtime) =>
        throw new NotSupportedException(
            "RandomClusterScenario is a replay-log header for a real cluster run. It has no model " +
            "state to configure.");
}
