namespace Kommander.Tests.Simulation;

/// <summary>
/// Declarative description of a deterministic simulation scenario.
/// Scenarios configure a <see cref="SimulationRuntime"/> without starting real threads or timers.
/// </summary>
public abstract class SimulationScenario
{
    /// <summary>Human-readable scenario name used in replay logs and failure reports.</summary>
    public abstract string Name { get; }

    /// <summary>PRNG seed for randomized scheduling modes.</summary>
    public virtual ulong Seed { get; init; } = 1;

    /// <summary>Number of simulated nodes in the cluster.</summary>
    public virtual int NodeCount { get; init; } = 3;

    /// <summary>Number of user partitions to model.</summary>
    public virtual int PartitionCount { get; init; } = 1;

    /// <summary>Maximum number of scheduler steps before the run terminates.</summary>
    public virtual int MaxSteps { get; init; } = 100;

    /// <summary>Maximum logical time before the run terminates.</summary>
    public virtual long MaxLogicalTime { get; init; } = 60_000;

    /// <summary>How the runtime selects the next enabled event.</summary>
    public virtual SimulationSchedulingMode SchedulingMode { get; init; } = SimulationSchedulingMode.Scripted;

    /// <summary>Scenario parameters recorded in replay logs.</summary>
    public virtual IReadOnlyDictionary<string, string> Parameters =>
        new Dictionary<string, string>
        {
            ["nodeCount"] = NodeCount.ToString(),
            ["partitionCount"] = PartitionCount.ToString(),
            ["maxSteps"] = MaxSteps.ToString(),
            ["maxLogicalTime"] = MaxLogicalTime.ToString(),
            ["schedulingMode"] = SchedulingMode.ToString(),
        };

    /// <summary>
    /// Configures initial simulation state on the runtime.
    /// Implementations must not start threads, timers, or real I/O.
    /// </summary>
    public abstract void Configure(SimulationRuntime runtime);

    /// <summary>
    /// Returns the fixed event sequence for <see cref="SimulationSchedulingMode.Scripted"/> scenarios.
    /// Other scheduling modes ignore this list.
    /// </summary>
    public virtual IReadOnlyList<SimulationEvent> ScriptedEvents() => [];

    /// <summary>
    /// Returns the currently enabled events for random, exhaustive, and replay scheduling modes.
    /// </summary>
    public virtual IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime) => [];
}
