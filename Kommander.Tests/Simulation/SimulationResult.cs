namespace Kommander.Tests.Simulation;

/// <summary>
/// Outcome of a completed or aborted simulation run.
/// </summary>
public sealed record SimulationResult
{
    /// <summary>Whether all invariants held for the entire run.</summary>
    public required bool Passed { get; init; }

    /// <summary>Scenario that was executed.</summary>
    public required string ScenarioName { get; init; }

    /// <summary>Seed used for the run.</summary>
    public required ulong Seed { get; init; }

    /// <summary>Scenario parameters recorded for replay and failure reports.</summary>
    public required IReadOnlyDictionary<string, string> Parameters { get; init; }

    /// <summary>Number of events applied before the run ended.</summary>
    public required int StepCount { get; init; }

    /// <summary>Final logical time reached by the run.</summary>
    public required long FinalLogicalTime { get; init; }

    /// <summary>Path to the replay log when one was written.</summary>
    public string? ReplayLogPath { get; init; }

    /// <summary>Last snapshot that passed all invariant checks.</summary>
    public SimulationSnapshot? LastValidSnapshot { get; init; }

    /// <summary>Snapshot at the point of failure when <see cref="Passed"/> is false.</summary>
    public SimulationSnapshot? FailingSnapshot { get; init; }

    /// <summary>Invariant violation details when <see cref="Passed"/> is false.</summary>
    public InvariantViolationException? Violation { get; init; }

    public static SimulationResult Success(
        SimulationScenario scenario,
        SimulationRuntime runtime,
        SimulationSnapshot finalSnapshot,
        string? replayLogPath = null) =>
        new()
        {
            Passed = true,
            ScenarioName = scenario.Name,
            Seed = scenario.Seed,
            Parameters = scenario.Parameters,
            StepCount = runtime.StepNumber,
            FinalLogicalTime = runtime.LogicalTick,
            ReplayLogPath = replayLogPath,
            LastValidSnapshot = finalSnapshot,
        };

    public static SimulationResult Failure(
        SimulationScenario scenario,
        SimulationRuntime runtime,
        InvariantViolationException violation,
        string? replayLogPath = null) =>
        new()
        {
            Passed = false,
            ScenarioName = scenario.Name,
            Seed = scenario.Seed,
            Parameters = scenario.Parameters,
            StepCount = runtime.StepNumber,
            FinalLogicalTime = runtime.LogicalTick,
            ReplayLogPath = replayLogPath,
            LastValidSnapshot = violation.LastValidSnapshot,
            FailingSnapshot = violation.FailingSnapshot,
            Violation = violation,
        };
}
