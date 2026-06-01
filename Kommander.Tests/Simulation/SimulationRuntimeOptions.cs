namespace Kommander.Tests.Simulation;

/// <summary>
/// Optional configuration for <see cref="SimulationRuntime"/> replay and logging behavior.
/// </summary>
public sealed record SimulationRuntimeOptions
{
    /// <summary>
    /// Path to read (replay mode) or write (random mode) JSONL replay records.
    /// </summary>
    public string? ReplayLogPath { get; init; }

    /// <summary>When true, random-mode runs write a replay log to <see cref="ReplayLogPath"/>.</summary>
    public bool WriteReplayLog { get; init; } = true;

    /// <summary>Virtual timer intervals used by <see cref="SimulationRuntime.VirtualTime"/>.</summary>
    public Time.VirtualTimeConfiguration? VirtualTimeConfiguration { get; init; }
}
