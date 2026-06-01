namespace Kommander.Tests.Simulation.Random;

/// <summary>
/// One recorded random decision made during a simulation run.
/// </summary>
public sealed record SimulationRandomChoice
{
    public required int Step { get; init; }
    public required long LogicalTime { get; init; }
    public required string ChoiceName { get; init; }
    public required int MinInclusive { get; init; }
    public required int MaxExclusive { get; init; }
    public required int Value { get; init; }
}
