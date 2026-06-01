namespace Kommander.Tests.Simulation;

/// <summary>
/// Controls how the simulation runtime chooses the next enabled event at each step.
/// </summary>
public enum SimulationSchedulingMode
{
    /// <summary>Execute a fixed sequence of events declared by the scenario.</summary>
    Scripted = 0,

    /// <summary>Enumerate all enabled event choices up to a configured bound.</summary>
    Exhaustive = 1,

    /// <summary>Choose enabled events using a seeded PRNG.</summary>
    Random = 2,

    /// <summary>Consume event choices from a recorded replay log.</summary>
    Replay = 3,
}
