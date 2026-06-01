namespace Kommander.Tests.Simulation;

/// <summary>
/// Lifecycle state of a simulated node within the DST harness.
/// </summary>
public enum SimulationNodeLifecycleStatus
{
    Stopped = 0,
    Running = 1,
    Paused = 2,
    Crashed = 3,
}
