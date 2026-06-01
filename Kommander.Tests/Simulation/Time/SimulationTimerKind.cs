namespace Kommander.Tests.Simulation.Time;

/// <summary>
/// Categories of virtual timers modeled in simulation instead of wall-clock callbacks.
/// </summary>
public enum SimulationTimerKind
{
    CheckLeader = 0,
    Heartbeat = 1,
    ElectionTimeout = 2,
    UpdateNodes = 3,
    Retry = 4,
    ProposalPoll = 5,
}
