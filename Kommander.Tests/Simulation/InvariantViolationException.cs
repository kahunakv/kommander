namespace Kommander.Tests.Simulation;

/// <summary>
/// Raised when a simulation invariant check fails after a step.
/// </summary>
public sealed class InvariantViolationException : Exception
{
    public InvariantViolationException(
        string invariantName,
        string message,
        int stepNumber,
        SimulationEvent? selectedEvent,
        SimulationSnapshot? lastValidSnapshot,
        SimulationSnapshot? failingSnapshot)
        : base(message)
    {
        InvariantName = invariantName;
        StepNumber = stepNumber;
        SelectedEvent = selectedEvent;
        LastValidSnapshot = lastValidSnapshot;
        FailingSnapshot = failingSnapshot;
    }

    /// <summary>Name of the invariant that was violated.</summary>
    public string InvariantName { get; }

    /// <summary>Step number at which the violation was detected.</summary>
    public int StepNumber { get; }

    /// <summary>Event that was applied immediately before the violation, if any.</summary>
    public SimulationEvent? SelectedEvent { get; }

    /// <summary>Last snapshot that passed all invariant checks.</summary>
    public SimulationSnapshot? LastValidSnapshot { get; }

    /// <summary>Snapshot captured after the failing event was applied.</summary>
    public SimulationSnapshot? FailingSnapshot { get; }
}
