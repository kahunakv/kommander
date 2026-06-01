namespace Kommander.Tests.Simulation;

/// <summary>
/// A single selectable unit of work in a deterministic simulation run.
/// Every event has a stable id so replay logs can reference it exactly.
/// </summary>
public sealed record SimulationEvent
{
    /// <summary>Stable identifier unique within a simulation run.</summary>
    public required long Id { get; init; }

    /// <summary>High-level event category used by the scheduler and replay log.</summary>
    public required SimulationEventType Type { get; init; }

    /// <summary>Human-readable summary for failure reports and replay inspection.</summary>
    public required string Summary { get; init; }

    /// <summary>Target node when the event applies to a specific node.</summary>
    public int? NodeId { get; init; }

    /// <summary>Target partition when the event applies to a specific partition.</summary>
    public int? PartitionId { get; init; }

    /// <summary>Logical time at which the event becomes eligible for selection.</summary>
    public long LogicalTime { get; init; }

    /// <summary>Virtual timer id when <see cref="Type"/> is <see cref="SimulationEventType.TimerTick"/>.</summary>
    public long? TimerId { get; init; }
}
