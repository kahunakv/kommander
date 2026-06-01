namespace Kommander.Tests.Simulation.Time;

/// <summary>
/// A single virtual timer waiting in the simulation queue.
/// </summary>
public sealed class VirtualTimer
{
    public required long TimerId { get; init; }
    public required int NodeId { get; init; }
    public required SimulationTimerKind Kind { get; init; }
    public required long FiresAtLogicalTime { get; set; }
    public int? PartitionId { get; init; }
    public long IntervalMs { get; init; }
    public Action<SimulationRuntime, VirtualTimer>? OnFire { get; set; }
    public long? ScheduledEventId { get; set; }
    public bool IsPeriodic => IntervalMs > 0;
}
