namespace Kommander.Tests.Simulation.Time;

/// <summary>
/// Priority queue of virtual timers ordered by fire time.
/// </summary>
public sealed class VirtualTimerQueue
{
    private readonly List<VirtualTimer> _timers = [];
    private long _nextTimerId = 1;

    public int Count => _timers.Count;

    public VirtualTimer Schedule(
        int nodeId,
        SimulationTimerKind kind,
        long firesAtLogicalTime,
        long intervalMs,
        int? partitionId,
        Action<SimulationRuntime, VirtualTimer>? onFire)
    {
        VirtualTimer timer = new()
        {
            TimerId = _nextTimerId++,
            NodeId = nodeId,
            Kind = kind,
            FiresAtLogicalTime = firesAtLogicalTime,
            PartitionId = partitionId,
            IntervalMs = intervalMs,
            OnFire = onFire,
        };

        _timers.Add(timer);
        return timer;
    }

    public bool TryGetTimer(long timerId, out VirtualTimer? timer)
    {
        timer = _timers.FirstOrDefault(candidate => candidate.TimerId == timerId);
        return timer is not null;
    }

    public void Cancel(long timerId) => _timers.RemoveAll(candidate => candidate.TimerId == timerId);

    public IReadOnlyList<VirtualTimer> GetPendingTimers() =>
        _timers.OrderBy(timer => timer.FiresAtLogicalTime).ThenBy(timer => timer.TimerId).ToList();

    public IReadOnlyList<SimulationPendingTimerSnapshot> GetPendingSnapshots() =>
        GetPendingTimers()
            .Select(timer => new SimulationPendingTimerSnapshot
            {
                TimerId = timer.TimerId,
                NodeId = timer.NodeId,
                TimerKind = timer.Kind.ToString(),
                FiresAtLogicalTime = timer.FiresAtLogicalTime,
            })
            .ToList();
}
