namespace Kommander.Tests.Simulation.Time;

/// <summary>
/// Deterministic logical clock with a queue of virtual timers.
/// Advancing time exposes timer events without executing them until the scheduler selects them.
/// </summary>
public sealed class VirtualTime
{
    private readonly VirtualTimerQueue _queue = new();

    public VirtualTime(VirtualTimeConfiguration? configuration = null)
    {
        Configuration = configuration ?? VirtualTimeConfiguration.Default;
    }

    public VirtualTimeConfiguration Configuration { get; }

    public VirtualTimerQueue Queue => _queue;

    public VirtualTimer ScheduleOneShot(
        int nodeId,
        SimulationTimerKind kind,
        long firesAtLogicalTime,
        int? partitionId,
        Action<SimulationRuntime, VirtualTimer>? onFire) =>
        _queue.Schedule(nodeId, kind, firesAtLogicalTime, intervalMs: 0, partitionId, onFire);

    public VirtualTimer SchedulePeriodic(
        int nodeId,
        SimulationTimerKind kind,
        long intervalMs,
        long initialDelayMs,
        int? partitionId,
        Action<SimulationRuntime, VirtualTimer>? onFire,
        long fromLogicalTime = 0) =>
        _queue.Schedule(
            nodeId,
            kind,
            fromLogicalTime + initialDelayMs,
            intervalMs,
            partitionId,
            onFire);

    public VirtualTimer ScheduleElectionTimeout(
        SimulationRuntime runtime,
        int nodeId,
        int partitionId,
        long fromLogicalTime)
    {
        int timeoutMs = runtime.Random.NextInt(
            $"election-timeout-{nodeId}-{partitionId}",
            Configuration.StartElectionTimeoutMs,
            Configuration.EndElectionTimeoutMs + 1);

        return ScheduleOneShot(
            nodeId,
            SimulationTimerKind.ElectionTimeout,
            fromLogicalTime + timeoutMs,
            partitionId,
            onFire: null);
    }

    public VirtualTimer ScheduleRetry(
        SimulationRuntime runtime,
        int nodeId,
        int partitionId,
        Action<SimulationRuntime, VirtualTimer>? onFire,
        long fromLogicalTime) =>
        ScheduleOneShot(
            nodeId,
            SimulationTimerKind.Retry,
            fromLogicalTime + Configuration.ProposalRetryDelayMs,
            partitionId,
            onFire);

    public VirtualTimer ScheduleProposalPoll(
        SimulationRuntime runtime,
        int nodeId,
        int partitionId,
        Action<SimulationRuntime, VirtualTimer>? onFire,
        long fromLogicalTime) =>
        ScheduleOneShot(
            nodeId,
            SimulationTimerKind.ProposalPoll,
            fromLogicalTime + Configuration.ProposalPollDelayMs,
            partitionId,
            onFire);

    /// <summary>
    /// Returns timer tick events for the next eligible fire time without executing them.
    /// </summary>
    public IReadOnlyList<SimulationEvent> GetEnabledTimerEvents(SimulationRuntime runtime)
    {
        IReadOnlyList<VirtualTimer> pending = _queue.GetPendingTimers();
        if (pending.Count == 0)
            return [];

        long nextFireTime = pending[0].FiresAtLogicalTime;
        return pending
            .Where(timer => timer.FiresAtLogicalTime == nextFireTime)
            .Select(timer => ToTimerEvent(runtime, timer))
            .ToList();
    }

    /// <summary>
    /// Returns all pending timer events sorted by fire time for scripted scenarios.
    /// </summary>
    public IReadOnlyList<SimulationEvent> GetAllScheduledTimerEvents(SimulationRuntime runtime) =>
        _queue.GetPendingTimers()
            .Select(timer => ToTimerEvent(runtime, timer))
            .ToList();

    /// <summary>
    /// Executes a timer callback and reschedules periodic timers.
    /// </summary>
    public void FireTimer(SimulationRuntime runtime, long timerId)
    {
        if (!_queue.TryGetTimer(timerId, out VirtualTimer? timer) || timer is null)
            throw new InvalidOperationException($"Virtual timer {timerId} was not found.");

        timer.OnFire?.Invoke(runtime, timer);

        if (timer.IsPeriodic)
        {
            timer.FiresAtLogicalTime = runtime.LogicalTick + timer.IntervalMs;
            timer.ScheduledEventId = null;
        }
        else
            _queue.Cancel(timerId);
    }

    public void AttachHandler(long timerId, Action<SimulationRuntime, VirtualTimer> onFire)
    {
        if (!_queue.TryGetTimer(timerId, out VirtualTimer? timer) || timer is null)
            throw new InvalidOperationException($"Virtual timer {timerId} was not found.");

        timer.OnFire = onFire;
    }

    private static SimulationEvent ToTimerEvent(SimulationRuntime runtime, VirtualTimer timer)
    {
        timer.ScheduledEventId ??= runtime.AllocateEventId();
        return new SimulationEvent
        {
            Id = timer.ScheduledEventId.Value,
            Type = SimulationEventType.TimerTick,
            Summary = $"{timer.Kind} timer {timer.TimerId} on node {timer.NodeId}",
            NodeId = timer.NodeId,
            PartitionId = timer.PartitionId,
            LogicalTime = timer.FiresAtLogicalTime,
            TimerId = timer.TimerId,
        };
    }
}
