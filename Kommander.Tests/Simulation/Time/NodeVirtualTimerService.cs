namespace Kommander.Tests.Simulation.Time;

/// <summary>
/// Schedules the standard Raft timer set for a simulated node using virtual time.
/// </summary>
public static class NodeVirtualTimerService
{
    public static void ScheduleStandardTimers(
        SimulationRuntime runtime,
        int nodeId,
        VirtualTimeConfiguration? configuration = null,
        int? partitionId = 0,
        Action<SimulationRuntime, VirtualTimer>? onCheckLeader = null,
        Action<SimulationRuntime, VirtualTimer>? onHeartbeat = null,
        Action<SimulationRuntime, VirtualTimer>? onUpdateNodes = null)
    {
        VirtualTimeConfiguration config = configuration ?? runtime.VirtualTime.Configuration;
        VirtualTime virtualTime = runtime.VirtualTime;
        long now = runtime.LogicalTick;

        virtualTime.SchedulePeriodic(
            nodeId,
            SimulationTimerKind.CheckLeader,
            config.CheckLeaderIntervalMs,
            config.TimerInitialDelayMs,
            partitionId,
            onCheckLeader,
            now);

        virtualTime.SchedulePeriodic(
            nodeId,
            SimulationTimerKind.Heartbeat,
            config.HeartbeatIntervalMs,
            config.TimerInitialDelayMs,
            partitionId,
            onHeartbeat,
            now);

        virtualTime.SchedulePeriodic(
            nodeId,
            SimulationTimerKind.UpdateNodes,
            config.UpdateNodesIntervalMs,
            config.TimerInitialDelayMs,
            partitionId: null,
            onUpdateNodes,
            now);
    }
}
