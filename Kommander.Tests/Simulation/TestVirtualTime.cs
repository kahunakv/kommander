using Kommander.Tests.Simulation.Time;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Acceptance tests for DST-3 virtual time and timer scheduling.
/// </summary>
public class TestVirtualTime
{
    [Fact]
    public void VirtualTime_AdvanceWithoutSleeping()
    {
        VirtualTimeScenario scenario = new();
        SimulationRuntime runtime = new(scenario);

        SimulationResult result = runtime.Run();

        Assert.True(result.Passed);
        Assert.True(result.FinalLogicalTime > 0);
        Assert.Equal(0, runtime.VirtualTime.Queue.Count);
    }

    [Fact]
    public void VirtualTime_TimersBecomeExplicitEvents()
    {
        TimerEventScenario scenario = new();
        SimulationRuntime runtime = new(scenario);

        IReadOnlyList<SimulationEvent> enabled = runtime.CollectEnabledEvents();

        Assert.NotEmpty(enabled);
        Assert.All(enabled, evt => Assert.Equal(SimulationEventType.TimerTick, evt.Type));
        Assert.All(enabled, evt => Assert.NotNull(evt.TimerId));
        Assert.Equal(2500, enabled[0].LogicalTime);
    }

    [Fact]
    public void VirtualTime_PeriodicTimerReschedulesAfterFire()
    {
        PeriodicTimerScenario scenario = new();
        SimulationRuntime runtime = new(scenario);
        IReadOnlyList<SimulationEvent> firstBatch = runtime.CollectEnabledEvents();

        runtime.ApplyEvent(firstBatch[0]);

        Assert.Equal(2500, runtime.LogicalTick);
        IReadOnlyList<SimulationPendingTimerSnapshot> pending = runtime.CaptureSnapshot().PendingTimers;
        Assert.Single(pending);
        Assert.Equal(2500 + 250, pending[0].FiresAtLogicalTime);
    }

    [Fact]
    public void VirtualTime_PeriodicTimerAssignsUniqueEventIdOnEachFire()
    {
        PeriodicTimerScenario scenario = new();
        SimulationRuntime runtime = new(scenario);

        IReadOnlyList<SimulationEvent> firstBatch = runtime.CollectEnabledEvents();
        runtime.ApplyEvent(firstBatch[0]);
        long firstEventId = runtime.EventHistory[0].Id;

        IReadOnlyList<SimulationEvent> secondBatch = runtime.CollectEnabledEvents();
        Assert.NotEmpty(secondBatch);
        Assert.NotEqual(firstEventId, secondBatch[0].Id);

        runtime.ApplyEvent(secondBatch[0]);

        Assert.Equal(2, runtime.EventHistory.Count);
        Assert.NotEqual(runtime.EventHistory[0].Id, runtime.EventHistory[1].Id);
    }

    [Fact]
    public void VirtualTime_StaleTimerEvent_DoesNotFireAtWrongLogicalTime()
    {
        PeriodicTimerScenario scenario = new();
        SimulationRuntime runtime = new(scenario);

        SimulationEvent staleEvent = runtime.CollectEnabledEvents()[0];
        runtime.ApplyEvent(staleEvent);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => runtime.ApplyEvent(staleEvent));

        Assert.Contains("is stale", ex.Message);
        Assert.Equal(2500, runtime.LogicalTick);
        Assert.Single(runtime.CaptureSnapshot().PendingTimers);
    }

    [Fact]
    public void VirtualTime_ElectionTimeoutsUseSeededRandom()
    {
        SimulationRuntime first = BuildElectionRuntime(seed: 42);
        SimulationRuntime second = BuildElectionRuntime(seed: 42);
        SimulationRuntime third = BuildElectionRuntime(seed: 99);

        IReadOnlyList<long> firstTimes = GetElectionFireTimes(first);
        IReadOnlyList<long> secondTimes = GetElectionFireTimes(second);
        IReadOnlyList<long> thirdTimes = GetElectionFireTimes(third);

        Assert.Equal(firstTimes, secondTimes);
        Assert.NotEqual(firstTimes, thirdTimes);
    }

    [Fact]
    public void VirtualTime_ThreeNodeElectionCompletesDeterministically()
    {
        SimulationRuntime first = BuildElectionRuntime(seed: 42);
        SimulationResult firstResult = first.Run();

        SimulationRuntime second = BuildElectionRuntime(seed: 42);
        SimulationResult secondResult = second.Run();

        Assert.True(firstResult.Passed);
        Assert.True(secondResult.Passed);
        Assert.Equal(GetLeaderNode(first), GetLeaderNode(second));
        Assert.NotNull(GetLeaderNode(first));
    }

    [Fact]
    public void NodeVirtualTimerService_SchedulesStandardRaftTimers()
    {
        StandardTimerScenario scenario = new();
        SimulationRuntime runtime = new(scenario);

        IReadOnlyList<SimulationPendingTimerSnapshot> pending = runtime.CaptureSnapshot().PendingTimers;

        Assert.Equal(3, pending.Count);
        Assert.Contains(pending, timer => timer.TimerKind == nameof(SimulationTimerKind.CheckLeader));
        Assert.Contains(pending, timer => timer.TimerKind == nameof(SimulationTimerKind.Heartbeat));
        Assert.Contains(pending, timer => timer.TimerKind == nameof(SimulationTimerKind.UpdateNodes));
    }

    private static SimulationRuntime BuildElectionRuntime(ulong seed)
    {
        ThreeNodeElectionScenario scenario = new() { Seed = seed };
        return new SimulationRuntime(scenario);
    }

    private static IReadOnlyList<long> GetElectionFireTimes(SimulationRuntime runtime) =>
        runtime.CaptureSnapshot()
            .PendingTimers
            .Where(timer => timer.TimerKind == nameof(SimulationTimerKind.ElectionTimeout))
            .Select(timer => timer.FiresAtLogicalTime)
            .OrderBy(time => time)
            .ToList();

    private static int? GetLeaderNode(SimulationRuntime runtime) =>
        runtime.CaptureSnapshot().PartitionLeaders.GetValueOrDefault(0)?.LeaderNodeId;

    private sealed class VirtualTimeScenario : SimulationScenario
    {
        private readonly List<SimulationEvent> _scriptedEvents = [];

        public override string Name => "virtual-time-advance";

        public override void Configure(SimulationRuntime runtime)
        {
            VirtualTimer timer = runtime.VirtualTime.ScheduleOneShot(
                nodeId: 0,
                SimulationTimerKind.Retry,
                firesAtLogicalTime: 100,
                partitionId: 0,
                onFire: null);

            _scriptedEvents.AddRange(runtime.VirtualTime.GetAllScheduledTimerEvents(runtime));
            runtime.VirtualTime.AttachHandler(timer.TimerId, (rt, _) =>
                rt.SetNodeLifecycleStatus(0, SimulationNodeLifecycleStatus.Running));
        }

        public override IReadOnlyList<SimulationEvent> ScriptedEvents() => _scriptedEvents;
    }

    private sealed class TimerEventScenario : SimulationScenario
    {
        public override string Name => "timer-events";

        public override void Configure(SimulationRuntime runtime)
        {
            runtime.VirtualTime.SchedulePeriodic(
                nodeId: 0,
                SimulationTimerKind.CheckLeader,
                intervalMs: 250,
                initialDelayMs: 2500,
                partitionId: 0,
                onFire: null);
        }

        public override IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime) => [];
    }

    private sealed class PeriodicTimerScenario : SimulationScenario
    {
        private readonly List<SimulationEvent> _scriptedEvents = [];

        public override string Name => "periodic-timer";

        public override void Configure(SimulationRuntime runtime)
        {
            runtime.VirtualTime.SchedulePeriodic(
                nodeId: 0,
                SimulationTimerKind.CheckLeader,
                intervalMs: 250,
                initialDelayMs: 2500,
                partitionId: 0,
                onFire: null);

            _scriptedEvents.AddRange(runtime.VirtualTime.GetAllScheduledTimerEvents(runtime));
        }

        public override IReadOnlyList<SimulationEvent> ScriptedEvents() => _scriptedEvents;
    }

    private sealed class StandardTimerScenario : SimulationScenario
    {
        public override string Name => "standard-timers";

        public override void Configure(SimulationRuntime runtime) =>
            NodeVirtualTimerService.ScheduleStandardTimers(runtime, nodeId: 0);
    }

    private sealed class ThreeNodeElectionScenario : SimulationScenario
    {
        private readonly List<SimulationEvent> _scriptedEvents = [];

        public override string Name => "three-node-election";

        public override int NodeCount => 3;

        public override void Configure(SimulationRuntime runtime)
        {
            for (int nodeId = 0; nodeId < NodeCount; nodeId++)
            {
                runtime.SetNodeLifecycleStatus(nodeId, SimulationNodeLifecycleStatus.Running);
                runtime.SetNodeRaftState(nodeId, currentTerm: 0);

                VirtualTimer timer = runtime.VirtualTime.ScheduleElectionTimeout(
                    runtime,
                    nodeId,
                    partitionId: 0,
                    fromLogicalTime: runtime.LogicalTick);

                runtime.VirtualTime.AttachHandler(timer.TimerId, HandleElectionTimeout);
            }

            _scriptedEvents.AddRange(runtime.VirtualTime.GetAllScheduledTimerEvents(runtime));
        }

        public override IReadOnlyList<SimulationEvent> ScriptedEvents() => _scriptedEvents;

        private static void HandleElectionTimeout(SimulationRuntime runtime, VirtualTimer timer)
        {
            if (runtime.CaptureSnapshot().PartitionLeaders.ContainsKey(0))
                return;

            runtime.SetNodeRaftState(timer.NodeId, currentTerm: 1, votedFor: $"node-{timer.NodeId}");
            runtime.SetPartitionLeader(partitionId: 0, leaderNodeId: timer.NodeId, term: 1);
        }
    }
}
