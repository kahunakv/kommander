namespace Kommander.Tests.Simulation;

/// <summary>
/// Acceptance tests for DST-1 simulation contracts.
/// </summary>
public class TestSimulationContracts
{
    [Fact]
    public void Scenario_CanBeDeclared_WithoutStartingThreadsOrTimers()
    {
        NullScenario scenario = new();
        SimulationRuntime runtime = new(scenario);

        Assert.Equal("null-scenario", scenario.Name);
        Assert.Equal(3, runtime.Scenario.NodeCount);
        Assert.Equal(0, runtime.StepNumber);
        Assert.Equal(3, runtime.CaptureSnapshot().Nodes.Count);
        Assert.All(runtime.CaptureSnapshot().Nodes, node =>
            Assert.Equal(SimulationNodeLifecycleStatus.Stopped, node.LifecycleStatus));
    }

    [Fact]
    public void Snapshot_CanBeSerialized_ForDebugging()
    {
        SimulationRuntime runtime = new(new SnapshotScenario());
        runtime.SetNodeLifecycleStatus(0, SimulationNodeLifecycleStatus.Running);
        runtime.SetNodeRaftState(0, currentTerm: 2, votedFor: "node-0", knownLeader: "node-0");
        runtime.SetPartitionLeader(partitionId: 0, leaderNodeId: 0, term: 2);
        runtime.EnqueuePendingNetworkMessage(new SimulationPendingMessageSnapshot
        {
            MessageId = 1,
            FromNode = "node-0",
            ToNode = "node-1",
            MessageType = "AppendLogs",
            ScheduledDeliveryTime = 100,
        });

        SimulationSnapshot snapshot = runtime.CaptureSnapshot();
        string json = snapshot.ToJson();
        SimulationSnapshot roundTripped = SimulationSnapshot.FromJson(json);

        Assert.False(string.IsNullOrWhiteSpace(json));
        Assert.Contains("\"logicalTick\"", json);
        Assert.Equal(snapshot.LogicalTick, roundTripped.LogicalTick);
        Assert.Equal(snapshot.StepNumber, roundTripped.StepNumber);
        Assert.Equal(snapshot.Nodes.Count, roundTripped.Nodes.Count);
        Assert.Equal(snapshot.PartitionLeaders.Count, roundTripped.PartitionLeaders.Count);
        Assert.Equal(snapshot.PendingNetworkMessages.Count, roundTripped.PendingNetworkMessages.Count);
        Assert.False(string.IsNullOrWhiteSpace(snapshot.ComputeContentHash()));
    }

    [Fact]
    public void Runtime_RunsScriptedEvents_AndChecksInvariants()
    {
        ScriptedScenario scenario = new();
        SimulationRuntime runtime = new(scenario);
        runtime.RegisterInvariant("nodes-remain-running", snapshot =>
        {
            Assert.All(snapshot.Nodes, node =>
                Assert.Equal(SimulationNodeLifecycleStatus.Running, node.LifecycleStatus));
        });

        SimulationResult result = runtime.Run();

        Assert.True(result.Passed);
        Assert.Equal(2, result.StepCount);
        Assert.Equal(2, runtime.EventHistory.Count);
        Assert.NotNull(result.LastValidSnapshot);
    }

    [Fact]
    public void Runtime_SurfacesInvariantViolations_WithStructuredException()
    {
        ScriptedScenario scenario = new();
        SimulationRuntime runtime = new(scenario);
        runtime.RegisterInvariant("max-one-step", snapshot =>
        {
            if (snapshot.StepNumber > 1)
                throw new InvalidOperationException("Exceeded one step.");
        });

        SimulationResult result = runtime.Run();

        Assert.False(result.Passed);
        Assert.NotNull(result.Violation);
        Assert.Equal("max-one-step", result.Violation.InvariantName);
        Assert.Equal(2, result.StepCount);
        Assert.NotNull(result.LastValidSnapshot);
        Assert.NotNull(result.FailingSnapshot);
    }

    [Fact]
    public void InvariantViolationException_CarriesFailureContext()
    {
        SimulationSnapshot lastValid = SimulationSnapshot.Empty(stepNumber: 0);
        SimulationSnapshot failing = SimulationSnapshot.Empty(stepNumber: 1);
        SimulationEvent selected = new()
        {
            Id = 1,
            Type = SimulationEventType.NodeStart,
            Summary = "start node 0",
            NodeId = 0,
        };

        InvariantViolationException ex = new(
            "one-leader-per-term",
            "two leaders in term 2",
            stepNumber: 1,
            selected,
            lastValid,
            failing);

        Assert.Equal("one-leader-per-term", ex.InvariantName);
        Assert.Equal(1, ex.StepNumber);
        Assert.Same(selected, ex.SelectedEvent);
        Assert.Same(lastValid, ex.LastValidSnapshot);
        Assert.Same(failing, ex.FailingSnapshot);
    }

    private sealed class NullScenario : SimulationScenario
    {
        public override string Name => "null-scenario";

        public override void Configure(SimulationRuntime runtime)
        {
        }
    }

    private sealed class SnapshotScenario : SimulationScenario
    {
        public override string Name => "snapshot-scenario";

        public override void Configure(SimulationRuntime runtime)
        {
            for (int nodeId = 0; nodeId < NodeCount; nodeId++)
                runtime.SetNodeLifecycleStatus(nodeId, SimulationNodeLifecycleStatus.Stopped);
        }
    }

    private sealed class ScriptedScenario : SimulationScenario
    {
        public override string Name => "scripted-scenario";

        public override void Configure(SimulationRuntime runtime)
        {
            for (int nodeId = 0; nodeId < NodeCount; nodeId++)
                runtime.SetNodeLifecycleStatus(nodeId, SimulationNodeLifecycleStatus.Running);
        }

        public override IReadOnlyList<SimulationEvent> ScriptedEvents() =>
        [
            new SimulationEvent
            {
                Id = 1,
                Type = SimulationEventType.NodeStart,
                Summary = "start node 0",
                NodeId = 0,
                LogicalTime = 0,
            },
            new SimulationEvent
            {
                Id = 2,
                Type = SimulationEventType.TimerTick,
                Summary = "election timeout on node 1",
                NodeId = 1,
                LogicalTime = 150,
            },
        ];
    }
}
