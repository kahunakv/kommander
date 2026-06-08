using System.Text.Json;
using System.Text.Json.Serialization;
using Kommander.Tests.Simulation.Replay;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Acceptance tests for DST-2 seeded PRNG and replay logging.
/// </summary>
public class TestSeededPrngAndReplay
{
    [Fact]
    public void RandomMode_SameSeed_ProducesSameEventSequence()
    {
        const ulong seed = 42;

        SimulationRuntime firstRuntime = CreateRandomRuntime(seed);
        SimulationResult first = firstRuntime.Run();

        SimulationRuntime secondRuntime = CreateRandomRuntime(seed);
        SimulationResult second = secondRuntime.Run();

        Assert.True(first.Passed);
        Assert.True(second.Passed);
        Assert.Equal(first.StepCount, second.StepCount);
        Assert.Equal(GetEventIds(firstRuntime), GetEventIds(secondRuntime));
    }

    [Fact]
    public void RandomMode_WritesReplayLog_WithRequiredFields()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            SimulationResult result = RunRandomScenario(seed: 99, replayPath);
            Assert.True(result.Passed);
            Assert.Equal(replayPath, result.ReplayLogPath);

            ReplayLogReader reader = new(replayPath);
            IReadOnlyList<ReplayLogEntry> eventEntries = reader.EventEntries;

            Assert.Equal(result.StepCount, eventEntries.Count);
            Assert.All(eventEntries, entry =>
            {
                Assert.Equal(ReplayLogEntry.CurrentVersion, entry.Version);
                Assert.Equal("event", entry.EntryType);
                Assert.Equal(99UL, entry.Seed);
                Assert.Equal("random-order", entry.Scenario);
                Assert.True(entry.EnabledEventCount > 0);
                Assert.NotNull(entry.SelectedEventId);
                Assert.False(string.IsNullOrWhiteSpace(entry.SelectedEventType));
                Assert.False(string.IsNullOrWhiteSpace(entry.SelectedEventSummary));
                Assert.False(string.IsNullOrWhiteSpace(entry.SnapshotHashBefore));
                Assert.False(string.IsNullOrWhiteSpace(entry.SnapshotHashAfter));
            });
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    [Fact]
    public void ReplayMode_ReplaysRecordedRun_Exactly()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            SimulationRuntime recordedRuntime = CreateRandomRuntime(seed: 7, replayPath);
            SimulationResult recorded = recordedRuntime.Run();

            SimulationRuntime replayedRuntime = CreateReplayRuntime(seed: 7, replayPath);
            SimulationResult replayed = replayedRuntime.Run();

            Assert.True(recorded.Passed);
            Assert.True(replayed.Passed);
            Assert.Equal(GetEventIds(recordedRuntime), GetEventIds(replayedRuntime));
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    [Fact]
    public void ReplayMode_FailsWhenEnabledEventSetDiverges()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            RunRandomScenario(seed: 13, replayPath);

            DivergentReplayScenario scenario = new(extraEnabledEvent: true)
            {
                Seed = 13,
                SchedulingMode = SimulationSchedulingMode.Replay,
            };

            SimulationRuntime runtime = new(
                scenario,
                new SimulationRuntimeOptions { ReplayLogPath = replayPath });

            SimulationResult result = runtime.Run();

            Assert.False(result.Passed);
            Assert.NotNull(result.Violation);
            Assert.Equal("replay-divergence", result.Violation.InvariantName);
            Assert.Contains("Enabled event set diverged", result.Violation.Message);
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    [Fact]
    public void ReplayMode_FailsWhenSelectedEventSummaryDiverges()
    {
        RunReplayDivergenceTest(
            new MutatedSummaryReplayScenario
            {
                Seed = 21,
                SchedulingMode = SimulationSchedulingMode.Replay,
            },
            "Selected event summary diverged");
    }

    [Fact]
    public void ReplayMode_FailsWhenSelectedEventTypeDiverges()
    {
        RunReplayDivergenceTest(
            new MutatedTypeReplayScenario
            {
                Seed = 23,
                SchedulingMode = SimulationSchedulingMode.Replay,
            },
            "Selected event type diverged");
    }

    [Fact]
    public void ReplayMode_FailsWhenSnapshotHashBeforeDiverges()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            RunRandomScenario(seed: 17, replayPath);
            TamperReplayLog(replayPath, entry => entry with { SnapshotHashBefore = "deadbeef" });

            SimulationResult result = RunReplayScenario(seed: 17, replayPath);

            AssertReplayDivergence(result, "Snapshot hash diverged before step");
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    [Fact]
    public void ReplayMode_FailsWhenSnapshotHashAfterDiverges()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            RunRandomScenario(seed: 19, replayPath);
            TamperReplayLog(replayPath, entry => entry with { SnapshotHashAfter = "deadbeef" });

            SimulationResult result = RunReplayScenario(seed: 19, replayPath);

            AssertReplayDivergence(result, "Snapshot hash diverged after step");
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    [Fact]
    public void ReplayMode_FailsWhenScenarioEndsEarlyWithUnconsumedReplayEntries()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            SimulationResult recorded = RunRandomScenario(seed: 31, replayPath);
            Assert.True(recorded.Passed);
            Assert.Equal(5, recorded.StepCount);

            EarlyStopReplayScenario scenario = new()
            {
                Seed = 31,
                SchedulingMode = SimulationSchedulingMode.Replay,
            };

            SimulationResult result = new SimulationRuntime(
                scenario,
                new SimulationRuntimeOptions { ReplayLogPath = replayPath }).Run();

            AssertReplayDivergence(result, "unconsumed log entr");
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    [Fact]
    public void ReplayMode_FailsWhenMaxStepsTruncatesRecordedReplay()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            RunRandomScenario(seed: 37, replayPath);

            RandomOrderScenario scenario = new()
            {
                Seed = 37,
                SchedulingMode = SimulationSchedulingMode.Replay,
                MaxSteps = 2,
            };

            SimulationResult result = new SimulationRuntime(
                scenario,
                new SimulationRuntimeOptions { ReplayLogPath = replayPath }).Run();

            AssertReplayDivergence(result, "unconsumed log entr");
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    [Fact]
    public void SimulationRandom_RecordsEveryChoice()
    {
        Random.SimulationRandom random = new(seed: 123);
        random.SetContext(step: 0, logicalTime: 0);

        int first = random.NextInt("a", 0, 10);
        int second = random.NextInt("b", 0, 10);

        Assert.Equal(2, random.RecordedChoices.Count);
        Assert.Equal("a", random.RecordedChoices[0].ChoiceName);
        Assert.Equal(first, random.RecordedChoices[0].Value);
        Assert.Equal("b", random.RecordedChoices[1].ChoiceName);
        Assert.Equal(second, random.RecordedChoices[1].Value);
    }

    private static SimulationRuntime CreateRandomRuntime(ulong seed, string? replayLogPath = null)
    {
        RandomOrderScenario scenario = new()
        {
            Seed = seed,
            SchedulingMode = SimulationSchedulingMode.Random,
            MaxSteps = 5,
        };

        SimulationRuntimeOptions? options = replayLogPath is null
            ? null
            : new SimulationRuntimeOptions { ReplayLogPath = replayLogPath };

        return new SimulationRuntime(scenario, options);
    }

    private static SimulationRuntime CreateReplayRuntime(ulong seed, string replayLogPath)
    {
        RandomOrderScenario scenario = new()
        {
            Seed = seed,
            SchedulingMode = SimulationSchedulingMode.Replay,
            MaxSteps = 5,
        };

        return new SimulationRuntime(
            scenario,
            new SimulationRuntimeOptions { ReplayLogPath = replayLogPath });
    }

    private static SimulationResult RunRandomScenario(ulong seed, string replayLogPath) =>
        CreateRandomRuntime(seed, replayLogPath).Run();

    private static SimulationResult RunReplayScenario(ulong seed, string replayLogPath) =>
        CreateReplayRuntime(seed, replayLogPath).Run();

    private static void RunReplayDivergenceTest(SimulationScenario scenario, string expectedMessageFragment)
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            RunRandomScenario(seed: scenario.Seed, replayPath);

            SimulationRuntime runtime = new(
                scenario,
                new SimulationRuntimeOptions { ReplayLogPath = replayPath });

            AssertReplayDivergence(runtime.Run(), expectedMessageFragment);
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    private static void AssertReplayDivergence(SimulationResult result, string expectedMessageFragment)
    {
        Assert.False(result.Passed);
        Assert.NotNull(result.Violation);
        Assert.Equal("replay-divergence", result.Violation.InvariantName);
        Assert.Contains(expectedMessageFragment, result.Violation.Message);
    }

    private static readonly JsonSerializerOptions ReplaySerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private static void TamperReplayLog(string replayPath, Func<ReplayLogEntry, ReplayLogEntry> mutate)
    {
        ReplayLogReader reader = new(replayPath);
        List<string> lines = [];

        foreach (ReplayLogEntry entry in reader.Entries)
        {
            ReplayLogEntry updated = string.Equals(entry.EntryType, "event", StringComparison.Ordinal)
                ? mutate(entry)
                : entry;

            lines.Add(JsonSerializer.Serialize(updated, ReplaySerializerOptions));
        }

        File.WriteAllLines(replayPath, lines);
    }

    private static IReadOnlyList<long> GetEventIds(SimulationRuntime runtime) =>
        runtime.EventHistory.Select(evt => evt.Id).ToList();

    private static string CreateTempReplayPath() =>
        Path.Combine(Path.GetTempPath(), $"kommander-dst-{Guid.NewGuid():N}.jsonl");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private class RandomOrderScenario : SimulationScenario
    {
        private static readonly SimulationEvent[] EventPool =
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
                LogicalTime = 100,
            },
            new SimulationEvent
            {
                Id = 3,
                Type = SimulationEventType.NetworkMessageDelivery,
                Summary = "deliver append to node 2",
                NodeId = 2,
                LogicalTime = 120,
            },
            new SimulationEvent
            {
                Id = 4,
                Type = SimulationEventType.WalWriteCompletion,
                Summary = "complete wal write on node 0",
                NodeId = 0,
                LogicalTime = 150,
            },
            new SimulationEvent
            {
                Id = 5,
                Type = SimulationEventType.ClientProposalStart,
                Summary = "start client proposal",
                PartitionId = 0,
                LogicalTime = 200,
            },
        ];

        public override string Name => "random-order";

        public override void Configure(SimulationRuntime runtime)
        {
            for (int nodeId = 0; nodeId < NodeCount; nodeId++)
                runtime.SetNodeLifecycleStatus(nodeId, SimulationNodeLifecycleStatus.Running);
        }

        public override IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime)
        {
            HashSet<long> applied = runtime.EventHistory.Select(evt => evt.Id).ToHashSet();
            return EventPool.Where(evt => !applied.Contains(evt.Id)).ToList();
        }
    }

    private sealed class DivergentReplayScenario : RandomOrderScenario
    {
        private readonly bool _extraEnabledEvent;

        public DivergentReplayScenario(bool extraEnabledEvent) =>
            _extraEnabledEvent = extraEnabledEvent;

        public override IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime)
        {
            IReadOnlyList<SimulationEvent> enabled = base.GetEnabledEvents(runtime);
            if (!_extraEnabledEvent || enabled.Count == 0)
                return enabled;

            SimulationEvent phantom = new()
            {
                Id = 999,
                Type = SimulationEventType.NetworkMessageDrop,
                Summary = "phantom drop",
                NodeId = 0,
                LogicalTime = LogicalTickFrom(runtime),
            };

            return enabled.Concat([phantom]).ToList();
        }

        private static long LogicalTickFrom(SimulationRuntime runtime) =>
            runtime.LogicalTick;
    }

    private sealed class MutatedSummaryReplayScenario : RandomOrderScenario
    {
        public override IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime)
        {
            IReadOnlyList<SimulationEvent> enabled = base.GetEnabledEvents(runtime);
            return enabled
                .Select(evt => evt with { Summary = $"{evt.Summary} (mutated)" })
                .ToList();
        }
    }

    private sealed class MutatedTypeReplayScenario : RandomOrderScenario
    {
        public override IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime)
        {
            IReadOnlyList<SimulationEvent> enabled = base.GetEnabledEvents(runtime);
            return enabled
                .Select(evt => evt with { Type = SimulationEventType.NodeCrash })
                .ToList();
        }
    }

    private sealed class EarlyStopReplayScenario : RandomOrderScenario
    {
        public override IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime)
        {
            if (runtime.StepNumber >= 2)
                return [];

            return base.GetEnabledEvents(runtime);
        }
    }
}
