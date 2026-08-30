using Kommander.Tests.Simulation.Time;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The determinism self-test.
///
/// <para><b>What it guards.</b> Every other guarantee of the simulation harness rests on one
/// claim: a scenario plus a seed produce one run and only one run. If that claim fails, a
/// recorded failure cannot be reproduced, a shrunk trace is not the trace that failed, and a
/// green run means nothing because the next run explores something else. So the claim is tested
/// directly, on every pull request, and a difference is a build failure rather than a flake.</para>
///
/// <para><b>How.</b> One scenario is run twice under one seed, with both runs writing a replay
/// log. The logs must be byte-identical. They carry the selected event of every step and the
/// snapshot hash before and after it, so equality of the files is equality of the whole
/// execution, not only of its outcome.</para>
/// </summary>
[Trait("Category", "DSTSmoke")]
public sealed class TestDeterminismSelfTest
{
    /// <summary>
    /// Seeds checked on every run. Several seeds, because a single one can be deterministic by
    /// accident: a scenario that always selects the same event is trivially reproducible and
    /// proves nothing about a scenario that branches.
    /// </summary>
    private static readonly ulong[] Seeds = [1, 7, 42, 20260829];

    [Fact]
    public void SameSeed_ProducesByteIdenticalReplayLogs()
    {
        foreach (ulong seed in Seeds)
        {
            string firstPath = CreateTempReplayPath();
            string secondPath = CreateTempReplayPath();

            try
            {
                SimulationResult first = RunOnce(seed, firstPath);
                SimulationResult second = RunOnce(seed, secondPath);

                Assert.True(first.Passed, $"Seed {seed}: first run did not pass.");
                Assert.True(second.Passed, $"Seed {seed}: second run did not pass.");
                Assert.True(first.StepCount > 1, $"Seed {seed}: the run was too short to be evidence.");

                byte[] firstBytes = File.ReadAllBytes(firstPath);
                byte[] secondBytes = File.ReadAllBytes(secondPath);

                Assert.True(
                    firstBytes.AsSpan().SequenceEqual(secondBytes),
                    $"Seed {seed}: the two replay logs differ, so a source of nondeterminism has " +
                    "entered the harness. Compare the two logs step by step; the first differing " +
                    "step names the event whose selection or snapshot hash changed.");
            }
            finally
            {
                DeleteIfExists(firstPath);
                DeleteIfExists(secondPath);
            }
        }
    }

    /// <summary>
    /// Two different seeds must explore different runs. Without this, a harness that ignored the
    /// seed entirely would pass the byte-identical check above and look correct while searching
    /// one schedule forever.
    /// </summary>
    [Fact]
    public void DifferentSeeds_ProduceDifferentRuns()
    {
        string firstPath = CreateTempReplayPath();
        string secondPath = CreateTempReplayPath();

        try
        {
            RunOnce(seed: 1, firstPath);
            RunOnce(seed: 20260829, secondPath);

            byte[] firstBytes = File.ReadAllBytes(firstPath);
            byte[] secondBytes = File.ReadAllBytes(secondPath);

            Assert.False(
                firstBytes.AsSpan().SequenceEqual(secondBytes),
                "Two different seeds produced identical runs, so the seed does not steer the search.");
        }
        finally
        {
            DeleteIfExists(firstPath);
            DeleteIfExists(secondPath);
        }
    }

    /// <summary>
    /// A recorded run replays exactly. This is the property a failure artifact depends on: the
    /// replay must reach the same state at every step, or the reproduction is not the failure.
    /// </summary>
    [Fact]
    public void RecordedRun_ReplaysExactly()
    {
        string replayPath = CreateTempReplayPath();

        try
        {
            SimulationResult recorded = RunOnce(seed: 42, replayPath);
            Assert.True(recorded.Passed);

            SelfTestScenario replayScenario = new()
            {
                Seed = 42,
                SchedulingMode = SimulationSchedulingMode.Replay,
            };

            using SimulationRuntime runtime = new(
                replayScenario,
                new SimulationRuntimeOptions { ReplayLogPath = replayPath, WriteReplayLog = false });

            SimulationResult replayed = runtime.Run();

            Assert.True(replayed.Passed, replayed.Violation?.Message);
            Assert.Equal(recorded.StepCount, replayed.StepCount);
            Assert.Equal(recorded.FinalLogicalTime, replayed.FinalLogicalTime);
        }
        finally
        {
            DeleteIfExists(replayPath);
        }
    }

    private static SimulationResult RunOnce(ulong seed, string replayPath)
    {
        SelfTestScenario scenario = new()
        {
            Seed = seed,
            SchedulingMode = SimulationSchedulingMode.Random,
        };

        using SimulationRuntime runtime = new(
            scenario,
            new SimulationRuntimeOptions { ReplayLogPath = replayPath, WriteReplayLog = true });

        return runtime.Run();
    }

    private static string CreateTempReplayPath() =>
        Path.Combine(Path.GetTempPath(), $"kommander-determinism-{Guid.NewGuid():N}.jsonl");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    /// <summary>
    /// A scenario that branches. Periodic timers on three nodes compete with a pool of network
    /// and storage events, so at almost every step the scheduler has several enabled choices and
    /// the seed decides which one runs. A scenario with one enabled event per step would make the
    /// self-test vacuous.
    /// </summary>
    private sealed class SelfTestScenario : SimulationScenario
    {
        private static readonly SimulationEvent[] EventPool =
        [
            new SimulationEvent
            {
                Id = 1001,
                Type = SimulationEventType.NetworkMessageDelivery,
                Summary = "deliver append to node 1",
                NodeId = 1,
                PartitionId = 0,
                LogicalTime = 0,
            },
            new SimulationEvent
            {
                Id = 1002,
                Type = SimulationEventType.NetworkMessageDelivery,
                Summary = "deliver append to node 2",
                NodeId = 2,
                PartitionId = 0,
                LogicalTime = 0,
            },
            new SimulationEvent
            {
                Id = 1003,
                Type = SimulationEventType.NetworkMessageDrop,
                Summary = "drop vote from node 2",
                NodeId = 2,
                PartitionId = 0,
                LogicalTime = 0,
            },
            new SimulationEvent
            {
                Id = 1004,
                Type = SimulationEventType.WalWriteCompletion,
                Summary = "complete wal write on node 0",
                NodeId = 0,
                PartitionId = 0,
                LogicalTime = 0,
            },
            new SimulationEvent
            {
                Id = 1005,
                Type = SimulationEventType.WalWriteFailure,
                Summary = "fail wal write on node 1",
                NodeId = 1,
                PartitionId = 0,
                LogicalTime = 0,
            },
            new SimulationEvent
            {
                Id = 1006,
                Type = SimulationEventType.ClientProposalStart,
                Summary = "start client proposal",
                PartitionId = 0,
                LogicalTime = 0,
            },
        ];

        public override string Name => "determinism-self-test";

        public override int NodeCount { get; init; } = 3;

        public override int MaxSteps { get; init; } = 60;

        public override long MaxLogicalTime { get; init; } = 20_000;

        public override void Configure(SimulationRuntime runtime)
        {
            for (int nodeId = 0; nodeId < NodeCount; nodeId++)
            {
                runtime.SetNodeLifecycleStatus(nodeId, SimulationNodeLifecycleStatus.Running);
                NodeVirtualTimerService.ScheduleStandardTimers(runtime, nodeId);
            }
        }

        public override IReadOnlyList<SimulationEvent> GetEnabledEvents(SimulationRuntime runtime)
        {
            HashSet<long> applied = runtime.EventHistory.Select(simulationEvent => simulationEvent.Id).ToHashSet();

            return EventPool
                .Where(simulationEvent => !applied.Contains(simulationEvent.Id))
                .Select(simulationEvent => simulationEvent with { LogicalTime = runtime.LogicalTick })
                .ToList();
        }
    }
}
