using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The generator on its own, with no cluster.
///
/// <para><b>Why test it separately.</b> A random search that quietly stopped drawing faults, or
/// that drew a different plan on each run of one seed, would still pass every cluster scenario it
/// produced — it would simply explore nothing. Those failures are invisible from the outside and
/// obvious from here, and these tests run in milliseconds rather than minutes.</para>
/// </summary>
[Trait("Category", "DSTSmoke")]
public sealed class TestRandomScenarioGenerator
{
    private static readonly string[] ThreeNodes = ["node1", "node2", "node3"];

    // ── Reproducibility ───────────────────────────────────────────────────

    /// <summary>
    /// One seed draws one plan. This is the property the whole random mode rests on: a failing
    /// seed that drew a different plan on the next run would be a failure nobody could reproduce.
    /// </summary>
    [Fact]
    public void SameSeed_DrawsTheSamePlan()
    {
        List<RandomScenarioAction> first = DrawPlan(seed: 42, count: 60);
        List<RandomScenarioAction> second = DrawPlan(seed: 42, count: 60);

        Assert.Equal(
            first.Select(action => action.Describe()),
            second.Select(action => action.Describe()));
    }

    /// <summary>
    /// Different seeds draw different plans. Without this the search would run one schedule under
    /// many names and report broad coverage it does not have.
    /// </summary>
    [Fact]
    public void DifferentSeeds_DrawDifferentPlans()
    {
        List<RandomScenarioAction> first = DrawPlan(seed: 42, count: 60);
        List<RandomScenarioAction> second = DrawPlan(seed: 43, count: 60);

        Assert.NotEqual(
            first.Select(action => action.Describe()),
            second.Select(action => action.Describe()));
    }

    // ── The two rules that keep a run productive ──────────────────────────

    /// <summary>
    /// The quorum budget holds at every point of a long plan.
    ///
    /// <para>The budget is what keeps a proposal reachable. Past it, a run stops exploring and
    /// starts paying ten real seconds per write into a cluster that cannot commit.</para>
    /// </summary>
    [Fact]
    public void TheQuorumBudget_IsNeverExceeded()
    {
        SimulationRandom random = new(seed: 7);
        RandomScenarioOptions options = new() { MaxImpairedNodes = 1 };
        RandomScenarioGenerator generator = new(random, options);

        for (int index = 0; index < 400; index++)
        {
            generator.Next(Observation());

            Assert.True(
                generator.ImpairedNodeCount <= options.MaxImpairedNodes,
                $"Action {index} left {generator.ImpairedNodeCount} nodes impaired.");
        }
    }

    /// <summary>
    /// A fault is healed once it is older than the age bound, whether or not the draw asks for it.
    ///
    /// <para>The weights here allow only lifecycle faults, so the first action breaks a node and
    /// every later draw finds the budget full and nothing else on offer. The heal must still
    /// arrive, and it must arrive exactly at the bound: a heal that came later would mean the bound
    /// is a preference, and a run would spend most of its length on a cluster that cannot
    /// work.</para>
    /// </summary>
    [Fact]
    public void AFault_IsHealedAtTheAgeBound()
    {
        RandomScenarioOptions options = new()
        {
            ClientWeight = 0,
            IdleWeight = 0,
            OutageWeight = 0,
            MaintenanceWeight = 0,
            NetworkFaultWeight = 0,
            StorageFaultWeight = 0,
            HealWeight = 0,
            LifecycleFaultWeight = 1,
            MaxImpairedNodes = 1,
            MaxFaultAgeInActions = 4,

            // Episodes would fill the gap the bound is being measured across.
            EnableFaultEpisodes = false,
        };

        RandomScenarioGenerator generator = new(new SimulationRandom(seed: 11), options);

        List<RandomScenarioAction> plan = [];

        // Exactly as far as the heal, and no further: the draw after it finds the budget free
        // again and starts a new fault, which would leave the count at one for reasons that have
        // nothing to do with the bound under test.
        for (int index = 0; index <= options.MaxFaultAgeInActions; index++)
            plan.Add(generator.Next(Observation()));

        Assert.Contains(
            plan[0].Kind,
            new[] { RandomScenarioActionKind.CrashNode, RandomScenarioActionKind.PauseNode });

        for (int index = 1; index < options.MaxFaultAgeInActions; index++)
            Assert.Equal(RandomScenarioActionKind.Idle, plan[index].Kind);

        RandomScenarioAction heal = plan[options.MaxFaultAgeInActions];

        Assert.Equal(
            plan[0].Kind == RandomScenarioActionKind.CrashNode
                ? RandomScenarioActionKind.RestartNode
                : RandomScenarioActionKind.ResumeNode,
            heal.Kind);

        Assert.Equal(plan[0].Target, heal.Target);
        Assert.Equal(0, generator.ImpairedNodeCount);
    }

    /// <summary>
    /// Nothing is left broken. A plan that ended with a fault still active would send the run into
    /// a convergence check the fault itself makes impossible.
    /// </summary>
    [Fact]
    public void HealAll_EndsEveryOutstandingFault()
    {
        SimulationRandom random = new(seed: 4);
        RandomScenarioGenerator generator = new(random, new RandomScenarioOptions());

        for (int index = 0; index < 30; index++)
            generator.Next(Observation());

        IReadOnlyList<RandomScenarioAction> heals = generator.HealAll();

        Assert.Equal(0, generator.ActiveFaultCount);
        Assert.All(heals, action => Assert.NotEqual(RandomScenarioActionKind.CrashNode, action.Kind));
        Assert.All(heals, action => Assert.NotEqual(RandomScenarioActionKind.StarveDisk, action.Kind));
    }

    /// <summary>
    /// A heal continues the plan's numbering rather than restarting it, so the artifact reads as
    /// one sequence.
    /// </summary>
    [Fact]
    public void HealAll_ContinuesThePlanNumbering()
    {
        SimulationRandom random = new(seed: 5);
        RandomScenarioGenerator generator = new(random, new RandomScenarioOptions());

        List<RandomScenarioAction> plan = [];

        for (int index = 0; index < 20; index++)
            plan.Add(generator.Next(Observation()));

        plan.AddRange(generator.HealAll());

        Assert.Equal(Enumerable.Range(0, plan.Count), plan.Select(action => action.Index));
    }

    // ── Choices that keep a run cheap ─────────────────────────────────────

    /// <summary>
    /// No append is drawn at a leader whose own disk is refusing writes.
    ///
    /// <para>A leader that cannot write its own entry cannot commit it, so the proposal waits out
    /// the ten real seconds of the quorum timeout and returns a timeout. The state is worth
    /// reaching through the fault; paying for it on every client draw is not.</para>
    /// </summary>
    [Fact]
    public void NoAppendIsDrawnAtALeaderWhoseDiskIsStarved()
    {
        RandomScenarioOptions options = new()
        {
            ClientWeight = 2,
            IdleWeight = 0,
            OutageWeight = 0,
            MaintenanceWeight = 0,
            NetworkFaultWeight = 0,
            StorageFaultWeight = 2,
            LifecycleFaultWeight = 0,
            HealWeight = 1,
            MaxFaultAgeInActions = 6,
        };

        RandomScenarioGenerator generator = new(new SimulationRandom(seed: 21), options);

        HashSet<string> starved = new(StringComparer.Ordinal);
        int appendsAtStarvedLeader = 0;
        int starveActions = 0;

        for (int index = 0; index < 200; index++)
        {
            RandomScenarioAction action = generator.Next(Observation());

            switch (action.Kind)
            {
                case RandomScenarioActionKind.StarveDisk:
                    starved.Add(action.Target!);
                    starveActions++;
                    break;

                case RandomScenarioActionKind.FreeDisk:
                    starved.Remove(action.Target!);
                    break;

                case RandomScenarioActionKind.AppendAtLeader when starved.Contains(action.Target!):
                    appendsAtStarvedLeader++;
                    break;
            }
        }

        Assert.True(starveActions > 0, "No disk was ever starved, so the rule was never exercised.");
        Assert.Equal(0, appendsAtStarvedLeader);
    }

    /// <summary>
    /// With no leader to write to, the client draw still produces a client operation. Refusals are
    /// worth issuing: a refused append must not appear in the log, and that rule needs refusals.
    /// </summary>
    [Fact]
    public void WithNoLeader_TheClientDrawStillWritesSomewhere()
    {
        RandomScenarioOptions options = new()
        {
            ClientWeight = 1,
            IdleWeight = 0,
            OutageWeight = 0,
            MaintenanceWeight = 0,
            NetworkFaultWeight = 0,
            StorageFaultWeight = 0,
            LifecycleFaultWeight = 0,
            HealWeight = 0,
        };

        RandomScenarioGenerator generator = new(new SimulationRandom(seed: 3), options);

        for (int index = 0; index < 20; index++)
        {
            RandomScenarioAction action = generator.Next(Observation(leader: null));

            Assert.Equal(RandomScenarioActionKind.AppendAtFollower, action.Kind);
            Assert.Contains(action.Target, ThreeNodes);
        }
    }

    /// <summary>
    /// A link action names two different endpoints. A link from a node to itself would be quietly
    /// meaningless, and the plan would record a fault the cluster never felt.
    /// </summary>
    [Fact]
    public void ALinkFault_NamesTwoDifferentEndpoints()
    {
        RandomScenarioOptions options = new()
        {
            ClientWeight = 0,
            IdleWeight = 0,
            OutageWeight = 0,
            MaintenanceWeight = 0,
            NetworkFaultWeight = 1,
            StorageFaultWeight = 0,
            LifecycleFaultWeight = 0,
            HealWeight = 1,
            MaxFaultAgeInActions = 2,
        };

        RandomScenarioGenerator generator = new(new SimulationRandom(seed: 9), options);

        int links = 0;

        for (int index = 0; index < 100; index++)
        {
            RandomScenarioAction action = generator.Next(Observation());

            if (action.Secondary is null)
                continue;

            links++;
            Assert.NotEqual(action.Target, action.Secondary);
        }

        Assert.True(links > 0, "No link action was drawn, so nothing was checked.");
    }

    /// <summary>
    /// The whole vocabulary is reachable from one seed and a long enough plan. A kind nothing can
    /// draw is a fault the harness believes it injects and never does.
    /// </summary>
    [Fact]
    public void ALongPlan_ReachesEveryFaultFamily()
    {
        SimulationRandom random = new(seed: 20260901);
        RandomScenarioGenerator generator = new(random, new RandomScenarioOptions());

        HashSet<RandomScenarioActionKind> seen = [];

        for (int index = 0; index < 2_000; index++)
            seen.Add(generator.Next(Observation()).Kind);

        foreach (RandomScenarioActionKind kind in Enum.GetValues<RandomScenarioActionKind>())
        {
            // FastDisk and FreeDisk arrive as heals, which the age bound also emits, so every kind
            // in the vocabulary must appear in a plan this long.
            Assert.Contains(kind, seen);
        }
    }

    /// <summary>
    /// A fault pulls its own life in behind it: use it, repair it, change leadership, use it again.
    ///
    /// <para>The order is the whole value. Several known defects need a leader elected after the
    /// damage and a write after that leader takes over, and a uniform draw reaches that five-step
    /// order almost never — measured, not assumed: thirty seeds of independent draws failed to
    /// re-find a defect that a scripted scenario finds every time.</para>
    ///
    /// <para>The targets are left open on purpose. An episode is decided before its steps run, so
    /// the leader it means is the one in place by then; the runner fills them in and records what
    /// it used.</para>
    /// </summary>
    [Fact]
    public void AFault_PullsItsOwnEpisodeInBehindIt()
    {
        RandomScenarioOptions options = new()
        {
            ClientWeight = 0,
            IdleWeight = 0,
            OutageWeight = 0,
            MaintenanceWeight = 0,
            NetworkFaultWeight = 0,
            LifecycleFaultWeight = 0,
            HealWeight = 0,
            StorageFaultWeight = 1,
            MaxFaultAgeInActions = 10,
            EnableFaultEpisodes = true,
        };

        RandomScenarioGenerator generator = new(new SimulationRandom(seed: 20260905), options);

        List<RandomScenarioAction> plan = [];

        for (int index = 0; index < 40; index++)
            plan.Add(generator.Next(Observation()));

        int start = plan.FindIndex(action => action.Kind == RandomScenarioActionKind.StarveDisk);

        Assert.True(start >= 0, "No starved disk was drawn, so no episode could follow one.");

        // The transient write-failure fault has no repair of its own, so it starts no episode. Find
        // the first fault whose next action is a client write: that is one that did.
        while (start >= 0 && plan[start + 1].Kind != RandomScenarioActionKind.AppendAtLeader)
            start = plan.FindIndex(start + 1, action => action.Kind == RandomScenarioActionKind.StarveDisk);

        Assert.True(start >= 0, "No storage fault started an episode.");

        Assert.Equal(RandomScenarioActionKind.AppendAtLeader, plan[start + 1].Kind);
        Assert.Equal(RandomScenarioActionKind.FreeDisk, plan[start + 2].Kind);
        Assert.Equal(plan[start].Target, plan[start + 2].Target);
        Assert.Equal(RandomScenarioActionKind.LeaderOutage, plan[start + 3].Kind);
        Assert.Equal(RandomScenarioActionKind.AppendAtLeader, plan[start + 4].Kind);
    }

    /// <summary>
    /// An episode's repair clears the fault, so the age bound never repairs it a second time. A
    /// second repair would arrive after the run had already moved on, and the plan would record a
    /// heal for something that was no longer broken.
    /// </summary>
    [Fact]
    public void AnEpisodeRepair_ClearsTheFault()
    {
        RandomScenarioOptions options = new()
        {
            ClientWeight = 0,
            IdleWeight = 0,
            OutageWeight = 0,
            MaintenanceWeight = 0,
            NetworkFaultWeight = 0,
            LifecycleFaultWeight = 0,
            HealWeight = 0,
            StorageFaultWeight = 1,
            MaxFaultAgeInActions = 3,
            EnableFaultEpisodes = true,
        };

        RandomScenarioGenerator generator = new(new SimulationRandom(seed: 20260906), options);

        List<RandomScenarioAction> plan = [];

        for (int index = 0; index < 60; index++)
            plan.Add(generator.Next(Observation()));

        int starves = plan.Count(action => action.Kind == RandomScenarioActionKind.StarveDisk);
        int frees = plan.Count(action => action.Kind == RandomScenarioActionKind.FreeDisk);

        Assert.True(starves > 0, "No storage fault was drawn.");
        Assert.True(frees <= starves, $"{frees} repairs for {starves} faults: one was repaired twice.");
    }

    // ── The plan as an artifact ───────────────────────────────────────────

    /// <summary>
    /// A plan survives the round trip through the file a failing run writes.
    ///
    /// <para>The artifact is the replay unit, so this is the property replay rests on. A plan that
    /// came back subtly different — a missing target, a dropped copy count — would replay a run
    /// nobody performed and report that a failure does not reproduce.</para>
    /// </summary>
    [Fact]
    public void APlan_SurvivesTheRoundTripThroughItsArtifact()
    {
        List<RandomScenarioAction> plan = DrawPlan(seed: 20260901, count: 200);

        string text = string.Join(Environment.NewLine, plan.Select(action => action.Describe()));

        IReadOnlyList<RandomScenarioAction> parsed = RandomScenarioPlan.Parse(text.Split(Environment.NewLine));

        Assert.Equal(plan, parsed);
    }

    /// <summary>
    /// A header line is skipped and an action line is not. The artifact carries both, and a parser
    /// that read the header as an action would replay something that never happened.
    /// </summary>
    [Fact]
    public void APlanArtifact_IsReadPastItsHeader()
    {
        string[] lines =
        [
            "seed=42",
            "actionCount=3",
            string.Empty,
            "000 Idle",
            "001 BlockLink node1 -> node2",
            "002 SlowDisk node3 latencyMs=40",
        ];

        IReadOnlyList<RandomScenarioAction> plan = RandomScenarioPlan.Parse(lines);

        Assert.Equal(3, plan.Count);
        Assert.Equal(RandomScenarioActionKind.BlockLink, plan[1].Kind);
        Assert.Equal("node1", plan[1].Target);
        Assert.Equal("node2", plan[1].Secondary);
        Assert.Equal(40, plan[2].Value);
    }

    /// <summary>
    /// An action the parser cannot read is refused rather than skipped. Skipping it would replay a
    /// shorter run and call it the same one.
    /// </summary>
    [Fact]
    public void AnUnreadableAction_IsRefused()
    {
        Assert.Throws<FormatException>(() => RandomScenarioPlan.Parse(["007 NoSuchAction node1"]));
        Assert.Throws<FormatException>(() => RandomScenarioPlan.Parse(["008 BlockLink a b c d"]));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static List<RandomScenarioAction> DrawPlan(ulong seed, int count)
    {
        RandomScenarioGenerator generator = new(new SimulationRandom(seed), new RandomScenarioOptions());

        List<RandomScenarioAction> plan = [];

        for (int index = 0; index < count; index++)
            plan.Add(generator.Next(Observation()));

        return plan;
    }

    private static RandomScenarioObservation Observation(string? leader = "node1") =>
        new()
        {
            Running = ThreeNodes,
            Crashed = [],
            Paused = [],
            Leader = leader,
        };
}
