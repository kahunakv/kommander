using Kommander.Tests.Simulation.Scenarios.Random;
using Kommander.Tests.Simulation.Shrinking;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The shrinker's algorithm, checked against a plain function instead of a cluster.
///
/// <para><b>Why the oracle is substituted here.</b> The algorithm has nothing to do with Raft. It
/// removes chunks, it normalises, and it compares signatures. Checking those against a real cluster
/// would cost minutes per case and would prove the cluster works, which other tests already do.
/// With a function standing in, every rule below is checked in milliseconds and the failures name
/// the rule that broke.</para>
///
/// <para>Each oracle here states a condition on the plan, so the smallest plan that meets it is
/// known in advance. That is what turns "the plan got smaller" into "the plan got as small as it
/// should have".</para>
/// </summary>
public sealed class TestPlanShrinker
{
    private const string Target = "invariant:committed-entries-agree";

    /// <summary>
    /// The padding goes and the cause stays.
    ///
    /// <para>The base case for everything else. Twenty idle actions around one crash, and only the
    /// crash matters; a shrinker that cannot do this cannot do anything.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_KeepsOnlyTheActionTheFailureNeeds()
    {
        List<RandomScenarioAction> plan = Padding(10);
        plan.Add(new RandomScenarioAction(10, RandomScenarioActionKind.CrashNode, "localhost:8002"));
        plan.AddRange(Padding(10));

        ShrinkResult result = await Shrink(
            plan,
            candidate => candidate.Any(action => action.Kind == RandomScenarioActionKind.CrashNode),
            new ShrinkOptions { MaxCandidates = 200 });

        Assert.Single(result.Shrunk);
        Assert.Equal(RandomScenarioActionKind.CrashNode, result.Shrunk[0].Kind);
        Assert.Equal("localhost:8002", result.Shrunk[0].Target);
    }

    /// <summary>
    /// A conjunction survives. The failure needs two actions in order, and the shrinker must keep
    /// both — the risk being a greedy pass that drops the first because the second alone still looks
    /// necessary.
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_KeepsBothHalvesOfAConjunction()
    {
        List<RandomScenarioAction> plan = Padding(6);
        plan.Add(new RandomScenarioAction(6, RandomScenarioActionKind.StarveDisk, "localhost:8001"));
        plan.AddRange(Padding(6));
        plan.Add(new RandomScenarioAction(13, RandomScenarioActionKind.AppendAtLeader));
        plan.AddRange(Padding(6));

        ShrinkResult result = await Shrink(
            plan,
            candidate =>
            {
                int starve = IndexOfKind(candidate, RandomScenarioActionKind.StarveDisk);
                int append = IndexOfKind(candidate, RandomScenarioActionKind.AppendAtLeader);

                return starve >= 0 && append > starve;
            },
            new ShrinkOptions { MaxCandidates = 200 });

        Assert.Equal(2, result.Shrunk.Count);
        Assert.Equal(RandomScenarioActionKind.StarveDisk, result.Shrunk[0].Kind);
        Assert.Equal(RandomScenarioActionKind.AppendAtLeader, result.Shrunk[1].Kind);
    }

    /// <summary>
    /// A candidate that fails for another reason is rejected.
    ///
    /// <para>The rule that decides whether a shrink means anything. Here every plan fails: the long
    /// one for the reason being chased, every shorter one for a different reason. A shrinker that
    /// only asked "did it throw" would strip this plan to nothing and report a minimal reproduction
    /// of a defect it was never called on.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_RefusesACandidateThatFailsForAnotherReason()
    {
        List<RandomScenarioAction> plan = Padding(8);

        PlanShrinker shrinker = new(
            (candidate, _) => Task.FromResult(
                new ShrinkAttempt(candidate.Count >= 8 ? Target : "invariant:something-else")),
            new ShrinkOptions { MaxCandidates = 200 });

        ShrinkResult result = await shrinker.ShrinkAsync(
            plan, Target, TestContext.Current.CancellationToken);

        Assert.Equal(plan.Count, result.Shrunk.Count);
        Assert.Equal(0, result.RemovalsAccepted);
    }

    /// <summary>
    /// A repair whose damage was cut is dropped with it.
    ///
    /// <para><c>RestartNode</c> is the one that matters: left behind on its own it starts a node
    /// nothing stopped, so the reduced plan would describe a run the reader could not reproduce. The
    /// oracle here accepts any plan at all, so the only thing under test is what normalisation
    /// leaves behind.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_NeverLeavesARepairWithoutItsFault()
    {
        List<RandomScenarioAction> plan =
        [
            new(0, RandomScenarioActionKind.CrashNode, "localhost:8003"),
            new(1, RandomScenarioActionKind.Idle),
            new(2, RandomScenarioActionKind.RestartNode, "localhost:8003"),
            new(3, RandomScenarioActionKind.AppendAtLeader),
        ];

        ShrinkResult result = await Shrink(
            plan,
            candidate => candidate.Any(action => action.Kind == RandomScenarioActionKind.AppendAtLeader),
            new ShrinkOptions { MaxCandidates = 200 });

        Assert.DoesNotContain(result.Shrunk, action => action.Kind == RandomScenarioActionKind.RestartNode);
        Assert.Single(result.Shrunk);
    }

    /// <summary>
    /// The budget is a hard stop, and what it stops on is still usable.
    ///
    /// <para>A shrink that ran out of runs must hand back a plan, not an exception. The flag is what
    /// tells the reader the plan is the best confirmed so far rather than the smallest there
    /// is.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_StopsOnItsBudgetAndSaysSo()
    {
        List<RandomScenarioAction> plan = Padding(40);

        ShrinkResult result = await Shrink(
            plan,
            _ => true,
            new ShrinkOptions { MaxCandidates = 3 });

        Assert.True(result.BudgetExhausted, "The shrink did not report its budget as spent.");
        Assert.Equal(3, result.CandidatesRun);
        Assert.True(result.Shrunk.Count > 0, "The shrink returned no plan at all.");
        Assert.True(result.Shrunk.Count < plan.Count, "The shrink returned the plan unchanged.");
    }

    /// <summary>
    /// A numeric parameter comes down once the removals are done. The failure here needs a duplicate
    /// link and nothing about how many copies, so the count must fall to the smallest value that
    /// still duplicates anything.
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_LowersANumericParameter()
    {
        List<RandomScenarioAction> plan =
        [
            new(0, RandomScenarioActionKind.DuplicateLink, "localhost:8001", "localhost:8002", 40),
        ];

        ShrinkResult result = await Shrink(
            plan,
            candidate => candidate.Any(action =>
                action.Kind == RandomScenarioActionKind.DuplicateLink && action.Value > 1),
            new ShrinkOptions { MaxCandidates = 200 });

        Assert.Single(result.Shrunk);
        Assert.Equal(2, result.Shrunk[0].Value);
        Assert.Equal(1, result.ParametersReduced);
    }

    /// <summary>
    /// The shrunk plan is still a plan: it survives the artifact round trip the failure report uses,
    /// with the same actions in the same order. A minimal reproduction nobody can replay is not a
    /// reproduction.
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrunkPlan_SurvivesItsArtifact()
    {
        List<RandomScenarioAction> plan = Padding(6);
        plan.Add(new RandomScenarioAction(6, RandomScenarioActionKind.FailWrites, "localhost:8002", null, 9));
        plan.AddRange(Padding(6));

        ShrinkResult result = await Shrink(
            plan,
            candidate => candidate.Any(action => action.Kind == RandomScenarioActionKind.FailWrites),
            new ShrinkOptions { MaxCandidates = 200 });

        // A subfolder of its own. A shrink artifact left at the top of dst-artifacts/ is collected
        // by the nightly job and read as a seed that failed, so a test's own file must not sit
        // where a real failure writes.
        string directory = Path.Combine(
            AppContext.BaseDirectory, "dst-artifacts", "shrink-round-trip");

        string path = result.WriteArtifact(directory, "shrink-round-trip");

        IReadOnlyList<RandomScenarioAction> parsed = RandomScenarioPlan.ParseFile(path);

        Assert.Equal(
            result.Shrunk.Select(action => action.Describe()),
            parsed.Select(action => action.Describe()));
    }

    /// <summary>
    /// A flaky reproduction is still a reproduction.
    ///
    /// <para>The oracle here fails on only every third run, which is what a real cluster does. At one
    /// attempt per candidate the shrinker would call most true reductions unnecessary and stop early;
    /// with three it must still reach the smallest plan.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_ToleratesAnOracleThatOnlySometimesReproduces()
    {
        List<RandomScenarioAction> plan = Padding(8);
        plan.Add(new RandomScenarioAction(8, RandomScenarioActionKind.PauseNode, "localhost:8003"));
        plan.AddRange(Padding(8));

        int call = 0;

        PlanShrinker shrinker = new(
            (candidate, _) =>
            {
                bool cause = candidate.Any(action => action.Kind == RandomScenarioActionKind.PauseNode);
                bool lucky = ++call % 3 == 0;

                return Task.FromResult(new ShrinkAttempt(
                    cause && lucky ? Target : FailureSignature.None));
            },
            new ShrinkOptions { MaxCandidates = 400, AttemptsPerCandidate = 3 });

        ShrinkResult result = await shrinker.ShrinkAsync(
            plan, Target, TestContext.Current.CancellationToken);

        Assert.Single(result.Shrunk);
        Assert.Equal(RandomScenarioActionKind.PauseNode, result.Shrunk[0].Kind);
    }

    /// <summary>
    /// The attempt count follows from the reproduction rate.
    ///
    /// <para>The arithmetic that removes the worst guess in a shrink. A plan that reproduces every
    /// time needs one attempt; one that reproduces rarely needs many; and a plan that never
    /// reproduces cannot be caught at all, so the ceiling is returned rather than a number that
    /// pretends otherwise.</para>
    /// </summary>
    [Theory]
    [Trait("Category", "DSTSmoke")]
    [InlineData(1.0, 1)]
    [InlineData(0.5, 4)]
    [InlineData(0.15, 15)]
    [InlineData(0.05, 40)]
    [InlineData(0.0, 40)]
    public void TheAttemptCount_FollowsTheReproductionRate(double rate, int expected)
    {
        Assert.Equal(expected, ReproductionRate.RequiredAttempts(rate));
    }

    /// <summary>
    /// A shrink can say what its own result is worth.
    ///
    /// <para>The number that stops a stalled shrink being read as a minimal plan. At six attempts
    /// against a fifteen per cent plan a candidate is caught around three times in five, so a
    /// search that removed nothing has probably rejected valid cuts.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void TheCatchProbability_SaysWhatAShrinkIsWorth()
    {
        Assert.InRange(ReproductionRate.CatchProbability(0.15, 6), 0.55, 0.65);
        Assert.InRange(ReproductionRate.CatchProbability(0.15, 15), 0.85, 0.95);

        Assert.Equal(1, ReproductionRate.CatchProbability(1.0, 1));
        Assert.Equal(0, ReproductionRate.CatchProbability(0.0, 100));
    }

    /// <summary>A shrink of a passing run is a caller error, not an empty result.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShrink_RefusesAPassingSignature()
    {
        PlanShrinker shrinker = new((_, _) => Task.FromResult(new ShrinkAttempt(FailureSignature.None)));

        await Assert.ThrowsAsync<ArgumentException>(() => shrinker.ShrinkAsync(
            Padding(3), FailureSignature.None, TestContext.Current.CancellationToken));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static Task<ShrinkResult> Shrink(
        IReadOnlyList<RandomScenarioAction> plan,
        Func<IReadOnlyList<RandomScenarioAction>, bool> fails,
        ShrinkOptions options)
    {
        PlanShrinker shrinker = new(
            (candidate, _) => Task.FromResult(
                new ShrinkAttempt(fails(candidate) ? Target : FailureSignature.None)),
            options);

        return shrinker.ShrinkAsync(plan, Target, TestContext.Current.CancellationToken);
    }

    private static List<RandomScenarioAction> Padding(int count) =>
        [.. Enumerable.Range(0, count).Select(index =>
            new RandomScenarioAction(index, RandomScenarioActionKind.Idle))];

    private static int IndexOfKind(
        IReadOnlyList<RandomScenarioAction> plan,
        RandomScenarioActionKind kind)
    {
        for (int index = 0; index < plan.Count; index++)
        {
            if (plan[index].Kind == kind)
                return index;
        }

        return -1;
    }
}
