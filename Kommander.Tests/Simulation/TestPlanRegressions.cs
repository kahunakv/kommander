using System.Globalization;
using System.Text;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Diagnostics;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;
using Kommander.Tests.Simulation.Shrinking;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Replays the failures somebody promoted.
///
/// <para><b>What promotion is for.</b> The random search finds a state once. Whether it finds it
/// again depends on the seed corpus, on the bounds, and on a cluster that does not repeat itself —
/// so a defect found on a Tuesday can be invisible on the Wednesday after somebody fixes something
/// else. A promoted plan does not move. It fails until the defect is fixed, and it keeps running
/// afterwards.</para>
///
/// <para><b>How to promote one.</b> A failing run writes <c>dst-artifacts/random-seed-N.plan.txt</c>
/// and, when the shrinker ran, a shorter file beside it. Copy either into
/// <c>Kommander.Tests/Simulation/Scenarios/Random/regressions/</c>. The shrunk one is worth
/// preferring: it says the same thing in fewer actions, and it carries the same header.</para>
///
/// <para><b>Why one test and not a theory row per plan.</b> A theory over an empty corpus is a test
/// run that fails for having no data, and the corpus is empty until somebody promotes something.
/// The loop reports every plan that failed rather than only the first, which is what a row per plan
/// would have given.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestPlanRegressions
{
    /// <summary>
    /// Replays of each promoted plan. A plan is not deterministic, so one green replay is weaker
    /// evidence than it looks; a nightly job can raise this.
    /// </summary>
    public const string RepeatsVariable = "KOMMANDER_DST_REPLAY_REPEATS";

    /// <summary>Plan file the shrink probe reads. The probe is skipped when it is unset.</summary>
    public const string ShrinkPlanVariable = "KOMMANDER_DST_SHRINK_PLAN";

    /// <summary>Runs of one candidate before the probe calls it a pass.</summary>
    public const string ShrinkAttemptsVariable = "KOMMANDER_DST_SHRINK_ATTEMPTS";

    /// <summary>Cluster runs the probe may spend in total.</summary>
    public const string ShrinkBudgetVariable = "KOMMANDER_DST_SHRINK_BUDGET";

    /// <summary>
    /// Runs of the whole plan before the shrink starts, used to measure how often it reproduces.
    ///
    /// <para>Twenty by default, and ten was measurably too few. The plan this feature was built for
    /// reproduces about fifteen per cent of the time, and ten runs miss such a plan entirely one
    /// time in five — which is exactly what happened on the first attempt. Twenty misses it about
    /// one time in twenty-five.</para>
    ///
    /// <para>No default is right for every rate, so the failure message says to raise this. The
    /// measurement is what sets the attempt count, and it is worth its own runs: a plan that
    /// reproduces three times in twenty needs about fifteen attempts per candidate, and nobody
    /// guesses that.</para>
    /// </summary>
    public const string ShrinkProbeRunsVariable = "KOMMANDER_DST_SHRINK_PROBE_RUNS";

    /// <summary>Substring of the library trace the diagnostic probe keeps.</summary>
    public const string TraceVariable = "KOMMANDER_DST_TRACE";

    private readonly ILogger<IRaft> logger;
    private readonly ITestOutputHelper output;

    public TestPlanRegressions(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
        output = outputHelper;
    }

    /// <summary>
    /// Every promoted plan holds every check, on every replay.
    ///
    /// <para>An empty corpus passes and says so. That is the honest reading: nothing has been
    /// promoted, which is a fact about the corpus rather than a fact about the library.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTRandom")]
    public async Task EveryPromotedPlan_StillHoldsEveryCheck()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        IReadOnlyList<RegressionPlan> plans = RegressionPlanCorpus.Load();

        if (plans.Count == 0)
            return;

        output.WriteLine(
            $"Replaying {plans.Count} plan(s) from {RegressionPlanCorpus.ConfiguredDirectory()}.");

        int repeats = ConfiguredRepeats();
        StringBuilder failures = new();

        foreach (RegressionPlan plan in plans)
        {
            int failed = 0;
            string? first = null;

            for (int attempt = 1; attempt <= repeats; attempt++)
            {
                string? failure = await ReplayAsync(plan, cancellationToken);

                if (failure is null)
                    continue;

                failed++;
                first ??= failure;
            }

            if (failed == 0)
                continue;

            // The rate, not the fact. Every failure this search finds is intermittent, and one in
            // twenty and twenty in twenty are different findings that need different next steps:
            // the first is a rare state to hunt, the second is a defect to fix. A message that
            // said only "it failed" would hide which one this is.
            failures.AppendLine(
                $"{plan.Name} failed {failed} of {repeats} replays. First failure: {first}");
        }

        Assert.True(failures.Length == 0, failures.ToString());
    }

    /// <summary>
    /// The loader reads back what a failing run writes.
    ///
    /// <para>The check the whole corpus rests on, and it needs no cluster. A plan is written in the
    /// artifact format, read back as a promoted plan, and compared on its actions, its seed and its
    /// bounds. If the round trip lost the bounds, every promoted regression would silently replay
    /// under the defaults — a test of something nobody recorded, passing quietly.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void APromotedPlan_KeepsItsActionsAndItsBounds()
    {
        RandomScenarioOptions options = new()
        {
            ActionCount = 9,
            StepsPerAction = 4,
            RecoveryStepBudget = 411,
            MaintenanceWeight = 7,
            CompactEveryOperations = 8,
            EnableFaultEpisodes = false,
        };

        RandomScenarioReport report = new()
        {
            Seed = 20260908,
            Options = options,
            Actions =
            [
                new(0, RandomScenarioActionKind.AppendAtLeader, "localhost:8001"),
                new(1, RandomScenarioActionKind.BlockLink, "localhost:8001", "localhost:8003"),
                new(2, RandomScenarioActionKind.DuplicateLink, "localhost:8002", "localhost:8003", 4),
                new(3, RandomScenarioActionKind.UnblockLink, "localhost:8001", "localhost:8003"),
            ],
            StepsRun = 40,
            History = new History.ClientHistory(),
            FinalCommitIndex = 3,
            InvariantChecks = 40,
        };

        string directory = Path.Combine(AppContext.BaseDirectory, "dst-artifacts", "promotion-check");
        string path = report.WriteArtifact(directory, "round-trip");

        RegressionPlan promoted = RegressionPlanCorpus.Read(path);

        Assert.Equal(report.Seed, promoted.Seed);

        Assert.Equal(
            report.Actions.Select(action => action.Describe()),
            promoted.Actions.Select(action => action.Describe()));

        // The bounds, one by one. A single equality on the record would pass while every value was
        // wrong together, and these are exactly the values a replay depends on.
        Assert.Equal(options.ActionCount, promoted.Options.ActionCount);
        Assert.Equal(options.StepsPerAction, promoted.Options.StepsPerAction);
        Assert.Equal(options.RecoveryStepBudget, promoted.Options.RecoveryStepBudget);
        Assert.Equal(options.MaintenanceWeight, promoted.Options.MaintenanceWeight);
        Assert.Equal(options.CompactEveryOperations, promoted.Options.CompactEveryOperations);
        Assert.Equal(options.EnableFaultEpisodes, promoted.Options.EnableFaultEpisodes);
    }

    /// <summary>
    /// A plan written before a knob existed still loads, at that knob's default.
    ///
    /// <para>The rule that keeps an old regression usable. Every promoted plan would become
    /// unreadable the day somebody adds an option, which is the opposite of what a corpus is for. A
    /// missing key is age; an unreadable value is corruption, and only the second is an error.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void APlanMissingANewerBound_LoadsAtItsDefault()
    {
        string directory = Path.Combine(AppContext.BaseDirectory, "dst-artifacts", "promotion-check");
        Directory.CreateDirectory(directory);

        string path = Path.Combine(directory, "older-format.plan.txt");

        File.WriteAllText(path, string.Join(
            Environment.NewLine,
            "seed=4242",
            "actionCount=5",
            "stepsPerAction=3",
            string.Empty,
            "000 Idle",
            "001 AppendAtLeader localhost:8001",
            string.Empty));

        RegressionPlan promoted = RegressionPlanCorpus.Read(path);

        Assert.Equal(4242UL, promoted.Seed);
        Assert.Equal(5, promoted.Options.ActionCount);
        Assert.Equal(new RandomScenarioOptions().MaintenanceWeight, promoted.Options.MaintenanceWeight);

        File.WriteAllText(path, string.Join(
            Environment.NewLine,
            "seed=4242",
            "actionCount=not-a-number",
            string.Empty,
            "000 Idle",
            string.Empty));

        Assert.Throws<FormatException>(() => RegressionPlanCorpus.Read(path));
    }

    /// <summary>
    /// A generated failure can be promoted, and the promoted file replays.
    ///
    /// <para>The acceptance for the whole idea, performed rather than described. A run is generated,
    /// its plan is written exactly as a failing run writes one, the file is copied into a corpus
    /// folder, loaded back through <see cref="RegressionPlanCorpus"/>, and replayed against a fresh
    /// cluster. Every step of the promotion a person would perform by hand happens here.</para>
    ///
    /// <para><b>Why a passing run is promoted rather than a failing one.</b> The permanent corpus
    /// must stay green, and the failures the search finds today are intermittent — promoting one
    /// would put a coin toss in the standing test set. What is under test here is the pipeline: that
    /// a plan written by a run survives the file, the loader and the replay. Whether a promoted plan
    /// still fails is a question about the library, and it is the question the corpus asks once
    /// somebody puts a real failure in it.</para>
    ///
    /// <para>The corpus folder used here is a temporary one, so this test never adds to the
    /// permanent set.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTRandom")]
    public async Task AGeneratedRun_CanBePromotedAndReplayed()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        const ulong seed = 20260911;
        RandomScenarioOptions options = new() { ActionCount = 8, StepsPerAction = 4 };

        RandomScenarioReport drawn;

        await using (SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = seed },
            logger,
            cancellationToken))
        {
            RandomScenarioRunner runner = new(cluster, options, new SimulationRandom(seed));
            drawn = await runner.RunAsync(cancellationToken);
        }

        // Promotion, in full: the artifact a failing run leaves behind, moved into a corpus folder.
        string artifacts = Path.Combine(AppContext.BaseDirectory, "dst-artifacts");
        string source = drawn.WriteArtifact(artifacts, "promotion-demo");

        string corpus = Path.Combine(artifacts, "promoted-corpus");
        Directory.CreateDirectory(corpus);

        string promoted = Path.Combine(corpus, Path.GetFileName(source));
        File.Copy(source, promoted, overwrite: true);

        IReadOnlyList<RegressionPlan> plans = RegressionPlanCorpus.Load(corpus);

        RegressionPlan plan = Assert.Single(plans);

        Assert.Equal(seed, plan.Seed);
        Assert.Equal(options.StepsPerAction, plan.Options.StepsPerAction);

        Assert.Equal(
            drawn.Actions.Select(action => action.Describe()),
            plan.Actions.Select(action => action.Describe()));

        string? failure = await ReplayAsync(plan, cancellationToken);

        Assert.True(failure is null, $"The promoted plan did not replay: {failure}");
    }

    /// <summary>
    /// Reduces one named plan to the actions its failure needs.
    ///
    /// <para><b>What this is for.</b> A lead arrives as a plan of twenty-five actions of which two
    /// or three matter. Answering "which ones" by hand costs an afternoon of re-runs; this runs the
    /// same re-runs. It is the step between "the state is reachable" and "here is the defect".</para>
    ///
    /// <para><b>Why it is driven by the environment and skipped otherwise.</b> A shrink costs one
    /// cluster run per attempt, and the useful settings depend entirely on the lead: a plan that
    /// reproduces three times in ten needs several attempts per candidate, and one that reproduces
    /// every time needs one. Those are decisions for the person holding the lead, not constants for
    /// a test set to carry.</para>
    ///
    /// <para><b>The reproduction rate is the setting that matters, so the probe measures it rather
    /// than asking.</b> It runs the whole plan ten times first, counts the failures, and derives an
    /// attempt count that catches such a plan nine times in ten. At one attempt a plan that fails
    /// three times in ten reads as passing on most candidates, and the shrinker then keeps almost
    /// every action and reports a reduction of nothing — which is indistinguishable from a plan
    /// whose every action matters. <see cref="ShrinkAttemptsVariable"/> overrides the derived
    /// count.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTProbe")]
    public async Task AConfiguredPlan_ShrinksToItsCause()
    {
        string? path = Environment.GetEnvironmentVariable(ShrinkPlanVariable);

        if (string.IsNullOrWhiteSpace(path))
        {
            Assert.Skip(
                $"Set {ShrinkPlanVariable} to a plan file to shrink it. " +
                $"Optional: {ShrinkAttemptsVariable} (default 3), {ShrinkBudgetVariable} (default 180).");
        }

        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        RegressionPlan plan = RegressionPlanCorpus.Read(path);

        output.WriteLine($"Shrinking {plan.Name}: {plan.Actions.Count} actions, seed {plan.Seed}.");

        int budget = Setting(ShrinkBudgetVariable, 180);
        int probeRuns = Setting(ShrinkProbeRunsVariable, 20);

        PlanOracle oracle = new ClusterPlanOracle(plan.Seed, plan.Options, logger).AsOracle();

        // The whole plan is run several times before anything is removed, and both things that
        // come out of it matter.
        //
        // The signature must come from a real run rather than a guess: a wrongly guessed signature
        // makes every candidate a rejection, and the shrink then reports that nothing could be
        // removed — which reads exactly like a plan whose every action matters.
        //
        // The reproduction rate decides the attempt count, and it is the setting a person is worst
        // placed to guess. Measuring it here costs ten runs and removes the guess.
        ShrinkAttempt? original = null;
        int reproduced = 0;

        for (int run = 0; run < probeRuns; run++)
        {
            ShrinkAttempt candidate = await oracle(plan.Actions, cancellationToken);

            if (candidate.Passed)
                continue;

            reproduced++;
            original ??= candidate;
        }

        if (original is null)
        {
            Assert.Fail(
                $"{plan.Name} held every check over {probeRuns} run(s), so there was no failure to " +
                $"shrink. Raise {ShrinkProbeRunsVariable} if the plan reproduces more rarely than that.");
        }

        double rate = (double)reproduced / probeRuns;
        int attempts = Setting(ShrinkAttemptsVariable, ReproductionRate.RequiredAttempts(rate));

        output.WriteLine($"Signature: {original.Signature}");

        output.WriteLine(
            $"Reproduced {reproduced} of {probeRuns} runs (rate {rate:P0}). Using {attempts} " +
            $"attempts per candidate, which catches such a plan " +
            $"{ReproductionRate.CatchProbability(rate, attempts):P0} of the time.");

        string directory = Path.GetDirectoryName(path) ?? AppContext.BaseDirectory;
        string name = Path.GetFileNameWithoutExtension(plan.Name);

        PlanShrinker shrinker = new(
            new ClusterPlanOracle(plan.Seed, plan.Options, logger).AsOracle(),
            new ShrinkOptions
            {
                MaxCandidates = budget,
                AttemptsPerCandidate = attempts,

                // Each confirmed reduction is written straight away. A shrink of an intermittent
                // plan runs for tens of minutes, and an interrupted run would otherwise lose
                // everything it learned — which has already happened once here.
                OnProgress = reduced => WriteProgress(directory, name, plan, reduced),
            });

        ShrinkResult result = await shrinker.ShrinkAsync(
            plan.Actions, original.Signature, cancellationToken);

        // The seed and the bounds only. The original run's step count and measurements would read
        // as this reduced plan's numbers, and a plan of three actions has nothing to do with the
        // four hundred steps the plan it came from took.
        result = result with { Header = PlanHeader.For(plan.Seed, plan.Options) };

        string written = result.WriteArtifact(directory, name);

        output.WriteLine($"Wrote {written}");
        output.WriteLine(result.Describe());

        Assert.True(
            result.Shrunk.Count < result.Original.Count,
            $"The shrink removed nothing over {result.CandidatesRun} runs. At a measured rate of " +
            $"{rate:P0} and {attempts} attempts a candidate is caught " +
            $"{ReproductionRate.CatchProbability(rate, attempts):P0} of the time, so this is only " +
            "evidence that every action is needed if that figure is high.");
    }

    /// <summary>
    /// Replays a named plan until it fails, and reports the library trace that explains it.
    ///
    /// <para><b>What this is for.</b> A reduced plan says which actions reach a state; it does not
    /// say why the state is wrong. The answer is usually already written down — the library logs its
    /// own decisions — but at <c>Debug</c>, under one category, alongside every other trace. This
    /// captures one trace by substring and prints what it said on the run that failed.</para>
    ///
    /// <para><b>Why the trace is printed rather than asserted on.</b> A log message is not a
    /// contract. This reports; it never decides. A rule worth keeping is read from a view or a
    /// snapshot and asserted there.</para>
    ///
    /// <para>Set <see cref="TraceVariable"/> to the substring to keep — for a repair that never
    /// happens, <c>backfill-decision</c> is the one that says what the leader decided and on what
    /// evidence.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTProbe")]
    public async Task AConfiguredPlan_ReportsTheTraceThatExplainsIt()
    {
        string? path = Environment.GetEnvironmentVariable(ShrinkPlanVariable);
        string? substring = Environment.GetEnvironmentVariable(TraceVariable);

        if (string.IsNullOrWhiteSpace(path) || string.IsNullOrWhiteSpace(substring))
        {
            Assert.Skip(
                $"Set {ShrinkPlanVariable} to a plan file and {TraceVariable} to the trace substring " +
                $"to capture. Optional: {ShrinkProbeRunsVariable} (default 20).");
        }

        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        RegressionPlan plan = RegressionPlanCorpus.Read(path);
        int runs = Setting(ShrinkProbeRunsVariable, 20);

        SubstringFileLogger capture = new(substring);

        using ILoggerFactory factory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(capture);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        ILogger<IRaft> tracing = factory.CreateLogger<IRaft>();

        for (int run = 1; run <= runs; run++)
        {
            capture.Clear();

            string? failure = await ReplayAsync(plan, cancellationToken, tracing);

            if (failure is null)
                continue;

            output.WriteLine($"Failed on run {run} of {runs}: {failure}");
            output.WriteLine($"Captured {capture.Lines.Count} line(s) matching '{substring}':");

            // The tail, not the whole capture. These traces are per-heartbeat and the run makes
            // hundreds; what matters is what the leader was deciding once the writes stopped, which
            // is the end of the list.
            foreach (string line in capture.Lines.TakeLast(40))
                output.WriteLine(line);

            Assert.Fail(
                $"Reported the trace for a failure on run {run} of {runs}. This probe always fails " +
                "when it reproduces — the output above is the result.");
        }

        Assert.Fail(
            $"{plan.Name} held every check over {runs} run(s), so there was no failure to explain. " +
            $"Raise {ShrinkProbeRunsVariable}.");
    }

    /// <summary>A folder nobody has promoted into is empty, not broken.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void AnEmptyCorpus_LoadsAsEmpty()
    {
        string directory = Path.Combine(
            AppContext.BaseDirectory, "dst-artifacts", "empty-corpus-check");

        Directory.CreateDirectory(directory);

        Assert.Empty(RegressionPlanCorpus.Load(directory));
        Assert.Empty(RegressionPlanCorpus.Load(Path.Combine(directory, "no-such-folder")));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Replays one plan and returns the failure text, or null when it held.
    ///
    /// <para>The failure is returned rather than thrown so the loop can report every plan. A
    /// regression run that stopped at the first failure would hide the rest, and the reader's next
    /// question is always how many.</para>
    /// </summary>
    private async Task<string?> ReplayAsync(
        RegressionPlan plan,
        CancellationToken cancellationToken,
        ILogger<IRaft>? replayLogger = null)
    {
        try
        {
            await using SimulationCluster cluster = await SimulationCluster.StartAsync(
                new SimulationClusterOptions
                {
                    NodeCount = 3,
                    PartitionCount = 1,
                    Seed = plan.Seed,
                    ConfigureNode = configuration =>
                        configuration.CompactEveryOperations = plan.Options.CompactEveryOperations,
                },
                replayLogger ?? logger,
                cancellationToken);

            RandomScenarioRunner runner = new(cluster, plan.Options, new SimulationRandom(plan.Seed));

            await runner.ReplayAsync(plan.Actions, cancellationToken);

            return null;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error)
        {
            return error.Message;
        }
    }

    /// <summary>
    /// Writes the best plan found so far, so an interrupted shrink leaves something behind.
    ///
    /// <para>A separate file from the finished one. A reader must be able to tell a shrink that
    /// completed from a shrink that was killed halfway: the first is a claim about what the failure
    /// needs, the second is only the furthest the search happened to get.</para>
    /// </summary>
    private static void WriteProgress(
        string directory,
        string name,
        RegressionPlan plan,
        IReadOnlyList<RandomScenarioAction> reduced)
    {
        ShrinkResult partial = new()
        {
            Original = plan.Actions,
            Shrunk = reduced,
            Signature = "in-progress",
            CandidatesRun = 0,
            RemovalsAccepted = 0,
            ParametersReduced = 0,
            BudgetExhausted = false,
            Header = PlanHeader.For(plan.Seed, plan.Options),
        };

        partial.WriteArtifact(directory, $"{name}.partial");
    }

    private static int Setting(string variable, int fallback)
    {
        string? text = Environment.GetEnvironmentVariable(variable);

        return int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int value)
               && value > 0
            ? value
            : fallback;
    }

    private static int ConfiguredRepeats()
    {
        string? text = Environment.GetEnvironmentVariable(RepeatsVariable);

        return int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int repeats)
               && repeats > 0
            ? repeats
            : 1;
    }
}
