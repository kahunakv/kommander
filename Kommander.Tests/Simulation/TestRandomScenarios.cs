using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Diagnostics;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;
using Kommander.Tests.Simulation.Shrinking;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Generated runs against a real three-node cluster.
///
/// <para><b>Why random runs at all, when the scripted scenarios already find defects.</b> Both
/// findings so far came from states next to a fault, which says that region is productive. A
/// scripted scenario can only visit the part of it somebody thought to write down, and each one
/// costs a person an afternoon. A generated plan costs a seed.</para>
///
/// <para><b>What a passing run means.</b> Every step is checked against the per-step invariants;
/// the end of the run adds the convergence check and the client-history check. A plan passes only
/// if all three agree, so a pass is a statement about node state, about where the run ended, and
/// about every promise made to a client along the way.</para>
///
/// <para><b>What a failing run gives back.</b> Two files beside the test binary: the plan in the
/// order it happened, and the entropy it consumed in the same replay format the model-layer runs
/// use. The plan is the replay unit — <see cref="RandomScenarioRunner.ReplayAsync"/> applies it
/// again, action for action.</para>
///
/// <para><b>What the seed does and does not promise.</b> It fixes every draw the generator makes.
/// It does not fix the run, because a draw depends on what the generator observed and these
/// clusters run on their own threads: two runs of one seed can observe different leaders and
/// diverge from there. Exact reproduction needs the driven scheduling mode, which costs minutes per
/// run rather than seconds. Stated plainly because the opposite is easy to assume: a failing seed
/// is a strong lead, not a guaranteed repeat, and the plan beside it is what makes the failure
/// re-runnable.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRandomScenarios
{
    private readonly ILogger<IRaft> logger;
    private readonly ITestOutputHelper output;

    public TestRandomScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
        output = outputHelper;
    }

    /// <summary>
    /// One short generated run, on every pull request.
    ///
    /// <para>Its job is not to search. It is to prove the machinery still works — that a plan is
    /// drawn, applied, healed and checked — so that the nightly search fails for reasons about
    /// Raft rather than reasons about the harness.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShortGeneratedRun_HoldsEveryCheck()
    {
        RandomScenarioOptions options = new()
        {
            ActionCount = 12,
            StepsPerAction = 5,
        };

        RandomScenarioReport report = await RunSeedAsync(
            20260901, options, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");
        Assert.Equal(options.ActionCount, report.Actions.Count(action => action.Index < options.ActionCount));

        // The recovery write is unconditional, so a run always ends with at least one entry in the
        // log. A run that acknowledged nothing would have checked a history of nothing.
        Assert.True(report.History.Count > 0, "No client operation was issued.");
        Assert.True(report.FinalCommitIndex >= 0, "The cluster committed nothing at all.");

        // Printed on every run, passing or not. What a search costs is the number that settles an
        // argument about whether an unusual run was the change or the machine, and it is worth
        // nothing if it only appears once something has already gone wrong.
        Assert.NotNull(report.Metrics);
        output.WriteLine(report.Metrics.Describe());

        Assert.True(report.Metrics.StepsPerSecond > 0, "The run reported no throughput.");

        Assert.InRange(report.Metrics.InvariantShare, 0, 1);

        // The checker reads every node's view and every node's store on every settled state, so a
        // run that reported no time at all in it has a timer around the wrong code.
        Assert.True(
            report.Metrics.InvariantTime > TimeSpan.Zero,
            "The run reported no time spent checking invariants, which cannot be true.");
    }

    /// <summary>
    /// A run that checkpoints really compacts.
    ///
    /// <para>Two invariants describe compaction: one says compaction is never asked to remove an
    /// entry above the certified checkpoint, the other tolerates a compacted head when it looks for
    /// a hole. Until a generated run actually compacted, both described something that never
    /// happened, and a rule nothing exercises is decoration. This run weights the draw towards
    /// client writes and checkpoints and then insists that entries really left the log.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task ARunThatCheckpoints_ReallyCompacts()
    {
        RandomScenarioOptions options = new()
        {
            ActionCount = 16,
            StepsPerAction = 5,
            ClientWeight = 30,
            MaintenanceWeight = 30,
            IdleWeight = 0,
            OutageWeight = 0,
            NetworkFaultWeight = 0,
            StorageFaultWeight = 0,
            LifecycleFaultWeight = 0,
            HealWeight = 0,

            // Low enough that a run of this length really compacts. The sweeping runs leave it at
            // the production default; see RandomScenarioOptions.CompactEveryOperations for why.
            CompactEveryOperations = 8,
        };

        RandomScenarioReport report = await RunSeedAsync(
            20260907, options, TestContext.Current.CancellationToken);

        Assert.True(
            report.CountOf(RandomScenarioActionKind.Checkpoint) > 0,
            "No checkpoint was drawn, so nothing could compact.");

        Assert.True(
            report.EntriesCompacted > 0,
            $"The run wrote checkpoints and compacted nothing ({report.EntriesCompacted} entries). " +
            "The compaction rules are describing something that does not happen.");
    }

    /// <summary>
    /// The nightly search. The corpus seeds always run; the sweep beside them moves with
    /// <see cref="RandomSeedCorpus.SeedBaseVariable"/>, and its length with
    /// <see cref="RandomSeedCorpus.SeedCountVariable"/>.
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRun_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, new RandomScenarioOptions(), TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");
    }

    /// <summary>
    /// A generated plan replays from the artifact a failure would leave behind.
    ///
    /// <para>The check the whole failure report depends on. A plan is written, read back, and
    /// applied to a fresh cluster; if the file could not carry the run, every failing plan the
    /// nightly search produces would be unusable, and nobody would find that out until the night it
    /// mattered.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTRandom")]
    public async Task AGeneratedPlan_ReplaysFromItsArtifact()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        RandomScenarioOptions options = new() { ActionCount = 10, StepsPerAction = 5 };

        RandomScenarioReport drawn = await RunSeedAsync(20260902, options, cancellationToken);

        string directory = Path.Combine(AppContext.BaseDirectory, "dst-artifacts");
        string path = drawn.WriteArtifact(directory, "replay-check");

        IReadOnlyList<RandomScenarioAction> plan = RandomScenarioPlan.ParseFile(path);

        Assert.Equal(drawn.Actions.Count, plan.Count);

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 20260902 },
            logger,
            cancellationToken);

        RandomScenarioRunner runner = new(cluster, options, new SimulationRandom(20260902));

        RandomScenarioReport replayed = await runner.ReplayAsync(plan, cancellationToken);

        // The plan is applied action for action. What the cluster does between the actions is its
        // own business, so the two runs are compared on the plan rather than on the outcome.
        Assert.Equal(
            plan.Select(action => action.Describe()),
            replayed.Actions.Select(action => action.Describe()));

        Assert.True(replayed.InvariantChecks > 0, "The replay checked no invariants.");
    }

    /// <summary>
    /// A failing run shrinks its own plan when the environment asks for it.
    ///
    /// <para><b>What this protects.</b> The nightly job turns the shrinker on with an environment
    /// variable and collects the file it leaves behind. Nothing else exercises that path, so a
    /// mistake in it — a variable read wrongly, an artifact written to the wrong folder, a shrink
    /// that throws over the finding — would stay invisible until the night a real seed failed, which
    /// is the worst possible moment to learn about it.</para>
    ///
    /// <para>The failure is forced by giving the run a recovery budget of one step, so the
    /// convergence check cannot pass. Every candidate the shrinker tries fails the same way, which
    /// is fine: what is under test is the wiring, not the reduction.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTRandom")]
    public async Task AFailingRun_ShrinksItsPlanWhenAsked()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        string directory = Path.Combine(
            AppContext.BaseDirectory, "dst-artifacts", "shrink-wiring-check");

        if (Directory.Exists(directory))
            Directory.Delete(directory, recursive: true);

        string? enabled = Environment.GetEnvironmentVariable(ShrinkPolicy.EnabledVariable);
        string? budget = Environment.GetEnvironmentVariable(ShrinkPolicy.BudgetVariable);

        try
        {
            Environment.SetEnvironmentVariable(ShrinkPolicy.EnabledVariable, "1");
            Environment.SetEnvironmentVariable(ShrinkPolicy.BudgetVariable, "3");

            RandomScenarioOptions options = new()
            {
                ActionCount = 4,
                StepsPerAction = 3,

                // Zero steps to converge, so the convergence check reports a failure without ever
                // looking. One step does not work: the check runs after each step and a healthy
                // cluster passes on the first, so the run would succeed and this test would prove
                // nothing. Zero is the only budget that fails whatever the cluster does, which is
                // what a test of the reporting path needs.
                RecoveryStepBudget = 0,
                ArtifactDirectory = directory,
            };

            InvalidOperationException error =
                await Assert.ThrowsAsync<InvalidOperationException>(
                    () => RunSeedAsync(20260912, options, cancellationToken));

            Assert.Contains("Shrunk:", error.Message, StringComparison.Ordinal);

            string[] shrunk = Directory.GetFiles(directory, "*.shrunk.plan.txt");

            Assert.NotEmpty(shrunk);

            // The file the nightly collects has to carry the bounds, or a reader who promotes it
            // gets a plan that replays under the defaults instead of under the run that failed.
            IReadOnlyDictionary<string, string> header =
                RandomScenarioPlan.ParseHeaderFile(shrunk[0]);

            Assert.Equal("20260912", header["seed"]);
            Assert.Equal("0", header["recoveryStepBudget"]);
            Assert.True(header.ContainsKey("signature"), "The shrunk plan does not name the failure.");
        }
        finally
        {
            Environment.SetEnvironmentVariable(ShrinkPolicy.EnabledVariable, enabled);
            Environment.SetEnvironmentVariable(ShrinkPolicy.BudgetVariable, budget);

            // The file this test writes is named exactly as a failing seed's file is, and the
            // nightly job collects those and reports them as failures. A test that left one behind
            // would make every green night look like a bad one.
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>
    /// Seeds this run will explore: the corpus first, then the sweep. Duplicates are dropped so a
    /// sweep that lands on a corpus seed does not produce two tests with one name.
    /// </summary>
    public static TheoryData<ulong> Seeds()
    {
        TheoryData<ulong> data = new();
        HashSet<ulong> seen = [];

        foreach (ulong seed in RandomSeedCorpus.Load())
        {
            if (seen.Add(seed))
                data.Add(seed);
        }

        IReadOnlyList<ulong> sweep = RandomSeedCorpus.Sweep(
            RandomSeedCorpus.ConfiguredSeedBase(), RandomSeedCorpus.ConfiguredSeedCount());

        foreach (ulong seed in sweep)
        {
            if (seen.Add(seed))
                data.Add(seed);
        }

        return data;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Fails a run that broke its cost budget, when a budget is in force.
    ///
    /// <para><b>Why a passing run is checked at all.</b> A run that stops making progress still
    /// passes: every check holds, it simply takes an order of magnitude longer, and nothing reports
    /// that. The loss is silent, which is the only kind worth building a detector for.</para>
    ///
    /// <para><b>Why it is off unless the environment asks.</b> A developer's machine is not a
    /// controlled environment. A suite that failed because a build was running beside it would teach
    /// people to ignore the failure, and the metrics would then be worth less than nothing. The
    /// continuous-integration jobs set the variable.</para>
    /// </summary>
    private static void RequireWithinBudget(ulong seed, RandomScenarioReport report)
    {
        if (report.Metrics is null)
            return;

        IReadOnlyList<string> breaches = SimulationBudgetPolicy.Current().Breaches(report.Metrics);

        Assert.True(
            breaches.Count == 0,
            $"Seed {seed} ran outside its budget.{Environment.NewLine}" +
            $"{string.Join(Environment.NewLine, breaches)}{Environment.NewLine}" +
            $"Measured: {report.Metrics.Describe()}");
    }


    /// <summary>
    /// Runs one seed and turns any failure into a report a reader can act on.
    ///
    /// <para>The catch is the point of this method. A generated failure with no plan beside it is
    /// a number and a stack trace, and the reader's first question — what did the run do? — has no
    /// answer. The plan and the replay log are written before the failure is rethrown, and the seed
    /// is named in the message so the run can be repeated on the spot.</para>
    /// </summary>
    private async Task<RandomScenarioReport> RunSeedAsync(
        ulong seed,
        RandomScenarioOptions options,
        CancellationToken cancellationToken)
    {
        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions
            {
                NodeCount = 3,
                PartitionCount = 1,
                Seed = seed,

                // A generated run is a few dozen entries long. At the production compaction cadence
                // no run would ever compact, and every rule about compaction would go unexercised.
                ConfigureNode = configuration =>
                    configuration.CompactEveryOperations = options.CompactEveryOperations,
            },
            logger,
            cancellationToken);

        SimulationRandom random = new(seed);
        RandomScenarioRunner runner = new(cluster, options, random);

        string directory = options.ArtifactDirectory
                           ?? Path.Combine(AppContext.BaseDirectory, "dst-artifacts");

        RandomScenarioReport partial;
        Exception failure;
        string planPath;
        string replayPath;

        try
        {
            RandomScenarioReport report = await runner.RunAsync(cancellationToken);

            RequireWithinBudget(seed, report);

            return report;
        }
        catch (Exception error)
        {
            partial = runner.Partial();

            planPath = partial.WriteArtifact(directory, "random");
            replayPath = runner.WriteReplayLog(directory, "random");
            failure = error;
        }

        // The failing cluster is disposed before a shrink begins. A shrink starts a fresh cluster
        // for every candidate, and this one is still holding its endpoints, its threads and
        // whatever fault the failure left behind.
        await cluster.DisposeAsync();

        string shrink = await ShrinkIfAskedAsync(
            seed, options, partial, failure, directory, cancellationToken);

        throw new InvalidOperationException(
            $"Random run failed on seed {seed}. Re-run this seed to reproduce it exactly." +
            $"{Environment.NewLine}Plan: {planPath}" +
            $"{Environment.NewLine}Replay: {replayPath}" +
            shrink +
            $"{Environment.NewLine}{partial.Describe()}",
            failure);
    }

    /// <summary>
    /// Reduces the failing plan to the actions the failure needs, when the environment asks for it.
    ///
    /// <para><b>Why this is opt-in.</b> A shrink costs one cluster run per candidate, tens of them
    /// for one failure. The nightly search is where a failure is the point and where a reader will
    /// read the answer in the morning; a pull request wants the failure reported and nothing more.
    /// See <see cref="ShrinkPolicy"/>.</para>
    ///
    /// <para><b>Why it cannot throw.</b> The original failure is the result. A shrink that broke —
    /// on its own budget, on a cluster that would not start — must not replace the finding with a
    /// report about the tool that was looking at it, so its own failure is written into the message
    /// as text and the original is rethrown regardless.</para>
    /// </summary>
    private async Task<string> ShrinkIfAskedAsync(
        ulong seed,
        RandomScenarioOptions options,
        RandomScenarioReport partial,
        Exception failure,
        string directory,
        CancellationToken cancellationToken)
    {
        if (!ShrinkPolicy.Enabled())
            return string.Empty;

        string signature = FailureSignature.Of(failure);

        if (signature == FailureSignature.None)
            return $"{Environment.NewLine}Shrink: skipped, the failure carried no signature.";

        try
        {
            PlanShrinker shrinker = new(
                new ClusterPlanOracle(seed, options, logger).AsOracle(),
                ShrinkPolicy.Options());

            ShrinkResult result = await shrinker.ShrinkAsync(
                partial.Actions, signature, cancellationToken);

            // The shrunk file carries the same header the plan artifact does, so it can be dropped
            // straight into the regression corpus without a reader reconstructing the bounds.
            result = result with { Header = PlanHeader.For(seed, options) };

            string path = result.WriteArtifact(directory, $"random-seed-{seed}");

            return $"{Environment.NewLine}Shrunk: {path} " +
                   $"({result.Original.Count} actions to {result.Shrunk.Count} " +
                   $"in {result.CandidatesRun} runs)";
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error)
        {
            return $"{Environment.NewLine}Shrink failed: {error.Message}";
        }
    }
}
