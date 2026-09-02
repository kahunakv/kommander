using System.Globalization;
using System.Text;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;
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

    private readonly ILogger<IRaft> logger;

    public TestPlanRegressions(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
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

        int repeats = ConfiguredRepeats();
        StringBuilder failures = new();

        foreach (RegressionPlan plan in plans)
        {
            for (int attempt = 1; attempt <= repeats; attempt++)
            {
                string? failure = await ReplayAsync(plan, cancellationToken);

                if (failure is null)
                    continue;

                failures.AppendLine(
                    $"{plan.Name} failed on replay {attempt} of {repeats}: {failure}");

                // One report per plan. A plan that fails on every replay would otherwise bury the
                // other plans under one message.
                break;
            }
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
    private async Task<string?> ReplayAsync(RegressionPlan plan, CancellationToken cancellationToken)
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
                logger,
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

    private static int ConfiguredRepeats()
    {
        string? text = Environment.GetEnvironmentVariable(RepeatsVariable);

        return int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int repeats)
               && repeats > 0
            ? repeats
            : 1;
    }
}
