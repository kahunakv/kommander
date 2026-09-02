using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;
using Kommander.Tests.Simulation.Shrinking;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The shrinker reduces a plan whose failure comes from a real cluster.
///
/// <para><b>Why this exists when the algorithm is already tested.</b> `TestPlanShrinker` proves the
/// search is correct against a function. It cannot prove the search survives contact with the thing
/// it was built for: a fresh cluster per candidate, a plan replayed action for action, and a
/// verdict read out of a real invariant. Between the two sit the cluster start, the replay, and the
/// signature extraction, and any of them could turn every candidate into the same answer and leave
/// a shrinker that reports success while reducing nothing.</para>
///
/// <para><b>Why the failure is injected rather than found.</b> A shrinker needs a failure that is
/// reliably there. A real defect that appears on one seed in eight would make this test flaky for a
/// reason that says nothing about the shrinker. So the cluster really runs the plan, and a real
/// invariant really fires — over storage this test corrupted on purpose, at a moment the plan
/// decides.</para>
///
/// <para><b>Why it is nightly.</b> Each candidate costs a cluster start. A dozen of them is a
/// minute or two, which belongs beside the seed sweep rather than on every pull request.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestPlanShrinkerAgainstACluster
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestPlanShrinkerAgainstACluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// A padded plan reduces to the one action its failure needs.
    ///
    /// <para>The failure fires only for a plan that wrote something, so the client append is the
    /// cause and the five idle actions around it are padding. The claim is that the shrinker ends
    /// holding the append and nothing else, and that the plan it hands back still fails for the same
    /// reason — the second half being what separates a reduction from a plan that merely got
    /// shorter.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTRandom")]
    public async Task AClusterBackedShrink_ReducesAPaddedPlanToItsCause()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        RandomScenarioOptions options = new() { ActionCount = 6, StepsPerAction = 3 };

        List<RandomScenarioAction> plan =
        [
            new(0, RandomScenarioActionKind.Idle),
            new(1, RandomScenarioActionKind.Idle),
            new(2, RandomScenarioActionKind.AppendAtLeader),
            new(3, RandomScenarioActionKind.Idle),
            new(4, RandomScenarioActionKind.Idle),
            new(5, RandomScenarioActionKind.Idle),
        ];

        int candidates = 0;

        PlanShrinker shrinker = new(
            async (candidate, token) =>
            {
                candidates++;
                return await RunCandidateAsync(candidate, options, token);
            },
            new ShrinkOptions { MaxCandidates = 20 });

        // Taken from the real run, not assumed. A signature guessed wrongly would make every
        // candidate a rejection and the test would pass with a plan that shrank not at all.
        ShrinkAttempt original = await RunCandidateAsync(plan, options, cancellationToken);

        Assert.NotEqual(FailureSignature.None, original.Signature);
        Assert.Equal($"invariant:{ClusterInvariantSet.CommittedEntriesAgree}", original.Signature);

        ShrinkResult result = await shrinker.ShrinkAsync(plan, original.Signature, cancellationToken);

        Assert.True(candidates > 0, "The shrinker ran no candidate against the cluster.");

        Assert.Single(result.Shrunk);
        Assert.Equal(RandomScenarioActionKind.AppendAtLeader, result.Shrunk[0].Kind);

        // The plan it returned is re-run once more. A shrinker is allowed to be wrong about which
        // action mattered; it is not allowed to hand back a plan that does not fail.
        ShrinkAttempt confirmation = await RunCandidateAsync(result.Shrunk, options, cancellationToken);

        Assert.Equal(original.Signature, confirmation.Signature);
    }

    // ── The oracle ────────────────────────────────────────────────────────

    /// <summary>
    /// Runs one candidate on a fresh cluster and reports what it did.
    ///
    /// <para>The injection sits after the replay and is conditional on the plan: a candidate that
    /// wrote nothing has no committed entry to corrupt, so it passes. That conditionality is the
    /// whole experiment — it is what gives the shrinker something real to discover.</para>
    /// </summary>
    private async Task<ShrinkAttempt> RunCandidateAsync(
        IReadOnlyList<RandomScenarioAction> plan,
        RandomScenarioOptions options,
        CancellationToken cancellationToken)
    {
        try
        {
            await using SimulationCluster cluster = await SimulationCluster.StartAsync(
                new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 20260910 },
                logger,
                cancellationToken);

            RandomScenarioRunner runner = new(cluster, options, new SimulationRandom(20260910));

            await runner.ReplayAsync(plan, cancellationToken);

            if (plan.Any(action => action.Kind == RandomScenarioActionKind.AppendAtLeader))
                await InjectDivergenceAsync(cluster, cancellationToken);

            await new ClusterInvariantRunner().CheckAsync(cluster, PartitionId, cancellationToken);

            return new ShrinkAttempt(FailureSignature.None);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error)
        {
            return new ShrinkAttempt(FailureSignature.Of(error), error.Message);
        }
    }

    /// <summary>
    /// Rewrites index one on a follower so it disagrees with the other two.
    ///
    /// <para>Index one is the leadership no-op every term writes, so it is present on every node in
    /// every run this test produces. Corrupting the client's own entry instead would depend on where
    /// in the log that entry landed, which moves with the plan.</para>
    /// </summary>
    private static async Task InjectDivergenceAsync(
        SimulationCluster cluster,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

        RaftPartitionView follower = views.First(view => view.Role != RaftNodeState.Leader);
        SimulationNode victim = cluster.Nodes.First(node => node.Endpoint == follower.Endpoint);

        victim.Wal.Write(
        [
            (PartitionId, new List<RaftLog>
            {
                new()
                {
                    Id = 1,
                    Term = 1,
                    Type = RaftLogType.Committed,
                    LogType = "Greeting",
                    LogData = "a value no leader ever proposed"u8.ToArray(),
                },
            }),
        ]);
    }
}
