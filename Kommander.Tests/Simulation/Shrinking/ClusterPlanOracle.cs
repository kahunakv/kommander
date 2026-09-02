using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// Runs a candidate plan against a real three-node cluster and reports what it did.
///
/// <para><b>A fresh cluster per candidate, always.</b> Every reduction has to be judged against the
/// same starting state the original failure had. Reusing a cluster would carry one candidate's logs,
/// terms and faults into the next, so a plan would be judged partly on what the plan before it did,
/// and the shrinker would keep or drop actions for reasons that are not in the plan. This is the
/// single largest cost in a shrink and it is not negotiable.</para>
///
/// <para><b>Why the seed stays fixed across candidates.</b> The seed no longer draws the plan — the
/// plan is given — so it now only settles the cluster's own choices. Holding it fixed removes one
/// source of difference between two candidates that differ by one action, which is the comparison
/// the whole search rests on.</para>
/// </summary>
public sealed class ClusterPlanOracle
{
    private readonly ulong seed;
    private readonly RandomScenarioOptions options;
    private readonly ILogger<IRaft> logger;

    public ClusterPlanOracle(ulong seed, RandomScenarioOptions options, ILogger<IRaft> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        this.seed = seed;
        this.options = options;
        this.logger = logger;
    }

    /// <summary>The oracle, in the shape <see cref="PlanShrinker"/> takes.</summary>
    public PlanOracle AsOracle() => RunAsync;

    /// <summary>
    /// Applies one candidate and turns its outcome into a signature.
    ///
    /// <para>A failure to start or tear down the cluster is reported as a signature of its own
    /// rather than swallowed. It is not the defect under study, and the comparison rejects it for
    /// that reason; reporting it means a shrink that hits a broken harness says so instead of
    /// quietly concluding every reduction is safe.</para>
    /// </summary>
    private async Task<ShrinkAttempt> RunAsync(
        IReadOnlyList<RandomScenarioAction> plan,
        CancellationToken cancellationToken)
    {
        try
        {
            await using SimulationCluster cluster = await SimulationCluster.StartAsync(
                new SimulationClusterOptions
                {
                    NodeCount = 3,
                    PartitionCount = 1,
                    Seed = seed,
                    ConfigureNode = configuration =>
                        configuration.CompactEveryOperations = options.CompactEveryOperations,
                },
                logger,
                cancellationToken);

            RandomScenarioRunner runner = new(cluster, options, new SimulationRandom(seed));

            await runner.ReplayAsync(plan, cancellationToken);

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
}
