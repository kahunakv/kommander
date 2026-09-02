namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// A plan that once failed, kept so it is run again for ever.
///
/// <para><b>Why a plan and not a seed.</b> A seed reproduces the draws, and the draws depend on what
/// the generator observed in a cluster whose nodes own their own threads. Two runs of one seed can
/// see different leaders and diverge from there, so a seed promoted as a regression tests a
/// different run every night. The plan removes the generator: the same actions in the same order,
/// whatever the cluster does between them.</para>
///
/// <para><b>Why the bounds travel with it.</b> The actions say what happened; the bounds say what the
/// run was allowed to do. A plan replayed at three steps per action is not the plan that failed at
/// six.</para>
/// </summary>
/// <param name="Name">The artifact's file name, used to name the failure.</param>
/// <param name="Seed">The seed the run was drawn from, for the cluster's own choices.</param>
/// <param name="Options">The bounds recorded in the file.</param>
/// <param name="Actions">The plan, in the order it happened.</param>
/// <param name="Header">Every header line, including the ones no option reads.</param>
public sealed record RegressionPlan(
    string Name,
    ulong Seed,
    RandomScenarioOptions Options,
    IReadOnlyList<RandomScenarioAction> Actions,
    IReadOnlyDictionary<string, string> Header);
