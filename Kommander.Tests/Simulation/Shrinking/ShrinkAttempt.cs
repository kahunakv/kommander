using Kommander.Tests.Simulation.Scenarios.Random;

namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// What happened when one candidate plan was run.
///
/// <para>Three outcomes, not two. A candidate that fails for another reason is neither a
/// reproduction nor a pass: it says the reduction moved the run to a different defect, and the
/// shrinker must reject it without concluding the removed action was needed. Collapsing that case
/// into "passed" is how a shrinker ends up minimising the wrong failure.</para>
/// </summary>
/// <param name="Signature">The reason it failed, or <see cref="FailureSignature.None"/>.</param>
/// <param name="Message">The failure text, kept so the final report can quote the real thing.</param>
public sealed record ShrinkAttempt(string Signature, string? Message = null)
{
    /// <summary>A run that failed for the reason the shrinker is chasing.</summary>
    public bool Reproduces(string target) => Signature == target;

    /// <summary>A run that held every check.</summary>
    public bool Passed => Signature == FailureSignature.None;
}

/// <summary>
/// Runs one candidate plan and reports what it did.
///
/// <para>The whole cluster sits behind this delegate on purpose. The shrinker's algorithm has
/// nothing to do with Raft, so it is testable against a plain function, and the expensive part is
/// substituted rather than mocked.</para>
/// </summary>
public delegate Task<ShrinkAttempt> PlanOracle(
    IReadOnlyList<RandomScenarioAction> plan,
    CancellationToken cancellationToken);
