using Kommander.Tests.Simulation.Scenarios.Random;

namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// The budget a shrink runs under.
///
/// <para><b>Why every field here is a bound and none is a target.</b> Shrinking is a search whose
/// cost is measured in whole cluster runs. A plan of forty actions has more removals available than
/// a night has minutes, so the useful question is not how small the plan can get but how small it
/// gets for a stated price. Each bound below turns one part of that price into a number a reader
/// can set.</para>
/// </summary>
public sealed record ShrinkOptions
{
    /// <summary>
    /// Candidate plans the shrinker may run in total, over every pass.
    ///
    /// <para>The hard stop. When it is spent the shrinker returns the smallest plan it confirmed so
    /// far and says the budget ran out, so a caller always gets a usable plan rather than a
    /// timeout.</para>
    /// </summary>
    public int MaxCandidates { get; init; } = 60;

    /// <summary>
    /// Runs of one candidate before it is called a pass.
    ///
    /// <para><b>This is the setting that decides whether the result is trustworthy.</b> These plans
    /// drive real clusters on their own threads, so a plan that fails does not fail every time. At
    /// one attempt a flaky reproduction reads as "the removed action was needed", and the shrinker
    /// keeps actions that do nothing. Raising it costs a cluster run per candidate and buys a
    /// smaller plan that is still a real reproduction.</para>
    /// </summary>
    public int AttemptsPerCandidate { get; init; } = 1;

    /// <summary>
    /// Actions the shrinker refuses to go below. A plan of nothing reproduces nothing, and stopping
    /// short of it saves the last few pointless runs.
    /// </summary>
    public int MinimumPlanLength { get; init; } = 1;

    /// <summary>
    /// Called with the plan each time a reduction is confirmed.
    ///
    /// <para><b>Why a shrink needs this at all.</b> A shrink of an intermittent failure runs for
    /// tens of minutes, and everything it learned is in the return value. A run that is interrupted
    /// — a cancelled job, a killed process, a laptop lid — loses all of it, and the next attempt
    /// starts from the original plan. Reporting each accepted reduction lets a caller keep the best
    /// plan so far on disk, so an interrupted shrink still leaves something to resume from.</para>
    ///
    /// <para>Progress only, never a decision. The search does not read anything back from the
    /// callback, and an exception from it is the caller's problem, not the shrinker's.</para>
    /// </summary>
    public Action<IReadOnlyList<RandomScenarioAction>>? OnProgress { get; init; }

    /// <summary>
    /// Whether to try smaller numeric parameters after the removals are done.
    ///
    /// <para>Second because it is worth less. Removing an action changes what the plan says;
    /// lowering a duplicate count from six to two only changes how loudly it says it. Running it
    /// last means the expensive removals happen while the budget is still whole.</para>
    /// </summary>
    public bool ReduceParameters { get; init; } = true;
}
