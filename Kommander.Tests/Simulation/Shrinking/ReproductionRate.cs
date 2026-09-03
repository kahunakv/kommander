namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// How often a plan reproduces its failure, and how many attempts that demands of a shrink.
///
/// <para><b>Why this arithmetic is worth a type.</b> The attempt count is the setting that decides
/// whether a shrink means anything, and it is the one a person is worst placed to guess. Set it too
/// low and the shrinker reads a flaky pass as "the removed action was needed": it keeps almost every
/// action and reports a reduction of nothing, which looks exactly like a plan whose every action
/// matters. That happened here — a plan reproducing about fifteen per cent of the time was shrunk at
/// six attempts, and the search stopped after one cut.</para>
///
/// <para><b>The trap is that it gets worse as the shrink succeeds.</b> A shorter plan usually
/// reproduces less often than the plan it came from, so the attempts needed rise exactly as the
/// search makes progress. A count chosen from the original plan's rate is already too low by the
/// time it matters.</para>
/// </summary>
public static class ReproductionRate
{
    /// <summary>
    /// Attempts needed to see at least one reproduction with the given confidence.
    ///
    /// <para>Each attempt is independent, so the chance of missing every time is
    /// <c>(1 - rate)^attempts</c>. Solving that for the confidence gives the count. A rate of one
    /// needs a single attempt; a rate at or below zero cannot be caught at all and returns the
    /// ceiling rather than pretending otherwise.</para>
    /// </summary>
    /// <param name="rate">Measured reproductions per run, from zero to one.</param>
    /// <param name="confidence">Chance of catching a reproduction, exclusive of one.</param>
    /// <param name="ceiling">Most attempts to ask for, whatever the arithmetic says.</param>
    public static int RequiredAttempts(double rate, double confidence = 0.9, int ceiling = 40)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(confidence, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(confidence, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(ceiling, 1);

        if (rate >= 1)
            return 1;

        if (rate <= 0)
            return ceiling;

        int attempts = (int)Math.Ceiling(Math.Log(1 - confidence) / Math.Log(1 - rate));

        return Math.Clamp(attempts, 1, ceiling);
    }

    /// <summary>
    /// The chance a shrink at this attempt count sees a plan that reproduces at this rate.
    ///
    /// <para>Reported so a shrink can say what its result is worth. A search that ran at a sixty per
    /// cent catch rate has probably rejected valid reductions, and a reader who is told that will
    /// re-run rather than conclude the plan is minimal.</para>
    /// </summary>
    public static double CatchProbability(double rate, int attempts)
    {
        if (rate >= 1)
            return 1;

        if (rate <= 0)
            return 0;

        return 1 - Math.Pow(1 - rate, Math.Max(1, attempts));
    }
}
