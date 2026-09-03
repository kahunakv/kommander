using System.Globalization;

namespace Kommander.Tests.Simulation.Diagnostics;

/// <summary>
/// Limits a run is expected to stay inside.
///
/// <para><b>What a budget is here, and what it is not.</b> It is a runaway detector, not a
/// performance gate. A run that stops making progress, or that spends nearly all its time in the
/// checker, is broken in a way nobody would otherwise notice — the suite still passes, only slower,
/// and the loss is silent. A budget catches that.</para>
///
/// <para><b>Why the limits are loose on purpose.</b> This suite is load-sensitive: measured runs of
/// one category have varied by a factor of thirty on one machine, and every one of those runs was
/// correct. A tight budget would turn that variation into failures nobody can reproduce, which is
/// the exact failure mode the metrics exist to prevent. Each limit below sits an order of magnitude
/// away from anything measured.</para>
///
/// <para>Off by default. A developer's machine is not a controlled environment, and a suite that
/// failed on a busy laptop would teach people to ignore it.</para>
/// </summary>
public sealed record SimulationBudget
{
    /// <summary>Real seconds one run may take. Zero means no limit.</summary>
    public double MaxRunSeconds { get; init; }

    /// <summary>
    /// Steps per real second a run must sustain. Zero means no limit.
    ///
    /// <para><b>This limit is set far lower than instinct suggests, and the first attempt at it was
    /// wrong.</b> A floor of five steps per second looked generous against runs that normally make
    /// fifty to two hundred — until it was checked against a real slow run. One measured category
    /// run took thirty times its usual wall time, which puts a five-hundred-step run near two steps
    /// per second, and that run was correct. A floor of five would have failed it and produced
    /// exactly the false regression these metrics exist to prevent.</para>
    ///
    /// <para>So this catches a stall, not a slowdown: below a step every ten seconds a run has
    /// stopped rather than slowed. Wall time is the limit that catches a runaway.</para>
    /// </summary>
    public double MinStepsPerSecond { get; init; }

    /// <summary>
    /// The largest part of a run that may be spent inside the invariant checks. Zero means no
    /// limit.
    /// </summary>
    public double MaxInvariantShare { get; init; }

    /// <summary>No limits. Metrics are still measured and still reported.</summary>
    public static SimulationBudget None => new();

    /// <summary>
    /// The limits a continuous-integration job runs under.
    ///
    /// <para>Derived from measurement, and from one measurement that corrected the first guess.
    /// Generated runs on this project's machine take ten to thirty seconds each and make roughly
    /// twenty to two hundred steps per second. Under load the same runs have taken thirty times
    /// longer and stayed correct, which is why the step floor is a stall detector rather than a
    /// speed limit — see <see cref="MinStepsPerSecond"/>. Wall time is the limit that catches a
    /// runaway: no correct run measured here has come within an order of magnitude of ten
    /// minutes.</para>
    /// </summary>
    public static SimulationBudget ContinuousIntegration => new()
    {
        MaxRunSeconds = 600,
        MinStepsPerSecond = 0.1,
        MaxInvariantShare = 0.95,
    };

    /// <summary>
    /// Every limit this run broke, as sentences. Empty when the run stayed inside its budget.
    ///
    /// <para>Every breach names the measurement and the limit, and says what a breach usually means.
    /// A budget failure that reads "too slow" sends a reader looking for a defect; one that says the
    /// run was probably sharing a machine sends them to a control run instead.</para>
    /// </summary>
    public IReadOnlyList<string> Breaches(SimulationMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(metrics);

        // Invariant culture in every message. A machine whose decimal separator is a comma would
        // report "1,8 steps per second", which reads as eighteen to half the people who see it.
        CultureInfo culture = CultureInfo.InvariantCulture;

        List<string> breaches = [];

        if (MaxRunSeconds > 0 && metrics.Elapsed.TotalSeconds > MaxRunSeconds)
        {
            breaches.Add(
                $"The run took {metrics.Elapsed.TotalSeconds.ToString("F0", culture)} s, over the " +
                $"{MaxRunSeconds.ToString("F0", culture)} s " +
                "limit. A run this far past its usual cost is normally wedged rather than slow; " +
                $"it made {metrics.StepsPerSecond.ToString("F1", culture)} steps per second.");
        }

        if (MinStepsPerSecond > 0 && metrics.Steps > 0 && metrics.StepsPerSecond < MinStepsPerSecond)
        {
            breaches.Add(
                $"The run made {metrics.StepsPerSecond.ToString("F1", culture)} steps per second, " +
                $"under the {MinStepsPerSecond.ToString("F1", culture)} floor. Below this a run " +
                "has stopped rather than slowed; a machine under load still steps, only slowly.");
        }

        if (MaxInvariantShare > 0 && metrics.InvariantShare > MaxInvariantShare)
        {
            breaches.Add(
                $"The run spent {metrics.InvariantShare.ToString("P0", culture)} of its time checking " +
                $"invariants, over the {MaxInvariantShare.ToString("P0", culture)} limit. The " +
                "search is paying for oracles rather than for exploration: either the step count " +
                "or the checks need changing.");
        }

        return breaches;
    }
}
