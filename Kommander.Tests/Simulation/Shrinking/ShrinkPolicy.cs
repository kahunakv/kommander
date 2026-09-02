using System.Globalization;

namespace Kommander.Tests.Simulation.Shrinking;

/// <summary>
/// Decides whether a failing run shrinks its own plan, and under what budget.
///
/// <para><b>Why this is off by default.</b> A shrink costs one cluster run per candidate, tens of
/// them for one failure. That price buys a reader hours, and it buys them only once the failure has
/// already happened — so it belongs to the nightly search, where a failure is the whole point, and
/// not to a pull request, where a failure is a thing to report and move on from. A default that
/// shrank would put the nightly's cost on every change.</para>
///
/// <para><b>Why an environment variable and not a constant.</b> The same test binary runs in both
/// places. The seed count is already chosen this way, so the nightly job sets one more name beside
/// the one it already sets.</para>
/// </summary>
public static class ShrinkPolicy
{
    /// <summary>Set this to <c>1</c> or <c>true</c> to make a failing run shrink its plan.</summary>
    public const string EnabledVariable = "KOMMANDER_DST_SHRINK";

    /// <summary>Overrides <see cref="ShrinkOptions.MaxCandidates"/> for the nightly job.</summary>
    public const string BudgetVariable = "KOMMANDER_DST_SHRINK_BUDGET";

    /// <summary>Whether the environment asked for a shrink.</summary>
    public static bool Enabled()
    {
        string? text = Environment.GetEnvironmentVariable(EnabledVariable);

        return text is not null
               && (text.Equals("1", StringComparison.Ordinal)
                   || text.Equals("true", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The budget a nightly shrink runs under.
    ///
    /// <para>Two attempts per candidate, not one. A real cluster does not fail the same plan every
    /// time, and at one attempt the shrinker reads a flaky pass as proof that the removed action was
    /// needed — it then keeps actions that do nothing, which is the failure mode a reader cannot see
    /// from the result.</para>
    /// </summary>
    public static ShrinkOptions Options()
    {
        string? text = Environment.GetEnvironmentVariable(BudgetVariable);

        int budget = int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int parsed)
                     && parsed > 0
            ? parsed
            : 40;

        return new ShrinkOptions { MaxCandidates = budget, AttemptsPerCandidate = 2 };
    }
}
