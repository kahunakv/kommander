
namespace Kommander.Tests;

/// <summary>
/// Scales wall-clock wait budgets in the cluster tests to the environment they run in.
///
/// <para>The condition-wait timeouts (10–30 s) are tuned for local developer hardware, where the
/// timing-sensitive in-process Raft clusters converge with a 10–40x margin. CI runners (GitHub
/// Actions: 2 vCPUs, shared, running the whole suite) blow through those budgets on tests that pass
/// locally in under a second — every observed CI failure has been a <c>WaitForCondition</c>-style
/// timeout with no assertion failure behind it. Scaling the budget in CI keeps the tests meaningful
/// (a genuine hang still fails) without letting runner load masquerade as regressions.</para>
///
/// <para><c>KOMMANDER_TEST_TIMEOUT_MULTIPLIER</c> overrides the multiplier explicitly (any positive
/// integer); otherwise <c>GITHUB_ACTIONS=true</c> — set by all GitHub-hosted runners — selects 3x,
/// and everything else runs at 1x (no local behavior change).</para>
/// </summary>
internal static class TestTimeouts
{
    public static readonly int Multiplier = ComputeMultiplier();

    private static int ComputeMultiplier()
    {
        if (int.TryParse(Environment.GetEnvironmentVariable("KOMMANDER_TEST_TIMEOUT_MULTIPLIER"), out int explicitMultiplier)
            && explicitMultiplier > 0)
            return explicitMultiplier;

        return Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true" ? 3 : 1;
    }

    public static int Scale(int timeoutMs) => timeoutMs * Multiplier;

    public static TimeSpan Scale(TimeSpan timeout) => timeout * Multiplier;
}
