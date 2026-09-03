namespace Kommander.Tests.Simulation.Diagnostics;

/// <summary>
/// Decides whether a run's cost is enforced, and under which limits.
///
/// <para><b>Why this is off unless asked for.</b> A developer's machine is not a controlled
/// environment. A suite that failed because a build was running beside it would teach people to
/// ignore the failure, and the one time it meant something they would ignore that too. The
/// continuous-integration jobs set the variable; nobody else has to.</para>
///
/// <para>The metrics are measured and reported whatever this says. Only the enforcement is
/// conditional — a number nobody looks at costs nothing, and it is the number that settles an
/// argument about whether a slow run was the change or the machine.</para>
/// </summary>
public static class SimulationBudgetPolicy
{
    /// <summary>Set this to <c>ci</c> to enforce the continuous-integration limits.</summary>
    public const string ModeVariable = "KOMMANDER_DST_BUDGET";

    /// <summary>The budget the environment asked for. No limits when it asked for nothing.</summary>
    public static SimulationBudget Current()
    {
        string? text = Environment.GetEnvironmentVariable(ModeVariable);

        return text is not null && text.Equals("ci", StringComparison.OrdinalIgnoreCase)
            ? SimulationBudget.ContinuousIntegration
            : SimulationBudget.None;
    }
}
