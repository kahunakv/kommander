using Kommander.Tests.Simulation.Diagnostics;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The cost budget: what it catches, and what it deliberately lets through.
///
/// <para><b>Why the second half matters as much as the first.</b> This suite is load-sensitive.
/// Measured runs of one category have varied by a factor of thirty on one machine, and every one of
/// those runs was correct. A budget that failed on that variation would produce failures nobody can
/// reproduce — the exact confusion the metrics exist to end. So the tests below check both that a
/// wedged run is caught and that a merely slow one is not.</para>
/// </summary>
public sealed class TestSimulationBudgets
{
    /// <summary>A run that stopped stepping altogether is caught, and the message says so.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ARunThatStoppedStepping_BreaksItsBudget()
    {
        // Two steps in two minutes. Not slow — stopped.
        SimulationMetrics wedged = Metrics(steps: 2, seconds: 120, invariantSeconds: 1);

        IReadOnlyList<string> breaches = SimulationBudget.ContinuousIntegration.Breaches(wedged);

        Assert.NotEmpty(breaches);
        Assert.Contains(breaches, text => text.Contains("steps per second", StringComparison.Ordinal));
    }

    /// <summary>
    /// A run on a loaded machine is not caught.
    ///
    /// <para>These are real numbers: a run of about five hundred steps that normally takes ten to
    /// thirty seconds, taking three hundred. That has been measured on this machine and the run was
    /// correct. The first version of the budget failed exactly this case, which is why the step
    /// floor is now a stall detector rather than a speed limit.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ARunOnALoadedMachine_StaysInsideItsBudget()
    {
        SimulationMetrics slow = Metrics(steps: 540, seconds: 300, invariantSeconds: 60);

        Assert.Empty(SimulationBudget.ContinuousIntegration.Breaches(slow));
    }

    /// <summary>A run that spends nearly all its time checking is caught.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ARunThatOnlyChecks_BreaksItsBudget()
    {
        SimulationMetrics checking = Metrics(steps: 540, seconds: 100, invariantSeconds: 99);

        IReadOnlyList<string> breaches = SimulationBudget.ContinuousIntegration.Breaches(checking);

        Assert.Contains(breaches, text => text.Contains("checking invariants", StringComparison.Ordinal));
    }

    /// <summary>A run far past its usual wall time is caught even while it keeps stepping.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void ARunFarPastItsUsualTime_BreaksItsBudget()
    {
        SimulationMetrics long_ = Metrics(steps: 900_000, seconds: 3_600, invariantSeconds: 10);

        IReadOnlyList<string> breaches = SimulationBudget.ContinuousIntegration.Breaches(long_);

        Assert.Contains(breaches, text => text.Contains("over the 600 s limit", StringComparison.Ordinal));
    }

    /// <summary>
    /// The empty budget lets everything through.
    ///
    /// <para>The default, and the state a developer's machine runs in. A run that broke every limit
    /// still passes, because nothing was asked for.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void TheEmptyBudget_CatchesNothing()
    {
        SimulationMetrics awful = Metrics(steps: 1, seconds: 10_000, invariantSeconds: 9_999);

        Assert.Empty(SimulationBudget.None.Breaches(awful));
    }

    /// <summary>The environment chooses the budget, and chooses nothing by default.</summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void TheEnvironmentChoosesTheBudget()
    {
        string? previous = Environment.GetEnvironmentVariable(SimulationBudgetPolicy.ModeVariable);

        try
        {
            Environment.SetEnvironmentVariable(SimulationBudgetPolicy.ModeVariable, null);
            Assert.Equal(SimulationBudget.None, SimulationBudgetPolicy.Current());

            Environment.SetEnvironmentVariable(SimulationBudgetPolicy.ModeVariable, "ci");
            Assert.Equal(SimulationBudget.ContinuousIntegration, SimulationBudgetPolicy.Current());

            // Anything else is not a budget. A misspelled value must not silently enforce limits
            // nobody asked for, nor silently drop limits somebody did.
            Environment.SetEnvironmentVariable(SimulationBudgetPolicy.ModeVariable, "nightly");
            Assert.Equal(SimulationBudget.None, SimulationBudgetPolicy.Current());
        }
        finally
        {
            Environment.SetEnvironmentVariable(SimulationBudgetPolicy.ModeVariable, previous);
        }
    }

    /// <summary>
    /// The collector measures a run and reports it in the artifact's own format.
    ///
    /// <para>One pair per line, because a single line holding every pair loads back as one key whose
    /// value is the rest of the line — and these pairs travel in a plan artifact's header.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public void TheCollectorMeasuresAndReportsInPairs()
    {
        SimulationMetricsCollector collector = new();

        using (collector.TimeInvariantCheck())
        {
            // Deliberately trivial. What is under test is that the timer runs and stops, not how
            // long anything takes.
        }

        SimulationMetrics measured = collector.Snapshot(steps: 100, actions: 12, invariantChecks: 90);

        Assert.Equal(100, measured.Steps);
        Assert.Equal(12, measured.Actions);
        Assert.Equal(90, measured.InvariantChecks);
        Assert.True(measured.ManagedBytes > 0, "The collector read no managed memory.");

        IReadOnlyList<(string Key, string Value)> pairs = measured.Pairs();

        Assert.Contains(pairs, pair => pair.Key == "metricStepsPerSecond");
        Assert.DoesNotContain(pairs, pair => pair.Value.Contains('=', StringComparison.Ordinal));
        Assert.DoesNotContain(pairs, pair => pair.Value.Contains(' ', StringComparison.Ordinal));
    }

    private static SimulationMetrics Metrics(int steps, double seconds, double invariantSeconds) =>
        new()
        {
            Steps = steps,
            Actions = 24,
            InvariantChecks = steps,
            Elapsed = TimeSpan.FromSeconds(seconds),
            InvariantTime = TimeSpan.FromSeconds(invariantSeconds),
            ManagedBytes = 64 * 1024 * 1024,
        };
}
