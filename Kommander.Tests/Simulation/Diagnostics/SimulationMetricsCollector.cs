using System.Diagnostics;

namespace Kommander.Tests.Simulation.Diagnostics;

/// <summary>
/// Measures a run while it happens.
///
/// <para><b>Why the invariant time is measured separately from everything else.</b> The two costs a
/// search can run away with are stepping the cluster and checking it, and they have opposite cures.
/// A run that is slow because it steps slowly is a machine problem; a run that is slow because it
/// checks constantly is a bounds problem. One total cannot tell them apart, so the checker is timed
/// on its own.</para>
///
/// <para>Not thread-safe, and it does not need to be. One run owns one collector, and the phases it
/// times run in sequence.</para>
/// </summary>
public sealed class SimulationMetricsCollector
{
    private readonly Stopwatch run = Stopwatch.StartNew();
    private readonly Stopwatch invariants = new();

    /// <summary>Times one invariant check. Dispose the result to stop the clock.</summary>
    public IDisposable TimeInvariantCheck()
    {
        invariants.Start();

        return new Stop(invariants);
    }

    /// <summary>The run so far. Safe to call before the run ends, for a partial report.</summary>
    public SimulationMetrics Snapshot(int steps, int actions, int invariantChecks) =>
        new()
        {
            Steps = steps,
            Actions = actions,
            InvariantChecks = invariantChecks,
            Elapsed = run.Elapsed,
            InvariantTime = invariants.Elapsed,

            // Read without forcing a collection. Forcing one would make the run slower to measure
            // in the same pass that measures its speed.
            ManagedBytes = GC.GetTotalMemory(forceFullCollection: false),
        };

    private sealed class Stop(Stopwatch stopwatch) : IDisposable
    {
        private bool stopped;

        public void Dispose()
        {
            if (stopped)
                return;

            stopwatch.Stop();
            stopped = true;
        }
    }
}
