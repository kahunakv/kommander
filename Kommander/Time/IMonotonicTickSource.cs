namespace Kommander.Time;

/// <summary>
/// The single source of monotonic ticks for the consensus path.
///
/// <para><b>Why this exists.</b> Every elapsed-time gate in Kommander — election timeout,
/// heartbeat cadence, voting window, quiesce-after, backfill damping, leadership barrier,
/// log throttles — measures a duration in <see cref="global::System.Diagnostics.Stopwatch"/>
/// ticks. A direct read of the process clock inside those gates makes a run depend on real
/// time. Deterministic Simulation Testing (DST) cannot replay such a run, because the same
/// seed produces different elapsed values on every execution.</para>
///
/// <para><b>The rule.</b> Consensus-path code must not call
/// <c>Stopwatch.GetTimestamp()</c> directly. It must read ticks from an
/// <see cref="IMonotonicTickSource"/>. Production wires
/// <see cref="SystemMonotonicTickSource"/>, which is the process clock. A simulation wires a
/// tick source that the harness advances explicitly. The repository check in
/// <c>scripts/check-determinism-boundary.sh</c> enforces the rule on every build.</para>
///
/// <para><b>What this is not.</b> Ticks are local. They are never comparable across nodes and
/// must never order events or identify log entries. <see cref="HybridLogicalClock"/> remains
/// the authority for both.</para>
/// </summary>
public interface IMonotonicTickSource
{
    /// <summary>
    /// Returns a monotonically non-decreasing local tick count.
    /// Two calls in program order never return a lower value on the second call.
    /// </summary>
    long GetTimestamp();

    /// <summary>
    /// Number of ticks in one second. Converts a tick difference into a duration.
    /// A simulation keeps the production value so that timeout arithmetic is identical.
    /// </summary>
    long Frequency { get; }

    /// <summary>
    /// Returns the time between two tick readings taken from this source.
    /// </summary>
    TimeSpan GetElapsedTime(long startTicks, long endTicks) =>
        new((long)((endTicks - startTicks) * (TimeSpan.TicksPerSecond / (double)Frequency)));
}
