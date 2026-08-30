using Kommander.Time;

namespace Kommander.Tests.Simulation.Time;

/// <summary>
/// The <see cref="IMonotonicTickSource"/> that a simulation installs into
/// <see cref="RaftConfiguration.TickSource"/> on every node of a run.
///
/// <para><b>What it does.</b> It reports a tick value derived from a logical millisecond
/// counter that only the harness advances. No consensus gate can therefore observe time
/// passing between two simulation events. An election timeout expires when the scenario says
/// it expires, and at no other moment.</para>
///
/// <para><b>Why the clock does not start at zero.</b> <c>RaftMonotonic.Elapsed</c> treats an
/// anchor of 0 as "never set" and answers <see cref="TimeSpan.MaxValue"/>. A tick source that
/// began at 0 would make the first heartbeat stamp on every node read as unset, which inverts
/// several freshness gates. The clock therefore starts at <see cref="BaseLogicalMilliseconds"/>,
/// one simulated hour, so no legitimate stamp is ever 0.</para>
///
/// <para><b>Frequency.</b> Equal to the process <c>Stopwatch.Frequency</c>. Timeout arithmetic
/// in the library converts ticks to a <see cref="TimeSpan"/> with that constant, so keeping it
/// identical means a simulated 2000 ms is the same duration the production code computes.</para>
///
/// <para><b>Thread safety.</b> Reads happen on executor and WAL threads while the harness
/// advances time from the simulation thread, so both sides go through interlocked access. The
/// counter never decreases: <see cref="AdvanceTo"/> ignores a value at or below the current one.</para>
/// </summary>
public sealed class VirtualTickSource : IMonotonicTickSource
{
    /// <summary>
    /// Logical time at which every run starts. One simulated hour, so that a tick stamp taken
    /// at the very first step is far from the "unset anchor" sentinel of 0.
    /// </summary>
    public const long BaseLogicalMilliseconds = 3_600_000;

    private long _logicalMilliseconds = BaseLogicalMilliseconds;
    private long _readCount;

    /// <inheritdoc />
    public long Frequency { get; } = global::System.Diagnostics.Stopwatch.Frequency;

    /// <summary>Current logical time in milliseconds since the run's epoch.</summary>
    public long LogicalMilliseconds => Interlocked.Read(ref _logicalMilliseconds);

    /// <summary>
    /// Number of tick reads served so far. A smoke test asserts this is non-zero, which proves
    /// the consensus path really consulted this source rather than the process clock.
    /// </summary>
    public long ReadCount => Interlocked.Read(ref _readCount);

    /// <inheritdoc />
    public long GetTimestamp()
    {
        Interlocked.Increment(ref _readCount);
        return ToTicks(Interlocked.Read(ref _logicalMilliseconds));
    }

    /// <summary>
    /// Moves logical time forward to <paramref name="logicalMilliseconds"/>.
    /// A value at or below the current time is ignored, which keeps the source monotonic even
    /// when a scenario replays an event out of order.
    /// </summary>
    public void AdvanceTo(long logicalMilliseconds)
    {
        while (true)
        {
            long current = Interlocked.Read(ref _logicalMilliseconds);
            if (logicalMilliseconds <= current)
                return;

            if (Interlocked.CompareExchange(ref _logicalMilliseconds, logicalMilliseconds, current) == current)
                return;
        }
    }

    /// <summary>Moves logical time forward by <paramref name="deltaMilliseconds"/>.</summary>
    public void AdvanceBy(long deltaMilliseconds)
    {
        if (deltaMilliseconds <= 0)
            return;

        Interlocked.Add(ref _logicalMilliseconds, deltaMilliseconds);
    }

    /// <summary>Converts a logical millisecond value into this source's tick units.</summary>
    public long ToTicks(long logicalMilliseconds) =>
        (long)(logicalMilliseconds * (Frequency / 1000.0));
}
