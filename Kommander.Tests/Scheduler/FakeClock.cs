
namespace Kommander.Tests.Scheduler;

/// <summary>
/// Deterministic fake clock for use in scheduler harness tests.
///
/// <para>All time progression is explicit: tests advance the clock with
/// <see cref="Advance"/> or <see cref="AdvanceTo"/> and never rely on
/// wall-clock sleeps.  The clock starts at a fixed epoch so test output
/// is reproducible across machines.</para>
/// </summary>
public sealed class FakeClock
{
    /// <summary>Fixed starting point used when no seed is provided.</summary>
    public static readonly DateTimeOffset Epoch = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private DateTimeOffset _now;

    /// <summary>Creates a clock starting at <see cref="Epoch"/>.</summary>
    public FakeClock() : this(Epoch) { }

    /// <summary>Creates a clock starting at <paramref name="start"/>.</summary>
    public FakeClock(DateTimeOffset start) => _now = start;

    /// <summary>Current simulated time.</summary>
    public DateTimeOffset UtcNow => _now;

    /// <summary>Current simulated time as a <see cref="DateTime"/> (UTC).</summary>
    public DateTime UtcNowDateTime => _now.UtcDateTime;

    /// <summary>Elapsed milliseconds since <see cref="Epoch"/>.</summary>
    public long ElapsedMilliseconds => (long)(_now - Epoch).TotalMilliseconds;

    /// <summary>Advances the clock by <paramref name="delta"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="delta"/> is negative.</exception>
    public void Advance(TimeSpan delta)
    {
        if (delta < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(delta), "Cannot advance the clock backwards.");
        _now += delta;
    }

    /// <summary>Convenience overload: advance by a number of milliseconds.</summary>
    public void AdvanceMs(long milliseconds) => Advance(TimeSpan.FromMilliseconds(milliseconds));

    /// <summary>Moves the clock forward to <paramref name="target"/> (must not be in the past).</summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="target"/> is before <see cref="UtcNow"/>.</exception>
    public void AdvanceTo(DateTimeOffset target)
    {
        if (target < _now)
            throw new ArgumentOutOfRangeException(nameof(target), "Cannot move the clock backwards.");
        _now = target;
    }

    /// <summary>
    /// Returns a monotonically increasing tick count derived from the current fake time,
    /// suitable for use wherever <see cref="Environment.TickCount64"/> would appear.
    /// </summary>
    public long TickCount64 => ElapsedMilliseconds;
}
