
using System.Diagnostics;

namespace Kommander.Diagnostics;

/// <summary>
/// A high-performance, allocation-free stopwatch that provides functionality to measure elapsed time
/// without relying on heap allocations. Designed to be used in latency-critical or performance-sensitive
/// scenarios where frequent timing operations are required.
/// </summary>
/// <remarks>
/// The <see cref="ValueStopwatch"/> is a value type and provides accurate timing information by utilizing
/// the high-resolution timestamp obtained from <see cref="System.Diagnostics.Stopwatch"/>. It is meant to
/// function as a lightweight alternative to the standard <see cref="Stopwatch"/> class for specific use cases.
/// </remarks>
public readonly struct ValueStopwatch
{
    private static readonly double TimestampToTicks = TimeSpan.TicksPerSecond / (double)Stopwatch.Frequency;

    private readonly long _startTimestamp;

    public bool IsActive => _startTimestamp != 0;

    private ValueStopwatch(long startTimestamp)
    {
        _startTimestamp = startTimestamp;
    }

    public static ValueStopwatch StartNew() => new(Stopwatch.GetTimestamp());

    public static long GetTimestamp() => Stopwatch.GetTimestamp();

    public static TimeSpan GetElapsedTime(long startTimestamp, long endTimestamp)
    {
        long timestampDelta = endTimestamp - startTimestamp;
        long ticks = (long)(TimestampToTicks * timestampDelta);
        return new(ticks);
    }

    public TimeSpan GetElapsedTime()
    {
        // Start timestamp can't be zero in an initialized ValueStopwatch. It would have to be literally the first thing executed when the machine boots to be 0.
        // So it being 0 is a clear indication of default(ValueStopwatch)
        if (!IsActive)
            throw new InvalidOperationException("An uninitialized, or 'default', ValueStopwatch cannot be used to get elapsed time.");

        long end = Stopwatch.GetTimestamp();
        return GetElapsedTime(_startTimestamp, end);
    }
    
    public long GetElapsedMilliseconds()
    {
        TimeSpan elapsedTime = GetElapsedTime();
        return (long) elapsedTime.TotalMilliseconds;
    }
}