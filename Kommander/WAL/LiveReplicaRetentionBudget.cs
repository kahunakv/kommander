using System.Diagnostics;

namespace Kommander.WAL;

/// <summary>
/// Sizes how far below the compaction checkpoint the WAL keeps entries for a lagging or briefly
/// absent replica, in time as well as in entries.
///
/// <para><b>Why a count alone is not enough.</b> <see cref="RaftConfiguration.CompactionLiveReplicaLagBudget"/>
/// is a count of entries, and a count shrinks in time as the write rate rises. The same 1,000,000
/// entries were about 80 s of log at 2,243 ops/s and about 37 s at 5,292 ops/s (the Caraxes fault
/// soak on the round-cost stack, ~27,000 entries/s). At the faster rate a 30-second kill plus
/// restore came back 1.7 M entries behind, below the floor, and the replica needed a whole-partition
/// snapshot instead of a log catch-up.</para>
///
/// <para><b>What this type measures.</b> The leader samples its commit index on each heartbeat round,
/// at most one sample per <see cref="SampleInterval"/>. <see cref="Observe"/> returns how many entries
/// were committed within the last <see cref="RaftConfiguration.CompactionLiveReplicaLagWindow"/>. It
/// interpolates linearly at the window's start when the samples straddle it (a gap in the history, for
/// example while this node was not leader, reads as a uniform rate across the gap). It extrapolates from
/// the oldest sample when the history is shorter than the window (a fresh leader). The commit index is
/// a position in the one log, so the difference counts every commit in the interval, whoever led it.</para>
///
/// <para><b>Which bound wins</b> (<see cref="Compose"/>): the effective budget is
/// <c>max(CompactionLiveReplicaLagBudget, min(entries in the window, CompactionLiveReplicaLagCap))</c>.
/// The count is a floor that is always honored, which is the behavior before this type existed. The
/// window can only raise retention above the count, and the cap bounds how far, as the WAL-disk
/// safety bound. Configuring a count above the cap keeps the count.</para>
///
/// <para>Executor thread only; not thread-safe.</para>
/// </summary>
internal sealed class LiveReplicaRetentionBudget
{
    /// <summary>Longest spacing between samples. The window is resolved to about this much.</summary>
    internal static readonly TimeSpan SampleInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The shortest history a fresh leader extrapolates from. Below it the rate is noise and the
    /// window contributes nothing, so the count budget applies.
    /// </summary>
    internal static readonly TimeSpan MinExtrapolationSpan = TimeSpan.FromMilliseconds(500);

    private readonly Queue<(long Ticks, long CommitIndex)> samples = new();

    /// <summary>The newest sample; meaningful only while <see cref="samples"/> is not empty.</summary>
    private (long Ticks, long CommitIndex) newest;

    /// <summary>Number of retained samples (tests).</summary>
    internal int SampleCount => samples.Count;

    /// <summary>
    /// Records the commit index observed at <paramref name="nowTicks"/> (Stopwatch ticks) and returns
    /// the number of entries committed within <paramref name="window"/> ending now. Returns 0 when the
    /// window is disabled, or when the history is too short to tell.
    /// </summary>
    public long Observe(long nowTicks, long commitIndex, TimeSpan window)
    {
        if (window <= TimeSpan.Zero || commitIndex < 0)
        {
            samples.Clear();
            return 0;
        }

        // A commit index that went backwards (a snapshot re-seed of this node's log) or a clock that
        // did would make every difference in the history meaningless.
        if (samples.Count > 0 && (commitIndex < newest.CommitIndex || nowTicks < newest.Ticks))
            samples.Clear();

        long intervalTicks = ToTicks(TimeSpan.FromTicks(Math.Min(SampleInterval.Ticks, Math.Max(1, window.Ticks / 16))));
        if (samples.Count == 0 || nowTicks - newest.Ticks >= intervalTicks)
        {
            newest = (nowTicks, commitIndex);
            samples.Enqueue(newest);
        }

        long windowTicks = ToTicks(window);
        long cutoff = nowTicks - windowTicks;

        // Keep exactly one sample at or before the cutoff, so the window's start can be interpolated.
        while (samples.Count >= 2 && SecondTicks() <= cutoff)
            samples.Dequeue();

        (long oldestTicks, long oldestIndex) = samples.Peek();

        if (oldestTicks <= cutoff)
        {
            if (samples.Count < 2)
                return Math.Max(0, commitIndex - oldestIndex);

            (long nextTicks, long nextIndex) = SecondSample();
            double fraction = nextTicks == oldestTicks ? 1.0 : (double)(cutoff - oldestTicks) / (nextTicks - oldestTicks);
            long indexAtCutoff = oldestIndex + (long)Math.Floor(fraction * (nextIndex - oldestIndex));
            return Math.Max(0, commitIndex - indexAtCutoff);
        }

        long spanTicks = nowTicks - oldestTicks;
        if (spanTicks < ToTicks(MinExtrapolationSpan))
            return 0;

        double scaled = (double)(commitIndex - oldestIndex) * windowTicks / spanTicks;
        return scaled >= long.MaxValue ? long.MaxValue : (long)Math.Ceiling(scaled);
    }

    /// <summary>
    /// The effective budget in entries: the count floor, raised to the window's entries up to the cap.
    /// A count at or below 0 disables the live-replica hold entirely (0). A cap at or below 0 turns the
    /// window off, so the count applies alone.
    /// </summary>
    public static long Compose(long countBudget, long windowEntries, long cap)
    {
        if (countBudget <= 0)
            return 0;

        long timed = cap > 0 ? Math.Min(Math.Max(0, windowEntries), cap) : 0;
        return Math.Max(countBudget, timed);
    }

    private long SecondTicks() => SecondSample().Ticks;

    private (long Ticks, long CommitIndex) SecondSample()
    {
        using Queue<(long Ticks, long CommitIndex)>.Enumerator e = samples.GetEnumerator();
        e.MoveNext();
        e.MoveNext();
        return e.Current;
    }

    private static long ToTicks(TimeSpan span) =>
        (long)Math.Min(span.TotalSeconds * Stopwatch.Frequency, long.MaxValue / 4);
}
