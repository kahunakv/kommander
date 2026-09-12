
using Kommander.WAL;

namespace Kommander.Tests.WAL;

/// <summary>
/// Pins the rate limit on the "compaction clamped by the application-durability floor" line.
///
/// <para>The line used to fire on every clamped compaction pass. On the 1.6.x Raft-log soaks a
/// loaded follower's floor sat hundreds of thousands of entries behind the checkpoint for the whole
/// run while still moving, so the "may be stalled" Warning fired ~7,500 times per follower in 22
/// minutes about a flusher that was not stalled. The contract is now: one line when a streak
/// starts, one per report interval while it lasts, "blocked … may be stalled" only once the floor
/// has not moved for a whole interval, and one line when it clears.</para>
/// </summary>
public sealed class TestDurabilityClampReporter
{
    private const long Interval = 1_000;

    private static DurabilityClampReporter.Outcome Pass(DurabilityClampReporter r, bool clamped, long floor, long now) =>
        r.Observe(clamped, floor, now);

    [Fact]
    public void UnclampedPasses_ReportNothing()
    {
        DurabilityClampReporter reporter = new(Interval);

        for (int i = 0; i < 10; i++)
            Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, clamped: false, floor: long.MaxValue, now: i).Kind);

        Assert.Equal(0, reporter.ClampedPasses);
    }

    /// <summary>
    /// The soak shape: thousands of clamped passes at sub-second spacing with a floor that keeps
    /// advancing. One line at the start, then one reminder per interval, never "stalled".
    /// </summary>
    [Fact]
    public void MovingFloor_LogsOncePerStreakThenOncePerInterval_NeverStalled()
    {
        DurabilityClampReporter reporter = new(Interval);

        List<(long Now, DurabilityClampReporter.Outcome Outcome)> reported = [];
        long floor = 100;

        // 5,000 passes, 5 per interval-unit, floor advancing every pass.
        for (int pass = 0; pass < 5_000; pass++)
        {
            long now = pass * (Interval / 5);
            floor += 10;

            DurabilityClampReporter.Outcome outcome = Pass(reporter, clamped: true, floor, now);
            if (outcome.Kind != DurabilityClampReporter.Report.None)
                reported.Add((now, outcome));
        }

        Assert.Equal(DurabilityClampReporter.Report.StreakStarted, reported[0].Outcome.Kind);
        Assert.Equal(1, reported[0].Outcome.ClampedPasses);

        // Every later line is a reminder, at least one interval after the previous one.
        for (int i = 1; i < reported.Count; i++)
        {
            Assert.Equal(DurabilityClampReporter.Report.Reminder, reported[i].Outcome.Kind);
            Assert.False(reported[i].Outcome.Stalled, "an advancing floor must never be called stalled");
            Assert.True(reported[i].Now - reported[i - 1].Now >= Interval);
            Assert.True(reported[i].Outcome.FloorAdvanceSinceLastReport > 0);
            Assert.Equal(5, reported[i].Outcome.PassesSinceLastReport);
        }

        // 5,000 passes spanning 1,000 intervals collapse to ~1,000 lines, not 5,000.
        Assert.InRange(reported.Count, 999, 1_001);
        Assert.Equal(5_000, reporter.ClampedPasses);
    }

    /// <summary>
    /// A floor that stops moving is the stall the Warning exists for: reported once, promptly
    /// after a whole interval without movement, and not again until the floor moves and stalls
    /// once more.
    /// </summary>
    [Fact]
    public void StuckFloor_ReportsStalledOnceAfterAnInterval_ThenRemindsAsBlocked()
    {
        DurabilityClampReporter reporter = new(Interval);

        Assert.Equal(DurabilityClampReporter.Report.StreakStarted, Pass(reporter, true, floor: 50, now: 0).Kind);

        // Unchanged floor, passes every tenth of an interval: nothing until a whole interval elapsed.
        for (long now = Interval / 10; now < Interval; now += Interval / 10)
            Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, true, floor: 50, now).Kind);

        DurabilityClampReporter.Outcome stalled = Pass(reporter, true, floor: 50, now: Interval);
        Assert.Equal(DurabilityClampReporter.Report.Stalled, stalled.Kind);
        Assert.True(stalled.Stalled);
        Assert.True(stalled.FloorUnchangedTicks >= Interval);
        Assert.Equal(11, stalled.ClampedPasses);

        // Still stuck: no second "stalled" line; the periodic reminder says "blocked" instead.
        for (long now = Interval + Interval / 10; now < 2 * Interval; now += Interval / 10)
            Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, true, floor: 50, now).Kind);

        DurabilityClampReporter.Outcome reminder = Pass(reporter, true, floor: 50, now: 2 * Interval);
        Assert.Equal(DurabilityClampReporter.Report.Reminder, reminder.Kind);
        Assert.True(reminder.Stalled);
        Assert.Equal(0, reminder.FloorAdvanceSinceLastReport);

        // The floor moves again: the stall re-arms, and a later stall is reported afresh.
        Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, true, floor: 60, now: 2 * Interval + 1).Kind);
        Assert.Equal(DurabilityClampReporter.Report.Stalled, Pass(reporter, true, floor: 60, now: 3 * Interval + 1).Kind);
    }

    /// <summary>
    /// A single unchanged pass is not a stall. At a 100-batch cadence passes run several times per
    /// second while the flusher ticks on its own schedule, so consecutive passes routinely see the
    /// same floor on a healthy node.
    /// </summary>
    [Fact]
    public void FloorUnchangedForLessThanAnInterval_IsNotCalledStalled()
    {
        DurabilityClampReporter reporter = new(Interval);

        Pass(reporter, true, floor: 10, now: 0);
        Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, true, floor: 10, now: 1).Kind);
        Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, true, floor: 10, now: 2).Kind);

        DurabilityClampReporter.Outcome reminder = Pass(reporter, true, floor: 20, now: Interval);
        Assert.Equal(DurabilityClampReporter.Report.Reminder, reminder.Kind);
        Assert.False(reminder.Stalled);
    }

    [Fact]
    public void StreakEnd_ReportsOnceWithTheStreakLength_AndANewStreakStartsFresh()
    {
        DurabilityClampReporter reporter = new(Interval);

        Pass(reporter, true, floor: 10, now: 0);
        Pass(reporter, true, floor: 20, now: 10);
        Pass(reporter, true, floor: 30, now: 20);

        DurabilityClampReporter.Outcome ended = Pass(reporter, clamped: false, floor: 40, now: 30);
        Assert.Equal(DurabilityClampReporter.Report.StreakEnded, ended.Kind);
        Assert.Equal(3, ended.ClampedPasses);
        Assert.Equal(30, ended.StreakElapsedTicks);
        Assert.Equal(0, reporter.ClampedPasses);

        // No repeat of the end line while unclamped.
        Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, false, floor: 50, now: 40).Kind);

        // The next clamped pass is a fresh streak: its own start line, count restarted.
        DurabilityClampReporter.Outcome restarted = Pass(reporter, true, floor: 50, now: 50);
        Assert.Equal(DurabilityClampReporter.Report.StreakStarted, restarted.Kind);
        Assert.Equal(1, restarted.ClampedPasses);
    }

    /// <summary>Interval &lt;= 0 keeps only the start and end lines.</summary>
    [Fact]
    public void DisabledInterval_KeepsOnlyStreakStartAndEnd()
    {
        DurabilityClampReporter reporter = new(0);

        Assert.Equal(DurabilityClampReporter.Report.StreakStarted, Pass(reporter, true, floor: 10, now: 0).Kind);

        for (long now = 1; now < 100_000; now += 1_000)
            Assert.Equal(DurabilityClampReporter.Report.None, Pass(reporter, true, floor: 10, now).Kind);

        Assert.Equal(DurabilityClampReporter.Report.StreakEnded, Pass(reporter, false, floor: long.MaxValue, now: 100_000).Kind);
    }
}
