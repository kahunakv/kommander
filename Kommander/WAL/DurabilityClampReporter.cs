
namespace Kommander.WAL;

/// <summary>
/// Rate-limits the "compaction clamped by the application-durability floor" diagnostic so a
/// healthy-but-lagging application flusher produces a handful of lines per streak instead of one
/// per compaction pass.
/// </summary>
/// <remarks>
/// <para><b>Why.</b> The per-pass warning was written for a stalled flusher: a clamped pass that
/// removes nothing, repeated forever, grows the WAL without bound. But the same condition is the
/// steady state of a loaded follower whose flusher lags the Raft log by hundreds of thousands of
/// entries and catches up in a saw-tooth: the floor is far below the checkpoint on every pass and
/// still moving. On the 1.6.x Raft-log soaks that fired ~7,500 times per follower in 22 minutes at
/// a 100-batch compaction cadence — noise that buried the line's one real signal, and the line
/// said "may be stalled" about a floor that was demonstrably advancing.</para>
///
/// <para><b>Contract.</b> A <em>streak</em> is a run of consecutive effective passes that were
/// clamped by the durability floor and removed nothing. Per streak the reporter asks the caller to
/// log:</para>
/// <list type="bullet">
///   <item>one <see cref="Report.StreakStarted"/> line on the first clamped pass (wording:
///   "clamped by", because a single pass cannot tell a lagging flusher from a stalled one);</item>
///   <item>one <see cref="Report.Stalled"/> line the first time the floor has not moved for a whole
///   report interval (wording: "blocked by … may be stalled"), re-armed once the floor moves again;</item>
///   <item>a <see cref="Report.Reminder"/> line at most once per report interval carrying the
///   current lag, the passes since the previous line and how far the floor advanced in between;</item>
///   <item>one <see cref="Report.StreakEnded"/> line on the first pass that is not a clamped
///   no-removal pass, so the log shows when the condition cleared and how long it lasted.</item>
/// </list>
///
/// <para>The stall verdict is time-based on purpose. At a 100-batch cadence passes run several
/// times per second while a flusher advances its floor on its own tick, so "unchanged since the
/// previous pass" is true several times per second on a perfectly healthy node; an unchanged
/// floor across a whole report interval is not.</para>
///
/// <para>Not thread-safe: called only from <c>RaftWriteAhead.RunCompactionPassAsync</c>, which
/// serializes passes through its in-flight flag.</para>
/// </remarks>
internal sealed class DurabilityClampReporter
{
    internal enum Report
    {
        None,
        StreakStarted,
        Stalled,
        Reminder,
        StreakEnded,
    }

    /// <summary>
    /// What the caller should log for the observed pass, with the figures the line carries.
    /// <paramref name="ClampedPasses"/> is the streak length including this pass (for
    /// <see cref="Report.StreakEnded"/>: the streak that just ended). <paramref name="PassesSinceLastReport"/>
    /// and <paramref name="FloorAdvanceSinceLastReport"/> cover the window since the previous
    /// reported line. <paramref name="Stalled"/> is true while the floor has not moved for a whole
    /// interval, which is what flips the wording from "clamped by" to "blocked by".
    /// </summary>
    internal readonly record struct Outcome(
        Report Kind,
        int ClampedPasses,
        int PassesSinceLastReport,
        long FloorAdvanceSinceLastReport,
        long StreakElapsedTicks,
        long FloorUnchangedTicks,
        bool Stalled);

    private readonly long reportIntervalTicks;

    private int clampedPasses;
    private int passesSinceReport;
    private long floorAtLastReport;
    private long lastReportTicks;
    private long streakStartTicks;

    private long lastFloor = long.MinValue;
    private long floorLastMovedTicks;
    private bool stalledReported;

    /// <param name="reportIntervalTicks">
    /// Minimum spacing between reminder lines and the window over which an unchanged floor counts
    /// as stalled, in tick-source ticks. Values &lt;= 0 disable both: only the streak-start and
    /// streak-end lines are emitted.
    /// </param>
    internal DurabilityClampReporter(long reportIntervalTicks)
    {
        this.reportIntervalTicks = reportIntervalTicks;
    }

    /// <summary>Clamped no-removal passes in the current streak; 0 when not in a streak.</summary>
    internal int ClampedPasses => clampedPasses;

    /// <summary>
    /// Records one effective compaction pass. <paramref name="clamped"/> is true when the pass was
    /// clamped by the durability floor and removed nothing; <paramref name="durabilityFloor"/> is
    /// the floor the pass observed (<see cref="long.MaxValue"/> when no floor applied).
    /// </summary>
    internal Outcome Observe(bool clamped, long durabilityFloor, long nowTicks)
    {
        bool floorMoved = durabilityFloor != lastFloor;
        lastFloor = durabilityFloor;

        if (!clamped)
        {
            if (clampedPasses == 0)
                return default;

            Outcome ended = new(
                Report.StreakEnded,
                clampedPasses,
                passesSinceReport,
                durabilityFloor == long.MaxValue ? 0 : durabilityFloor - floorAtLastReport,
                nowTicks - streakStartTicks,
                0,
                Stalled: false);

            clampedPasses = 0;
            passesSinceReport = 0;
            stalledReported = false;
            return ended;
        }

        clampedPasses++;
        passesSinceReport++;

        if (clampedPasses == 1)
        {
            // First clamped pass: the stall clock starts here. Whether the floor moved relative to
            // an earlier unclamped pass says nothing about the flusher's health.
            streakStartTicks = nowTicks;
            floorLastMovedTicks = nowTicks;
            return Emit(Report.StreakStarted, durabilityFloor, nowTicks, stalled: false);
        }

        if (floorMoved)
        {
            floorLastMovedTicks = nowTicks;
            stalledReported = false;
        }

        if (reportIntervalTicks <= 0)
            return default;

        long floorUnchangedTicks = nowTicks - floorLastMovedTicks;
        bool stalled = floorUnchangedTicks >= reportIntervalTicks;

        if (stalled && !stalledReported)
        {
            stalledReported = true;
            return Emit(Report.Stalled, durabilityFloor, nowTicks, stalled: true);
        }

        if (nowTicks - lastReportTicks >= reportIntervalTicks)
            return Emit(Report.Reminder, durabilityFloor, nowTicks, stalled);

        return default;
    }

    private Outcome Emit(Report kind, long durabilityFloor, long nowTicks, bool stalled)
    {
        Outcome outcome = new(
            kind,
            clampedPasses,
            passesSinceReport,
            kind == Report.StreakStarted ? 0 : durabilityFloor - floorAtLastReport,
            nowTicks - streakStartTicks,
            nowTicks - floorLastMovedTicks,
            stalled);

        passesSinceReport = 0;
        floorAtLastReport = durabilityFloor;
        lastReportTicks = nowTicks;
        return outcome;
    }
}
