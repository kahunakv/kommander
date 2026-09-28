using System.Diagnostics;
using Kommander.WAL;

namespace Kommander.Tests.WAL;

/// <summary>
/// Covers <see cref="LiveReplicaRetentionBudget"/>: the live-replica retention sized in time from the
/// leader's commit history, and the composition with the count and the cap.
///
/// <para>The motivating numbers are the Caraxes fault soak on the round-cost stack: ~27,000 entries/s,
/// a 1,000,000-entry count that was then ~37 s of log, and a replica that came back 1.7 M entries
/// behind after a 30-second kill.</para>
/// </summary>
public sealed class TestLiveReplicaRetentionBudget
{
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(3);

    private static long Ms(long ms) => ms * Stopwatch.Frequency / 1000;

    private const long Start = 1_000_000;

    /// <summary>
    /// Heartbeats every 100 ms at a steady rate: once the history covers the window, the budget is
    /// the entries committed in the window, whatever the sampling.
    /// </summary>
    [Fact]
    public void SteadyRate_IsTheEntriesCommittedInTheWindow()
    {
        LiveReplicaRetentionBudget budget = new();
        const long entriesPerSecond = 27_000;

        long result = 0;
        for (long ms = 0; ms <= 300_000; ms += 100)
            result = budget.Observe(Start + Ms(ms), ms * entriesPerSecond / 1000, Window);

        long expected = entriesPerSecond * (long)Window.TotalSeconds;
        Assert.InRange(result, expected - entriesPerSecond, expected + entriesPerSecond);

        // The history is bounded by the window, not the run: about one sample per second.
        Assert.InRange(budget.SampleCount, 2, (int)Window.TotalSeconds + 2);
    }

    /// <summary>
    /// The soak's numbers: 1,000,000 entries was ~37 s of log at ~27,000 entries/s. The three-minute
    /// window keeps ~4.9 M entries, which covers the 1.7 M the killed replica came back behind.
    /// </summary>
    [Fact]
    public void AtTheFaultSoakRate_TheBudgetCoversTheKilledReplicasGap()
    {
        LiveReplicaRetentionBudget budget = new();

        long windowEntries = 0;
        for (long ms = 0; ms <= 240_000; ms += 250)
            windowEntries = budget.Observe(Start + Ms(ms), 34_000_000 + ms * 27, Window);

        long effective = LiveReplicaRetentionBudget.Compose(1_000_000, windowEntries, 10_000_000);

        Assert.True(effective > 1_700_000, $"budget {effective} does not cover a 1.7 M-entry gap");
        Assert.InRange(effective, 4_800_000, 4_900_000);
    }

    /// <summary>
    /// A fresh leader has less history than the window. It extrapolates from what it has, once that
    /// covers the minimum span, and contributes nothing before (the count applies alone).
    /// </summary>
    [Fact]
    public void ShortHistory_Extrapolates_AfterTheMinimumSpan()
    {
        LiveReplicaRetentionBudget budget = new();

        Assert.Equal(0, budget.Observe(Start, 5_000, Window));
        Assert.Equal(0, budget.Observe(Start + Ms(100), 5_100, Window));

        // 10 s at 1,000 entries/s: extrapolated to 180,000 over three minutes.
        long result = 0;
        for (long ms = 200; ms <= 10_000; ms += 100)
            result = budget.Observe(Start + Ms(ms), 5_000 + ms, Window);

        Assert.InRange(result, 179_000, 181_000);
    }

    /// <summary>
    /// A gap in the history (this node was not leader, or was quiesced) is interpolated at the
    /// window's start rather than counted in full. The commit index still counts every commit in
    /// between, whoever led it.
    /// </summary>
    [Fact]
    public void GapInHistory_IsInterpolatedAtTheWindowStart()
    {
        LiveReplicaRetentionBudget budget = new();

        budget.Observe(Start, 0, Window);
        budget.Observe(Start + Ms(1_000), 1_000, Window);

        // Ten minutes later, 600,000 entries on: 1,000 entries/s across the gap. The window holds the
        // last three minutes of it, not all ten.
        long result = budget.Observe(Start + Ms(601_000), 601_000, Window);

        Assert.InRange(result, 179_000, 181_000);
    }

    /// <summary>
    /// A commit index that moves backwards (this node's log re-seeded by a snapshot) resets the history
    /// instead of producing a negative or inflated count.
    /// </summary>
    [Fact]
    public void CommitIndexRegression_ResetsTheHistory()
    {
        LiveReplicaRetentionBudget budget = new();

        for (long ms = 0; ms <= 5_000; ms += 100)
            budget.Observe(Start + Ms(ms), 100_000 + ms * 10, Window);

        Assert.Equal(0, budget.Observe(Start + Ms(5_100), 10, Window));
        Assert.Equal(1, budget.SampleCount);
    }

    /// <summary>A zero window turns the time term off and drops the history.</summary>
    [Fact]
    public void DisabledWindow_ContributesNothing()
    {
        LiveReplicaRetentionBudget budget = new();

        for (long ms = 0; ms <= 5_000; ms += 100)
            budget.Observe(Start + Ms(ms), ms * 10, Window);

        Assert.Equal(0, budget.Observe(Start + Ms(5_100), 51_000, TimeSpan.Zero));
        Assert.Equal(0, budget.SampleCount);
    }

    /// <summary>
    /// The composition, and which bound wins: the count is a floor that always holds, the window
    /// raises it up to the cap, a count above the cap keeps the count, a count of 0 disables the hold,
    /// and a cap of 0 turns the window off.
    /// </summary>
    [Theory]
    [InlineData(1_000_000L, 4_860_000L, 10_000_000L, 4_860_000L)]
    [InlineData(1_000_000L, 400_000L, 10_000_000L, 1_000_000L)]
    [InlineData(1_000_000L, 40_000_000L, 10_000_000L, 10_000_000L)]
    [InlineData(20_000_000L, 40_000_000L, 10_000_000L, 20_000_000L)]
    [InlineData(0L, 4_860_000L, 10_000_000L, 0L)]
    [InlineData(1_000_000L, 4_860_000L, 0L, 1_000_000L)]
    [InlineData(1_000_000L, -5L, 10_000_000L, 1_000_000L)]
    public void Compose_CountIsTheFloor_TheCapBoundsTheWindow(long count, long windowEntries, long cap, long expected)
    {
        Assert.Equal(expected, LiveReplicaRetentionBudget.Compose(count, windowEntries, cap));
    }

    /// <summary>Composing an already composed budget changes nothing (the WAL re-clamps what the leader published).</summary>
    [Theory]
    [InlineData(1_000_000L, 4_860_000L, 10_000_000L)]
    [InlineData(20_000_000L, 40_000_000L, 10_000_000L)]
    [InlineData(1_000_000L, 40_000_000L, 10_000_000L)]
    [InlineData(1_000_000L, 4_860_000L, 0L)]
    public void Compose_IsIdempotent(long count, long windowEntries, long cap)
    {
        long once = LiveReplicaRetentionBudget.Compose(count, windowEntries, cap);
        Assert.Equal(once, LiveReplicaRetentionBudget.Compose(count, once, cap));
    }
}
