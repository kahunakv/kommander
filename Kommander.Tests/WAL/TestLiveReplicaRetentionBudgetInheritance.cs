using System.Diagnostics;
using Kommander.WAL;

namespace Kommander.Tests.WAL;

/// <summary>
/// A node that wins an election has no commit history of its own, so its rate-scaled budget answers
/// the configured minimum for a full window; a replica its predecessor was holding the log for was
/// compacted past in that gap (a CamusDB fault soak: the successor's first retained entry sat exactly
/// one million entries below its checkpoint while the follower, just seeded, needed 125,000 more).
/// The depth inherited from the predecessor stands in until the window has history of its own.
/// </summary>
public class TestLiveReplicaRetentionBudgetInheritance
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(10);

    private static long Ticks(double seconds) => (long)(seconds * Stopwatch.Frequency);

    [Fact]
    public void InheritedDepth_StandsIn_UntilTheWindowElapses()
    {
        LiveReplicaRetentionBudget budget = new();
        long now = Ticks(1_000);

        budget.Inherit(5_000, now);

        // No history yet: the inherited depth is the answer.
        Assert.Equal(5_000, budget.Observe(now, commitIndex: 100, Window));
        Assert.Equal(5_000, budget.InheritedEntries);

        // Half a window later, still standing in.
        Assert.Equal(5_000, budget.Observe(now + Ticks(5), commitIndex: 200, Window));

        // A full window later the inherited depth lapses and the window's own history answers.
        long observed = budget.Observe(now + Ticks(10), commitIndex: 300, Window);
        Assert.Equal(0, budget.InheritedEntries);
        Assert.True(observed < 5_000, $"own history answers after the window: {observed}");
    }

    [Fact]
    public void OwnHistoryAboveTheInheritedDepth_Wins()
    {
        LiveReplicaRetentionBudget budget = new();
        long now = Ticks(1_000);

        budget.Inherit(10, now);
        budget.Observe(now, commitIndex: 0, Window);

        // Five seconds at 1,000 entries/s: the window extrapolates well above the inherited 10.
        long observed = budget.Observe(now + Ticks(5), commitIndex: 5_000, Window);
        Assert.True(observed > 10, $"the window's own history must win: {observed}");
    }

    [Fact]
    public void NothingInherited_OrADisabledWindow_LeavesTheBudgetAlone()
    {
        LiveReplicaRetentionBudget budget = new();
        long now = Ticks(1_000);

        Assert.Equal(0, budget.Observe(now, commitIndex: 100, Window));

        budget.Inherit(0, now);
        Assert.Equal(0, budget.Observe(now, commitIndex: 100, Window));

        budget.Inherit(5_000, now);
        Assert.Equal(0, budget.Observe(now, commitIndex: 100, TimeSpan.Zero));
        Assert.Equal(0, budget.InheritedEntries);
    }
}
