
using Kommander.Data;
using Kommander.System;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Tests the background checker's evaluation loop, including the resample-before-failing rule for
/// confirmation-required (transient) violations.
/// </summary>
public class TestClusterInvariantChecker
{
    private static ClusterView TwoLeaders() => new(1,
        [
            new RaftPartitionView("a:1", 1, RaftNodeState.Leader, 5, "a:1", 0, 0, 0, false, ClusterMemberRole.Voter),
            new RaftPartitionView("b:1", 1, RaftNodeState.Leader, 5, "b:1", 0, 0, 0, false, ClusterMemberRole.Voter),
        ], [], [], [], new Dictionary<string, long>());

    private static ClusterView OneLeader() => new(1,
        [
            new RaftPartitionView("a:1", 1, RaftNodeState.Leader, 5, "a:1", 0, 0, 0, false, ClusterMemberRole.Voter),
            new RaftPartitionView("b:1", 1, RaftNodeState.Follower, 5, "a:1", 0, 0, 0, false, ClusterMemberRole.Voter),
        ], [], [], [], new Dictionary<string, long>());

    [Fact]
    public async Task PersistentViolation_IsConfirmedAndRecorded()
    {
        await using ClusterInvariantChecker checker = new(
            _ => Task.FromResult(TwoLeaders()), pollInterval: TimeSpan.FromMilliseconds(20));
        checker.Start();

        await WaitUntil(() => checker.FirstViolation is not null, 3000);
        Assert.NotNull(checker.FirstViolation);
        Assert.Equal("ElectionSafety", checker.FirstViolation!.Invariant);
    }

    [Fact]
    public async Task TransientViolation_ThatClearsOnResample_IsNotRecorded()
    {
        int seq = 0;
        // Every odd sample shows two leaders; the resample (next call) shows one → the checker discards it.
        await using ClusterInvariantChecker checker = new(
            _ => Task.FromResult(Interlocked.Increment(ref seq) % 2 == 1 ? TwoLeaders() : OneLeader()),
            pollInterval: TimeSpan.FromMilliseconds(20));
        checker.Start();

        await Task.Delay(500);
        Assert.Null(checker.FirstViolation);
    }

    [Fact]
    public async Task HistoricalViolation_FailsImmediately_NoResampleNeeded()
    {
        // Commit monotonicity is historical (RequiresConfirmation=false): recorded on the first bad sample.
        ClusterView regressed = new(1,
            [new RaftPartitionView("a:1", 1, RaftNodeState.Follower, 5, "b:1", 3, 0, 0, false, ClusterMemberRole.Voter)],
            [], [], [], new Dictionary<string, long>());

        // First sample establishes commit=10; second regresses to 3.
        int seq = 0;
        await using ClusterInvariantChecker checker = new(_ =>
        {
            int n = Interlocked.Increment(ref seq);
            ClusterView v = n == 1
                ? new ClusterView(1, [new RaftPartitionView("a:1", 1, RaftNodeState.Follower, 5, "b:1", 10, 0, 0, false, ClusterMemberRole.Voter)], [], [], [], new Dictionary<string, long>())
                : regressed;
            return Task.FromResult(v);
        }, pollInterval: TimeSpan.FromMilliseconds(20));
        checker.Start();

        await WaitUntil(() => checker.FirstViolation is not null, 3000);
        Assert.Equal("CommitMonotonicity", checker.FirstViolation!.Invariant);
    }

    private static async Task WaitUntil(Func<bool> cond, int timeoutMs)
    {
        for (int elapsed = 0; elapsed < timeoutMs && !cond(); elapsed += 20)
            await Task.Delay(20);
    }
}
