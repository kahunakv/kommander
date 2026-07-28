
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
        // NoCommittedRollback is historical (RequiresConfirmation=false): a committed entry replaced by a
        // different digest is real data loss that cannot self-heal, so it is recorded on the first bad sample
        // without a confirmation resample. (Commit monotonicity, by contrast, is now confirmation-required
        // because its gap-aware frontier can dip transiently.)
        ClusterView rollback = await RollbackViewAsync();

        await using ClusterInvariantChecker checker = new(
            _ => Task.FromResult(rollback), pollInterval: TimeSpan.FromMilliseconds(20));
        checker.Start();

        await WaitUntil(() => checker.FirstViolation is not null, 3000);
        Assert.Equal("NoCommittedRollback", checker.FirstViolation!.Invariant);
    }

    /// <summary>Builds a view where node a:1 holds a different digest at a committed index than the observed commit.</summary>
    private static async Task<ClusterView> RollbackViewAsync()
    {
        HashChainStateMachine committedRef = new("ref", 1);
        foreach (RaftLog l in new[] { Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [30]) })
            await committedRef.OnReplicationReceived(1, l);
        ulong committedDigest = committedRef.Snapshot().MetaByIndex[3].EntryDigest;

        HashChainStateMachine replaced = new("a:1", 1);
        foreach (RaftLog l in new[] { Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [99]) }) // idx 3 differs
            await replaced.OnReplicationReceived(1, l);

        return new ClusterView(1, [], [replaced.Snapshot()],
            [new CommitObservation(1, 3, 1, committedDigest)], [], new Dictionary<string, long>());
    }

    private static RaftLog Log(long id, long term, byte[] payload) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "chaos", LogData = payload };

    private static async Task WaitUntil(Func<bool> cond, int timeoutMs)
    {
        for (int elapsed = 0; elapsed < timeoutMs && !cond(); elapsed += 20)
            await Task.Delay(20);
    }
}
