
using Kommander.Data;
using Kommander.Tests.Scheduler;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Safety invariant: after a network partition heals, all nodes that were
/// followers must converge to the winning leader's committed log.
///
/// During a partition, a follower may accumulate proposed (but uncommitted)
/// entries that conflict with the leader's view.  When the partition heals the
/// follower's conflicting entries must be discarded and replaced with the
/// leader's committed entries.
///
/// The tests here operate at the WAL layer: they simulate what the Raft
/// AppendLogs / rollback / reapply flow must produce in the WAL after healing.
/// </summary>
public class TestLogMatchingAfterPartitionHeal
{
    // ── Basic log matching ────────────────────────────────────────────────

    [Fact]
    public void LogMatching_FollowerConvergesToLeaderAfterHeal()
    {
        // Leader WAL: entries 1–5 all committed.
        FakeWAL leader = new();
        leader.Write([(0, CommittedLogs(1, [1, 2, 3, 4, 5]))]);
        leader.DrainAll();

        // Follower WAL during partition: 1–3 committed, then 4–5 proposed (diverged).
        FakeWAL follower = new();
        follower.Write([(0, CommittedLogs(1, [1, 2, 3]))]);
        follower.Write([(0, ProposedLogs(2, [4, 5]))]);   // stale proposed from old term
        follower.DrainAll();

        // Simulate partition heal: overwrite follower's entries 4–5 with leader's committed versions.
        follower.Write([(0, CommittedLogs(1, [4, 5]))]);
        follower.DrainAll();

        // Both WALs must agree on the committed log set.
        RaftSafetyAssert.WalsAgreeOnCommittedLogs(leader, follower, partitionId: 0);
        RaftSafetyAssert.CommittedLogsAreMonotonic(follower, partitionId: 0);
    }

    [Fact]
    public void LogMatching_LeaderCommits_FollowerProposedEntryIsReplaced()
    {
        FakeWAL follower = new();

        // Follower proposed log 3 under term 2 (stale leader).
        follower.Write([(0, [new() { Id = 3, Term = 2, Type = RaftLogType.Proposed }])]);
        follower.DrainAll();

        Assert.Equal(RaftLogType.Proposed, follower.ReadLogs(0).First(l => l.Id == 3).Type);

        // New leader overwrites entry 3 as committed under term 3.
        follower.Write([(0, [new() { Id = 3, Term = 3, Type = RaftLogType.Committed }])]);
        follower.DrainAll();

        RaftLog entry = follower.ReadLogs(0).First(l => l.Id == 3);
        Assert.Equal(RaftLogType.Committed, entry.Type);
        Assert.Equal(3L, entry.Term);
    }

    // ── Logs from diverged terms are replaced ─────────────────────────────

    [Fact]
    public void LogMatching_DivergentFollowerLogs_AreOverwrittenByLeader()
    {
        FakeWAL wal = new();

        // Diverged follower state: 1,2,3 committed, then 4,5 under a stale term.
        wal.Write([(1, CommittedLogs(1, [1, 2, 3]))]);
        wal.Write([(1, ProposedLogs(99, [4, 5]))]);  // stale term that never became leader
        wal.DrainAll();

        // Healing: leader (term 2) commits its authoritative 4 and 5.
        wal.Write([(1, CommittedLogs(2, [4, 5]))]);
        wal.DrainAll();

        List<RaftLog> logs = wal.ReadLogs(partitionId: 1);

        // Entry 4 and 5 must now be Committed under term 2.
        Assert.Equal(RaftLogType.Committed, logs.First(l => l.Id == 4).Type);
        Assert.Equal(2L, logs.First(l => l.Id == 4).Term);
        Assert.Equal(RaftLogType.Committed, logs.First(l => l.Id == 5).Type);

        RaftSafetyAssert.CommittedLogsAreMonotonic(wal, partitionId: 1);
        RaftSafetyAssert.CommittedTermsAreNonDecreasing(wal, partitionId: 1);
    }

    // ── Log matching across multiple partitions ───────────────────────────

    [Fact]
    public void LogMatching_MultiplePartitions_EachHealsIndependently()
    {
        FakeWAL leader = new();
        FakeWAL follower = new();

        // Partition 0: fully committed on leader, diverged on follower.
        leader.Write([(0, CommittedLogs(1, [1, 2, 3]))]);
        follower.Write([(0, CommittedLogs(1, [1, 2]))]);
        follower.Write([(0, ProposedLogs(5, [3]))]);

        // Partition 1: in sync.
        leader.Write([(1, CommittedLogs(1, [10, 11]))]);
        follower.Write([(1, CommittedLogs(1, [10, 11]))]);

        leader.DrainAll();
        follower.DrainAll();

        // Heal partition 0.
        follower.Write([(0, CommittedLogs(1, [3]))]);
        follower.DrainAll();

        RaftSafetyAssert.WalsAgreeOnCommittedLogs(leader, follower, partitionId: 0);
        RaftSafetyAssert.WalsAgreeOnCommittedLogs(leader, follower, partitionId: 1);
    }

    // ── Empty follower catches up from scratch ────────────────────────────

    [Fact]
    public void LogMatching_EmptyFollower_CatchesUpToLeader()
    {
        FakeWAL leader = new();
        leader.Write([(0, CommittedLogs(2, [1, 2, 3, 4, 5]))]);
        leader.DrainAll();

        FakeWAL newFollower = new();
        // Follower has no logs yet.
        Assert.Equal(0L, newFollower.GetMaxLog(partitionId: 0));

        // Replay leader's committed entries onto the new follower.
        List<RaftLog> leaderLogs = leader.ReadLogs(partitionId: 0);
        newFollower.Write([(0, leaderLogs)]);
        newFollower.DrainAll();

        RaftSafetyAssert.WalsAgreeOnCommittedLogs(leader, newFollower, partitionId: 0);
    }

    // ── Helpers ───────────────────────────────────────────────────────────
    // Use explicit array syntax to avoid C# params overload ambiguity.

    private static List<RaftLog> CommittedLogs(long term, long[] ids) =>
        ids.Select(id => new RaftLog { Id = id, Term = term, Type = RaftLogType.Committed }).ToList();

    private static List<RaftLog> ProposedLogs(long term, long[] ids) =>
        ids.Select(id => new RaftLog { Id = id, Term = term, Type = RaftLogType.Proposed }).ToList();
}
