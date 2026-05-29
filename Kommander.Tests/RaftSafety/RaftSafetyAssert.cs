
using Kommander.Data;
using Kommander.Tests.Scheduler;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Shared assertion helpers for Raft safety regression tests.
///
/// These helpers express the non-negotiable correctness rules from the refactor plan
/// in terms that can be asserted against <see cref="FakeWAL"/> state.  They are
/// intentionally static so every test class can use them without inheritance.
/// </summary>
public static class RaftSafetyAssert
{
    /// <summary>
    /// Verifies that all committed log entries for a partition have strictly increasing
    /// IDs — the Raft log-matching / commit-monotonicity invariant.
    /// </summary>
    public static void CommittedLogsAreMonotonic(FakeWAL wal, int partitionId)
    {
        List<RaftLog> logs = wal.ReadLogs(partitionId);
        List<RaftLog> committed = logs
            .Where(l => l.Type == RaftLogType.Committed || l.Type == RaftLogType.CommittedCheckpoint)
            .OrderBy(l => l.Id)
            .ToList();

        for (int i = 1; i < committed.Count; i++)
        {
            Assert.True(
                committed[i].Id > committed[i - 1].Id,
                $"Partition {partitionId}: committed log IDs are not monotonically increasing: " +
                $"{committed[i - 1].Id} >= {committed[i].Id}");
        }
    }

    /// <summary>
    /// Verifies that every committed log entry has a term that is >= the term of the
    /// previous committed entry (terms must be non-decreasing within committed logs).
    /// </summary>
    public static void CommittedTermsAreNonDecreasing(FakeWAL wal, int partitionId)
    {
        List<RaftLog> logs = wal.ReadLogs(partitionId);
        List<RaftLog> committed = logs
            .Where(l => l.Type == RaftLogType.Committed || l.Type == RaftLogType.CommittedCheckpoint)
            .OrderBy(l => l.Id)
            .ToList();

        for (int i = 1; i < committed.Count; i++)
        {
            Assert.True(
                committed[i].Term >= committed[i - 1].Term,
                $"Partition {partitionId}: committed log term decreased: " +
                $"entry {committed[i].Id} has term {committed[i].Term} < previous term {committed[i - 1].Term}");
        }
    }

    /// <summary>
    /// Verifies that no RolledBack entry has the same log ID as a Committed entry for
    /// the same partition.  A log that was committed must never appear rolled back.
    /// </summary>
    public static void NoCommittedLogIsRolledBack(FakeWAL wal, int partitionId)
    {
        List<RaftLog> logs = wal.ReadLogs(partitionId);

        HashSet<long> committedIds = logs
            .Where(l => l.Type == RaftLogType.Committed || l.Type == RaftLogType.CommittedCheckpoint)
            .Select(l => l.Id)
            .ToHashSet();

        foreach (RaftLog log in logs.Where(l => l.Type == RaftLogType.RolledBack || l.Type == RaftLogType.RolledBackCheckpoint))
        {
            Assert.False(
                committedIds.Contains(log.Id),
                $"Partition {partitionId}: log entry {log.Id} appears both Committed and RolledBack.");
        }
    }

    /// <summary>
    /// Verifies that two WAL instances agree on the set of committed log IDs for a
    /// partition.  Used to assert log matching after a simulated partition heal.
    /// </summary>
    public static void WalsAgreeOnCommittedLogs(FakeWAL wal1, FakeWAL wal2, int partitionId)
    {
        HashSet<long> ids1 = CommittedIds(wal1, partitionId);
        HashSet<long> ids2 = CommittedIds(wal2, partitionId);

        Assert.True(
            ids1.SetEquals(ids2),
            $"Partition {partitionId}: WAL committed log sets differ. " +
            $"Only in WAL1: [{string.Join(',', ids1.Except(ids2))}] " +
            $"Only in WAL2: [{string.Join(',', ids2.Except(ids1))}]");
    }

    private static HashSet<long> CommittedIds(FakeWAL wal, int partitionId) =>
        wal.ReadLogs(partitionId)
           .Where(l => l.Type == RaftLogType.Committed || l.Type == RaftLogType.CommittedCheckpoint)
           .Select(l => l.Id)
           .ToHashSet();

    /// <summary>
    /// Computes the Raft quorum threshold for a cluster of <paramref name="totalNodes"/>.
    /// Mirrors the formula used in <c>RaftStateActor.ReceivedVote</c>:
    /// <c>Math.Max(2, Math.Floor((totalNodes) / 2f) + 1)</c>.
    /// </summary>
    public static int Quorum(int totalNodes) =>
        Math.Max(2, (int)Math.Floor(totalNodes / 2f) + 1);
}
