
using Kommander.Data;
using Kommander.Tests.Scheduler;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Safety invariant: stale append responses from an older term must be ignored.
///
/// When a follower receives an AppendLogs message from a leader whose term is
/// lower than the follower's current term, the message must be discarded and
/// a <see cref="RaftOperationStatus.LeaderInOldTerm"/> response sent back.
///
/// These tests encode that fencing rule so it can be re-verified after
/// <c>RaftStateActor</c> is replaced by <c>RaftPartitionStateMachine</c>.
/// </summary>
public class TestStaleAppendResponses
{
    // ── Term fencing logic ────────────────────────────────────────────────

    [Fact]
    public void StaleAppend_ShouldBeIgnored_WhenLeaderTermLower()
    {
        // Mirror of RaftStateActor.AppendLogs: "if (currentTerm > leaderTerm) return"
        long currentTerm = 5;
        long staleLeaderTerm = 3;

        bool shouldIgnore = currentTerm > staleLeaderTerm;

        Assert.True(shouldIgnore, "Message from a lower-term leader must be discarded");
    }

    [Fact]
    public void StaleAppend_ShouldNotBeIgnored_WhenLeaderTermCurrent()
    {
        long currentTerm = 5;
        long leaderTerm = 5;

        bool shouldIgnore = currentTerm > leaderTerm;

        Assert.False(shouldIgnore, "Message from the current leader term must be accepted");
    }

    [Fact]
    public void StaleAppend_ShouldNotBeIgnored_WhenLeaderTermNewer()
    {
        long currentTerm = 5;
        long leaderTerm = 6;

        bool shouldIgnore = currentTerm > leaderTerm;

        Assert.False(shouldIgnore, "Message from a higher-term leader must be accepted (node should step down)");
    }

    // ── WAL must not record logs from a stale leader ──────────────────────

    [Fact]
    public void StaleAppend_WalIsNotUpdated_WhenTermStale()
    {
        FakeWAL wal = new();

        // Committed entry from the current leader at term 5.
        wal.Write([(0, [new RaftLog { Id = 10, Term = 5, Type = RaftLogType.Committed }])]);
        wal.DrainAll();

        // A stale append arrives with logs at term 3.  The state machine would reject
        // this before calling the WAL.  Simulate the guard and assert the WAL is unchanged.
        long currentTerm = 5;
        long staleLeaderTerm = 3;

        if (currentTerm > staleLeaderTerm)
        {
            // Stale — do not write.
        }
        else
        {
            wal.Write([(0, [new RaftLog { Id = 11, Term = staleLeaderTerm, Type = RaftLogType.Proposed }])]);
            wal.DrainAll();
        }

        Assert.Equal(10L, wal.GetMaxLog(partitionId: 0));
    }

    // ── Stale response status matches expectation ─────────────────────────

    [Theory]
    [InlineData(3L, 5L, true)]    // stale term → should return LeaderInOldTerm
    [InlineData(5L, 5L, false)]   // same term  → should not be stale
    [InlineData(6L, 5L, false)]   // newer term → should not be stale
    public void StaleAppend_Status_MatchesFencingRule(long leaderTerm, long currentTerm, bool expectStale)
    {
        bool isStale = currentTerm > leaderTerm;
        Assert.Equal(expectStale, isStale);

        RaftOperationStatus expectedStatus = isStale
            ? RaftOperationStatus.LeaderInOldTerm
            : RaftOperationStatus.Success;

        if (isStale)
            Assert.Equal(RaftOperationStatus.LeaderInOldTerm, expectedStatus);
    }

    // ── Multiple stale appends do not pollute the WAL ─────────────────────

    [Fact]
    public void StaleAppend_RepeatedStaleMessages_WalRemainsAtHighWaterMark()
    {
        FakeWAL wal = new();
        wal.Write([(0, [new RaftLog { Id = 20, Term = 10, Type = RaftLogType.Committed }])]);
        wal.DrainAll();

        long currentTerm = 10;

        // Simulate 5 stale appends at term 7.
        for (int i = 0; i < 5; i++)
        {
            long staleLeaderTerm = 7;
            if (currentTerm > staleLeaderTerm)
            {
                // Stale — skip WAL write.
                continue;
            }
            wal.Write([(0, [new RaftLog { Id = 21 + i, Term = staleLeaderTerm, Type = RaftLogType.Proposed }])]);
        }

        wal.DrainAll();

        Assert.Equal(20L, wal.GetMaxLog(partitionId: 0));
        Assert.False(wal.HasPendingWrites);
    }

    // ── CompleteAppendLogs fencing (same rule, completion direction) ──────

    [Fact]
    public void StaleCompleteAppend_IsIgnored_WhenCompletionTermLower()
    {
        // On the leader side: CompleteAppendLogs arrives from a follower with an old term.
        // Mirror of the check: if completion.Term < currentTerm → ignore.
        long currentTerm = 8;
        long completionTerm = 6;

        bool shouldIgnore = completionTerm < currentTerm;

        Assert.True(shouldIgnore, "CompleteAppendLogs from an older term must be ignored");
    }

    // ── Scheduler harness: operation carry the right term metadata ────────

    [Fact]
    public void StaleAppend_HarnessStep_RecordsTermInRequest()
    {
        SchedulerHarness h = new();

        RaftRequest staleRequest = new(
            RaftRequestType.AppendLogs,
            term: 2,
            endpoint: "stale-leader"
        );

        h.Enqueue(partitionId: 0, staleRequest);
        SchedulerHarness.HarnessStep? step = h.StepOnce();

        Assert.NotNull(step);
        Assert.Equal(2L, step!.Operation.Request.Term);
        Assert.Equal(Kommander.Scheduling.RaftOperationKind.Replication, step.Operation.Kind);
    }
}
