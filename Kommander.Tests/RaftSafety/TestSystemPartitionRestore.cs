
using Kommander.Data;
using Kommander.Tests.Scheduler;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Safety invariant: the system partition must be fully restored before any
/// user partition is activated.
///
/// Partition 0 is reserved for Kommander's system configuration (partition
/// topology, membership changes).  User partitions must not begin accepting
/// normal Raft operations until the system partition has completed its WAL
/// restore and published its committed state.
///
/// These tests model the restore gate using <see cref="FakeWAL"/> and the
/// <see cref="SchedulerHarness"/>, verifying the ordering invariant that will
/// be enforced by <c>RaftPartitionExecutor</c> once the actor layer is replaced.
/// </summary>
public class TestSystemPartitionRestore
{
    private const int SystemPartitionId = 0;

    // ── Restore ordering ──────────────────────────────────────────────────

    [Fact]
    public void SystemRestore_CompletesBeforeUserPartitionReadsCommittedState()
    {
        FakeWAL wal = new();

        // Write system partition entries (pending) and user partition entries (pending).
        wal.Write([(SystemPartitionId, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed }])]);
        wal.Write([(1, [new RaftLog { Id = 100, Term = 1, Type = RaftLogType.Committed }])]);

        // Neither partition is readable yet — both writes are pending.
        Assert.Equal(0L, wal.GetMaxLog(SystemPartitionId));
        Assert.Equal(0L, wal.GetMaxLog(partitionId: 1));

        // Drain system partition first.
        wal.DrainOne(); // drains the system write
        Assert.Equal(1L, wal.GetMaxLog(SystemPartitionId));
        Assert.Equal(0L, wal.GetMaxLog(partitionId: 1)); // user still pending

        // Gate: user partition should not be considered restored until its own drain completes.
        bool systemRestored = wal.GetMaxLog(SystemPartitionId) > 0;
        bool userRestored = wal.GetMaxLog(partitionId: 1) > 0;

        Assert.True(systemRestored);
        Assert.False(userRestored, "User partition must not be considered restored before its WAL drain completes");

        // Drain user partition.
        wal.DrainOne();
        Assert.Equal(100L, wal.GetMaxLog(partitionId: 1));
    }

    [Fact]
    public void SystemRestore_UserPartitionHasNoPendingOps_UntilSystemIsRestored()
    {
        // Model: Maintenance operations (restore) must complete before Client/Control ops.
        SchedulerHarness h = new();

        // Enqueue restore for system partition (Maintenance kind).
        RaftRequest sysRestoreReq = new(RaftRequestType.CheckLeader);
        h.Enqueue(SystemPartitionId, sysRestoreReq);

        // Enqueue user operation.
        RaftRequest userOp = new(RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: true);
        h.Enqueue(partitionId: 1, userOp);

        // Execute in order — system restore first.
        SchedulerHarness.HarnessStep? step1 = h.StepOnce();
        SchedulerHarness.HarnessStep? step2 = h.StepOnce();

        Assert.NotNull(step1);
        Assert.Equal(SystemPartitionId, step1!.Operation.PartitionId);

        Assert.NotNull(step2);
        Assert.Equal(1, step2!.Operation.PartitionId);
    }

    // ── WAL restore is idempotent ─────────────────────────────────────────

    [Fact]
    public void SystemRestore_ReplayingWalTwice_IsIdempotent()
    {
        FakeWAL wal = new();

        List<RaftLog> systemLogs =
        [
            new() { Id = 1, Term = 1, Type = RaftLogType.Committed },
            new() { Id = 2, Term = 1, Type = RaftLogType.Committed },
        ];

        // Replay once.
        wal.Write([(SystemPartitionId, systemLogs)]);
        wal.DrainAll();

        // Replay again (simulates a restart with the same WAL entries).
        wal.Write([(SystemPartitionId, systemLogs)]);
        wal.DrainAll();

        List<RaftLog> stored = wal.ReadLogs(SystemPartitionId);
        Assert.Equal(2, stored.Count);
        Assert.Single(stored, l => l.Id == 1);
        Assert.Single(stored, l => l.Id == 2);
    }

    // ── User partitions isolated from system partition ────────────────────

    [Fact]
    public void SystemRestore_SystemPartitionState_DoesNotLeakToUserPartitions()
    {
        FakeWAL wal = new();

        wal.Write([(SystemPartitionId, [new RaftLog { Id = 999, Term = 1, Type = RaftLogType.Committed }])]);
        wal.DrainAll();

        // User partition 1 must not see system partition logs.
        Assert.Equal(0L, wal.GetMaxLog(partitionId: 1));
        Assert.Empty(wal.ReadLogs(partitionId: 1));
    }

    // ── Restore blocking semantics ────────────────────────────────────────

    [Fact]
    public void SystemRestore_BeforeRestore_UserPartitionHasZeroMaxLog()
    {
        FakeWAL wal = new();
        Assert.Equal(0L, wal.GetMaxLog(SystemPartitionId));
        Assert.Equal(0L, wal.GetMaxLog(partitionId: 1));
    }

    [Fact]
    public void SystemRestore_AfterSystemRestore_UserPartitionStillRequiresOwnRestore()
    {
        FakeWAL wal = new();

        // System partition is restored.
        wal.Write([(SystemPartitionId, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed }])]);
        wal.DrainAll();

        // User partition 1 has not been restored yet.
        // Any read of user partition must reflect its own state, not the system's.
        Assert.Equal(0L, wal.GetMaxLog(partitionId: 1));
    }

    // ── Multiple user partitions require individual restore gates ─────────

    [Fact]
    public void SystemRestore_MultipleUserPartitions_EachRequiresOwnRestoreGate()
    {
        FakeWAL wal = new();

        // Restore system, then partition 1, then partition 2 — in order.
        wal.Write([(0, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed }])]);
        wal.Write([(1, [new RaftLog { Id = 10, Term = 1, Type = RaftLogType.Committed }])]);
        wal.Write([(2, [new RaftLog { Id = 20, Term = 1, Type = RaftLogType.Committed }])]);

        // Drain one at a time and check the gate state.
        wal.DrainOne(); // system
        Assert.True(wal.GetMaxLog(0) > 0, "System partition restored");
        Assert.Equal(0L, wal.GetMaxLog(1));
        Assert.Equal(0L, wal.GetMaxLog(2));

        wal.DrainOne(); // partition 1
        Assert.True(wal.GetMaxLog(1) > 0, "Partition 1 restored");
        Assert.Equal(0L, wal.GetMaxLog(2));

        wal.DrainOne(); // partition 2
        Assert.True(wal.GetMaxLog(2) > 0, "Partition 2 restored");
    }

    // ── System log terms are non-decreasing ──────────────────────────────

    [Fact]
    public void SystemRestore_SystemWalTerms_AreNonDecreasing()
    {
        FakeWAL wal = new();

        wal.Write([(SystemPartitionId, [
            new() { Id = 1, Term = 1, Type = RaftLogType.Committed },
            new() { Id = 2, Term = 2, Type = RaftLogType.Committed },
            new() { Id = 3, Term = 2, Type = RaftLogType.Committed },
        ])]);
        wal.DrainAll();

        RaftSafetyAssert.CommittedTermsAreNonDecreasing(wal, SystemPartitionId);
        RaftSafetyAssert.CommittedLogsAreMonotonic(wal, SystemPartitionId);
    }
}
