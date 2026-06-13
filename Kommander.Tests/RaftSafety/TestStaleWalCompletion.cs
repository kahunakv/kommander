
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Tests.Scheduler;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Safety invariant: stale WAL completions from an older term, a rolled-back
/// operation, or a superseded operation sequence must be dropped silently.
///
/// These tests encode the fencing logic of <see cref="RaftOperationCompletion"/>
/// so that future implementations of <c>RaftPartitionExecutor</c> and
/// <c>RaftIoScheduler</c> can verify the invariant without relying on the actor
/// completion path.
/// </summary>
public class TestStaleWalCompletion
{
    // ── Term fencing ──────────────────────────────────────────────────────

    [Fact]
    public void StaleCompletion_OlderTerm_ShouldBeDropped()
    {
        long currentTerm = 7;
        RaftOperationCompletion completion = new(
            PartitionId: 0,
            OperationId: 1,
            Term: 4,          // stale — submitted in term 4, current term is 7
            LogIndexStart: 10,
            LogIndexEnd: 12,
            OperationKind: RaftOperationKind.Replication,
            Status: RaftOperationStatus.Success
        );

        bool isStale = completion.Term < currentTerm;
        Assert.True(isStale, "Completion from an older term must be considered stale");
    }

    [Fact]
    public void StaleCompletion_CurrentTerm_ShouldBeAccepted()
    {
        long currentTerm = 7;
        RaftOperationCompletion completion = new(
            PartitionId: 0,
            OperationId: 5,
            Term: 7,
            LogIndexStart: 10,
            LogIndexEnd: 12,
            OperationKind: RaftOperationKind.Replication,
            Status: RaftOperationStatus.Success
        );

        bool isStale = completion.Term < currentTerm;
        Assert.False(isStale, "Completion from the current term must not be dropped");
    }

    // ── Operation-ID fencing ──────────────────────────────────────────────
    // The OperationId is the authoritative pending-map key.
    // There is no longer an embedded WALWriteOperation to cross-check against;
    // instead the state machine looks up pendingWalOperations[OperationId] and
    // discards the completion if the entry is absent.  The test below models
    // the abstract rule (unknown id → stale); the live handler is exercised in
    // TestWalCompletionFences.OrphanLeaderPropose_CompletionIsDiscardedWithoutException.

    [Fact]
    public void StaleCompletion_OperationIdMismatch_ShouldBeDropped()
    {
        long expectedOperationId = 42L;
        RaftOperationCompletion completion = new(
            PartitionId: 0,
            OperationId: 41L,   // old operation id — a newer operation was submitted
            Term: 5,
            LogIndexStart: 1,
            LogIndexEnd: 3,
            OperationKind: RaftOperationKind.Replication,
            Status: RaftOperationStatus.Success
        );

        bool isStale = completion.OperationId != expectedOperationId;
        Assert.True(isStale, "Completion with mismatched operation ID must be dropped");
    }

    // ── FakeWAL: pending write drained after rollback ─────────────────────

    [Fact]
    public void StaleCompletion_ProposedWriteDrainedAfterRollback_DoesNotCommit()
    {
        FakeWAL wal = new();

        // Step 1: propose entry 5.
        wal.Write([(0, [new RaftLog { Id = 5, Term = 2, Type = RaftLogType.Proposed }])]);

        // Step 2: before the propose is drained, a rollback entry is written and drained.
        wal.Write([(0, [new RaftLog { Id = 5, Term = 2, Type = RaftLogType.RolledBack }])]);
        // Drain the rollback first (it was submitted second but we drain all pending here).
        // In reality the executor must check the fencing metadata; here we model the scenario.
        wal.DrainAll();

        // The last writer wins (the WAL is dictionary-keyed by Id).
        RaftLog entry = wal.ReadLogs(0).Single(l => l.Id == 5);

        // The rollback arrived second so the stored state must reflect it.
        Assert.Equal(RaftLogType.RolledBack, entry.Type);
    }

    [Fact]
    public void StaleCompletion_LateSuccessAfterRollback_IsIdentifiedAsStalByFencingRecord()
    {
        // After a rollback the executor increments the expected operation id.
        long pendingOperationId = 10L;  // current
        long staleOperationId = 9L;     // the old proposed-write operation

        RaftOperationCompletion staleCompletion = new(
            PartitionId: 0,
            OperationId: staleOperationId,
            Term: 3,
            LogIndexStart: 5,
            LogIndexEnd: 5,
            OperationKind: RaftOperationKind.Client,
            Status: RaftOperationStatus.Success
        );

        bool shouldDrop = staleCompletion.OperationId != pendingOperationId;
        Assert.True(shouldDrop, "Completion for a superseded operation must be dropped");
    }

    // ── FakeWAL: injected failure completion is not applied ───────────────

    [Fact]
    public void StaleCompletion_FailedWrite_WalStateUnchanged()
    {
        FakeWAL wal = new();
        wal.Write([(0, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed }])]);
        wal.DrainAll();

        // Simulate a failed write (e.g. RocksDB error).
        wal.NextWriteResult = RaftOperationStatus.Errored;
        wal.Write([(0, [new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed }])]);
        wal.DrainAll();

        Assert.Equal(1L, wal.GetMaxLog(partitionId: 0));
        Assert.Equal(RaftOperationStatus.Errored, wal.CompletedWrites.Last().InjectedResult);
    }

    // ── Completion record value semantics ─────────────────────────────────

    [Fact]
    public void StaleCompletion_RecordEquality_FencingFieldsMatter()
    {
        RaftOperationCompletion a = new(0, 10, 5, 1, 3, RaftOperationKind.Replication, RaftOperationStatus.Success);
        RaftOperationCompletion b = a with { Term = 4 }; // different term
        RaftOperationCompletion c = a with { OperationId = 11 }; // different op id

        Assert.NotEqual(a, b);
        Assert.NotEqual(a, c);
        Assert.Equal(a, a with { }); // same values → equal
    }

    // ── Scheduler harness: fencing metadata flows through steps ──────────

    [Fact]
    public void StaleCompletion_HarnessStep_OperationIdIsMonotonic()
    {
        SchedulerHarness h = new();

        h.Enqueue(0, RaftRequestType.ReplicateLogs);
        h.Enqueue(0, RaftRequestType.CommitLogs);

        SchedulerHarness.HarnessStep? step1 = h.StepOnce();
        SchedulerHarness.HarnessStep? step2 = h.StepOnce();

        Assert.NotNull(step1);
        Assert.NotNull(step2);
        Assert.True(step2!.Operation.SequenceNumber > step1!.Operation.SequenceNumber,
            "Sequence numbers must be strictly increasing so stale completions can be identified");
    }

    // ── Correct completion must still apply the write ─────────────────────

    [Fact]
    public void StaleCompletion_CorrectFencing_WritesAreApplied()
    {
        FakeWAL wal = new();
        long currentTerm = 3;
        long currentOperationId = 7;

        RaftOperationCompletion completion = new(
            PartitionId: 0,
            OperationId: currentOperationId,
            Term: currentTerm,
            LogIndexStart: 1,
            LogIndexEnd: 5,
            OperationKind: RaftOperationKind.Replication,
            Status: RaftOperationStatus.Success
        );

        // Both fencing checks pass.
        bool isStale = completion.Term < currentTerm || completion.OperationId != currentOperationId;
        Assert.False(isStale, "A completion with matching term and operation ID is not stale");

        // Apply the write.
        wal.Write([(0, [new RaftLog { Id = 1, Term = currentTerm, Type = RaftLogType.Committed }])]);
        wal.DrainAll();

        Assert.Equal(1L, wal.GetMaxLog(partitionId: 0));
    }
}
