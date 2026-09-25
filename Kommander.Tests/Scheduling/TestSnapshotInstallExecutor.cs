
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Direct unit tests for the follower-side snapshot install on the state machine's single-writer path
/// (<see cref="RaftPartitionStateMachine.InstallSnapshotAsync"/>), exercising Raft "Rule 7": stale-term
/// rejection, higher-term durable step-down, accepted-leader consistency, idempotency at/below the
/// installed boundary, the import→WAL-boundary ordering with fault-point retries, and that the installed
/// checkpoint boundary is stamped with <c>LastIncludedTerm</c> (not the leader term). Uses a fake host and
/// a capturing WAL facade so each rule can be isolated; the actual suffix retain/truncate behaviour of the
/// boundary op is covered by the WAL conformance tests.
/// </summary>
public class TestSnapshotInstallExecutor
{
    // Shared monotonic tick so import and boundary order are comparable across the two fakes.
    // Methods within an xUnit test class run sequentially, so this is not raced.
    private static int _globalOrder;

    // ── tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StaleLeaderTerm_RejectedWithoutImportingOrWritingBoundary()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(5); // currentTerm = 5

        RaftResponse resp = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 4, leaderTerm: 3));

        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task HigherLeaderTerm_TakesDurableStepDown()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(2); // Leader at term 2, host.Leader = local

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "leaderX:1"));

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(5, sm.CurrentTerm);
        Assert.Equal("leaderX:1", host.Leader);
        // Durable step-down persisted the new term with the leader as the term's vote target.
        Assert.Equal(5, wal.PersistedTerm);
        Assert.Equal("leaderX:1", wal.PersistedVotedFor);
        Assert.True(transfer.ImportCalled);
    }

    [Fact]
    public async Task ConflictingLeaderSameTerm_Rejected()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        // Adopt leaderA at term 4 via a first install, then a different endpoint claims the same term.
        sm.SetLeaderForTesting(1);
        await sm.InstallSnapshotAsync(Install(snapshotIndex: 50, lastIncludedTerm: 4, leaderTerm: 4, leaderEndpoint: "leaderA:1"));
        Assert.Equal("leaderA:1", host.Leader);

        transfer.ImportCalled = false;
        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 60, lastIncludedTerm: 4, leaderTerm: 4, leaderEndpoint: "leaderB:1"));

        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.False(transfer.ImportCalled);
    }

    [Fact]
    public async Task Install_StampsCheckpointWithLastIncludedTerm_NotLeaderTerm()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);

        await sm.InstallSnapshotAsync(Install(snapshotIndex: 200, lastIncludedTerm: 9, leaderTerm: 12, leaderEndpoint: "L:1"));

        Assert.Equal(1, wal.BoundaryCallCount);
        Assert.Equal(200, wal.BoundarySnapshotIndex);
        Assert.Equal(9, wal.BoundaryTerm); // LastIncludedTerm, not the leader term (12)
    }

    [Fact]
    public async Task ImportPrecedesBoundary()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);

        await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 3, leaderTerm: 3, leaderEndpoint: "L:1"));

        Assert.True(transfer.ImportOrder > 0, "import should have run");
        Assert.True(wal.BoundaryOrder > 0, "boundary should have run");
        Assert.True(transfer.ImportOrder < wal.BoundaryOrder, "import must precede the WAL boundary write");
    }

    [Fact]
    public async Task IdempotentWhenInstalledBoundaryCoversIndexWithMatchingTerm()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        // An installed snapshot boundary already covers the index with a matching term.
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;

        // Retrying the same (index, term) is an idempotent success — no import, no boundary, and no step-down
        // (checked before term adoption so a caught-up node is never disrupted).
        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 50, lastIncludedTerm: 5, leaderTerm: 6, leaderEndpoint: "L:1"));

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        // The reply says a skip happened, so the sender can tell it from an import.
        Assert.Equal(SnapshotInstallOutcome.SkippedAlreadyCovered, resp.SnapshotOutcome);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);  // not stepped down
        Assert.Equal(1, sm.CurrentTerm);
    }

    [Fact]
    public async Task CheckpointMarkerOverAGap_DoesNotSkipTheInstall()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        // A follower holds entries through N-3, a CommittedCheckpoint row landed at N over the gap
        // N-2..N-1, and the boundary reports N with a matching term. The install at N must run:
        // the checkpoint certifies entries this node never held, which is exactly what the
        // snapshot repairs. (Every backend withholds the checkpoint id in this state; the fence
        // here is the second line.)
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 97;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.Equal(SnapshotInstallOutcome.Installed, resp.SnapshotOutcome);
        Assert.True(transfer.ImportCalled);
        Assert.Equal(1, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task IdempotentSkip_WhenThePresenceFrontierCoversTheIndex()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100; // the boundary is contiguously held

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));

        Assert.Equal(SnapshotInstallOutcome.SkippedAlreadyCovered, resp.SnapshotOutcome);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task Skip_WithApplyCursorBehindTheBoundary_DeliversEveryCoveredEntryFirst()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        // The follower holds 1..100 contiguously and a certified checkpoint row at 100 landed, but its
        // apply cursor never reached them (a drain withheld at a hole backfill filled later, or apply
        // lag). The boundary covers the index, so the install is skipped — and the skip must hand the
        // application every committed entry it covers, never just move the cursor over them.
        wal.Logs.AddRange(CommittedRun(1, 100, term: 5));
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));

        Assert.Equal(SnapshotInstallOutcome.SkippedAlreadyCovered, resp.SnapshotOutcome);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
        Assert.Equal(Enumerable.Range(1, 100).Select(i => (long)i), host.Delivered);
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);
    }

    [Fact]
    public async Task Skip_Repeated_DoesNotRedeliver()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        wal.Logs.AddRange(CommittedRun(1, 100, term: 5));
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100;

        await sm.InstallSnapshotAsync(Install(snapshotIndex: 60, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));
        Assert.Equal(Enumerable.Range(1, 60).Select(i => (long)i), host.Delivered);

        // A later skip delivers only what lies above the cursor; a re-skip of a covered index delivers nothing.
        await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));
        await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));

        Assert.Equal(Enumerable.Range(1, 100).Select(i => (long)i), host.Delivered);
        Assert.False(transfer.ImportCalled);
    }

    [Fact]
    public async Task Skip_WithAnUnresolvedEntryBelowTheIndex_InstallsInstead()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        // Entry 60's commit marker never landed here: the drain cannot deliver past it, so the covered
        // range cannot be handed to the application and skipping would lose 60..100. Import instead.
        wal.Logs.AddRange(CommittedRun(1, 100, term: 5));
        wal.Logs[59].Type = RaftLogType.Proposed;
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.Equal(SnapshotInstallOutcome.Installed, resp.SnapshotOutcome);
        Assert.True(transfer.ImportCalled);
        Assert.Equal(1, wal.BoundaryCallCount);
        Assert.Equal(Enumerable.Range(1, 59).Select(i => (long)i), host.Delivered);
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);
    }

    [Fact]
    public async Task ApplicationAppliedPastTheIndex_SkipsTheImport_WhateverTheBoundarySays()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        wal.Logs.AddRange(CommittedRun(1, 100, term: 5));
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100;

        // Bring the apply cursor to 100 through the covered-boundary skip.
        await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);

        // The persisted checkpoint sits below the next snapshot's index (a checkpoint row withheld
        // because it once landed over a gap), so the boundary does not cover it — but the
        // application does. Importing would replace state that already runs past the index.
        wal.LastCheckpoint = 40;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 60, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));

        Assert.Equal(SnapshotInstallOutcome.SkippedAlreadyCovered, resp.SnapshotOutcome);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
        Assert.Equal(Enumerable.Range(1, 100).Select(i => (long)i), host.Delivered);
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);
    }

    [Fact]
    public async Task ApplicationAppliedPastTheIndex_WithAConflictingTermThere_RefusesRatherThanImports()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        wal.Logs.AddRange(CommittedRun(1, 100, term: 5));
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100;
        await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));
        wal.LastCheckpoint = 40;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 60, lastIncludedTerm: 9, leaderTerm: 9, leaderEndpoint: "L:1"));

        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);
    }

    [Fact]
    public async Task OrdinarySuffixPastIndexWithoutBoundary_StillImports()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        // Ordinary proposed/committed entries reach the index, but NO snapshot boundary is installed: the
        // application state still needs the import, so this must NOT short-circuit.
        wal.MaxLog = 100;
        wal.LastCheckpoint = -1;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 50, lastIncludedTerm: 5, leaderTerm: 5, leaderEndpoint: "L:1"));

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.Equal(SnapshotInstallOutcome.Installed, resp.SnapshotOutcome);
        Assert.True(transfer.ImportCalled);
        Assert.Equal(1, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task StaleHigherIndexRequestFromOldLeader_RejectedNotIdempotent()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(5); // currentTerm = 5
        // An unrelated high WAL max but no installed boundary: a stale old-leader request must be rejected by
        // term validation, not acknowledged idempotently off the WAL max.
        wal.MaxLog = 100;
        wal.LastCheckpoint = -1;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 80, lastIncludedTerm: 2, leaderTerm: 3, leaderEndpoint: "old:1"));

        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.Equal(SnapshotInstallOutcome.Rejected, resp.SnapshotOutcome);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task ConflictingBoundaryTermAtIndex_FallsThroughToInstall()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        // A boundary exists at the index but with a conflicting term: not the same snapshot, so it must go
        // through a fresh install rather than short-circuit.
        wal.LastCheckpoint = 50;
        wal.TermAtIndex = 3;

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 50, lastIncludedTerm: 9, leaderTerm: 9, leaderEndpoint: "L:1"));

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.True(transfer.ImportCalled);
        Assert.Equal(1, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task ImportFailure_ThenRetry_IsIdempotentAndSucceeds()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);

        transfer.ThrowOnImport = true;
        RaftResponse first = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 3, leaderTerm: 3, leaderEndpoint: "L:1"));
        Assert.Equal(RaftOperationStatus.Errored, first.Status);
        Assert.Equal(0, wal.BoundaryCallCount); // import failed before the boundary write

        // Sender retries the same snapshot; import now succeeds and the boundary is installed.
        transfer.ThrowOnImport = false;
        RaftResponse second = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 3, leaderTerm: 3, leaderEndpoint: "L:1"));
        Assert.Equal(RaftOperationStatus.Success, second.Status);
        Assert.Equal(1, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task WalBoundaryFailure_ThenRetry_Succeeds()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);

        wal.BoundaryStatus = RaftOperationStatus.Errored;
        RaftResponse first = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 3, leaderTerm: 3, leaderEndpoint: "L:1"));
        Assert.Equal(RaftOperationStatus.Errored, first.Status);
        Assert.True(transfer.ImportCalled);   // import ran; WAL write failed

        // Retry: the repeated import is safe (idempotent) and the boundary now persists.
        wal.BoundaryStatus = RaftOperationStatus.Success;
        RaftResponse second = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 3, leaderTerm: 3, leaderEndpoint: "L:1"));
        Assert.Equal(RaftOperationStatus.Success, second.Status);
    }

    [Fact]
    public async Task SystemStateKind_RoutesToSystemImporter()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);

        RaftResponse resp = await sm.InstallSnapshotAsync(new SnapshotInstallRequest
        {
            PartitionId = 1,
            SnapshotIndex = 100,
            LastIncludedTerm = 3,
            LeaderTerm = 3,
            LeaderEndpoint = "L:1",
            Kind = SnapshotKind.SystemState,
            Snapshot = new MemoryStream([1, 2, 3]),
        });

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.Equal(1, host.SystemTransfer.ImportCount);   // routed to ImportPartitionState
        Assert.False(transfer.ImportCalled);                // NOT the range importer
        Assert.Equal(1, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task SystemStateKind_NoSystemTransfer_Rejected()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        host.SystemTransferEnabled = false;
        sm.SetLeaderForTesting(1);

        RaftResponse resp = await sm.InstallSnapshotAsync(new SnapshotInstallRequest
        {
            PartitionId = 1,
            SnapshotIndex = 100,
            LastIncludedTerm = 3,
            LeaderTerm = 3,
            LeaderEndpoint = "L:1",
            Kind = SnapshotKind.SystemState,
            Snapshot = new MemoryStream([1, 2, 3]),
        });

        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.Equal(0, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task PartitionStateKind_RoutesToPartitionImporter()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);

        RaftResponse resp = await sm.InstallSnapshotAsync(new SnapshotInstallRequest
        {
            PartitionId = 1,
            SnapshotIndex = 100,
            LastIncludedTerm = 3,
            LeaderTerm = 3,
            LeaderEndpoint = "L:1",
            Kind = SnapshotKind.PartitionState,
            Snapshot = new MemoryStream([1, 2, 3]),
        });

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.Equal(1, host.PartitionTransfer.ImportCount);   // routed to the new importer
        Assert.False(transfer.ImportCalled);                   // NOT the range importer
        Assert.Equal(0, host.SystemTransfer.ImportCount);      // NOT the system importer
        Assert.Equal(1, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task PartitionStateKind_NoPartitionTransfer_Rejected()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        host.PartitionTransferEnabled = false;
        sm.SetLeaderForTesting(1);

        RaftResponse resp = await sm.InstallSnapshotAsync(new SnapshotInstallRequest
        {
            PartitionId = 1,
            SnapshotIndex = 100,
            LastIncludedTerm = 3,
            LeaderTerm = 3,
            LeaderEndpoint = "L:1",
            Kind = SnapshotKind.PartitionState,
            Snapshot = new MemoryStream([1, 2, 3]),
        });

        // A PartitionState blob must never fall through to ImportRange — the payloads are not
        // interchangeable — so a missing registration is a rejection, and no boundary is written.
        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task RangeKind_NoStateMachineTransfer_Rejected()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        host.RangeTransferEnabled = false;
        sm.SetLeaderForTesting(1);

        RaftResponse resp = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 3, leaderTerm: 3));

        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.Equal(0, wal.BoundaryCallCount);
    }

    [Fact]
    public async Task LegacySender_RejectedWhenCompatibilityDisabled()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        // AllowLegacySnapshotSenders defaults to false.

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 100, lastIncludedTerm: 0, leaderTerm: 0, leaderEndpoint: ""));

        Assert.Equal(RaftOperationStatus.Errored, resp.Status);
        Assert.False(transfer.ImportCalled);
    }

    [Fact]
    public async Task LegacySender_AcceptedWhenCompatibilityEnabled()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        host.Configuration.AllowLegacySnapshotSenders = true;
        sm.SetLeaderForTesting(4);

        RaftResponse resp = await sm.InstallSnapshotAsync(
            Install(snapshotIndex: 100, lastIncludedTerm: 0, leaderTerm: 0, leaderEndpoint: ""));

        Assert.Equal(RaftOperationStatus.Success, resp.Status);
        Assert.True(transfer.ImportCalled);
        // Legacy path stamps the checkpoint with the local current term (no authoritative last-included term).
        Assert.Equal(4, wal.BoundaryTerm);
    }

    /// <summary>
    /// A transfer that answers a re-seed request replaces application state the log already covers:
    /// the covering checkpoint boundary that would otherwise short-circuit the install as already
    /// held is not consulted, and the import and the boundary write both run.
    /// </summary>
    [Fact]
    public async Task ForcedInstall_OverACoveringBoundary_ImportsInsteadOfSkipping()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        wal.Logs.AddRange(CommittedRun(1, 100, term: 5));
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100;

        // The ordinary install at the covered index is a skip: everything below is delivered instead.
        RaftResponse skipped = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5));
        Assert.Equal(SnapshotInstallOutcome.SkippedAlreadyCovered, skipped.SnapshotOutcome);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);

        // The same index, requested: imported, with the boundary re-stamped, and the cursor at the index.
        RaftResponse forced = await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5, forced: true));

        Assert.Equal(SnapshotInstallOutcome.Installed, forced.SnapshotOutcome);
        Assert.True(transfer.ImportCalled);
        Assert.Equal(1, wal.BoundaryCallCount);
        Assert.Equal(100, wal.BoundarySnapshotIndex);
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);
    }

    /// <summary>
    /// Requested or not, an install below the apply cursor would erase applied entries: the cursor rule
    /// wins over the request. The requester keeps its applies held to keep this from happening.
    /// </summary>
    [Fact]
    public async Task ForcedInstall_BelowTheApplyCursor_IsStillNotImported()
    {
        (RaftPartitionStateMachine sm, FakeHost host, CapturingFacade wal, CapturingTransfer transfer) = Build();
        sm.SetLeaderForTesting(1);
        wal.Logs.AddRange(CommittedRun(1, 100, term: 5));
        wal.MaxLog = 100;
        wal.LastCheckpoint = 100;
        wal.TermAtIndex = 5;
        wal.PresentIndex = 100;
        await sm.InstallSnapshotAsync(Install(snapshotIndex: 100, lastIncludedTerm: 5, leaderTerm: 5));
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);

        RaftResponse resp = await sm.InstallSnapshotAsync(Install(snapshotIndex: 60, lastIncludedTerm: 5, leaderTerm: 5, forced: true));

        Assert.Equal(SnapshotInstallOutcome.SkippedAlreadyCovered, resp.SnapshotOutcome);
        Assert.False(transfer.ImportCalled);
        Assert.Equal(0, wal.BoundaryCallCount);
        Assert.Equal(100, (await sm.GetPartitionView()).LastAppliedIndex);
    }

    // ── harness ─────────────────────────────────────────────────────────────────

    private static (RaftPartitionStateMachine, FakeHost, CapturingFacade, CapturingTransfer) Build()
    {
        CapturingTransfer transfer = new();
        FakeHost host = new(transfer);
        CapturingFacade wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        return (sm, host, wal, transfer);
    }

    private static IEnumerable<RaftLog> CommittedRun(long from, long to, long term)
    {
        for (long id = from; id <= to; id++)
            yield return new RaftLog { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "app" };
    }

    private static SnapshotInstallRequest Install(
        long snapshotIndex, long lastIncludedTerm, long leaderTerm, string leaderEndpoint = "L:1", bool forced = false) =>
        new()
        {
            PartitionId = 1,
            SnapshotIndex = snapshotIndex,
            LastIncludedTerm = lastIncludedTerm,
            LeaderTerm = leaderTerm,
            LeaderEndpoint = leaderEndpoint,
            Kind = SnapshotKind.Range,
            Snapshot = new MemoryStream([1, 2, 3, 4]),
            Forced = forced,
        };

    private sealed class CapturingTransfer : IRaftStateMachineTransfer
    {
        public bool ImportCalled { get; set; }
        public int ImportOrder { get; private set; }
        public bool ThrowOnImport { get; set; }

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            if (ThrowOnImport)
                throw new InvalidOperationException("import failed (injected)");
            ImportCalled = true;
            ImportOrder = Interlocked.Increment(ref _globalOrder);
            return Task.CompletedTask;
        }
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    private sealed class CapturingFacade : IRaftWalFacade
    {
        public long MaxLog { get; set; }
        public long LastCheckpoint { get; set; } = -1;
        public long TermAtIndex { get; set; } = -1;

        /// <summary>Contiguous presence frontier; -1 = not tracked (the facade default).</summary>
        public long PresentIndex { get; set; } = -1;

        public long GetPresentIndex() => PresentIndex;

        /// <summary>Entries the facade holds, ascending by id; read by the committed-apply drain.</summary>
        public List<RaftLog> Logs { get; } = [];
        public RaftOperationStatus BoundaryStatus { get; set; } = RaftOperationStatus.Success;

        public int BoundaryCallCount { get; private set; }
        public long BoundarySnapshotIndex { get; private set; } = -1;
        public long BoundaryTerm { get; private set; } = -1;
        public int BoundaryOrder { get; private set; }

        public long PersistedTerm { get; private set; } = -1;
        public string? PersistedVotedFor { get; private set; }

        public ValueTask<(RaftOperationStatus Status, bool SuffixTruncated)> InstallSnapshotBoundaryAsync(
            long snapshotIndex, long lastIncludedTerm)
        {
            BoundaryCallCount++;
            BoundarySnapshotIndex = snapshotIndex;
            BoundaryTerm = lastIncludedTerm;
            BoundaryOrder = Interlocked.Increment(ref _globalOrder);
            return ValueTask.FromResult((BoundaryStatus, false));
        }

        public ValueTask<bool> PersistHardStateAsync(long currentTerm, string? votedFor)
        {
            PersistedTerm = currentTerm;
            PersistedVotedFor = votedFor;
            return ValueTask.FromResult(true);
        }

        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(LastCheckpoint);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(TermAtIndex);

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(MaxLog);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(Logs.Where(l => l.Id >= startLogIndex).Take(maxEntries).ToList());
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class CapturingSystemTransfer : IRaftSystemStateTransfer
    {
        public int ImportCount { get; private set; }

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
        {
            ImportCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class CapturingPartitionStateTransfer : IRaftPartitionStateTransfer
    {
        public int ImportCount { get; private set; }

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
        {
            ImportCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class FakeHost : IRaftPartitionHost
    {
        private readonly IRaftStateMachineTransfer transfer;
        public CapturingSystemTransfer SystemTransfer { get; } = new();
        public CapturingPartitionStateTransfer PartitionTransfer { get; } = new();
        public bool SystemTransferEnabled { get; set; } = true;
        public bool RangeTransferEnabled { get; set; } = true;
        public bool PartitionTransferEnabled { get; set; } = true;
        public FakeHost(IRaftStateMachineTransfer transfer) => this.transfer = transfer;

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "self:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration { get; } = new() { NodeId = 1, Host = "self", Port = 9000, InitialPartitions = 1 };
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [];

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public List<long> Delivered { get; } = [];
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l)
        {
            Delivered.Add(l.Id);
            return Task.FromResult(true);
        }
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => RangeTransferEnabled ? transfer : null;
        public IRaftSystemStateTransfer? SystemStateTransfer => SystemTransferEnabled ? SystemTransfer : null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer => PartitionTransferEnabled ? PartitionTransfer : null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }
}
