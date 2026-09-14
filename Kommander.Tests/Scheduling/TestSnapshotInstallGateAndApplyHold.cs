
using Kommander;
using Kommander.Consensus;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Coverage for the mid-install test hook — the pair that makes "a snapshot installed on a follower
/// with a log tail still pending" a constructible state:
/// <see cref="IRaft.SetSnapshotInstallGateForTesting"/> suspends the install between its ordered
/// steps, and <see cref="IRaft.HoldConsumerAppliesForTesting"/> keeps committed entries pending in
/// the log so a tail exists above the boundary in the first place.
///
/// <para>The behaviour under test is step 4 of the install: the durable WAL boundary retains the
/// suffix above the index when its stored term matches and truncates it on conflict. The
/// consumer's post-install state plus the retained tail must produce the same result as a replica
/// that never installed — which is what the last two tests assert directly, against a control
/// replica fed the same entries.</para>
/// </summary>
public class TestSnapshotInstallGateAndApplyHold
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    /// <summary>Records consumer applies in order and every snapshot import.</summary>
    private sealed class RecordingHost : IRaftPartitionHost
    {
        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
        };

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public bool IsMember(string endpoint) => true;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [];

        /// <summary>Ordered log of apply events produced by the consumer callbacks.</summary>
        public List<string> EventLog { get; } = [];

        public RecordingTransfer Transfer { get; } = new();

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string ep, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string ep, RaftResponderRequest req) { }
        public Task InvokeLeaderChanged(int p, string leader) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"Applied:{log.Id}");
            return Task.FromResult(true);
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"SystemApplied:{log.Id}");
            return Task.FromResult(true);
        }

        public void InvokeReplicationError(int p, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => Transfer;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode n, SnapshotRequest r, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    private sealed class RecordingTransfer : IRaftStateMachineTransfer
    {
        public int ImportCount { get; private set; }

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            ImportCount++;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// WAL facade that keeps real entries and models the boundary install the way
    /// <c>InMemoryWAL.InstallSnapshotBoundary</c> does: the suffix above the index is retained when
    /// the stored entry at the index carries the boundary term, and truncated on conflict. That
    /// retain-or-truncate decision is the behaviour these tests are about, so a fake that always
    /// retained would prove nothing.
    /// </summary>
    private sealed class SnapshotWalFacade : IRaftWalFacade
    {
        private long _nextOperationId;
        private long _commitIndex = -1;
        private long _lastCheckpoint = -1;
        private readonly SortedDictionary<long, RaftLog> _entries = [];

        public long LastOperationId => _nextOperationId;

        public int BoundaryCallCount { get; private set; }
        public bool LastSuffixTruncated { get; private set; }

        /// <summary>Ids currently present, so a test can assert the tail survived (or did not).</summary>
        public IReadOnlyList<long> PresentIds => [.. _entries.Keys];

        public long GetCommitIndex() => _commitIndex;

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
            => ValueTask.FromResult(_entries.Values.Where(l => l.Id >= start && l.Type == RaftLogType.Committed).Take(max).ToList());

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
            => ValueTask.FromResult(_entries.Values.Where(l => l.Id >= start).Take(max).ToList());

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_entries.Count > 0 ? _entries.Keys.Max() : 0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(_lastCheckpoint);

        public ValueTask<long> GetAnyTermAtAsync(long logIndex)
            => ValueTask.FromResult(_entries.TryGetValue(logIndex, out RaftLog? log) ? log.Term : -1L);

        public ValueTask<bool> PersistHardStateAsync(long currentTerm, string? votedFor) => ValueTask.FromResult(true);

        public ValueTask<(RaftOperationStatus Status, bool SuffixTruncated)> InstallSnapshotBoundaryAsync(
            long snapshotIndex, long lastIncludedTerm)
        {
            BoundaryCallCount++;

            bool matches = _entries.TryGetValue(snapshotIndex, out RaftLog? existing) && existing.Term == lastIncludedTerm;
            bool truncated = false;

            if (!matches)
            {
                List<long> above = _entries.Keys.Where(id => id > snapshotIndex).ToList();
                foreach (long id in above)
                    _entries.Remove(id);

                truncated = above.Count > 0;
                if (truncated && _commitIndex > snapshotIndex)
                    _commitIndex = snapshotIndex;
            }

            _entries[snapshotIndex] = new RaftLog { Id = snapshotIndex, Term = lastIncludedTerm, Type = RaftLogType.CommittedCheckpoint };
            _lastCheckpoint = truncated ? snapshotIndex : Math.Max(_lastCheckpoint, snapshotIndex);

            LastSuffixTruncated = truncated;
            return ValueTask.FromResult((RaftOperationStatus.Success, truncated));
        }

        public void SeedCommitFrontierFromSnapshot(long snapshotIndex, long snapshotTerm = 0, bool suffixTruncated = false)
        {
            if (snapshotIndex > _commitIndex)
                _commitIndex = snapshotIndex;
        }

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            foreach (RaftLog log in logs)
                _entries[log.Id] = log;

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderPropose,
                (1, logs), timestamp, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
                else if (log.Type == RaftLogType.ProposedCheckpoint)
                    log.Type = RaftLogType.CommittedCheckpoint;

                _entries[log.Id] = log;

                if (log.Id > _commitIndex)
                    _commitIndex = log.Id;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderCommit, (1, logs), logIndex: maxId);
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.RolledBack;

                _entries[log.Id] = log;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderRollback, (1, logs), logIndex: maxId);
        }

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1)
            => logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) Build()
    {
        RecordingHost host = new();
        SnapshotWalFacade wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        return (sm, host, wal);
    }

    private static RaftWalCompletion MakeCompletion(long operationId, WALWriteOperationType type, long minLogIndex, long maxLogIndex) =>
        new(PartitionId: 1, OperationId: operationId, Term: -1L,
            MinLogIndex: minLogIndex, MaxLogIndex: maxLogIndex,
            OperationType: type, Status: RaftOperationStatus.Success);

    /// <summary>Proposes and commits one entry through the sole-voter leader path.</summary>
    private static async Task CommitEntryAsync(RaftPartitionStateMachine sm, SnapshotWalFacade wal, long id)
    {
        sm.ReplicateLogs([new() { Id = id, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: null);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, id));
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, id, id));
    }

    private static SnapshotInstallRequest Install(long snapshotIndex, long lastIncludedTerm, long leaderTerm, string leaderEndpoint = "L:1") =>
        new()
        {
            PartitionId = 1,
            SnapshotIndex = snapshotIndex,
            LastIncludedTerm = lastIncludedTerm,
            LeaderTerm = leaderTerm,
            LeaderEndpoint = leaderEndpoint,
            Kind = SnapshotKind.Range,
            Snapshot = new MemoryStream([1, 2, 3, 4]),
        };

    private static async Task<long> AppliedCursorAsync(RaftPartitionStateMachine sm) =>
        (await sm.GetPartitionView()).LastAppliedIndex;

    // ── gate ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// The gate fires once per install at the phase it is registered for, and reports the request's
    /// index, boundary term and kind alongside this node's state at that point. The apply cursor it
    /// reports is the pre-seed cursor at every phase, because the cursor is seeded only after the
    /// last of them.
    /// </summary>
    [Theory]
    [InlineData(SnapshotInstallPhase.BeforeImport)]
    [InlineData(SnapshotInstallPhase.AfterImport)]
    [InlineData(SnapshotInstallPhase.AfterBoundary)]
    public async Task Gate_FiresOncePerInstall_WithTheRequestAndCursorState(SnapshotInstallPhase phase)
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        for (long id = 1; id <= 3; id++)
            await CommitEntryAsync(sm, wal, id);

        List<SnapshotInstallSignal> seen = [];
        sm.SetSnapshotInstallGateForTesting(new SnapshotInstallGate(phase, signal =>
        {
            seen.Add(signal);
            return ValueTask.CompletedTask;
        }));

        RaftResponse response = await sm.InstallSnapshotAsync(Install(snapshotIndex: 2, lastIncludedTerm: 1, leaderTerm: 5));

        Assert.Equal(RaftOperationStatus.Success, response.Status);

        SnapshotInstallSignal signal = Assert.Single(seen);
        Assert.Equal(phase, signal.Phase);
        Assert.Equal(1, signal.PartitionId);
        Assert.Equal(2L, signal.SnapshotIndex);
        Assert.Equal(1L, signal.BoundaryTerm);
        Assert.Equal(SnapshotKind.Range, signal.Kind);
        Assert.Equal(3L, signal.LocalMaxLogId);
        Assert.Equal(3L, signal.LastAppliedIndex);

        // The import ran exactly once whichever phase gated it: a gate suspends, it never reorders.
        Assert.Equal(1, host.Transfer.ImportCount);
        Assert.Equal(1, wal.BoundaryCallCount);
    }

    /// <summary>
    /// A gate that never releases must fail the install rather than wedge the partition: the
    /// operation ends <c>Errored</c> once the bound expires, the partition keeps serving, and the
    /// sender's retry succeeds once the gate is removed.
    /// </summary>
    [Fact]
    public async Task GateThatNeverReleases_FailsTheInstall_AndTheRetrySucceedsOnceItIsRemoved()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        host.Configuration.LeadershipBarrierTimeout = TimeSpan.FromMilliseconds(200);
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await CommitEntryAsync(sm, wal, 1);

        TaskCompletionSource neverReleases = new(TaskCreationOptions.RunContinuationsAsynchronously);
        SnapshotInstallGate gate = new(SnapshotInstallPhase.BeforeImport, _ => new ValueTask(neverReleases.Task));
        sm.SetSnapshotInstallGateForTesting(gate);

        RaftResponse blocked = await sm.InstallSnapshotAsync(Install(snapshotIndex: 1, lastIncludedTerm: 1, leaderTerm: 5));

        Assert.Equal(RaftOperationStatus.Errored, blocked.Status);
        Assert.Equal(0, host.Transfer.ImportCount);         // failed before the import
        Assert.Equal(0, wal.BoundaryCallCount);

        // The partition is still serving: it answers an ordinary operation immediately.
        Assert.Equal(1L, await AppliedCursorAsync(sm));

        sm.ClearSnapshotInstallGateForTesting(gate);

        RaftResponse retry = await sm.InstallSnapshotAsync(Install(snapshotIndex: 1, lastIncludedTerm: 1, leaderTerm: 5));

        Assert.Equal(RaftOperationStatus.Success, retry.Status);
        Assert.Equal(1, host.Transfer.ImportCount);
        Assert.Equal(1, wal.BoundaryCallCount);

        neverReleases.SetResult();
    }

    /// <summary>
    /// The idempotent re-install short-circuit returns before any ordered step, so it must not fire
    /// the gate — a gate that fired there would suspend an operation that does nothing.
    /// </summary>
    [Fact]
    public async Task IdempotentReinstall_DoesNotFireTheGate()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        for (long id = 1; id <= 3; id++)
            await CommitEntryAsync(sm, wal, id);

        // First install lands the boundary at 2.
        Assert.Equal(RaftOperationStatus.Success,
            (await sm.InstallSnapshotAsync(Install(snapshotIndex: 2, lastIncludedTerm: 1, leaderTerm: 5))).Status);

        int fired = 0;
        sm.SetSnapshotInstallGateForTesting(new SnapshotInstallGate(SnapshotInstallPhase.BeforeImport, _ =>
        {
            fired++;
            return ValueTask.CompletedTask;
        }));

        RaftResponse repeat = await sm.InstallSnapshotAsync(Install(snapshotIndex: 2, lastIncludedTerm: 1, leaderTerm: 5));

        Assert.Equal(RaftOperationStatus.Success, repeat.Status);
        Assert.Equal(0, fired);
        Assert.Equal(1, host.Transfer.ImportCount);         // no second import either
    }

    /// <summary>
    /// A gate at <see cref="SnapshotInstallPhase.AfterImport"/> proves the ordering is unchanged:
    /// the import has already run when it fires and the boundary has not.
    /// </summary>
    [Fact]
    public async Task GateAfterImport_SeesTheImportDone_AndTheBoundaryNotYetWritten()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await CommitEntryAsync(sm, wal, 1);

        int importsAtGate = -1;
        int boundariesAtGate = -1;

        sm.SetSnapshotInstallGateForTesting(new SnapshotInstallGate(SnapshotInstallPhase.AfterImport, _ =>
        {
            importsAtGate = host.Transfer.ImportCount;
            boundariesAtGate = wal.BoundaryCallCount;
            return ValueTask.CompletedTask;
        }));

        Assert.Equal(RaftOperationStatus.Success,
            (await sm.InstallSnapshotAsync(Install(snapshotIndex: 1, lastIncludedTerm: 1, leaderTerm: 5))).Status);

        Assert.Equal(1, importsAtGate);
        Assert.Equal(0, boundariesAtGate);
    }

    // ── consumer-apply hold ────────────────────────────────────────────────────

    /// <summary>
    /// While applies are held, committed entries accumulate, the consumer sees none, and the node
    /// advertises no applied progress it did not make. Resuming delivers everything that
    /// accumulated, in log id order and exactly once.
    /// </summary>
    [Fact]
    public async Task AppliesHeld_EntriesAccumulateUndelivered_AndResumeDeliversThemInOrderOnce()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        await CommitEntryAsync(sm, wal, 1);
        Assert.Equal(["Applied:1"], host.EventLog);
        Assert.Equal(1L, await AppliedCursorAsync(sm));

        sm.HoldConsumerAppliesForTesting(replyCorrelationId: null);

        for (long id = 2; id <= 5; id++)
            await CommitEntryAsync(sm, wal, id);

        // Committed and durable, but undelivered — and the advertised cursor did not move.
        Assert.Equal(["Applied:1"], host.EventLog);
        Assert.Equal(1L, await AppliedCursorAsync(sm));
        Assert.Equal(5L, wal.GetCommitIndex());

        await sm.ResumeConsumerAppliesForTesting(replyCorrelationId: null);

        Assert.Equal(["Applied:1", "Applied:2", "Applied:3", "Applied:4", "Applied:5"], host.EventLog);
        Assert.Equal(5L, await AppliedCursorAsync(sm));

        // Resuming again delivers nothing further: the applied cursor is the exactly-once guard.
        await sm.ResumeConsumerAppliesForTesting(replyCorrelationId: null);
        Assert.Equal(["Applied:1", "Applied:2", "Applied:3", "Applied:4", "Applied:5"], host.EventLog);
    }

    /// <summary>
    /// Stopping the partition clears the hold, so a test that exits without resuming cannot leave a
    /// node that never applies again.
    /// </summary>
    [Fact]
    public async Task PartitionStop_ClearsTheApplyHold()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        sm.HoldConsumerAppliesForTesting(replyCorrelationId: null);
        await CommitEntryAsync(sm, wal, 1);
        Assert.Empty(host.EventLog);

        sm.ResetTestingState();                             // what RaftPartition.Dispose runs

        await CommitEntryAsync(sm, wal, 2);
        Assert.Contains("Applied:2", host.EventLog);
    }

    /// <summary>
    /// The case the hand-off exists for: a snapshot installed on a node whose log tail is still
    /// pending. The boundary term matches the tail, so the suffix is retained; the consumer receives
    /// the import; resuming delivers the tail exactly once and in order; and the node's final
    /// consumer state equals that of a replica which took the same entries without any install.
    /// </summary>
    [Fact]
    public async Task SnapshotInstalledOverAPendingTail_RetainsTheSuffix_AndConvergesWithANeverInstalledReplica()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        // Entries 1..5 are delivered normally; 6..8 commit while the consumer is paused.
        for (long id = 1; id <= 5; id++)
            await CommitEntryAsync(sm, wal, id);

        sm.HoldConsumerAppliesForTesting(replyCorrelationId: null);

        for (long id = 6; id <= 8; id++)
            await CommitEntryAsync(sm, wal, id);

        Assert.Equal(5L, await AppliedCursorAsync(sm));
        Assert.Equal(8L, wal.GetCommitIndex());

        // A snapshot whose boundary is 5, with a term matching the tail.
        RaftResponse response = await sm.InstallSnapshotAsync(Install(snapshotIndex: 5, lastIncludedTerm: 1, leaderTerm: 7));

        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(1, host.Transfer.ImportCount);
        Assert.False(wal.LastSuffixTruncated);
        Assert.Equal([6L, 7L, 8L], wal.PresentIds.Where(id => id > 5).ToList());

        // The import did not smuggle the pending tail into the consumer.
        Assert.Equal(["Applied:1", "Applied:2", "Applied:3", "Applied:4", "Applied:5"], host.EventLog);

        await sm.ResumeConsumerAppliesForTesting(replyCorrelationId: null);

        // The retained tail is delivered exactly once, in order, and nothing at or below the
        // boundary is re-delivered.
        Assert.Equal(
            ["Applied:1", "Applied:2", "Applied:3", "Applied:4", "Applied:5", "Applied:6", "Applied:7", "Applied:8"],
            host.EventLog);

        // Control: a replica that took the same entries and never installed anything.
        (RaftPartitionStateMachine control, RecordingHost controlHost, SnapshotWalFacade controlWal) = Build();
        await control.ForceLeaderForTestingAsync(replyCorrelationId: null);
        for (long id = 1; id <= 8; id++)
            await CommitEntryAsync(control, controlWal, id);

        Assert.Equal(controlHost.EventLog, host.EventLog);
        Assert.Equal(await AppliedCursorAsync(control), await AppliedCursorAsync(sm));
    }

    /// <summary>
    /// The conflicting-term twin: the same shape with a boundary term the tail does not match. The
    /// suffix is truncated, and the discarded entries are never delivered — resuming applies
    /// nothing, because the entries no longer exist.
    /// </summary>
    [Fact]
    public async Task SnapshotWithConflictingTailTerm_TruncatesTheSuffix_AndNeverDeliversTheDiscardedEntries()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, SnapshotWalFacade wal) = Build();
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        for (long id = 1; id <= 5; id++)
            await CommitEntryAsync(sm, wal, id);

        sm.HoldConsumerAppliesForTesting(replyCorrelationId: null);

        for (long id = 6; id <= 8; id++)
            await CommitEntryAsync(sm, wal, id);

        // Boundary term 9 conflicts with the stored term 1 at index 5: not the same history.
        RaftResponse response = await sm.InstallSnapshotAsync(Install(snapshotIndex: 5, lastIncludedTerm: 9, leaderTerm: 9));

        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.True(wal.LastSuffixTruncated);
        Assert.DoesNotContain(wal.PresentIds, id => id > 5);

        await sm.ResumeConsumerAppliesForTesting(replyCorrelationId: null);

        Assert.Equal(["Applied:1", "Applied:2", "Applied:3", "Applied:4", "Applied:5"], host.EventLog);
        Assert.DoesNotContain("Applied:6", host.EventLog);
        Assert.DoesNotContain("Applied:7", host.EventLog);
        Assert.DoesNotContain("Applied:8", host.EventLog);
    }
}
