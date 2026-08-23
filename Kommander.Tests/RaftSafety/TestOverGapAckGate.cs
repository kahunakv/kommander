
using Kommander;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Regression tests for the quorum-integrity gate on follower append acks (the Scenario09
/// applied-prefix hole, Vorpal feature 84c75177-4f4d-4db5-8b72-f19b90cf5c21).
///
/// <para>The leader counts a Success append ack toward propose quorum with no further proof,
/// while election freshness advertises only the CONTIGUOUS presence frontier. A follower that
/// persisted a batch ABOVE a gap (the unanchored live-propose broadcast writes over gaps by
/// design) can therefore help commit an entry it can never defend in an election: a candidate
/// without the entry out-advertises it, wins, and overwrites the committed entry — which the
/// old leader has already applied. The fix: a follower acks Success only when its presence
/// frontier covers the batch; otherwise it reports LogMismatch anchored at the presence
/// frontier so the leader backfills the gap first. Both ack sites are gated — the WAL
/// completion ack (WalCompletionRouter.CompleteFollowerAppend) and the duplicate re-ack
/// (FollowerAppendHandler.AppendLogsCoreAsync).</para>
/// </summary>
public sealed class TestOverGapAckGate
{
    /// <summary>
    /// WAL stub with controllable presence/commit/max frontiers. In "plan nothing" mode
    /// <see cref="EnqueueProposeOrCommit"/> returns null, which drives the duplicate re-ack path.
    /// </summary>
    private sealed class GapStubWal : IRaftWalFacade
    {
        public long PresentIndex { get; set; } = -1;
        public long CommitIndexValue { get; set; }
        public long MaxLogValue { get; set; }
        public bool PlanNothing { get; set; }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(MaxLogValue);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => CommitIndexValue;
        public long GetPresentIndex() => PresentIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
            => PlanNothing ? null : MakeNoOp();
        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.FollowerAppend, (0, []));
    }

    private sealed class NullSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    private static (RaftPartitionStateMachine Sm, TestWalCompletionFences.StubHost Host, GapStubWal Wal) Build()
    {
        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        GapStubWal wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NullSink(), NullLogger<IRaft>.Instance);
        return (sm, host, wal);
    }

    private static CompleteAppendLogsRequest SingleAck(TestWalCompletionFences.StubHost host)
    {
        (_, RaftResponderRequest request) = Assert.Single(host.EnqueuedResponses);
        Assert.Equal(RaftResponderRequestType.CompleteAppendLogs, request.Type);
        Assert.NotNull(request.CompleteAppendLogsRequest);
        return request.CompleteAppendLogsRequest!;
    }

    // ── Completion ack (WalCompletionRouter.CompleteFollowerAppend) ─────────────────

    [Fact]
    public async Task AppendOverGap_CompletionAck_IsLogMismatchAnchoredAtPresence()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, GapStubWal wal) = Build();
        wal.PresentIndex = 5;       // contiguous only through 5 — the batch at 7 sits over a gap
        wal.CommitIndexValue = 5;
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 7, Term = 1, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }]);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: 0, Term: -1, MinLogIndex: 7, MaxLogIndex: 7,
            OperationType: WALWriteOperationType.FollowerAppend, Status: RaftOperationStatus.Success));

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.LogMismatch, ack.Status);
        Assert.Equal(5, ack.CommitIndex);
    }

    [Fact]
    public async Task ContiguousAppend_CompletionAck_IsSuccess()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, GapStubWal wal) = Build();
        wal.PresentIndex = 7;       // presence covers the batch — normal contiguous append
        wal.CommitIndexValue = 6;
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 7, Term = 1, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }]);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: 0, Term: -1, MinLogIndex: 7, MaxLogIndex: 7,
            OperationType: WALWriteOperationType.FollowerAppend, Status: RaftOperationStatus.Success));

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.Success, ack.Status);
        Assert.Equal(6, ack.CommitIndex);
    }

    [Fact]
    public async Task AppendWithoutPresenceTracking_CompletionAck_StaysSuccess()
    {
        // A facade that does not track presence (-1) must keep the legacy behavior.
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, GapStubWal wal) = Build();
        wal.PresentIndex = -1;
        wal.CommitIndexValue = 6;
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 7, Term = 1, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }]);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: 0, Term: -1, MinLogIndex: 7, MaxLogIndex: 7,
            OperationType: WALWriteOperationType.FollowerAppend, Status: RaftOperationStatus.Success));

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.Success, ack.Status);
    }

    // ── Duplicate re-ack (FollowerAppendHandler.AppendLogsCoreAsync) ────────────────

    [Fact]
    public async Task DuplicateHeldOverGap_ReackIsLogMismatchAnchoredAtPresence()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, GapStubWal wal) = Build();
        wal.PlanNothing = true;     // batch already present in the WAL → duplicate path
        wal.PresentIndex = 5;       // held above a gap
        wal.CommitIndexValue = 5;
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 7, Term = 1, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }]);

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.LogMismatch, ack.Status);
        Assert.Equal(5, ack.CommitIndex);
    }

    [Fact]
    public async Task DuplicateHeldContiguously_ReackIsSuccess()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, GapStubWal wal) = Build();
        wal.PlanNothing = true;
        wal.PresentIndex = 7;       // contiguously grounded — re-ack supplies the quorum ack
        wal.CommitIndexValue = 7;
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 7, Term = 1, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }]);

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.Success, ack.Status);
    }
}
