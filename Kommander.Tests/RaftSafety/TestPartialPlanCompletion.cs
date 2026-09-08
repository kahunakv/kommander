
using Kommander;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Regression tests for the min-log cross-check in <c>WalCompletionRouter.CompleteWalOperationAsync</c>
/// when the WAL plans a strict subset of a follower append.
///
/// <para>The completion envelope's <c>MinLogIndex</c> is computed over the entries the WAL actually
/// wrote (<c>FairWalScheduler.BuildCompletion</c> over <c>op.Logs</c>), while the pending record keeps
/// the batch as received. <c>RaftWriteAhead.EnqueueProposeOrCommit</c> skips a stale Proposed copy of
/// an id the follower has already resolved, so a proposal retry or an overlapping backfill batch —
/// the normal traffic after a gap — plans fewer entries than it received and its envelope min sits
/// above the batch min. The router used to require equality and discarded such completions: no ack
/// reached the leader, the fast-path apply was skipped, and the peer stayed in backfill. Observed in
/// the Caraxes <c>bank-rebase</c> soaks as dozens of <c>min-log-index mismatch</c> discards per
/// minute on every follower (CamusDB feature 80af367a).</para>
///
/// <para>The check must still reject a real mix-up: an envelope whose min is below anything the
/// batch carried, or names an id the batch never held.</para>
/// </summary>
public sealed class TestPartialPlanCompletion
{
    /// <summary>
    /// WAL stub that plans a strict subset of every append: the lowest id is treated as an already
    /// resolved stale duplicate and dropped from the planned operation, mirroring the production
    /// planner. Presence and commit frontiers are controllable so the ack gate sees a grounded batch.
    /// </summary>
    private sealed class SubsetPlanStubWal : IRaftWalFacade
    {
        public long PresentIndex { get; set; } = -1;
        public long CommitIndexValue { get; set; }
        public long MaxLogValue { get; set; }
        public List<RaftLog>? LastPlanned { get; private set; }

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
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => Plan(logs);
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => Plan(logs);
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => Plan(logs);
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
            => Plan(logs ?? []);
        public void NotifyCommitted() { }

        private WALWriteOperation Plan(List<RaftLog> received)
        {
            long min = received.Min(l => l.Id);
            LastPlanned = received.Where(l => l.Id != min).ToList();
            return new WALWriteOperation(_ => { }, 0, WALWriteOperationType.FollowerAppend, (1, LastPlanned));
        }
    }

    private sealed class NullSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    private static (RaftPartitionStateMachine Sm, TestWalCompletionFences.StubHost Host, SubsetPlanStubWal Wal) Build()
    {
        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        SubsetPlanStubWal wal = new() { PresentIndex = 8, CommitIndexValue = 8, MaxLogValue = 8 };
        RaftPartitionStateMachine sm = new(host, wal, new NullSink(), NullLogger<IRaft>.Instance);
        return (sm, host, wal);
    }

    private static RaftLog Proposed(long id) => new() { Id = id, Term = 1, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] };

    /// <summary>
    /// The envelope built over the planned subset (min 8, batch min 7) is exactly the scheduler's
    /// output for this batch. It must be routed: the leader gets its Success ack.
    /// </summary>
    [Fact]
    public async Task PlannedSubset_EnvelopeMinAboveBatchMin_IsRoutedAndAcked()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, SubsetPlanStubWal wal) = Build();

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2), [Proposed(7), Proposed(8)]);
        Assert.Equal([8L], wal.LastPlanned!.Select(l => l.Id));

        // Exactly what FairWalScheduler.BuildCompletion produces for the planned operation.
        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: 0, Term: -1, MinLogIndex: 8, MaxLogIndex: 8,
            OperationType: WALWriteOperationType.FollowerAppend, Status: RaftOperationStatus.Success));

        (_, RaftResponderRequest request) = Assert.Single(host.EnqueuedResponses);
        Assert.Equal(RaftResponderRequestType.CompleteAppendLogs, request.Type);
        Assert.Equal(RaftOperationStatus.Success, request.CompleteAppendLogsRequest!.Status);
        Assert.Equal(8, request.CompleteAppendLogsRequest.CommitIndex);
    }

    /// <summary>A full-plan envelope (min equals the batch min) keeps working as before.</summary>
    [Fact]
    public async Task FullPlan_EnvelopeMinEqualsBatchMin_IsRoutedAndAcked()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, _) = Build();

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2), [Proposed(7), Proposed(8)]);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: 0, Term: -1, MinLogIndex: 7, MaxLogIndex: 8,
            OperationType: WALWriteOperationType.FollowerAppend, Status: RaftOperationStatus.Success));

        (_, RaftResponderRequest request) = Assert.Single(host.EnqueuedResponses);
        Assert.Equal(RaftOperationStatus.Success, request.CompleteAppendLogsRequest!.Status);
    }

    /// <summary>An envelope min below everything the batch carried is a real mix-up and stays discarded.</summary>
    [Fact]
    public async Task EnvelopeMinBelowBatch_IsDiscardedWithoutAck()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, _) = Build();

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2), [Proposed(7), Proposed(8)]);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: 0, Term: -1, MinLogIndex: 3, MaxLogIndex: 8,
            OperationType: WALWriteOperationType.FollowerAppend, Status: RaftOperationStatus.Success));

        Assert.Empty(host.EnqueuedResponses);
    }

    /// <summary>An envelope min the batch never held (above its max) is a real mix-up and stays discarded.</summary>
    [Fact]
    public async Task EnvelopeMinNotInBatch_IsDiscardedWithoutAck()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, _) = Build();

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2), [Proposed(7), Proposed(8)]);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            PartitionId: 1, OperationId: 0, Term: -1, MinLogIndex: 9, MaxLogIndex: 9,
            OperationType: WALWriteOperationType.FollowerAppend, Status: RaftOperationStatus.Success));

        Assert.Empty(host.EnqueuedResponses);
    }
}
