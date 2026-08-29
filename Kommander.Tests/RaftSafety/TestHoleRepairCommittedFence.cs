using Kommander;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Regression tests for the committed-frontier fence on the follower's log-hole repair
/// (<c>FollowerAppendHandler</c>).
///
/// <para>An anchored AppendEntries whose <c>prevLogIndex</c> lands on a missing row makes the
/// follower delete everything above the hole, so the leader can heal it in one forward backfill
/// pass. One premise licenses that delete: a row above an unfilled gap can never have earned quorum
/// credit here, so this node's advertised commit frontier stays below the hole. Two mechanisms hold
/// the premise up — the frontier advances only contiguously, and the over-gap ack gate withholds
/// the Success ack that would let such a row count toward quorum.</para>
///
/// <para>The fence tests the premise instead of assuming it. A frontier that reaches the anchor
/// means the node advertises a resolved prefix covering an id it does not hold, so a leader may
/// have counted a row that is about to be deleted; the repair then falls back to a backfill
/// anchored at the contiguous presence frontier, which fixes the hole from below and deletes
/// nothing.</para>
/// </summary>
public sealed class TestHoleRepairCommittedFence
{
    /// <summary>
    /// WAL stub with settable frontiers that records every truncation boundary it is asked for.
    /// <see cref="GetAnyTermAtAsync"/> always reports "no entry", which is what puts the append
    /// path on the hole branch.
    /// </summary>
    private sealed class HoleStubWal : IRaftWalFacade
    {
        public long PresentIndex { get; set; } = -1;
        public long CommitIndexValue { get; set; }
        public long MaxLogValue { get; set; }

        /// <summary>Every truncation boundary requested, in call order.</summary>
        public List<long> TruncateCalls { get; } = [];

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(MaxLogValue);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => CommitIndexValue;
        public long GetPresentIndex() => PresentIndex;

        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId)
        {
            TruncateCalls.Add(afterLogId);
            if (MaxLogValue > afterLogId)
                MaxLogValue = afterLogId;
            return ValueTask.FromResult(MaxLogValue);
        }

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => MakeNoOp();
        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.FollowerAppend, (0, []));
    }

    private sealed class NullSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    private static (RaftPartitionStateMachine Sm, TestWalCompletionFences.StubHost Host, HoleStubWal Wal) Build()
    {
        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        HoleStubWal wal = new();
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

    /// <summary>
    /// The frontier (6) covers the anchor (5) that reads as a hole: the node advertises a prefix it
    /// does not hold, so a row in the range about to be deleted may be one a leader counted toward
    /// quorum. Nothing may be truncated; the reply anchors the leader's backfill at the contiguous
    /// presence frontier so the hole is repaired from below.
    /// </summary>
    [Fact]
    public async Task HoleUnderTheAdvertisedFrontier_IsNotTruncated_AndAsksForABackfill()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, HoleStubWal wal) = Build();
        wal.PresentIndex = 3;       // contiguous only through 3
        wal.CommitIndexValue = 6;   // …yet the node advertises a resolved prefix through 6
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 6, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] }],
            prevLogIndex: 5, prevLogTerm: 1);

        Assert.Empty(wal.TruncateCalls);

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.LogMismatch, ack.Status);
        Assert.Equal(3, ack.CommitIndex);
    }

    /// <summary>
    /// The ordinary case must be untouched: with the frontier (3) below the anchor (5) the premise
    /// holds — every row above the hole is provably uncommitted — so the repair still truncates the
    /// orphaned tail and reports the post-truncation max, which lets the leader heal the gap in one
    /// forward pass.
    /// </summary>
    [Fact]
    public async Task HoleAboveTheAdvertisedFrontier_StillTruncatesTheOrphanedTail()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, HoleStubWal wal) = Build();
        wal.PresentIndex = 3;
        wal.CommitIndexValue = 3;   // the frontier stops below the hole, as it must
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 6, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] }],
            prevLogIndex: 5, prevLogTerm: 1);

        long truncateBoundary = Assert.Single(wal.TruncateCalls);
        Assert.Equal(4, truncateBoundary);

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.LogMismatch, ack.Status);
        Assert.Equal(4, ack.CommitIndex);
    }

    /// <summary>
    /// A facade that does not track presence (-1) reports the local max as the backfill anchor
    /// rather than the frontier, so the leader still backtracks a slot at a time instead of
    /// deleting. The fence must not depend on presence tracking to refuse.
    /// </summary>
    [Fact]
    public async Task HoleUnderTheAdvertisedFrontier_WithoutPresenceTracking_StillRefusesToTruncate()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, HoleStubWal wal) = Build();
        wal.PresentIndex = -1;      // presence not tracked
        wal.CommitIndexValue = 6;
        wal.MaxLogValue = 7;

        await sm.AppendLogsAsync("leader-node", term: 1, host.HybridLogicalClock.SendOrLocalEvent(2),
            [new RaftLog { Id = 6, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] }],
            prevLogIndex: 5, prevLogTerm: 1);

        Assert.Empty(wal.TruncateCalls);

        CompleteAppendLogsRequest ack = SingleAck(host);
        Assert.Equal(RaftOperationStatus.LogMismatch, ack.Status);
        Assert.Equal(7, ack.CommitIndex);
    }
}
