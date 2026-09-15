using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// The follower's Log Matching check for a backfill anchored on an entry the leader compacted
/// (<c>FollowerAppendHandler</c>, DST FINDING 6).
///
/// <para>A leader ships <c>prevLogTerm = -1</c> when the anchor entry is below its own compaction
/// floor. The follower used to compare its real term with -1, call it divergence, and reject the
/// batch — and because the leader's batch read had succeeded, nothing escalated and the leader
/// re-shipped the same batch forever. The follower now accepts that anchor when it lies inside its
/// own committed prefix, which Leader Completeness makes safe: the leader compacts committed entries
/// only, and a committed entry at the same index is the same entry.</para>
///
/// <para>The two rejection tests matter as much as the acceptance test. The rule is narrow on
/// purpose: above the committed frontier, and for any term other than -1, the check is exactly what
/// it was.</para>
/// </summary>
public sealed class TestLeaderCompactedAnchor
{
    /// <summary>
    /// WAL stub with a settable term at every index and a settable commit frontier, recording every
    /// batch the follower accepted for writing.
    /// </summary>
    private sealed class AnchorStubWal : IRaftWalFacade
    {
        public long LocalTermAtAnyIndex { get; set; }
        public long CommitIndexValue { get; set; }
        public long MaxLogValue { get; set; }

        /// <summary>Every batch handed to the write path, in call order.</summary>
        public List<List<RaftLog>> Enqueued { get; } = [];

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(MaxLogValue);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(LocalTermAtAnyIndex);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => CommitIndexValue;
        public long GetPresentIndex() => CommitIndexValue;
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(MaxLogValue);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
        {
            if (logs is not null)
                Enqueued.Add(logs);

            return MakeNoOp();
        }

        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.FollowerAppend, (0, []));
    }

    private sealed class NullSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    private static (RaftPartitionStateMachine Sm, TestWalCompletionFences.StubHost Host, AnchorStubWal Wal) Build()
    {
        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        AnchorStubWal wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NullSink(), NullLogger<IRaft>.Instance);
        sm.MarkRestoredForTesting();
        return (sm, host, wal);
    }

    private static List<RaftLog> Batch(long firstId, long lastId)
    {
        List<RaftLog> logs = [];
        for (long id = firstId; id <= lastId; id++)
            logs.Add(new RaftLog { Id = id, Term = 3, Type = RaftLogType.Committed, LogType = "t", LogData = [1] });
        return logs;
    }

    private static bool AnyLogMismatch(TestWalCompletionFences.StubHost host) =>
        host.EnqueuedResponses.Any(response =>
            response.Item2.CompleteAppendLogsRequest?.Status == RaftOperationStatus.LogMismatch);

    /// <summary>
    /// The FINDING 6 shape: the follower committed through 4, holds entry 4 in term 3, and the
    /// leader anchors a batch from 5 on its compacted entry 4. The batch must go to the write path.
    /// </summary>
    [Fact]
    public async Task ACompactedAnchorInsideTheCommittedPrefix_IsAccepted()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, AnchorStubWal wal) = Build();
        wal.LocalTermAtAnyIndex = 3;
        wal.CommitIndexValue = 4;
        wal.MaxLogValue = 4;

        await sm.AppendLogsAsync("leader-node", term: 3, host.HybridLogicalClock.SendOrLocalEvent(2),
            Batch(5, 7), prevLogIndex: 4, prevLogTerm: -1);

        Assert.False(AnyLogMismatch(host), "The follower rejected a batch anchored inside its committed prefix.");

        List<RaftLog> accepted = Assert.Single(wal.Enqueued);
        Assert.Equal([5L, 6L, 7L], accepted.Select(log => log.Id));
    }

    /// <summary>
    /// The same -1 anchor one entry above the committed frontier. Nothing vouches for an uncommitted
    /// entry at the anchor, so the check must reject exactly as before.
    /// </summary>
    [Fact]
    public async Task ACompactedAnchorAboveTheCommittedFrontier_IsStillRejected()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, AnchorStubWal wal) = Build();
        wal.LocalTermAtAnyIndex = 3;
        wal.CommitIndexValue = 3;
        wal.MaxLogValue = 4;

        await sm.AppendLogsAsync("leader-node", term: 3, host.HybridLogicalClock.SendOrLocalEvent(2),
            Batch(5, 7), prevLogIndex: 4, prevLogTerm: -1);

        Assert.True(AnyLogMismatch(host), "An anchor above the committed frontier was accepted on a -1 term.");
        Assert.Empty(wal.Enqueued);
    }

    /// <summary>
    /// A real term disagreement inside the committed prefix is not the compacted case and must stay a
    /// rejection. The rule keys on -1, the one value that means "the leader no longer holds it".
    /// </summary>
    [Fact]
    public async Task ARealTermMismatchInsideTheCommittedPrefix_IsStillRejected()
    {
        (RaftPartitionStateMachine sm, TestWalCompletionFences.StubHost host, AnchorStubWal wal) = Build();
        wal.LocalTermAtAnyIndex = 3;
        wal.CommitIndexValue = 4;
        wal.MaxLogValue = 4;

        await sm.AppendLogsAsync("leader-node", term: 3, host.HybridLogicalClock.SendOrLocalEvent(2),
            Batch(5, 7), prevLogIndex: 4, prevLogTerm: 2);

        Assert.True(AnyLogMismatch(host), "A real term mismatch was accepted.");
        Assert.Empty(wal.Enqueued);
    }
}
