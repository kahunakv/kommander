using Kommander;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Verifies the completion router's durable presence advance: a WAL completion that reports
/// success must call <see cref="IRaftWalFacade.MarkDurablyWritten"/> with the ids the write
/// carried — BEFORE the term and pending fences, because a write that reached the disk is a fact
/// about this node's disk regardless of which term submitted it or whether the operation is still
/// tracked — and a failed completion must not. The published commit index is gated on this
/// advance; a fenced success that skipped it would pin the published value below the truth.
/// </summary>
public sealed class TestSuccessfulCompletionDurableAdvance
{
    // ── Recording facade ───────────────────────────────────────────────────

    private sealed class RecordingWal : IRaftWalFacade
    {
        public readonly List<(long Min, long Max, long[]? Sparse)> Durable = [];

        public void MarkDurablyWritten(long minLogIndex, long maxLogIndex, long[]? sparseLogIds) =>
            Durable.Add((minLogIndex, maxLogIndex, sparseLogIds));

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => MakeNoOp();
        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class RelaySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;
        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    private static (RaftPartitionExecutor Executor, RecordingWal Wal) BuildExecutor()
    {
        TestWalCompletionFences.StubHost host = new(partitionId: 0);
        RecordingWal wal = new();
        RelaySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, 0, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return (executor, wal);
    }

    private static RaftWalCompletion Completion(
        WALWriteOperationType type,
        RaftOperationStatus status,
        long min = 5,
        long max = 8,
        long writtenMax = 7,
        long[]? sparse = null,
        long term = -1,
        long operationId = 9999) =>
        new(
            PartitionId: 0,
            OperationId: operationId,
            Term: term,
            MinLogIndex: min,
            MaxLogIndex: max,
            OperationType: type,
            Status: status,
            WrittenMaxLogIndex: writtenMax,
            SparseLogIds: sparse);

    private static RaftRequest Request(RaftWalCompletion completion) =>
        new(RaftRequestType.WriteOperationCompleted, completion);

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A successful, orphaned (unregistered) completion advances durable presence over the ids
    /// the write carried — the WRITTEN max, not the operation's index field — even though the
    /// pending fence discards the completion afterwards.
    /// </summary>
    [Fact]
    public async Task SuccessfulOrphanedCompletion_AdvancesDurablePresenceOverTheWrittenIds()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success)),
                TestContext.Current.CancellationToken);

            (long min, long max, long[]? sparse) = Assert.Single(wal.Durable);
            Assert.Equal(5, min);
            Assert.Equal(7, max);
            Assert.Null(sparse);
        }
    }

    /// <summary>The advance survives the term fence: a completion from a stale term still landed on disk.</summary>
    [Fact]
    public async Task SuccessfulCompletionFromAStaleTerm_StillAdvancesDurablePresence()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.FollowerAppend, RaftOperationStatus.Success, term: 42, sparse: [5, 7])),
                TestContext.Current.CancellationToken);

            (long min, long max, long[]? sparse) = Assert.Single(wal.Durable);
            Assert.Equal(5, min);
            Assert.Equal(7, max);
            Assert.NotNull(sparse);
            Assert.Equal([5L, 7L], sparse);
        }
    }

    /// <summary>A failed completion wrote nothing, so it advances nothing.</summary>
    [Fact]
    public async Task FailedCompletion_DoesNotAdvanceDurablePresence()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.FollowerAppend, RaftOperationStatus.Errored)),
                TestContext.Current.CancellationToken);

            Assert.Empty(wal.Durable);
        }
    }

    /// <summary>A completion that carried no log ids (index-less) advances nothing.</summary>
    [Fact]
    public async Task IndexLessCompletion_DoesNotAdvanceDurablePresence()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.LeaderCommit, RaftOperationStatus.Success, min: -1, max: -1, writtenMax: -1)),
                TestContext.Current.CancellationToken);

            Assert.Empty(wal.Durable);
        }
    }
}
