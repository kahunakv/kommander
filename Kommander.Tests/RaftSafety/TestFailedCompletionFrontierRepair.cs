
using Kommander;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Verifies the completion router's failed-write frontier repair: a WAL completion that reports
/// a failure must trigger <see cref="IRaftWalFacade.RegressFrontiersAfterFailedWriteAsync"/> with
/// the flags matching what the operation's enqueue path advanced — and it must do so BEFORE the
/// term and pending fences, because a failed write is a fact about this node's disk regardless of
/// which term submitted it or whether the operation is still tracked.
/// </summary>
public sealed class TestFailedCompletionFrontierRepair
{
    // ── Recording facade ───────────────────────────────────────────────────

    private sealed class RecordingWal : IRaftWalFacade
    {
        public readonly List<(long Min, long Max, bool Presence, bool Commit)> Regressions = [];

        public ValueTask RegressFrontiersAfterFailedWriteAsync(long minLogIndex, long maxLogIndex, bool regressPresence, bool regressCommit)
        {
            Regressions.Add((minLogIndex, maxLogIndex, regressPresence, regressCommit));
            return ValueTask.CompletedTask;
        }

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
        long max = 7,
        long term = -1,
        long operationId = 9999) =>
        new(
            PartitionId: 0,
            OperationId: operationId,
            Term: term,
            MinLogIndex: min,
            MaxLogIndex: max,
            OperationType: type,
            Status: status);

    private static RaftRequest Request(RaftWalCompletion completion) =>
        new(RaftRequestType.WriteOperationCompleted, completion);

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A failed leader propose regresses only the presence frontier — its enqueue advanced no
    /// commit state. The operation is deliberately unregistered (orphan): the repair must run
    /// even though the pending fence will discard the completion afterwards.
    /// </summary>
    [Fact]
    public async Task FailedLeaderPropose_RegressesPresenceOnly_EvenWhenOrphaned()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.LeaderPropose, RaftOperationStatus.Errored)),
                TestContext.Current.CancellationToken);

            (long min, long max, bool presence, bool commit) = Assert.Single(wal.Regressions);
            Assert.Equal(5, min);
            Assert.Equal(7, max);
            Assert.True(presence);
            Assert.False(commit);
        }
    }

    /// <summary>A failed leader commit regresses only the commit frontier.</summary>
    [Fact]
    public async Task FailedLeaderCommit_RegressesCommitOnly()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.LeaderCommit, RaftOperationStatus.Errored)),
                TestContext.Current.CancellationToken);

            (_, _, bool presence, bool commit) = Assert.Single(wal.Regressions);
            Assert.False(presence);
            Assert.True(commit);
        }
    }

    /// <summary>A failed follower append regresses both frontiers — its batch mixes new rows and resolutions.</summary>
    [Fact]
    public async Task FailedFollowerAppend_RegressesBothFrontiers()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.FollowerAppend, RaftOperationStatus.Errored)),
                TestContext.Current.CancellationToken);

            (_, _, bool presence, bool commit) = Assert.Single(wal.Regressions);
            Assert.True(presence);
            Assert.True(commit);
        }
    }

    /// <summary>
    /// A failed rollback triggers no regression: rollback markers re-write ids that are already
    /// durably present, so its enqueue advanced neither frontier.
    /// </summary>
    [Fact]
    public async Task FailedLeaderRollback_TriggersNoRegression()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.LeaderRollback, RaftOperationStatus.Errored)),
                TestContext.Current.CancellationToken);

            Assert.Empty(wal.Regressions);
        }
    }

    /// <summary>
    /// The repair runs before the term fence: a failed write submitted in a superseded term is
    /// still a fact about this node's disk, so a mismatched term must not skip the regression.
    /// </summary>
    [Fact]
    public async Task TermMismatchedFailure_StillRegresses()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.FollowerAppend, RaftOperationStatus.Errored, term: 99)),
                TestContext.Current.CancellationToken);

            Assert.Single(wal.Regressions);
        }
    }

    /// <summary>Successful completions and index-less failures must never regress anything.</summary>
    [Fact]
    public async Task SuccessOrIndexlessCompletions_TriggerNoRegression()
    {
        (RaftPartitionExecutor executor, RecordingWal wal) = BuildExecutor();
        using (executor)
        {
            await executor.Ask(
                Request(Completion(WALWriteOperationType.FollowerAppend, RaftOperationStatus.Success)),
                TestContext.Current.CancellationToken);

            await executor.Ask(
                Request(Completion(WALWriteOperationType.FollowerAppend, RaftOperationStatus.Errored, min: -1, max: -1)),
                TestContext.Current.CancellationToken);

            Assert.Empty(wal.Regressions);
        }
    }
}
