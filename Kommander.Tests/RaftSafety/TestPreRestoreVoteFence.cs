using Kommander;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Regression tests for the pre-restore vote fence in <see cref="RaftPartitionExecutor"/>.
///
/// <para>Everything a vote decision reads is at its pre-restore init until Phase 2 finishes:
/// <c>CurrentTerm</c> is 0, the vote record is empty, and the freshness position is 0. The durable
/// <c>(term, votedFor)</c> pair is loaded by <c>CompleteRestoreAsync</c> and nowhere else. A node
/// paused in Phase 1 therefore grants unconditionally, and the grant path then persists the new
/// pair OVER the record it has not read — so a node that already voted in that term before a crash
/// casts a second durable vote in the same term, and two leaders in one term follow.</para>
///
/// <para>The fence drops <c>RequestVote</c> until the restore completes. A denial is silence on
/// this path, so dropping is protocol-safe and costs at most one election round.</para>
/// </summary>
public sealed class TestPreRestoreVoteFence
{
    /// <summary>
    /// WAL facade whose restore Phase 1 parks until the test releases it, so the pre-restore window
    /// is deterministic rather than a race against the thread pool.
    /// </summary>
    private sealed class GatedRestoreWal : IRaftWalFacade
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Lets restore Phase 1 return, which drives Phase 2 and clears the fence.</summary>
        public void ReleaseRestore() => _gate.TrySetResult();

        public async ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            await _gate.Task.ConfigureAwait(false);
            return [];
        }

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

    private static (RaftPartitionExecutor Executor, TestWalCompletionFences.StubHost Host, GatedRestoreWal Wal) Build()
    {
        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        GatedRestoreWal wal = new();
        RelaySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId: 1, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return (executor, host, wal);
    }

    private static RaftRequest Vote(long term, HLCTimestamp timestamp, bool preVote = false) =>
        new(RaftRequestType.RequestVote,
            term: term,
            commitIndex: 5,          // candidate's last log index
            timestamp: timestamp,
            endpoint: "candidate-node",
            preVote: preVote,
            lastLogTerm: term);

    /// <summary>
    /// A vote request that arrives while Phase 1 is still parked must produce no grant at all. The
    /// same request after the restore completes proves the fence is the reason, and not the stub's
    /// own freshness state.
    /// </summary>
    [Fact]
    public async Task RequestVoteBeforeRestoreCompletes_IsDropped_AndGrantedAfter()
    {
        (RaftPartitionExecutor executor, TestWalCompletionFences.StubHost host, GatedRestoreWal wal) = Build();

        try
        {
            // The Ask completes off the fence itself, so this is a synchronization point, not a sleep.
            await executor.Ask(Vote(term: 1, host.HybridLogicalClock.SendOrLocalEvent(2)),
                TestContext.Current.CancellationToken);

            Assert.False(executor.IsRestored);
            Assert.DoesNotContain(host.EnqueuedResponses, r => r.Item2.Type == RaftResponderRequestType.Vote);

            wal.ReleaseRestore();
            await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            await executor.Ask(Vote(term: 2, host.HybridLogicalClock.SendOrLocalEvent(2)),
                TestContext.Current.CancellationToken);

            Assert.Contains(host.EnqueuedResponses, r => r.Item2.Type == RaftResponderRequestType.Vote);
        }
        finally
        {
            wal.ReleaseRestore();
            executor.Dispose();
        }
    }

    /// <summary>
    /// The pre-vote probe rides the same request type and is fenced with it. A pre-vote grant is
    /// not durable, but it is answered from the same pre-restore freshness position of 0, so an
    /// unrestored node would help elect a candidate it may be strictly fresher than.
    /// </summary>
    [Fact]
    public async Task PreVoteBeforeRestoreCompletes_IsDropped()
    {
        (RaftPartitionExecutor executor, TestWalCompletionFences.StubHost host, GatedRestoreWal wal) = Build();

        try
        {
            await executor.Ask(Vote(term: 1, host.HybridLogicalClock.SendOrLocalEvent(2), preVote: true),
                TestContext.Current.CancellationToken);

            Assert.False(executor.IsRestored);
            Assert.DoesNotContain(host.EnqueuedResponses, r => r.Item2.Type == RaftResponderRequestType.Vote);
        }
        finally
        {
            wal.ReleaseRestore();
            executor.Dispose();
        }
    }

    /// <summary>
    /// The fence is deliberately narrow. An unrestored follower that stopped acking appends would
    /// stall its leader for no safety gain, so <c>AppendLogs</c> still flows and still answers.
    /// </summary>
    [Fact]
    public async Task AppendLogsBeforeRestoreCompletes_StillAnswers()
    {
        (RaftPartitionExecutor executor, TestWalCompletionFences.StubHost host, GatedRestoreWal wal) = Build();

        try
        {
            await executor.Ask(
                new RaftRequest(RaftRequestType.AppendLogs,
                    term: 1,
                    timestamp: host.HybridLogicalClock.SendOrLocalEvent(2),
                    endpoint: "leader-node"),
                TestContext.Current.CancellationToken);

            Assert.False(executor.IsRestored);
            Assert.Contains(host.EnqueuedResponses, r => r.Item2.Type == RaftResponderRequestType.CompleteAppendLogs);
        }
        finally
        {
            wal.ReleaseRestore();
            executor.Dispose();
        }
    }
}
