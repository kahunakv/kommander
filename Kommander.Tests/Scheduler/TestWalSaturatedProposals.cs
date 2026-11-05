using Kommander.Data;
using Kommander.Scheduling;
using Kommander.WAL.Data;
using Kommander.Tests.RaftSafety;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// What a caller sees when the WAL refuses or fails one proposal. A full queue is a definite refusal,
/// so it answers <see cref="RaftOperationStatus.ProposalQueueFull"/>. A failure in a coalesced batch
/// reaches only the caller it belongs to, because the other proposals in the batch can already be
/// queued to the WAL and can still commit. Either way, no reply entry stays behind.
/// </summary>
public sealed class TestWalSaturatedProposals
{
    private const string Accept = "accept";
    private const string Refuse = "refuse";
    private const string Fail = "fail";
    private const string Gate = "gate";

    /// <summary>Decides from the log type of the proposal, so one batch can mix the outcomes.</summary>
    private sealed class SelectiveWal : IRaftWalFacade
    {
        private long operationId;

        public List<string> Attempts { get; } = [];

        public bool RefuseCheckpoints { get; set; }

        public ManualResetEventSlim GateEntered { get; } = new();

        public ManualResetEventSlim GateRelease { get; } = new();

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit)
        {
            string type = logs[0].LogType ?? "";

            // Holds the worker inside a proposal, so that the next proposals drain as one batch.
            if (type == Gate)
            {
                GateEntered.Set();
                GateRelease.Wait(TimeSpan.FromSeconds(30));
                return MakeOperation();
            }

            Attempts.Add(type);

            if (type == Refuse || (RefuseCheckpoints && logs[0].Type == RaftLogType.ProposedCheckpoint))
                throw new BackpressureExceededException(partitionId: 1, currentDepth: 4096);

            if (type == Fail)
                throw new InvalidOperationException("storage failure");

            return MakeOperation();
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeOperation();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeOperation();

        public WALWriteOperation? EnqueueProposeOrCommit(
            List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
            => MakeOperation();

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public void NotifyCommitted() { }

        // Never completes: an accepted proposal stays pending, which is what a caller of a proposal
        // queued to a busy WAL observes.
        private WALWriteOperation MakeOperation() =>
            new(_ => { }, operationId: ++operationId, WALWriteOperationType.LeaderPropose, (1, []));
    }

    private sealed class RelayReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;

        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    private static async Task<(RaftPartitionExecutor Executor, SelectiveWal Wal)> BuildLeader()
    {
        SelectiveWal wal = new();
        RelayReplySink sink = new();

        RaftPartitionStateMachine stateMachine = new(
            new TestWalCompletionFences.StubHost(partitionId: 1), wal, sink, NullLogger<IRaft>.Instance);

        RaftPartitionExecutor executor = new(stateMachine, partitionId: 1, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();

        // Client operations wait for the restore, and the restore must not undo the promotion.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(10), ct);
        await executor.Ask(new RaftRequest(RaftRequestType.ForceLeaderForTesting), ct).WaitAsync(TimeSpan.FromSeconds(10), ct);
        wal.Attempts.Clear();

        return (executor, wal);
    }

    private static Task<RaftResponse> Propose(RaftPartitionExecutor executor, string type) =>
        executor.Ask(new RaftRequest(
            RaftRequestType.ReplicateLogs,
            [new RaftLog { Id = 0, Type = RaftLogType.Proposed, LogType = type, LogData = [1] }],
            autoCommit: true), TestContext.Current.CancellationToken);

    /// <summary>Posts the proposals while the worker waits on a gate, so they drain as one batch.</summary>
    private static List<Task<RaftResponse>> ProposeAsOneBatch(RaftPartitionExecutor executor, SelectiveWal wal, params string[] types)
    {
        _ = Propose(executor, Gate);
        Assert.True(wal.GateEntered.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken), "The worker did not reach the gate.");

        List<Task<RaftResponse>> tasks = [];
        foreach (string type in types)
            tasks.Add(Propose(executor, type));

        wal.GateRelease.Set();
        return tasks;
    }

    /// <summary>Returns once the executor has finished everything posted before this call.</summary>
    private static Task Settle(RaftPartitionExecutor executor) =>
        executor.Ask(new RaftRequest(RaftRequestType.GetNodeState), TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

    [Fact]
    public async Task Batch_WhenWalRefusesOneProposal_OnlyThatCallerGetsQueueFull()
    {
        (RaftPartitionExecutor executor, SelectiveWal wal) = await BuildLeader();
        using RaftPartitionExecutor owned = executor;

        List<Task<RaftResponse>> tasks = ProposeAsOneBatch(executor, wal, Accept, Refuse, Accept);
        Task<RaftResponse> first = tasks[0];
        Task<RaftResponse> refused = tasks[1];
        Task<RaftResponse> last = tasks[2];

        RaftResponse response = await refused.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await Settle(executor);

        Assert.Equal(RaftOperationStatus.ProposalQueueFull, response.Status);
        Assert.Equal(1, executor.TotalBatchesExecuted);

        // The proposal after the refused one was still attempted, and both accepted callers still wait.
        Assert.Equal([Accept, Refuse, Accept], wal.Attempts);
        Assert.False(first.IsCompleted);
        Assert.False(last.IsCompleted);

        // Two accepted proposals plus the gate proposal: the refused one left nothing behind.
        Assert.Equal(3, executor.PendingReplyCountForTesting);
    }

    [Fact]
    public async Task Batch_WhenOneProposalFails_OnlyThatCallerSeesTheException()
    {
        (RaftPartitionExecutor executor, SelectiveWal wal) = await BuildLeader();
        using RaftPartitionExecutor owned = executor;

        List<Task<RaftResponse>> tasks = ProposeAsOneBatch(executor, wal, Accept, Fail, Accept);
        Task<RaftResponse> first = tasks[0];
        Task<RaftResponse> failed = tasks[1];
        Task<RaftResponse> last = tasks[2];

        await Assert.ThrowsAsync<InvalidOperationException>(() => failed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        await Settle(executor);

        Assert.Equal([Accept, Fail, Accept], wal.Attempts);
        Assert.False(first.IsCompleted);
        Assert.False(last.IsCompleted);
        Assert.Equal(3, executor.PendingReplyCountForTesting);
    }

    [Fact]
    public async Task SingleProposal_WhenWalRefuses_AnswersQueueFullAndLeavesNoReplyEntry()
    {
        (RaftPartitionExecutor executor, _) = await BuildLeader();
        using RaftPartitionExecutor owned = executor;

        Task<RaftResponse> refused = Propose(executor, Refuse);

        RaftResponse response = await refused.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await Settle(executor);

        Assert.Equal(RaftOperationStatus.ProposalQueueFull, response.Status);
        Assert.Equal(0, executor.PendingReplyCountForTesting);
    }

    [Fact]
    public async Task SingleProposal_WhenItFails_ThrowsToTheCallerAndLeavesNoReplyEntry()
    {
        (RaftPartitionExecutor executor, _) = await BuildLeader();
        using RaftPartitionExecutor owned = executor;

        Task<RaftResponse> failed = Propose(executor, Fail);

        await Assert.ThrowsAsync<InvalidOperationException>(() => failed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        await Settle(executor);

        Assert.Equal(0, executor.PendingReplyCountForTesting);
    }

    [Fact]
    public async Task Checkpoint_WhenWalRefuses_AnswersQueueFullAndLeavesNoReplyEntry()
    {
        (RaftPartitionExecutor executor, SelectiveWal wal) = await BuildLeader();
        using RaftPartitionExecutor owned = executor;
        wal.RefuseCheckpoints = true;

        Task<RaftResponse> refused = executor.Ask(new RaftRequest(RaftRequestType.ReplicateCheckpoint), TestContext.Current.CancellationToken);

        RaftResponse response = await refused.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await Settle(executor);

        Assert.Equal(RaftOperationStatus.ProposalQueueFull, response.Status);
        Assert.Equal(0, executor.PendingReplyCountForTesting);
    }
}
