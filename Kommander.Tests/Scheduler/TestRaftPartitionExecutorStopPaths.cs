using Kommander.Data;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// The stop and fail-stop paths of <see cref="RaftPartitionExecutor"/>:
///
/// <list type="bullet">
///   <item>Stop cancels every <c>Ask</c> reply the state machine has registered but not yet
///         answered — a caller waiting on an applied-index barrier must wake up, not hang on a
///         partition that will never process anything again.</item>
///   <item>Stop while the WAL restore is still loading cancels the restore once the load returns,
///         and the stopped executor refuses further posts.</item>
///   <item>A restore whose Phase 2 (replay) throws fail-stops the partition: the restore task
///         faults, the partition stays unrestored, and the executor refuses new work instead of
///         running as a zombie that acks appends on a term and vote record that were never loaded.</item>
///   <item>An Ask whose caller token is already cancelled settles immediately without entering
///         the queue.</item>
/// </list>
/// </summary>
public sealed class TestRaftPartitionExecutorStopPaths
{
    // ── Stubs (mirrors the acceptance-test harness) ─────────────────────────

    private sealed class StubHost : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
            EnableCheckQuorum = false,
        };

        public int PartitionId { get; }
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "test-node";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => _config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = Array.Empty<RaftNode>();
        public List<(string Endpoint, RaftResponderRequest Request)> EnqueuedResponses { get; } = [];
        public IReadOnlyList<RaftNode> Nodes => NodesOverride;

        public StubHost(int partitionId = 0) => PartitionId = partitionId;

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => EnqueuedResponses.Add((endpoint, request));
        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(false);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(false);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) => Task.FromResult(new SnapshotResponse(false));
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }

    private sealed class StubWal : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) => MakeNoOpOperation();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOpOperation();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOpOperation();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => MakeNoOpOperation();
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOpOperation() =>
            new(_ => { }, operationId: 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    /// <summary>A WAL whose restore Phase 1 parks on a gate the test controls.</summary>
    private sealed class GatedRestoreWal(Task gate) : IRaftWalFacade
    {
        public async ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            await gate.ConfigureAwait(false);
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

    /// <summary>A WAL whose restore Phase 2 (replay) throws, simulating a corrupt tail.</summary>
    private sealed class ThrowingReplayWal : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => throw new InvalidOperationException("Simulated replay failure.");
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

    private sealed class TestReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;

        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    private static (RaftPartitionExecutor executor, RaftPartitionStateMachine sm, StubHost host) BuildExecutor(
        int partitionId,
        IRaftWalFacade wal)
    {
        StubHost host = new(partitionId);
        TestReplySink relaySink = new();
        RaftPartitionStateMachine sm = new(host, wal, relaySink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        relaySink.Executor = executor;
        executor.Start();
        return (executor, sm, host);
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Stop must cancel every registered-but-unanswered Ask: the applied-index barrier below
    /// waits for an index the stopped partition will never reach, and its caller has to observe
    /// the stop as a cancellation rather than wait forever.
    /// </summary>
    [Fact]
    public async Task Stop_CancelsAskRepliesTheStateMachineHasNotAnswered()
    {
        var (executor, _, _) = BuildExecutor(partitionId: 11, new StubWal());
        await executor.RestoreTask;

        Task<RaftResponse> waiter = executor.Ask(
            new RaftRequest(RaftRequestType.WaitLocalApplication, commitIndex: 5_000),
            TestContext.Current.CancellationToken);

        // The barrier is processed and parked: it holds a registered reply, not a queued request.
        await executor.DrainAsync(TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.False(waiter.IsCompleted, "the applied-index barrier must park while the index is unreachable");

        executor.Stop();

        await Assert.ThrowsAsync<TaskCanceledException>(() => waiter);
        Assert.Throws<InvalidOperationException>(() =>
            executor.Post(new RaftRequest(RaftRequestType.CheckLeader)));

        executor.Dispose();
    }

    /// <summary>
    /// Stopping while Phase 1 of the restore is still loading cancels the restore once the load
    /// returns (the logs would be replayed into a partition that is going away), and the stopped
    /// executor refuses further work at the door.
    /// </summary>
    [Fact]
    public async Task Stop_DuringRestorePhaseOne_CancelsTheRestoreWhenTheLoadReturns()
    {
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var (executor, _, _) = BuildExecutor(partitionId: 12, new GatedRestoreWal(gate.Task));

        Assert.False(executor.RestoreTask.IsCompleted);

        executor.Stop();

        Assert.Throws<InvalidOperationException>(() =>
            executor.Post(new RaftRequest(RaftRequestType.CheckLeader)));

        gate.SetResult();

        await Assert.ThrowsAsync<TaskCanceledException>(
            () => executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        executor.Dispose();
    }

    /// <summary>
    /// A restore whose Phase 2 replay throws fail-stops the partition: the restore task faults
    /// with the replay exception, the restore gate never opens, and the executor refuses new
    /// work — never the zombie shape of an unrestored partition that still acks appends.
    /// </summary>
    [Fact]
    public async Task RestorePhaseTwoFailure_FailStopsThePartitionAndFaultsTheRestoreTask()
    {
        var (executor, _, _) = BuildExecutor(partitionId: 13, new ThrowingReplayWal());

        InvalidOperationException failure = await Assert.ThrowsAsync<InvalidOperationException>(
            () => executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Contains("Simulated replay failure.", failure.Message);

        Assert.False(executor.IsRestored);
        Assert.Throws<InvalidOperationException>(() =>
            executor.Post(new RaftRequest(RaftRequestType.CheckLeader)));

        executor.Dispose();
    }

    /// <summary>
    /// An Ask whose caller token is already cancelled settles as cancelled immediately — the
    /// registration fires when it is attached, before the operation can be answered, so the caller
    /// observes cancellation without waiting for the executor to produce a reply.
    /// </summary>
    [Fact]
    public async Task Ask_WithAnAlreadyCancelledToken_SettlesCancelledWithoutWaitingForAReply()
    {
        var (executor, _, _) = BuildExecutor(partitionId: 14, new StubWal());
        await executor.RestoreTask;

        CancellationTokenSource cts = new();
        cts.Cancel();

        Task<RaftResponse> ask = executor.Ask(
            new RaftRequest(RaftRequestType.WaitLocalApplication, commitIndex: 5_000),
            cts.Token);

        await Assert.ThrowsAsync<TaskCanceledException>(
            () => ask.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));

        executor.Stop();
        executor.Dispose();
    }
}
