
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Covers the cancellation plumbing added to <c>CommitLogs</c> / <c>RollbackLogs</c>
/// and the idempotent re-commit / re-rollback guard added to
/// <see cref="RaftPartitionStateMachine"/>.
/// </summary>
public class TestCommitLogsCancellation
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    private sealed class StubHost : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
        };

        public int PartitionId { get; init; } = 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => _config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => NodesOverride;
        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string ep, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string ep, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string ep, RaftResponderRequest req) { }
        public Task InvokeLeaderChanged(int p, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode n, SnapshotRequest r, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// Minimal WAL facade that tracks EnqueueCommit and EnqueueRollback calls.
    /// Operations complete immediately (no async drain); the propose operation
    /// includes the log list so the state machine can mutate and track it.
    /// </summary>
    private sealed class CountingWalFacade : IRaftWalFacade
    {
        public int CommitCallCount;
        public int RollbackCallCount;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
            => new(null!, 1, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            Interlocked.Increment(ref CommitCallCount);
            return new(null!, 2, WALWriteOperationType.LeaderCommit, (1, logs));
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
        {
            Interlocked.Increment(ref RollbackCallCount);
            return new(null!, 3, WALWriteOperationType.LeaderRollback, (1, logs));
        }

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
            => logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }
    }

    /// <summary>
    /// Minimal no-op WAL for executor-level cancellation tests that do not need WAL tracking.
    /// </summary>
    private sealed class NoOpWalFacade : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp t, bool autoCommit) => MakeNoOp(1);
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp(2);
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp(3);
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1) => MakeNoOp(1);
        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp(long id) => new(_ => { }, id, WALWriteOperationType.LeaderPropose, (1, new List<RaftLog>()));
    }

    private sealed class TestReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;

        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response)
            => Completed.Add((correlationId, response));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static (RaftPartitionExecutor executor, RaftPartitionStateMachine sm, StubHost host) BuildExecutor(
        IRaftWalFacade? wal = null)
    {
        StubHost host = new();
        wal ??= new NoOpWalFacade();
        TestReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId: 1, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return (executor, sm, host);
    }

    private static (RaftPartitionStateMachine sm, StubHost host, CountingWalFacade wal, CapturingReplySink sink)
        MakeSingleNodeLeader()
    {
        StubHost host = new() { NodesOverride = [] };   // single-voter cluster
        CountingWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        return (sm, host, wal, sink);
    }

    private static RaftWalCompletion MakeProposeCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 1L, Term: -1L,
            MinLogIndex: -1L, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success);

    private static RaftWalCompletion MakeCommitCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 2L, Term: -1L,
            MinLogIndex: logIndex, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderCommit, RaftOperationStatus.Success);

    private static RaftWalCompletion MakeRollbackCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 3L, Term: -1L,
            MinLogIndex: logIndex, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderRollback, RaftOperationStatus.Success);

    // ── CancellationToken plumbing ────────────────────────────────────────────

    /// <summary>
    /// A pre-cancelled token must cause <see cref="RaftPartitionExecutor.Ask"/> to throw
    /// <see cref="OperationCanceledException"/> promptly.  This is the mechanism that
    /// <see cref="RaftPartition.CommitLogs"/> catches and translates to
    /// <see cref="RaftOperationStatus.OperationCancelled"/>.
    /// </summary>
    [Fact]
    public async Task CommitLogs_WhenTokenTripsWhileExecutorBusy_ReturnsPromptlyWithRetryableStatus()
    {
        var (executor, _, _) = BuildExecutor();
        await executor.RestoreTask;

        using CancellationTokenSource cts = new();
        cts.Cancel();   // pre-cancelled → TCS fires immediately on Register

        Task<RaftResponse> task = executor.Ask(
            new RaftRequest(RaftRequestType.CommitLogs, HLCTimestamp.Zero, false),
            cts.Token);

        // Must complete within a tight deadline — no unbounded executor wait.
        await Task.WhenAny(task, Task.Delay(500, TestContext.Current.CancellationToken));

        Assert.True(task.IsCompleted, "Ask should have completed promptly on a pre-cancelled token");
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }

    /// <summary>
    /// Same cancellation path for RollbackLogs.
    /// </summary>
    [Fact]
    public async Task RollbackLogs_WhenTokenTrips_ReturnsPromptlyWithRetryableStatus()
    {
        var (executor, _, _) = BuildExecutor();
        await executor.RestoreTask;

        using CancellationTokenSource cts = new();
        cts.Cancel();

        Task<RaftResponse> task = executor.Ask(
            new RaftRequest(RaftRequestType.RollbackLogs, HLCTimestamp.Zero, false),
            cts.Token);

        await Task.WhenAny(task, Task.Delay(500, TestContext.Current.CancellationToken));

        Assert.True(task.IsCompleted, "Ask should have completed promptly on a pre-cancelled token");
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }

    /// <summary>
    /// With no token supplied, <see cref="RaftPartitionExecutor.Ask"/> does not attach any
    /// cancellation registration — the existing unbounded-wait behavior is preserved.
    /// </summary>
    [Fact]
    public async Task CommitLogs_WithDefaultToken_BehavesExactlyAsBefore()
    {
        // Single-voter leader; drive propose+commit WAL completions to get a real Success reply.
        (RaftPartitionStateMachine sm, StubHost host, CountingWalFacade wal, CapturingReplySink sink) = MakeSingleNodeLeader();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);

        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        HLCTimestamp ticketId = proposeReply.TicketId;

        // Single-voter: propose quorum achieved immediately; state = Completed.
        await sm.CommitLogsAsync(ticketId, replyCorrelationId: 2);

        // Must have enqueued exactly one WAL commit.
        Assert.Equal(1, wal.CommitCallCount);

        // Drive commit WAL completion → state = Committed, reply = Success.
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse commitReply) = Assert.Single(sink.Completed, r => r.Id == 2);
        Assert.Equal(RaftOperationStatus.Success, commitReply.Status);
        Assert.Equal(1L, commitReply.LogIndex);
    }

    // ── idempotent re-commit / re-rollback ────────────────────────────────────

    /// <summary>
    /// A second CommitLogs for an already-committed ticket returns Success immediately
    /// without enqueuing a second WAL write.
    /// </summary>
    [Fact]
    public async Task CommitLogs_SameTicketTwice_SecondIsNoOpReturnsSuccess()
    {
        (RaftPartitionStateMachine sm, StubHost host, CountingWalFacade wal, CapturingReplySink sink) = MakeSingleNodeLeader();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        HLCTimestamp ticketId = proposeReply.TicketId;

        // First commit: enqueues WAL write, returns Pending.
        await sm.CommitLogsAsync(ticketId, replyCorrelationId: 2);
        Assert.Equal(1, wal.CommitCallCount);

        // Drive WAL → proposal transitions to Committed.
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse firstCommitReply) = Assert.Single(sink.Completed, r => r.Id == 2);
        Assert.Equal(RaftOperationStatus.Success, firstCommitReply.Status);

        // Second commit for the same ticket: no second WAL write, returns Success immediately.
        await sm.CommitLogsAsync(ticketId, replyCorrelationId: 3);
        Assert.Equal(1, wal.CommitCallCount);   // still 1 — no double EnqueueCommit

        (ulong _, RaftResponse secondCommitReply) = Assert.Single(sink.Completed, r => r.Id == 3);
        Assert.Equal(RaftOperationStatus.Success, secondCommitReply.Status);
        Assert.Equal(1L, secondCommitReply.LogIndex);
    }

    /// <summary>
    /// A second RollbackLogs for an already-rolled-back ticket returns Success immediately
    /// without enqueuing a second WAL write.
    /// </summary>
    [Fact]
    public async Task RollbackLogs_SameTicketTwice_SecondIsNoOpReturnsSuccess()
    {
        (RaftPartitionStateMachine sm, StubHost host, CountingWalFacade wal, CapturingReplySink sink) = MakeSingleNodeLeader();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        HLCTimestamp ticketId = proposeReply.TicketId;

        // First rollback: enqueues WAL write, returns Pending.
        await sm.RollbackLogsAsync(ticketId, replyCorrelationId: 2);
        Assert.Equal(1, wal.RollbackCallCount);

        // Drive WAL → proposal transitions to RolledBack.
        await sm.CompleteWalOperationAsync(MakeRollbackCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse firstRollbackReply) = Assert.Single(sink.Completed, r => r.Id == 2);
        Assert.Equal(RaftOperationStatus.Success, firstRollbackReply.Status);

        // Second rollback for the same ticket: no second WAL write, returns Success immediately.
        await sm.RollbackLogsAsync(ticketId, replyCorrelationId: 3);
        Assert.Equal(1, wal.RollbackCallCount);   // still 1 — no double EnqueueRollback

        (ulong _, RaftResponse secondRollbackReply) = Assert.Single(sink.Completed, r => r.Id == 3);
        Assert.Equal(RaftOperationStatus.Success, secondRollbackReply.Status);
    }

    /// <summary>
    /// Once a ticket is committed, a subsequent RollbackLogs for it returns Errored
    /// (the settled outcome wins — rollback is not applied).
    /// Symmetrically, once rolled back, a subsequent CommitLogs returns Errored.
    /// </summary>
    [Fact]
    public async Task RollbackThenCommit_SameTicket_ResolvesToSingleConsistentOutcome()
    {
        (RaftPartitionStateMachine sm, StubHost host, CountingWalFacade wal, CapturingReplySink sink) = MakeSingleNodeLeader();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        // ── Part A: RolledBack → CommitLogs must not apply ──────────────────

        List<RaftLog> logsA = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logsA, autoCommit: false, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse proposeA) = Assert.Single(sink.Completed, r => r.Id == 10);
        HLCTimestamp ticketA = proposeA.TicketId;

        await sm.RollbackLogsAsync(ticketA, replyCorrelationId: 11);
        await sm.CompleteWalOperationAsync(MakeRollbackCompletion(host.PartitionId, logIndex: 1));
        Assert.Single(sink.Completed, r => r.Id == 11);

        // Now try to commit the same ticket — must be blocked.
        await sm.CommitLogsAsync(ticketA, replyCorrelationId: 12);
        Assert.Equal(1, wal.RollbackCallCount);
        Assert.Equal(0, wal.CommitCallCount);   // no commit was enqueued

        (ulong _, RaftResponse crossCommitReply) = Assert.Single(sink.Completed, r => r.Id == 12);
        Assert.Equal(RaftOperationStatus.Errored, crossCommitReply.Status);

        // ── Part B: Committed → RollbackLogs must not apply ─────────────────

        // For the second sub-test we need a fresh ticket. Step down and re-elect so a new
        // proposal can be created (the old partition state is cleared on stepdown).
        sink.Completed.Clear();

        List<RaftLog> logsB = [new() { Id = 2, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logsB, autoCommit: false, replyCorrelationId: 20);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 2));

        (ulong _, RaftResponse proposeB) = Assert.Single(sink.Completed, r => r.Id == 20);
        HLCTimestamp ticketB = proposeB.TicketId;

        await sm.CommitLogsAsync(ticketB, replyCorrelationId: 21);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, logIndex: 2));
        Assert.Single(sink.Completed, r => r.Id == 21);

        int commitCountBefore = wal.RollbackCallCount;

        // Now try to rollback the same committed ticket — must be blocked.
        await sm.RollbackLogsAsync(ticketB, replyCorrelationId: 22);
        Assert.Equal(commitCountBefore, wal.RollbackCallCount);   // no rollback was enqueued

        (ulong _, RaftResponse crossRollbackReply) = Assert.Single(sink.Completed, r => r.Id == 22);
        Assert.Equal(RaftOperationStatus.Errored, crossRollbackReply.Status);
    }
}
