
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Tests.Scheduler;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Validates the event-driven write completion path:
/// <see cref="RaftProposalQuorum.GetWaiterTask"/> completes immediately when the
/// state machine reaches a terminal state (committed, rolled-back, or leader loss)
/// rather than requiring the polling loop in <c>WaitForQuorum</c>.
/// </summary>
public class TestProposalWaiterCompletion
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static (RaftPartitionStateMachine sm, FakePartitionHost host, FakeWalFacade wal, CapturingReplySink sink) MakeLeader(
        IReadOnlyList<RaftNode>? nodes = null)
    {
        FakePartitionHost host = new() { NodesOverride = nodes ?? [] };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        return (sm, host, wal, sink);
    }

    private static RaftWalCompletion MakeProposeCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 1, Term: -1,
            MinLogIndex: -1, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success);

    private static RaftWalCompletion MakeCommitCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 2, Term: -1,
            MinLogIndex: -1, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderCommit, RaftOperationStatus.Success);

    private static RaftWalCompletion MakeRollbackCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 3, Term: -1,
            MinLogIndex: -1, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderRollback, RaftOperationStatus.Success);

    // ── GetTicketWaiterTask ────────────────────────────────────────────────────

    [Fact]
    public void GetTicketWaiterTask_UnknownTicket_ReturnsNull()
    {
        (RaftPartitionStateMachine sm, _, _, _) = MakeLeader();

        Task<(RaftProposalTicketState, long)>? task = sm.GetTicketWaiterTask(HLCTimestamp.Zero);

        Assert.Null(task);
    }

    // ── single-node auto-commit ────────────────────────────────────────────────

    [Fact]
    public async Task SingleNode_AutoCommit_WaiterCompletesCommitted()
    {
        // No peers — single-voter leader commits immediately on propose quorum.
        (RaftPartitionStateMachine sm, FakePartitionHost host, _, CapturingReplySink sink) = MakeLeader(nodes: []);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: true, replyCorrelationId: 10);

        // Proposal is not yet in activeProposals — WAL hasn't completed.
        Assert.Null(sm.GetTicketWaiterTask(HLCTimestamp.Zero));

        // Drive propose WAL completion → CompleteLeaderPropose → single-voter commit path.
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        // Extract ticketId from the reply that CompleteLeaderPropose sent.
        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 10);
        Assert.Equal(RaftOperationStatus.Success, proposeReply.Status);
        HLCTimestamp ticketId = proposeReply.TicketId;

        Task<(RaftProposalTicketState, long)>? waiterTask = sm.GetTicketWaiterTask(ticketId);
        Assert.NotNull(waiterTask);

        // Single-node commit path runs inside CompleteLeaderPropose and fires
        // TryReleaseTicketOnQuorumDurable (WalSingleFsyncCommit off → no-op there)
        // then the commit WAL. Drive the commit completion.
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, logIndex: 1));

        // Task must already be completed.
        Assert.True(waiterTask.IsCompleted);
        (RaftProposalTicketState state, long commitIndex) = await waiterTask;
        Assert.Equal(RaftProposalTicketState.Committed, state);
        Assert.Equal(1L, commitIndex);
    }

    // ── multi-node auto-commit ─────────────────────────────────────────────────

    [Fact]
    public async Task MultiNode_AutoCommit_WaiterCompletesAfterFollowerAck()
    {
        // Two voters: node-a (leader) + node-b (follower). Quorum = 2.
        (RaftPartitionStateMachine sm, FakePartitionHost host, _, CapturingReplySink sink) = MakeLeader(nodes: [new("node-b")]);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        // With peers, ForceLeader starts an election; simulate node-b's vote to win.
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: true, replyCorrelationId: 11);

        // Drive propose WAL → CompleteLeaderPropose registers proposal in activeProposals.
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 11);
        HLCTimestamp ticketId = proposeReply.TicketId;

        Task<(RaftProposalTicketState, long)>? waiterTask = sm.GetTicketWaiterTask(ticketId);
        Assert.NotNull(waiterTask);

        // Not yet completed — still waiting for node-b ack.
        Assert.False(waiterTask.IsCompleted);

        // Simulate follower ack: node-b completes its append at the ticketId timestamp.
        await sm.CompleteAppendLogsAsync("node-b", ticketId, RaftOperationStatus.Success, committedIndex: 0);

        // Quorum reached (node-a self + node-b). With WalSingleFsyncCommit off, waiter is
        // completed by CompleteLeaderCommit after the commit WAL write.
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, logIndex: 1));

        Assert.True(waiterTask.IsCompleted);
        (RaftProposalTicketState state, long commitIndex) = await waiterTask;
        Assert.Equal(RaftProposalTicketState.Committed, state);
        Assert.Equal(1L, commitIndex);
    }

    // ── manual two-phase: propose-quorum completes the waiter ───────────────────

    [Fact]
    public async Task ManualPropose_WaiterCompletesCommittedOnProposeQuorum()
    {
        // Two voters for the manual two-phase path. The public ReplicateLogs(autoCommit:false)
        // caller awaits the PROPOSE phase: it must succeed on propose-quorum-durable, ahead of any
        // explicit commit. (The explicit CommitLogs/RollbackLogs results return through the
        // reply-correlation path, not this waiter.) Regression guard: the manual path must not
        // return early without completing the waiter, which would stall the caller for 10 s.
        (RaftPartitionStateMachine sm, FakePartitionHost host, _, CapturingReplySink sink) = MakeLeader(nodes: [new("node-b")]);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 12);

        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 12);
        HLCTimestamp ticketId = proposeReply.TicketId;

        Task<(RaftProposalTicketState, long)>? waiterTask = sm.GetTicketWaiterTask(ticketId);
        Assert.NotNull(waiterTask);

        // Not yet quorum — still waiting for node-b's append ack.
        Assert.False(waiterTask.IsCompleted);

        // Achieve propose quorum (autoCommit=false → State becomes Completed). The waiter must
        // complete with Committed here, matching the legacy CheckTicketCompletion semantics.
        await sm.CompleteAppendLogsAsync("node-b", ticketId, RaftOperationStatus.Success, committedIndex: 0);

        Assert.True(waiterTask.IsCompleted);
        (RaftProposalTicketState state, long idx) = await waiterTask;
        Assert.Equal(RaftProposalTicketState.Committed, state);
        Assert.Equal(1L, idx);
    }

    // ── leader stepdown while waiting ──────────────────────────────────────────

    [Fact]
    public async Task StepDown_WhileProposalPending_WaiterCompletesNotFound()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _, CapturingReplySink sink) = MakeLeader(nodes: [new("node-b")]);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: true, replyCorrelationId: 13);

        // Drive propose WAL → proposal enters activeProposals.
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 13);
        HLCTimestamp ticketId = proposeReply.TicketId;

        Task<(RaftProposalTicketState, long)>? waiterTask = sm.GetTicketWaiterTask(ticketId);
        Assert.NotNull(waiterTask);
        Assert.False(waiterTask.IsCompleted);

        // Step down — FailAllActiveProposalWaiters() must complete the waiter before clearing.
        await sm.StepDownAsync(replyCorrelationId: null);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.True(waiterTask.IsCompleted);
        (RaftProposalTicketState state, long _) = await waiterTask;
        Assert.Equal(RaftProposalTicketState.NotFound, state);
    }

    // ── CompleteWaiter is idempotent ────────────────────────────────────────────

    [Fact]
    public async Task CompleteWaiter_CalledMultipleTimes_IsIdempotent()
    {
        var quorum = new RaftProposalQuorum(
            [new() { Id = 1, Term = 1 }],
            autoCommit: true,
            startTimestamp: HLCTimestamp.Zero);

        Task<(RaftProposalTicketState, long)> task = quorum.GetWaiterTask();

        quorum.CompleteWaiter(RaftProposalTicketState.Committed, 1L);
        quorum.CompleteWaiter(RaftProposalTicketState.Committed, 2L); // second call is a no-op

        Assert.True(task.IsCompleted);
        Assert.Equal((RaftProposalTicketState.Committed, 1L), await task);
    }

    // ── Pool Reset drains old waiter ───────────────────────────────────────────

    [Fact]
    public async Task PooledReset_PendingWaiterCompleted_WithNotFound()
    {
        var logs = new List<RaftLog> { new() { Id = 1, Term = 1 } };
        var quorum = new RaftProposalQuorum(logs, autoCommit: true, startTimestamp: HLCTimestamp.Zero);

        Task<(RaftProposalTicketState, long)> oldTask = quorum.GetWaiterTask();
        Assert.False(oldTask.IsCompleted);

        // Simulate pool reuse via Reset — the old waiter must be drained.
        quorum.Reset(logs, autoCommit: false, startTimestamp: HLCTimestamp.Zero);

        Assert.True(oldTask.IsCompleted);
        (RaftProposalTicketState state, long idx) = await oldTask;
        Assert.Equal(RaftProposalTicketState.NotFound, state);
        Assert.Equal(-1L, idx);

        // New task is separate and not yet completed.
        Task<(RaftProposalTicketState, long)> newTask = quorum.GetWaiterTask();
        Assert.False(newTask.IsCompleted);
        Assert.NotSame(oldTask, newTask);
    }

    // ── inner test helpers ─────────────────────────────────────────────────────

    private sealed class FakeWalFacade : IRaftWalFacade, IDisposable
    {
        private readonly FakeWAL _wal = new();

        public void Dispose() => _wal.Dispose();

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            IReadOnlyList<RaftLog> none = [];
            return ValueTask.FromResult(none);
        }

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_wal.GetMaxLog(partitionId: 1));

        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);

        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(_wal.GetCurrentTerm(partitionId: 1));

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);

        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public long GetCommitIndex() => _wal.GetMaxLog(partitionId: 1);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, 1, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 2, WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 3, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) =>
            Completed.Add((correlationId, response));
    }

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        public int PartitionId { get; init; } = 1;

        public string Leader { get; set; } = "";

        public string LocalEndpoint => "node-a";

        public int LocalNodeId => 1;

        public ClusterMemberRole LocalRole { get; set; } = ClusterMemberRole.Voter;

        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();

        public IReadOnlyList<RaftNode> Nodes => NodesOverride;

        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = [new("node-b")];

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request) { }

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;

        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }
}
