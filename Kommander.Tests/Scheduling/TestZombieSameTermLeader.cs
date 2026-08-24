using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Regression tests for the zombie same-term leader (the Caraxes run-J split-brain).
///
/// <para>
/// The incident: a SIGSTOPed leader resumed, adopted the new term from the elected leader's own
/// traffic, and then its queued LeaderCommit WAL completions fanned out AppendLogs stamped with
/// the adopted term — a term it never won. Receivers treat AppendLogs as authoritatively
/// identifying the term's leader, so the elected leader stepped down inside its own term and the
/// partition's in-flight replication was orphaned.
/// </para>
///
/// <para>Two independent fences close it, each covered here:</para>
/// <list type="bullet">
///   <item>Send side — <c>CompleteLeaderCommit</c> / <c>CompleteLeaderRollback</c> are
///         leader-state fenced like <c>CompleteLeaderPropose</c>: after a step-down they answer
///         the caller with <see cref="RaftOperationStatus.NodeIsNotLeader"/> and fan out
///         nothing.</item>
///   <item>Receive side — a node in Leader state rejects an equal-term AppendLogs from another
///         endpoint (one leader per term, Raft §5.2) instead of adopting the sender.</item>
/// </list>
/// </summary>
public class TestZombieSameTermLeader
{
    private const string VoterA = "follower-a:9001";
    private const string VoterB = "follower-b:9002";

    // ── receive side: equal-term AppendLogs at a Leader ───────────────────────

    /// <summary>
    /// A Leader that receives an equal-term AppendLogs from another endpoint must keep its
    /// leadership and reject the sender — adopting it is how the elected leader abdicated
    /// inside its own term.
    /// </summary>
    [Fact]
    public async Task Leader_RejectsEqualTermAppendLogsFromAnotherEndpoint()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();
        long term = sm.CurrentTerm;

        host.Requests.Clear();
        await sm.AppendLogsAsync(VoterA, term, host.HybridLogicalClock.TrySendOrLocalEvent(1), null);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal(term, sm.CurrentTerm);
        Assert.NotEqual(VoterA, host.Leader);

        CompleteAppendLogsRequest? rejection = host.Requests
            .Select(r => r.CompleteAppendLogsRequest)
            .FirstOrDefault(r => r?.Status == RaftOperationStatus.LogsFromAnotherLeader);

        Assert.NotNull(rejection);
        Assert.Equal(term, rejection!.Term);
    }

    /// <summary>
    /// Scope control: the fence must cover only the equal-term case. A HIGHER-term AppendLogs is
    /// a legitimate new leader announcing itself and must still depose this node.
    /// </summary>
    [Fact]
    public async Task Leader_IsStillDeposedByHigherTermAppendLogs()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();
        long term = sm.CurrentTerm;

        await sm.AppendLogsAsync(VoterA, term + 1, host.HybridLogicalClock.TrySendOrLocalEvent(1), null);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(term + 1, sm.CurrentTerm);
        Assert.Equal(VoterA, host.Leader);
    }

    /// <summary>
    /// Scope control for the follower half: the vote path adopts a higher term while keeping the
    /// old term's leader knowledge, so the winner's first equal-term AppendLogs is how a follower
    /// learns the real leader. The fence must not block that adoption.
    /// </summary>
    [Fact]
    public async Task Follower_StillAdoptsEqualTermLeaderAfterVotePathTermAdoption()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRestored();

        // A candidate campaigns at term 5; the vote path adopts the term either way.
        await sm.VoteAsync(new(VoterA), voteTerm: 5, remoteMaxLogId: 0,
                           host.HybridLogicalClock.TrySendOrLocalEvent(1));
        Assert.Equal(5, sm.CurrentTerm);

        // The term-5 winner announces itself with an equal-term AppendLogs: must be adopted.
        await sm.AppendLogsAsync(VoterB, 5, host.HybridLogicalClock.TrySendOrLocalEvent(1), null);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(VoterB, host.Leader);
    }

    // ── send side: commit/rollback completions after a step-down ──────────────

    /// <summary>
    /// The run-J trigger in miniature: a commit's WAL completion lands after the node stepped
    /// down. The fan-out must not run (it would broadcast AppendLogs stamped with whatever term
    /// this node holds now), and the caller must be answered — not orphaned — with
    /// <see cref="RaftOperationStatus.NodeIsNotLeader"/>.
    /// </summary>
    [Fact]
    public async Task CommitCompletion_AfterStepDown_DoesNotFanOutAndAnswersCaller()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CapturingReplySink sink) = await BuildLeaderWithSink();

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 12);
        await sm.CompleteWalOperationAsync(MakeCompletion(host.PartitionId, operationId: 1, WALWriteOperationType.LeaderPropose));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 12);
        HLCTimestamp ticketId = proposeReply.TicketId;

        // Propose quorum via both voter acks, then enqueue the explicit commit.
        await sm.CompleteAppendLogsAsync(VoterA, ticketId, RaftOperationStatus.Success, committedIndex: 0);
        await sm.CommitLogsAsync(ticketId, replyCorrelationId: 20);

        // The step-down races the commit fsync: it wins, and only then does the completion land.
        await sm.StepDownAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);

        host.Requests.Clear();
        await sm.CompleteWalOperationAsync(MakeCompletion(host.PartitionId, operationId: 2, WALWriteOperationType.LeaderCommit));

        Assert.DoesNotContain(host.Requests, r => r.Type == RaftResponderRequestType.AppendLogs);
        (ulong _, RaftResponse commitReply) = Assert.Single(sink.Completed, r => r.Id == 20);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, commitReply.Status);
    }

    /// <summary>Same fence on the rollback completion path.</summary>
    [Fact]
    public async Task RollbackCompletion_AfterStepDown_DoesNotFanOutAndAnswersCaller()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CapturingReplySink sink) = await BuildLeaderWithSink();

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 13);
        await sm.CompleteWalOperationAsync(MakeCompletion(host.PartitionId, operationId: 1, WALWriteOperationType.LeaderPropose));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 13);
        HLCTimestamp ticketId = proposeReply.TicketId;

        // Rollback requires propose quorum (state Completed) first.
        await sm.CompleteAppendLogsAsync(VoterA, ticketId, RaftOperationStatus.Success, committedIndex: 0);
        await sm.RollbackLogsAsync(ticketId, replyCorrelationId: 21);

        await sm.StepDownAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);

        host.Requests.Clear();
        await sm.CompleteWalOperationAsync(MakeCompletion(host.PartitionId, operationId: 3, WALWriteOperationType.LeaderRollback));

        Assert.DoesNotContain(host.Requests, r => r.Type == RaftResponderRequestType.AppendLogs);
        (ulong _, RaftResponse rollbackReply) = Assert.Single(sink.Completed, r => r.Id == 21);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, rollbackReply.Status);
    }

    /// <summary>
    /// Scope control: while the node IS still leader, a commit completion must keep fanning out —
    /// the fence must not suppress ordinary commit propagation.
    /// </summary>
    [Fact]
    public async Task CommitCompletion_WhileStillLeader_StillFansOut()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CapturingReplySink sink) = await BuildLeaderWithSink();

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 14);
        await sm.CompleteWalOperationAsync(MakeCompletion(host.PartitionId, operationId: 1, WALWriteOperationType.LeaderPropose));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 14);
        HLCTimestamp ticketId = proposeReply.TicketId;

        await sm.CompleteAppendLogsAsync(VoterA, ticketId, RaftOperationStatus.Success, committedIndex: 0);
        await sm.CommitLogsAsync(ticketId, replyCorrelationId: 22);

        host.Requests.Clear();
        await sm.CompleteWalOperationAsync(MakeCompletion(host.PartitionId, operationId: 2, WALWriteOperationType.LeaderCommit));

        Assert.Contains(host.Requests, r => r.Type == RaftResponderRequestType.AppendLogs);
        (ulong _, RaftResponse commitReply) = Assert.Single(sink.Completed, r => r.Id == 22);
        Assert.Equal(RaftOperationStatus.Success, commitReply.Status);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static RaftWalCompletion MakeCompletion(int partitionId, long operationId, WALWriteOperationType type) =>
        new(partitionId, OperationId: operationId, Term: -1,
            MinLogIndex: -1, MaxLogIndex: 1,
            type, RaftOperationStatus.Success);

    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildRestored()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = await BuildRestoredWithSink();
        return (sm, host);
    }

    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildLeader()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = await BuildLeaderWithSink();
        return (sm, host);
    }

    private static async Task<(RaftPartitionStateMachine, CapturingHost, CapturingReplySink)> BuildRestoredWithSink()
    {
        CapturingHost host = new();
        StubWal wal = new();
        CapturingReplySink sink = new();

        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        return (sm, host, sink);
    }

    private static async Task<(RaftPartitionStateMachine, CapturingHost, CapturingReplySink)> BuildLeaderWithSink()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CapturingReplySink sink) = await BuildRestoredWithSink();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        long term = sm.CurrentTerm;
        await sm.ReceivedVoteAsync(VoterA, term, remoteMaxLogId: 0);
        await sm.ReceivedVoteAsync(VoterB, term, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        return (sm, host, sink);
    }

    // ── stubs (same shape as TestHigherTermAppendAck; WAL adds per-type operation ids) ─────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public bool IsMember(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) => Requests.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(true));
    }

    private sealed class StubWal : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) =>
            new(_ => { }, 1, WALWriteOperationType.LeaderPropose, (1, logs), ts, autoCommit: autoCommit, term: term);
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(_ => { }, 2, WALWriteOperationType.LeaderCommit, (1, logs));
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(_ => { }, 3, WALWriteOperationType.LeaderRollback, (1, logs));
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
}
