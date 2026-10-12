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
/// The failure and edge paths of routing a WAL completion back to the operation that enqueued it:
/// a leader rollback completing after the entries were fanned out to the peers, the
/// fan-out-before-local-write propose completion (both outcomes), a propose completion that
/// arrives term-fenced after a step-down, and a follower append whose write errored.
///
/// <para>The shared contract under test: a completion is an operation's only completion — the
/// caller's reply correlation must be resolved on EVERY path (success, error, fence), never
/// leaked, never answered twice; and the quorum/registry state must move exactly once.</para>
/// </summary>
public class TestWalCompletionFailureRouting
{
    private const string Voter = "node-b";
    private const string LeaderEndpoint = "leader:8000";

    // ── leader rollback completing after the entries were fanned out ─────────

    /// <summary>
    /// A rollback whose entries were already replicated to quorum: closing quorum settles the
    /// propose waiter (it observes the quorum outcome and is never retroactively re-resolved),
    /// and the abort's own contract is carried by the caller reply and the durable state — the
    /// completion flips the proposal to RolledBack, answers the reply correlation with Success,
    /// and broadcasts the resolution so the peers' WALs stop holding the range as Proposed.
    /// A duplicate completion for the same operation must be discarded — exactly one answer,
    /// exactly one state transition.
    /// </summary>
    [Fact]
    public async Task RollbackCompletion_AfterFanOut_ResolvesWaiterCallerAndPeers_ExactlyOnce()
    {
        (RaftPartitionStateMachine sm, RoutingHost host, RoutingWal wal, CapturingReplySink sink) = await BuildLeaderAsync(extraVoters: 2);

        (_, HLCTimestamp ticket) = sm.ReplicateLogs([Log(id: 1)], autoCommit: false, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, wal.LastProposeOperationId));
        await sm.CompleteAppendLogsAsync(Voter, ticket, RaftOperationStatus.Success, committedIndex: 0);
        await sm.CompleteAppendLogsAsync("extra-voter-0", ticket, RaftOperationStatus.Success, committedIndex: 0);

        // Quorum settles the waiter even without a commit; the waiter keeps that outcome — the
        // rollback below changes the durable types and the caller's answer, not this task.
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        Assert.True(waiter!.IsCompleted);
        Assert.Equal(RaftProposalTicketState.Committed, (await waiter).Item1);

        await sm.RollbackLogsAsync(ticket, replyCorrelationId: 20);
        host.Outbound.Clear();

        await sm.CompleteWalOperationAsync(RollbackCompletion(host.PartitionId, wal.LastRollbackOperationId, minLogIndex: 1, maxLogIndex: 1));

        // The caller's abort succeeded...
        (ulong _, RaftResponse reply) = Assert.Single(sink.Completed, r => r.Id == 20);
        Assert.Equal(RaftOperationStatus.Success, reply.Status);

        // ...the entries persisted as a rollback, and every peer received the resolution.
        RaftLog rolled = Assert.Single(wal.LastRolledBackBatch!);
        Assert.Equal(RaftLogType.RolledBack, rolled.Type);
        RaftResponderRequest[] resolutions = [.. host.Outbound.Where(m => m.Type == RaftResponderRequestType.AppendLogs)];
        Assert.Equal(host.Nodes.Count, resolutions.Length);
        foreach (RaftResponderRequest resolution in resolutions)
        {
            RaftLog shipped = Assert.Single(resolution.AppendLogsRequest!.Logs!);
            Assert.Equal(RaftLogType.RolledBack, shipped.Type);
        }

        // A re-delivered completion for the same operation is discarded: no second reply, no
        // second broadcast.
        int repliesBefore = sink.Completed.Count;
        await sm.CompleteWalOperationAsync(RollbackCompletion(host.PartitionId, wal.LastRollbackOperationId, minLogIndex: 1, maxLogIndex: 1));
        Assert.Equal(repliesBefore, sink.Completed.Count);
    }

    /// <summary>
    /// A rollback write the engine refused must not wedge the abort: the caller is answered with
    /// the error, and a retried rollback completes normally — the first refusal leaves nothing
    /// half-rolled-back.
    /// </summary>
    [Fact]
    public async Task ErroredRollbackCompletion_AnswersTheCaller_AndStaysRedrivable()
    {
        (RaftPartitionStateMachine sm, RoutingHost host, RoutingWal wal, CapturingReplySink sink) = await BuildLeaderAsync(extraVoters: 2);

        (_, HLCTimestamp ticket) = sm.ReplicateLogs([Log(id: 1)], autoCommit: false, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, wal.LastProposeOperationId));
        await sm.CompleteAppendLogsAsync(Voter, ticket, RaftOperationStatus.Success, committedIndex: 0);
        await sm.CompleteAppendLogsAsync("extra-voter-0", ticket, RaftOperationStatus.Success, committedIndex: 0);

        await sm.RollbackLogsAsync(ticket, replyCorrelationId: 20);
        await sm.CompleteWalOperationAsync(
            RollbackCompletion(host.PartitionId, wal.LastRollbackOperationId, minLogIndex: 1, maxLogIndex: 1, RaftOperationStatus.Errored));

        (ulong _, RaftResponse failed) = Assert.Single(sink.Completed, r => r.Id == 20);
        Assert.Equal(RaftOperationStatus.Errored, failed.Status);

        // The retry: the proposal never reached RolledBack, so the abort is re-drivable and this
        // time the write lands.
        await sm.RollbackLogsAsync(ticket, replyCorrelationId: 21);
        await sm.CompleteWalOperationAsync(RollbackCompletion(host.PartitionId, wal.LastRollbackOperationId, minLogIndex: 1, maxLogIndex: 1));

        (ulong _, RaftResponse retried) = Assert.Single(sink.Completed, r => r.Id == 21);
        Assert.Equal(RaftOperationStatus.Success, retried.Status);
        Assert.Equal(RaftLogType.RolledBack, Assert.Single(wal.LastRolledBackBatch!).Type);
    }

    // ── propose fanned out before the local write ────────────────────────────

    /// <summary>
    /// The early fan-out's failed leg: the peers hold the batch whatever happens to the leader's
    /// own write, so a failed local write is an UNKNOWN outcome, never a definite refusal — the
    /// caller is answered <see cref="RaftOperationStatus.ProposalOutcomeUnknown"/>, the waiter is
    /// failed, the proposal leaves the registry (no later ack can complete it) and nothing commits.
    /// </summary>
    [Fact]
    public async Task FannedOutPropose_FailedLocalWrite_AnswersUnknown_FailsWaiter_NeverCommits()
    {
        (RaftPartitionStateMachine sm, RoutingHost host, RoutingWal wal, CapturingReplySink sink) =
            await BuildLeaderAsync(fanOutBeforeLocalWrite: true);

        (_, HLCTimestamp ticket) = sm.ReplicateLogs([Log(id: 1)], autoCommit: true, replyCorrelationId: 30);

        // The batch reached the peers while the leader's own write was still queued.
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.AppendLogs);

        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        Assert.False(waiter!.IsCompleted);

        // Both followers ack first — quorum now hangs on the leader's own durability alone.
        await sm.CompleteAppendLogsAsync(Voter, ticket, RaftOperationStatus.Success, committedIndex: 0);

        // ...and the leader's write fails.
        await sm.CompleteWalOperationAsync(
            ProposeCompletion(host.PartitionId, wal.LastProposeOperationId, RaftOperationStatus.Errored));

        (ulong _, RaftResponse reply) = Assert.Single(sink.Completed, r => r.Id == 30);
        Assert.Equal(RaftOperationStatus.ProposalOutcomeUnknown, reply.Status);
        Assert.True(waiter.IsCompleted);
        Assert.Equal(RaftProposalTicketState.NotFound, (await waiter).Item1);

        // Nothing committed and nothing will: the proposal is gone from the registry.
        Assert.Equal(0, wal.CommitCount);
        await sm.CompleteAppendLogsAsync(Voter, ticket, RaftOperationStatus.Success, committedIndex: 0);
        Assert.Equal(0, wal.CommitCount);
    }

    /// <summary>
    /// The early fan-out's happy leg with the followers first: the acks arrived while the local
    /// write was queued, so it is the leader's own propose completion that completes the quorum —
    /// the auto-commit must fire from there, and the caller's reply is Success.
    /// </summary>
    [Fact]
    public async Task FannedOutPropose_FollowerAcksFirst_LocalCompletionDrivesTheCommit()
    {
        (RaftPartitionStateMachine sm, RoutingHost host, RoutingWal wal, CapturingReplySink sink) =
            await BuildLeaderAsync(fanOutBeforeLocalWrite: true);

        (_, HLCTimestamp ticket) = sm.ReplicateLogs([Log(id: 1)], autoCommit: true, replyCorrelationId: 31);

        await sm.CompleteAppendLogsAsync(Voter, ticket, RaftOperationStatus.Success, committedIndex: 0);
        Assert.Equal(0, wal.CommitCount);

        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, wal.LastProposeOperationId));

        (ulong _, RaftResponse reply) = Assert.Single(sink.Completed, r => r.Id == 31);
        Assert.Equal(RaftOperationStatus.Success, reply.Status);
        Assert.Equal(1, wal.CommitCount);
        RaftLog committed = Assert.Single(wal.LastCommittedBatch!);
        Assert.Equal(RaftLogType.Committed, committed.Type);
    }

    // ── a completion fenced by a later term ──────────────────────────────────

    /// <summary>
    /// A propose completion that lands after a higher term deposed this leader must not fan out —
    /// a non-leader's broadcast would re-install it as the term's leader on the peers. The caller
    /// is answered <see cref="RaftOperationStatus.ProposalOutcomeUnknown"/> (the entry is appended
    /// and may still commit via the next leader), never orphaned.
    /// </summary>
    [Fact]
    public async Task ProposeCompletion_FencedByALaterTerm_AnswersUnknown_WithoutFanningOut()
    {
        (RaftPartitionStateMachine sm, RoutingHost host, RoutingWal wal, CapturingReplySink sink) = await BuildLeaderAsync();

        sm.ReplicateLogs([Log(id: 1)], autoCommit: true, replyCorrelationId: 40);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        // A RequestVote from a higher term deposes the leader before its write completes.
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-c"), voteTerm: 5, remoteMaxLogId: 0, timestamp: ts, preVote: false, remoteLastLogTerm: 0);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(5, sm.CurrentTerm);

        host.Outbound.Clear();
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, wal.LastProposeOperationId, term: 1));

        (ulong _, RaftResponse reply) = Assert.Single(sink.Completed, r => r.Id == 40);
        Assert.Equal(RaftOperationStatus.ProposalOutcomeUnknown, reply.Status);
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.AppendLogs);
        Assert.Equal(0, wal.CommitCount);
    }

    // ── follower append completion edges ─────────────────────────────────────

    /// <summary>
    /// A follower append whose WAL write errored: nothing is delivered to the consumer, the leader
    /// is answered with the error (so it re-sends), and the follower heals on the re-delivery —
    /// the same batch then applies and acks Success.
    /// </summary>
    [Fact]
    public async Task FollowerAppend_ErroredCompletion_DeliversNothing_AcksError_ThenHealsOnRedelivery()
    {
        (RaftPartitionStateMachine sm, FollowerHost host, RoutingWal wal) = BuildFollower();

        List<RaftLog> batch =
        [
            new() { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ];

        HLCTimestamp ts = new(1, 1, 0);
        await sm.AppendLogsAsync(LeaderEndpoint, term: 1, timestamp: ts, logs: batch);
        await sm.CompleteWalOperationAsync(
            FollowerCompletion(host.PartitionId, wal.LastAppendOperationId, minLogIndex: 1, maxLogIndex: 2, status: RaftOperationStatus.Errored, term: 1));

        Assert.Empty(host.Delivered);
        CompleteAppendLogsRequest? ack = host.Outbound
            .Select(r => r.CompleteAppendLogsRequest)
            .FirstOrDefault(r => r is not null);
        Assert.NotNull(ack);
        Assert.Equal(RaftOperationStatus.Errored, ack!.Status);

        // The leader re-sends the same prefix; this time the write lands.
        host.Outbound.Clear();
        await sm.AppendLogsAsync(LeaderEndpoint, term: 1, timestamp: ts, logs: batch);
        await sm.CompleteWalOperationAsync(
            FollowerCompletion(host.PartitionId, wal.LastAppendOperationId, minLogIndex: 1, maxLogIndex: 2, term: 1));

        Assert.Equal([1L, 2L], host.Delivered);
        CompleteAppendLogsRequest? healed = host.Outbound
            .Select(r => r.CompleteAppendLogsRequest)
            .FirstOrDefault(r => r is not null);
        Assert.NotNull(healed);
        Assert.Equal(RaftOperationStatus.Success, healed!.Status);
        Assert.Equal(2, healed.CommitIndex);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static RaftLog Log(long id) => new() { Id = id, Term = 1, LogType = "t", LogData = [1] };

    private static RaftWalCompletion ProposeCompletion(
        int partitionId, long operationId, RaftOperationStatus status = RaftOperationStatus.Success, long term = -1) =>
        new(partitionId, operationId, term,
            MinLogIndex: -1, MaxLogIndex: 1,
            WALWriteOperationType.LeaderPropose, status);

    private static RaftWalCompletion RollbackCompletion(
        int partitionId, long operationId, long minLogIndex, long maxLogIndex, RaftOperationStatus status = RaftOperationStatus.Success) =>
        new(partitionId, operationId, Term: -1,
            MinLogIndex: minLogIndex, MaxLogIndex: maxLogIndex,
            WALWriteOperationType.LeaderRollback, status);

    private static RaftWalCompletion FollowerCompletion(
        int partitionId, long operationId, long minLogIndex, long maxLogIndex, RaftOperationStatus status = RaftOperationStatus.Success, long term = -1) =>
        new(partitionId, operationId, term,
            MinLogIndex: minLogIndex, MaxLogIndex: maxLogIndex,
            WALWriteOperationType.FollowerAppend, status);

    private static async Task<(RaftPartitionStateMachine, RoutingHost, RoutingWal, CapturingReplySink)> BuildLeaderAsync(
        bool fanOutBeforeLocalWrite = false, int extraVoters = 0)
    {
        // Only the rollback tests widen the membership: a quorum-closing ack must NOT precede
        // the abort, while the propose tests rely on one ack finishing the round.
        List<RaftNode> nodes = [new RaftNode(Voter)];
        for (int i = 0; i < extraVoters; i++)
            nodes.Add(new RaftNode($"extra-voter-{i}"));

        RoutingHost host = new() { Nodes = nodes };
        // The configuration default fans out before the local write; most tests here pin the
        // serial order so the propose completion is the one that fans out.
        host.Config.FanOutBeforeLocalWrite = fanOutBeforeLocalWrite;
        RoutingWal wal = new();
        CapturingReplySink sink = new();

        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        await sm.CompleteRestoreAsync([]);
        sm.MarkRestoredForTesting();
        sm.SetPostToExecutor(_ => { });

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        // Leadership needs a majority of grants, so every configured voter must answer —
        // the rollback tests widen the membership precisely so quorum stays out of reach.
        foreach (RaftNode node in host.Nodes)
            await sm.ReceivedVoteAsync(node.Endpoint, voteTerm: sm.CurrentTerm, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        return (sm, host, wal, sink);
    }

    private static (RaftPartitionStateMachine, FollowerHost, RoutingWal) BuildFollower()
    {
        FollowerHost host = new();
        RoutingWal wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);

        // Restore completion is what releases the consumer-apply hold: without it the
        // follower acks but never delivers, which is not the behavior under test.
        sm.CompleteRestoreAsync([]).GetAwaiter().GetResult();
        sm.MarkRestoredForTesting();
        sm.SetPostToExecutor(_ => { });

        return (sm, host, wal);
    }

    // ── stubs ────────────────────────────────────────────────────────────────

    private sealed class RoutingHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            StartElectionTimeout = 100_000,
            EndElectionTimeout = 200_000,
        };

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public List<RaftResponderRequest> Outbound { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public long GetMonotonicTimestamp() => global::System.Diagnostics.Stopwatch.GetTimestamp();
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public long GetLastNodeActivityTicks(string endpoint, int partitionId) => 0;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => Outbound.Add(request);

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task InvokeLeadershipLost(int partitionId, long term) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    private sealed class FollowerHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
        {
            // Inline delivery: the semantics under test are the errored ack and the healed
            // consumer delivery on re-delivery, not the apply-turn lane mechanics (covered by
            // the dedicated follower-apply-turn suite).
            FollowerApplyInOwnTurn = false,
            Host = "follower",
            Port = 8001,
            InitialPartitions = 1,
            StartElectionTimeout = 100_000,
            EndElectionTimeout = 200_000,
        };

        public List<long> Delivered { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = LeaderEndpoint;
        public string LocalEndpoint => "follower:8001";
        public int LocalNodeId => 2;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; } = [new RaftNode(LeaderEndpoint)];

        public List<RaftResponderRequest> Outbound { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public long GetMonotonicTimestamp() => global::System.Diagnostics.Stopwatch.GetTimestamp();
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public long GetLastNodeActivityTicks(string endpoint, int partitionId) => 0;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => Outbound.Add(request);

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log)
        {
            Delivered.Add(log.Id);
            return Task.FromResult(true);
        }
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// WAL facade that records each enqueue with a unique operation id and captures the batches as
    /// the write path retyped them, so a test can complete the real operation and inspect what the
    /// completion actually routed.
    /// </summary>
    private sealed class RoutingWal : IRaftWalFacade
    {
        private long _nextOperationId;
        private readonly Dictionary<long, RaftLog> _entries = new();

        public long LastProposeOperationId { get; private set; }
        public long LastCommitOperationId { get; private set; }
        public long LastRollbackOperationId { get; private set; }
        public long LastAppendOperationId { get; private set; }

        public int CommitCount { get; private set; }

        public List<RaftLog>? LastCommittedBatch { get; private set; }
        public List<RaftLog>? LastRolledBackBatch { get; private set; }

        /// <summary>The gap-aware committed frontier the follower reports on a successful append.</summary>
        public long CommitIndexValue { get; private set; }

        public long GetCommitIndex() => CommitIndexValue;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
        {
            // The follower apply turn delivers exactly what this read returns, contiguously
            // from the requested start — the same shape the real WAL guarantees.
            List<RaftLog> result = [];
            for (long id = start; result.Count < max && _entries.TryGetValue(id, out RaftLog? log); id++)
                result.Add(log);

            return ValueTask.FromResult(result);
        }
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            LastProposeOperationId = Interlocked.Increment(ref _nextOperationId);
            return new(null!, LastProposeOperationId, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            CommitCount++;
            LastCommitOperationId = Interlocked.Increment(ref _nextOperationId);

            // Mirrors RaftWriteAhead.EnqueueCommit: the resolution re-types the rows in place
            // before the write is queued, and everything downstream (the registry, the peer
            // broadcast) sees the resolved types.
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
                else if (log.Type == RaftLogType.ProposedCheckpoint)
                    log.Type = RaftLogType.CommittedCheckpoint;
            }

            LastCommittedBatch = [.. logs];
            return new(null!, LastCommitOperationId, WALWriteOperationType.LeaderCommit, (1, logs));
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
        {
            LastRollbackOperationId = Interlocked.Increment(ref _nextOperationId);

            // Mirrors RaftWriteAhead.EnqueueRollback: same in-place re-typing.
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.RolledBack;
                else if (log.Type == RaftLogType.ProposedCheckpoint)
                    log.Type = RaftLogType.RolledBackCheckpoint;
            }

            LastRolledBackBatch = [.. logs];
            return new(null!, LastRollbackOperationId, WALWriteOperationType.LeaderRollback, (1, logs));
        }

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1)
        {
            if (logs is null || logs.Count == 0)
                return null;

            LastAppendOperationId = Interlocked.Increment(ref _nextOperationId);

            // The leader told us these ids are resolved, so the frontier this follower reports
            // covers the batch — exactly what CompleteFollowerAppend reads back.
            long maxId = 0;
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Committed && log.Id > CommitIndexValue)
                    CommitIndexValue = log.Id;
                if (log.Id > maxId)
                    maxId = log.Id;

                _entries[log.Id] = log;
            }

            return new(null!, LastAppendOperationId, WALWriteOperationType.FollowerAppend, (1, logs), t, endpoint: ep, term: term, logIndex: maxId);
        }

        public void NotifyCommitted() { }
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) => Completed.Add((correlationId, response));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
