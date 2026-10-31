
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
/// A settled proposal stays registered for its ticket window, but its entries do not: the commit and
/// rollback completions release the quorum's entry list once the marker is durable and the resolution
/// is broadcast, and the record answers its ticket from the retained last index. Until then (an
/// unresolved proposal that may still be re-sent, or a manual two-phase proposal awaiting its commit)
/// the entries stay. The list itself is never emptied, because the WAL write and queued sends share it.
/// </summary>
public class TestSettledProposalEntries
{
    // ── the quorum alone ──────────────────────────────────────────────────────

    [Fact]
    public void ReleaseEntries_DropsTheListAndKeepsTheLastIndex()
    {
        List<RaftLog> logs = [new() { Id = 5, Term = 1 }, new() { Id = 6, Term = 1 }, new() { Id = 7, Term = 1 }];
        RaftProposalQuorum quorum = new(logs, autoCommit: true, HLCTimestamp.Zero);

        Assert.Equal(7L, quorum.LastLogIndex);
        Assert.False(quorum.EntriesReleased);

        quorum.ReleaseEntries();

        Assert.Empty(quorum.Logs);
        Assert.True(quorum.EntriesReleased);
        Assert.Equal(7L, quorum.LastLogIndex);
        // The list is shared with the WAL write and the transport: it is dropped, never emptied.
        Assert.Equal(3, logs.Count);

        // A second release is a no-op.
        quorum.ReleaseEntries();
        Assert.Equal(7L, quorum.LastLogIndex);

        // A pooled reuse answers from the new list again.
        quorum.Reset([new() { Id = 9, Term = 2 }], autoCommit: false, HLCTimestamp.Zero);
        Assert.False(quorum.EntriesReleased);
        Assert.Equal(9L, quorum.LastLogIndex);

        quorum.Clear();
        Assert.False(quorum.EntriesReleased);
        Assert.Equal(-1L, quorum.LastLogIndex);
    }

    [Fact]
    public void LastLogIndex_OfAProposalWithoutEntries_IsMinusOne()
    {
        RaftProposalQuorum quorum = new([], autoCommit: true, HLCTimestamp.Zero);

        Assert.Equal(-1L, quorum.LastLogIndex);
        quorum.ReleaseEntries();
        Assert.Equal(-1L, quorum.LastLogIndex);
        Assert.False(quorum.EntriesReleased);
    }

    // ── through the state machine ─────────────────────────────────────────────

    [Fact]
    public async Task SoleVoter_AutoCommit_ReleasesTheEntriesAtTheCommitCompletion_AndTheTicketStillAnswers()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t", LogData = new byte[1024] }];
        sm.ReplicateLogs(logs, autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 10);
        RaftProposalQuorum proposal = Assert.IsType<RaftProposalQuorum>(sm.GetRegisteredProposalForTesting(ticket));

        // The commit marker is queued but not durable: the entries are still the proposal's to re-send.
        Assert.Single(proposal.Logs);
        Assert.False(proposal.EntriesReleased);

        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        Assert.True(proposal.EntriesReleased);
        Assert.Empty(proposal.Logs);
        Assert.Equal(1L, proposal.LastLogIndex);
        Assert.Same(proposal, sm.GetRegisteredProposalForTesting(ticket));
        Assert.Equal((RaftProposalTicketState.Committed, 1L), sm.CheckTicketCompletion(ticket));

        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter);

        // The entry reached the consumer and the caller's list was left intact.
        Assert.Equal(["Applied:1"], host.EventLog);
        Assert.Single(logs);
        Assert.Equal(RaftLogType.Committed, logs[0].Type);
    }

    [Fact]
    public async Task TwoVoters_TheEntriesStayWhileTheProposalIsUnresolved_AndGoAtTheCommitCompletion()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build(nodes: [new("node-b")]);
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 11);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 11);
        RaftProposalQuorum proposal = Assert.IsType<RaftProposalQuorum>(sm.GetRegisteredProposalForTesting(ticket));

        // Short of quorum: the retry path may still re-send these entries to node-b.
        Assert.Equal(RaftProposalState.Incomplete, proposal.State);
        Assert.Single(proposal.Logs);

        // Quorum reached (the single-fsync fast path may already report Committed); the commit marker
        // is queued but not durable, so the entries stay until its completion.
        await sm.CompleteAppendLogsAsync("node-b", ticket, RaftOperationStatus.Success, committedIndex: 0);
        Assert.NotEqual(RaftProposalState.Incomplete, proposal.State);
        Assert.Single(proposal.Logs);
        Assert.False(proposal.EntriesReleased);

        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        Assert.Equal(RaftProposalState.Committed, proposal.State);
        Assert.True(proposal.EntriesReleased);
        Assert.Equal((RaftProposalTicketState.Committed, 1L), sm.CheckTicketCompletion(ticket));
    }

    [Fact]
    public async Task ManualTwoPhase_KeepsTheEntriesUntilTheExplicitCommitCompletes()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: false, replyCorrelationId: 12);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 12);
        RaftProposalQuorum proposal = Assert.IsType<RaftProposalQuorum>(sm.GetRegisteredProposalForTesting(ticket));

        // Propose-quorum durable, awaiting the caller's commit: the commit marker is written from these entries.
        Assert.Equal(RaftProposalState.Completed, proposal.State);
        Assert.Single(proposal.Logs);
        Assert.False(proposal.EntriesReleased);

        await sm.CommitLogsAsync(ticket, replyCorrelationId: 20);
        Assert.Single(proposal.Logs);

        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        Assert.True(proposal.EntriesReleased);
        Assert.Equal(1L, proposal.LastLogIndex);
        Assert.Equal((RaftProposalTicketState.Committed, 1L), sm.CheckTicketCompletion(ticket));
        Assert.Equal(["Applied:1"], host.EventLog);
        Assert.Contains(sink.Completed, r => r.Id == 20 && r.Response.Status == RaftOperationStatus.Success);
    }

    [Fact]
    public async Task ManualTwoPhase_RollbackCompletionReleasesTheEntries()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 13);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 13);
        RaftProposalQuorum proposal = Assert.IsType<RaftProposalQuorum>(sm.GetRegisteredProposalForTesting(ticket));
        Assert.Single(proposal.Logs);

        await sm.RollbackLogsAsync(ticket, replyCorrelationId: 21);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderRollback, 1, 1));

        Assert.Equal(RaftProposalState.RolledBack, proposal.State);
        Assert.True(proposal.EntriesReleased);
        Assert.Equal(1L, proposal.LastLogIndex);
        Assert.Equal(RaftLogType.RolledBack, logs[0].Type);
        Assert.Contains(sink.Completed, r => r.Id == 21 && r.Response.Status == RaftOperationStatus.Success);
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private sealed class RecordingHost : IRaftPartitionHost
    {
        public RaftConfiguration Configuration { get; } = new()
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
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => NodesOverride;
        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = [];

        public List<string> EventLog { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string ep, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string ep, RaftResponderRequest req) { }
        public Task InvokeLeaderChanged(int p, string leader) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"Applied:{log.Id}");
            return Task.FromResult(true);
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"SystemApplied:{log.Id}");
            return Task.FromResult(true);
        }

        public void InvokeReplicationError(int p, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode n, SnapshotRequest r, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// WAL facade with one operation id per enqueue, retaining every entry and mutating its type in place
    /// on commit and rollback as the real backend does. Completions are driven by the test.
    /// </summary>
    private sealed class PipelinedWalFacade : IRaftWalFacade
    {
        private long _nextOperationId;
        private long _commitIndex;
        private readonly SortedDictionary<long, RaftLog> _entries = [];

        public long LastOperationId => _nextOperationId;

        public long GetCommitIndex() => _commitIndex;

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
            => ValueTask.FromResult(_entries.Values.Where(l => l.Id >= start && l.Type == RaftLogType.Committed).Take(max).ToList());

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
            => ValueTask.FromResult(_entries.Values.Where(l => l.Id >= start).Take(max).ToList());

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_entries.Count > 0 ? _entries.Keys.Max() : 0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            foreach (RaftLog log in logs)
                _entries[log.Id] = log;

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderPropose,
                (1, logs), timestamp, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
                else if (log.Type == RaftLogType.ProposedCheckpoint)
                    log.Type = RaftLogType.CommittedCheckpoint;

                _entries[log.Id] = log;

                if (log.Id > _commitIndex)
                    _commitIndex = log.Id;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderCommit, (1, logs), logIndex: maxId);
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.RolledBack;
                else if (log.Type == RaftLogType.ProposedCheckpoint)
                    log.Type = RaftLogType.RolledBackCheckpoint;

                _entries[log.Id] = log;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderRollback, (1, logs), logIndex: maxId);
        }

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1)
            => logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];
        public void TryComplete(ulong correlationId, RaftResponse response) => Completed.Add((correlationId, response));
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static RaftWalCompletion MakeCompletion(long operationId, WALWriteOperationType type, long minLogIndex, long maxLogIndex) =>
        new(PartitionId: 1, OperationId: operationId, Term: -1L,
            MinLogIndex: minLogIndex, MaxLogIndex: maxLogIndex,
            OperationType: type, Status: RaftOperationStatus.Success);

    private static (RaftPartitionStateMachine sm, RecordingHost host, PipelinedWalFacade wal, CapturingReplySink sink) Build(
        IReadOnlyList<RaftNode>? nodes = null)
    {
        RecordingHost host = new() { NodesOverride = nodes ?? [] };
        PipelinedWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        return (sm, host, wal, sink);
    }

    private static async Task BecomeSoleLeaderAsync(RaftPartitionStateMachine sm)
    {
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    private static HLCTimestamp TicketOf(CapturingReplySink sink, ulong correlationId)
    {
        (ulong _, RaftResponse reply) = Assert.Single(sink.Completed, r => r.Id == correlationId);
        Assert.Equal(RaftOperationStatus.Success, reply.Status);
        return reply.TicketId;
    }
}
