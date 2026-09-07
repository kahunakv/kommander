
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
/// Coverage for the reply-hold test hook
/// (<see cref="IRaft.HoldCommittedProposalRepliesForTesting"/>): the seam that makes
/// "durable at quorum, not yet answered" a constructible state.
///
/// <para>A finalize awaits the very completion its reply rides on, so in an ordinary in-process
/// test "the commit is durable on a quorum" and "the coordinator learned it committed" are the same
/// event. The hook separates them by holding <b>only the reply</b> — the commit fan-out, the local
/// applies and the commit-frontier advance all run first — so a consumer can drive the failures
/// that live between the two.</para>
///
/// <para>These tests assert the hook's own contract: what is held, what is never held, that a hold
/// resolves exactly once whichever path wins, and that no hold can outlive the test that installed
/// it.</para>
/// </summary>
public class TestProposalReplyHold
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Host that records consumer applies in order, so a test can assert the entry reached the
    /// local state machine while its caller was still waiting.
    /// </summary>
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

        /// <summary>Ordered log of apply events produced by the consumer callbacks.</summary>
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
    /// WAL facade that assigns a unique operation id per enqueue (so several proposals can be
    /// pending at once) and retains every entry it has seen, mutating types in place on
    /// commit/rollback exactly as the real backend does. The test drives completions by hand.
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

    /// <summary>Collects held replies and lets a test wait for the off-turn callback.</summary>
    private sealed class HoldCollector : IDisposable
    {
        private readonly SemaphoreSlim signal = new(0);
        private readonly List<HeldProposalReply> replies = [];

        public IReadOnlyList<HeldProposalReply> Replies
        {
            get { lock (replies) return [.. replies]; }
        }

        public void OnHeld(HeldProposalReply reply)
        {
            lock (replies)
                replies.Add(reply);

            signal.Release();
        }

        /// <summary>Waits for the next callback. The hook queues it to the thread pool, so a test
        /// that asserts immediately after the completion would race it.</summary>
        public async Task<HeldProposalReply> WaitForNextAsync()
        {
            Assert.True(await signal.WaitAsync(TimeSpan.FromSeconds(5)), "no reply was held within 5s");
            lock (replies)
                return replies[^1];
        }

        /// <summary>Asserts no further callback arrives within a short window.</summary>
        public async Task AssertNoMoreAsync()
        {
            Assert.False(await signal.WaitAsync(TimeSpan.FromMilliseconds(250)), "a second hold was reported for one ticket");
        }

        public void Dispose() => signal.Dispose();
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

    /// <summary>Promotes the sole-voter state machine to leader.</summary>
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

    // ── tests ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Regression guard for the null path: with no hook registered the write path answers exactly
    /// as it does today. Every completion site pays one null check and nothing else.
    /// </summary>
    [Fact]
    public async Task NoHook_ProposalAnswersAsUsual()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);

        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        Assert.True(waiter.IsCompleted);
        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter);
        Assert.Equal(["Applied:1"], host.EventLog);
    }

    /// <summary>
    /// The state the hook exists to construct: the entry is committed on this node and delivered to
    /// its consumer, while the proposer's task is still incomplete. Releasing then returns exactly
    /// what the unheld path returns.
    /// </summary>
    [Fact]
    public async Task HeldReply_EntryIsCommittedAndAppliedWhileTheCallerWaits()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        using HoldCollector collector = new();
        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);

        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        HeldProposalReply held = await collector.WaitForNextAsync();

        // Durable, applied and visible as committed on this node — and unanswered.
        Assert.False(waiter.IsCompleted);
        Assert.Equal(["Applied:1"], host.EventLog);
        Assert.Equal((RaftProposalTicketState.Committed, 1L), sm.CheckTicketCompletion(ticket));
        Assert.Equal(1, wal.GetCommitIndex());

        Assert.Equal(ticket, held.TicketId);
        Assert.Equal(1L, held.CommitIndex);
        Assert.Equal(1L, held.Term);
        Assert.Equal([1L], held.LogIds);
        Assert.Equal(1, held.PartitionId);

        held.Release();

        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        // Release is idempotent and a later Drop cannot undo it.
        held.Release();
        held.Drop();
        Assert.True(held.IsResolved);
    }

    /// <summary>
    /// <c>Drop</c> models a killed leader without killing the process: the caller is abandoned and
    /// ends exactly as a real proposal timeout ends — the waiter never completes, which is what
    /// <c>WaitForQuorum</c> turns into <see cref="RaftOperationStatus.ProposalTimeout"/>.
    /// </summary>
    [Fact]
    public async Task DroppedReply_LeavesTheCallerToTimeOut()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        using HoldCollector collector = new();
        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);

        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        HeldProposalReply held = await collector.WaitForNextAsync();
        held.Drop();

        await Assert.ThrowsAsync<TimeoutException>(() => waiter.WaitAsync(TimeSpan.FromMilliseconds(150), TestContext.Current.CancellationToken));
        Assert.False(waiter.IsCompleted);

        // The entry itself is untouched: durable and committed, exactly as the state this models.
        Assert.Equal((RaftProposalTicketState.Committed, 1L), sm.CheckTicketCompletion(ticket));
    }

    /// <summary>
    /// The single-fsync fast path and the commit completion both fire for one auto-commit proposal.
    /// That must produce one hold, not two, and the site reported is the one that fired first.
    /// </summary>
    [Fact]
    public async Task FastPathAndCommitCompletion_HoldOnce_AndReportTheFirstSite()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        host.Configuration.WalSingleFsyncCommit = true;
        await BecomeSoleLeaderAsync(sm);

        using HoldCollector collector = new();
        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);

        // The propose completion releases the ticket on quorum-durable: hold #1.
        long proposeOp = wal.LastOperationId;
        await sm.CompleteWalOperationAsync(MakeCompletion(proposeOp, WALWriteOperationType.LeaderPropose, -1, 1));

        HeldProposalReply held = await collector.WaitForNextAsync();
        Assert.Equal(ProposalReplySite.QuorumDurableFastPath, held.Site);

        // The commit completion fires the same waiter again; it must be swallowed.
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));
        await collector.AssertNoMoreAsync();

        Assert.Single(collector.Replies);

        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        Assert.False(waiter.IsCompleted);

        held.Release();
        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// The manual two-phase propose completes its caller on propose-quorum-durable, so that site is
    /// held too — and the site is reported.
    /// </summary>
    [Fact]
    public async Task ManualProposeQuorum_IsHeld_AndReportsItsSite()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build(nodes: [new("node-b")]);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        using HoldCollector collector = new();
        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: false, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);

        await sm.CompleteAppendLogsAsync("node-b", ticket, RaftOperationStatus.Success, committedIndex: 0);

        HeldProposalReply held = await collector.WaitForNextAsync();
        Assert.Equal(ProposalReplySite.ManualProposeQuorum, held.Site);
        Assert.False(waiter.IsCompleted);

        held.Release();
        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A failure for a held ticket discards the hold and answers the failure. A proposal cannot be
    /// simultaneously held-as-committed and failed, and the failure is the outcome the caller must
    /// see; a later <c>Release</c> from the test thread is then a no-op.
    /// </summary>
    [Fact]
    public async Task FailureWhileHeld_DiscardsTheHold_AndTheFailureAnswersTheCaller()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build(nodes: [new("node-b")]);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);

        using HoldCollector collector = new();
        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: false, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));

        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);

        await sm.CompleteAppendLogsAsync("node-b", ticket, RaftOperationStatus.Success, committedIndex: 0);
        HeldProposalReply held = await collector.WaitForNextAsync();
        Assert.False(waiter.IsCompleted);

        // The proposal is rolled back while its success reply is held.
        await sm.RollbackLogsAsync(ticket, replyCorrelationId: 11);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderRollback, 1, 1));

        (RaftProposalTicketState state, long index) = await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftProposalTicketState.NotFound, state);
        Assert.Equal(-1L, index);

        Assert.True(held.IsResolved);
        held.Release();                                     // no-op: the failure already answered
        Assert.Equal((RaftProposalTicketState.NotFound, -1L), await waiter);
    }

    /// <summary>
    /// Disposing the registration restores ordinary behaviour and releases everything it was
    /// holding — a test that throws mid-way must not leave a node unable to answer.
    /// </summary>
    [Fact]
    public async Task Dispose_ReleasesEveryHold_AndRestoresOrdinaryBehaviour()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        using HoldCollector collector = new();
        IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));
        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        await collector.WaitForNextAsync();
        Assert.False(waiter.IsCompleted);

        registration.Dispose();
        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        registration.Dispose();                             // idempotent

        // Ordinary behaviour restored: the next proposal answers without being held.
        sm.ReplicateLogs([new() { Id = 2, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 11);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 2));
        HLCTimestamp ticket2 = TicketOf(sink, 11);
        Task<(RaftProposalTicketState, long)>? waiter2 = sm.GetTicketWaiterTask(ticket2);
        Assert.NotNull(waiter2);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 2, 2));

        Assert.True(waiter2.IsCompleted);
        Assert.Equal((RaftProposalTicketState.Committed, 2L), await waiter2);
    }

    /// <summary>
    /// A partition that stops releases everything held, through the same path that clears every
    /// other test hook. Nothing a hook holds can survive the partition it was installed on.
    /// </summary>
    [Fact]
    public async Task PartitionStop_ReleasesEveryHold()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        using HoldCollector collector = new();
        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));
        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        await collector.WaitForNextAsync();
        Assert.False(waiter.IsCompleted);

        sm.ResetTestingState();                             // what RaftPartition.Dispose runs

        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A hold that outlives <see cref="RaftConfiguration.ProposalTimeout"/> self-releases: past the
    /// caller's own bound the hold changes nothing and would only hide a leak.
    /// </summary>
    [Fact]
    public async Task HoldExceedingProposalTimeout_SelfReleases()
    {
        (RaftPartitionStateMachine sm, RecordingHost host, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        host.Configuration.ProposalTimeout = TimeSpan.FromMilliseconds(150);
        await BecomeSoleLeaderAsync(sm);

        using HoldCollector collector = new();
        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(collector.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));
        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        HeldProposalReply held = await collector.WaitForNextAsync();

        // Never resolved by the test: the bound answers the caller instead.
        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.True(held.IsResolved);
    }

    /// <summary>
    /// A second registration replaces the first and releases everything the first held, so a
    /// re-registration cannot strand a caller.
    /// </summary>
    [Fact]
    public async Task SecondRegistration_ReplacesTheFirst_AndReleasesWhatItHeld()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        using HoldCollector first = new();
        using IDisposable firstRegistration = sm.HoldCommittedProposalRepliesForTesting(first.OnHeld);

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));
        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        await first.WaitForNextAsync();
        Assert.False(waiter.IsCompleted);

        using HoldCollector second = new();
        using IDisposable secondRegistration = sm.HoldCommittedProposalRepliesForTesting(second.OnHeld);

        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        // The replacement owns the partition now.
        sm.ReplicateLogs([new() { Id = 2, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 11);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 2));
        HLCTimestamp ticket2 = TicketOf(sink, 11);
        Task<(RaftProposalTicketState, long)>? waiter2 = sm.GetTicketWaiterTask(ticket2);
        Assert.NotNull(waiter2);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 2, 2));

        HeldProposalReply heldBySecond = await second.WaitForNextAsync();
        Assert.Equal(ticket2, heldBySecond.TicketId);
        Assert.False(waiter2.IsCompleted);
        heldBySecond.Release();
        Assert.Equal((RaftProposalTicketState.Committed, 2L), await waiter2.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A callback that throws must never become a silent hold: the exception is swallowed by the
    /// hook (logged) and the reply is released.
    /// </summary>
    [Fact]
    public async Task ThrowingCallback_ReleasesTheReply()
    {
        (RaftPartitionStateMachine sm, _, PipelinedWalFacade wal, CapturingReplySink sink) = Build();
        await BecomeSoleLeaderAsync(sm);

        using IDisposable registration = sm.HoldCommittedProposalRepliesForTesting(
            _ => throw new InvalidOperationException("callback failed (injected)"));

        sm.ReplicateLogs([new() { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderPropose, -1, 1));
        HLCTimestamp ticket = TicketOf(sink, 10);
        Task<(RaftProposalTicketState, long)>? waiter = sm.GetTicketWaiterTask(ticket);
        Assert.NotNull(waiter);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, 1, 1));

        Assert.Equal((RaftProposalTicketState.Committed, 1L), await waiter.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }
}
