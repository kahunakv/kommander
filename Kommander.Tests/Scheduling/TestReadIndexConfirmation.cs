using System.Diagnostics;
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
/// Covers read-index leadership confirmation (<c>ConfirmLeadershipAsync</c>): a leader must prove
/// it is still the leader with a same-term quorum ack round — and catch its applied frontier up to
/// the commit index captured at confirmation — before a local read may be served as authoritative.
/// A minority-partitioned leader believes it leads until it <i>receives</i> a higher-term message,
/// so the pure-local-belief gates (<c>AmILeader</c>) let it serve stale reads; these tests pin the
/// quorum-round semantics that close that hole, plus the optional check-quorum step-down that
/// bounds how long such a leader lingers.
///
/// <list type="bullet">
///   <item>A healthy leader confirms within one ack round; concurrent callers coalesce into a
///         single forced heartbeat round, and a fresh confirmation is reused as a fast path.</item>
///   <item>A leader that never collects acks (minority partition) fails all waiters within
///         <c>LeadershipConfirmationTimeout</c>, enforced from the leader tick.</item>
///   <item>An unpublished leader (promotion barrier armed: <c>nodeState==Leader</c> but
///         <c>host.Leader</c> not self) can never confirm.</item>
///   <item>Quorum acks alone are not enough: the reply waits for the applied frontier to cover
///         the captured commit index.</item>
///   <item>A quiesced leader is woken by a confirmation request — the ack round is the proof, so
///         quiescence only needs the wake-up path.</item>
///   <item>Check-quorum: an isolated leader steps down after the configured window; a leader that
///         keeps hearing a voter majority does not.</item>
/// </list>
/// </summary>
public class TestReadIndexConfirmation
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        public int PartitionId { get; init; } = 1;

        public string Leader { get; set; } = "";

        public string LocalEndpoint => "node-a";

        public int LocalNodeId => 1;

        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;

        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();

        /// <summary>Injectable monotonic clock; null uses the real <see cref="Stopwatch"/>.
        /// Drive it to cross timeout windows deterministically. Never set it to 0 —
        /// <c>MonotonicElapsed</c> treats a 0 anchor as "never".</summary>
        public long? MonotonicOverride { get; set; }

        public long GetMonotonicTimestamp() => MonotonicOverride ?? Stopwatch.GetTimestamp();

        /// <summary>Advances the injected monotonic clock by the given wall time.</summary>
        public void AdvanceMonotonic(TimeSpan by) =>
            MonotonicOverride = (MonotonicOverride ?? Stopwatch.GetTimestamp())
                + (long)(Stopwatch.Frequency * by.TotalSeconds);

        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public List<(string Endpoint, RaftResponderRequest Request)> EnqueuedRequests { get; } = [];

        public int CountEnqueued(RaftResponderRequestType type) =>
            EnqueuedRequests.Count(r => r.Request.Type == type);

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request) =>
            EnqueuedRequests.Add((endpoint, request));

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new SnapshotResponse(false));
    }

    private sealed class FakeWalFacade : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            IReadOnlyList<RaftLog> none = [];
            return ValueTask.FromResult(none);
        }

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

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

    // ── helpers ────────────────────────────────────────────────────────────────

    private static (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) MakeLeader(
        params string[] peers)
    {
        FakePartitionHost host = new() { Nodes = peers.Select(p => new RaftNode(p)).ToList() };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, new FakeWalFacade(), sink, NullLogger<IRaft>.Instance);

        sm.SetLeaderForTesting(term: 1);
        // Align the frontiers the way a real restore does (lastApplied = commit index); the fake
        // WAL is empty, so both sit at -1 and a confirmation is not blocked on a phantom apply.
        sm.SetLocalCommittedIndexForTesting(-1);
        return (sm, host, sink);
    }

    private static Task Ack(RaftPartitionStateMachine sm, FakePartitionHost host, string endpoint, long committedIndex = -1) =>
        sm.CompleteAppendLogsAsync(
            endpoint,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            RaftOperationStatus.Success,
            committedIndex,
            responseTerm: 1).AsTask();

    // ── healthy leader: one round, coalescing, fast path ──────────────────────

    [Fact]
    public async Task HealthyLeader_ConfirmsOnQuorumAck_ConcurrentCallersCoalesce()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b", "node-c");
        host.EnqueuedRequests.Clear();

        // Two concurrent confirmations: the first opens the round and fires one forced heartbeat
        // to each peer; the second joins the in-flight round without any extra traffic.
        await sm.ConfirmLeadershipAsync(replyCorrelationId: 1);
        await sm.ConfirmLeadershipAsync(replyCorrelationId: 2);

        Assert.Equal(2, host.CountEnqueued(RaftResponderRequestType.AppendLogs));
        Assert.Empty(sink.Completed);   // no acks yet — nothing may confirm

        // One same-term voter ack (self + node-b = 2 of 3 voters) confirms BOTH waiters.
        await Ack(sm, host, "node-b");

        Assert.Equal(2, sink.Completed.Count);
        Assert.All(sink.Completed, r => Assert.Equal(RaftOperationStatus.Success, r.Response.Status));
        Assert.Contains(sink.Completed, r => r.Id == 1);
        Assert.Contains(sink.Completed, r => r.Id == 2);

        // Fast path: a confirmation completed within the last heartbeat interval is reused —
        // no new round, no new heartbeats.
        int enqueuedBefore = host.CountEnqueued(RaftResponderRequestType.AppendLogs);
        await sm.ConfirmLeadershipAsync(replyCorrelationId: 3);

        Assert.Equal(enqueuedBefore, host.CountEnqueued(RaftResponderRequestType.AppendLogs));
        Assert.Contains(sink.Completed, r => r.Id == 3 && r.Response.Status == RaftOperationStatus.Success);
    }

    // ── minority-partitioned leader: fails within the timeout ─────────────────

    [Fact]
    public async Task PartitionedLeader_NoAcks_FailsWithinConfirmationTimeout()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await sm.ConfirmLeadershipAsync(replyCorrelationId: 1);
        Assert.Empty(sink.Completed);

        // Below the timeout: the tick must NOT fail the waiter yet.
        host.AdvanceMonotonic(host.Configuration.LeadershipConfirmationTimeout / 2);
        await sm.CheckPartitionLeadershipAsync();
        Assert.Empty(sink.Completed);

        // Past the timeout: the waiter fails — a minority-side leader can never collect the acks,
        // and the caller must get a retry/redirect instead of blocking forever.
        host.AdvanceMonotonic(host.Configuration.LeadershipConfirmationTimeout);
        await sm.CheckPartitionLeadershipAsync();

        (ulong id, RaftResponse response) = Assert.Single(sink.Completed);
        Assert.Equal(1UL, id);
        Assert.NotEqual(RaftOperationStatus.Success, response.Status);

        // Check-quorum is off by default: the isolated leader keeps its (useless) leadership.
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    // ── unpublished leadership can never confirm ──────────────────────────────

    [Fact]
    public async Task UnpublishedLeader_BarrierStillArmed_ConfirmationFailsImmediately()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b", "node-c");

        // While the promotion barrier is armed, nodeState is Leader but host.Leader is not
        // published — exactly the window in which serving reads would leak inherited-entry state.
        // Model it directly through the same gate the barrier uses.
        host.Leader = "";
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);    // the AmILeader fallback view

        await sm.ConfirmLeadershipAsync(replyCorrelationId: 5);

        (ulong id, RaftResponse response) = Assert.Single(sink.Completed);
        Assert.Equal(5UL, id);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, response.Status);
    }

    [Fact]
    public async Task Follower_ConfirmationFailsImmediately()
    {
        FakePartitionHost host = new() { Nodes = [new RaftNode("node-b")] };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, new FakeWalFacade(), sink, NullLogger<IRaft>.Instance);

        await sm.ConfirmLeadershipAsync(replyCorrelationId: 6);

        (ulong id, RaftResponse response) = Assert.Single(sink.Completed);
        Assert.Equal(6UL, id);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, response.Status);
    }

    // ── applied-frontier wait: quorum acks alone are not enough ───────────────

    [Fact]
    public async Task Confirmation_WaitsForAppliedFrontier_NotJustQuorumAcks()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b");

        // Commit frontier ahead of the applied frontier (-1): entry 1 is durably committed but the
        // consumer has not applied it yet — a read served now would miss it.
        sm.SetLocalCommittedIndexForTesting(1);

        await sm.ConfirmLeadershipAsync(replyCorrelationId: 7);
        await Ack(sm, host, "node-b");

        // Quorum confirmed, but the reply must be withheld until applied >= 1.
        Assert.DoesNotContain(sink.Completed, r => r.Id == 7);

        // Drive a real commit of entry 1 through the write path; CompleteLeaderCommit's apply
        // advances the applied frontier, which releases the parked confirmation.
        sm.ReplicateLogs([new RaftLog { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 10);
        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            host.PartitionId, OperationId: 1, Term: -1, MinLogIndex: -1, MaxLogIndex: 1,
            WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 10);
        await sm.CompleteAppendLogsAsync("node-b", proposeReply.TicketId, RaftOperationStatus.Success, committedIndex: 0);
        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            host.PartitionId, OperationId: 2, Term: -1, MinLogIndex: -1, MaxLogIndex: 1,
            WALWriteOperationType.LeaderCommit, RaftOperationStatus.Success));

        Assert.Contains(sink.Completed, r => r.Id == 7 && r.Response.Status == RaftOperationStatus.Success);
    }

    // ── quiesced leader: confirmation wakes the heartbeat path ────────────────

    [Fact]
    public async Task QuiescedLeader_ConfirmationWakesHeartbeats_AndConfirms()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b", "node-c");

        sm.SetQuiescedForTesting(true);
        host.EnqueuedRequests.Clear();

        // A quiesced leader has stopped heartbeating; the confirmation must fire its own ack
        // round (the round-trip IS the proof), so appends go out despite quiescence.
        await sm.ConfirmLeadershipAsync(replyCorrelationId: 9);
        Assert.Equal(2, host.CountEnqueued(RaftResponderRequestType.AppendLogs));

        await Ack(sm, host, "node-c");

        Assert.Contains(sink.Completed, r => r.Id == 9 && r.Response.Status == RaftOperationStatus.Success);
    }

    // ── leadership loss fails all pending waiters ─────────────────────────────

    [Fact]
    public async Task StepDown_FailsPendingConfirmationWaiters()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b", "node-c");

        await sm.ConfirmLeadershipAsync(replyCorrelationId: 11);
        Assert.Empty(sink.Completed);

        await sm.StepDownAsync(replyCorrelationId: null);

        Assert.Contains(sink.Completed, r => r.Id == 11 && r.Response.Status == RaftOperationStatus.NodeIsNotLeader);
    }

    // ── check-quorum ──────────────────────────────────────────────────────────

    private static void EnableCheckQuorum(FakePartitionHost host)
    {
        host.Configuration.EnableCheckQuorum = true;
        host.Configuration.CheckQuorumIntervalMultiplier = 2;
        host.Configuration.HeartbeatInterval = TimeSpan.FromMilliseconds(10);
    }

    [Fact]
    public async Task CheckQuorum_IsolatedLeader_StepsDownWithinWindow()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        EnableCheckQuorum(host);
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        // No acks ever arrive (minority partition). Past the window (2 × 10 ms), the leader must
        // step down instead of heartbeating into the void indefinitely.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(50));
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal("", host.Leader);
    }

    [Fact]
    public async Task CheckQuorum_LeaderHearingMajority_DoesNotStepDown()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        EnableCheckQuorum(host);
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        // Acks keep arriving from one voter peer (self + node-b = majority of 3): ticks must keep
        // refreshing the window and the leader must hold.
        for (int i = 0; i < 5; i++)
        {
            host.AdvanceMonotonic(TimeSpan.FromMilliseconds(8));
            await Ack(sm, host, "node-b");
            await sm.CheckPartitionLeadershipAsync();
            Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        }

        // Then the acks stop: the same leader steps down once the window elapses.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(50));
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }
}
