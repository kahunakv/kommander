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
/// Covers the state-machine half of follower catch-up confirmation
/// (<c>IRaft.ConfirmLocalApplicationAsync</c>): <c>WaitLocalApplication</c> parks a caller until
/// the local applied frontier covers a leader-confirmed commit index. The leadership proof lives
/// on the remote leader (its quorum ack round produced the index), so the wait itself is
/// role-agnostic — but it must stay bounded in <b>every</b> node state, because a stable follower
/// whose apply stalls never goes through a leadership-loss transition that would fail its waiters.
///
/// <list type="bullet">
///   <item>An applied frontier already covering the index completes immediately.</item>
///   <item>A lagging frontier parks the caller; the apply path releases it when the entry
///         applies — quorum semantics on the leader, catch-up semantics on a follower.</item>
///   <item>The wait expires from the tick on a NON-leader within
///         <c>LeadershipConfirmationTimeout</c> — the follower-side bound this feature adds.</item>
///   <item>Leadership transitions fail parked waiters (fail closed, never "confirmed enough").</item>
///   <item>The leader's confirmation reply carries the round's captured read index, which is what
///         a remote follower waits on (<c>GetReadIndex</c> wire contract).</item>
/// </list>
/// </summary>
public class TestFollowerCatchUpConfirmation
{
    // ── stubs (mirrors TestReadIndexConfirmation) ─────────────────────────────

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
        /// Never set it to 0 — <c>MonotonicElapsed</c> treats a 0 anchor as "never".</summary>
        public long? MonotonicOverride { get; set; }

        public long GetMonotonicTimestamp() => MonotonicOverride ?? Stopwatch.GetTimestamp();

        public void AdvanceMonotonic(TimeSpan by) =>
            MonotonicOverride = (MonotonicOverride ?? Stopwatch.GetTimestamp())
                + (long)(Stopwatch.Frequency * by.TotalSeconds);

        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public List<(string Endpoint, RaftResponderRequest Request)> EnqueuedRequests { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }
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

    private static (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) MakeFollower(
        params string[] peers)
    {
        FakePartitionHost host = new() { Nodes = peers.Select(p => new RaftNode(p)).ToList() };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, new FakeWalFacade(), sink, NullLogger<IRaft>.Instance);
        return (sm, host, sink);
    }

    private static (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) MakeLeader(
        params string[] peers)
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeFollower(peers);
        sm.SetLeaderForTesting(term: 1);
        // Align the frontiers the way a real restore does (lastApplied = commit index); the fake
        // WAL is empty, so both sit at -1 and a confirmation is not blocked on a phantom apply.
        sm.SetLocalCommittedIndexForTesting(-1);
        return (sm, host, sink);
    }

    /// <summary>Drives a full commit of log id 1 through the leader write path so the applied
    /// frontier genuinely advances (the same sequence the read-index tests use).</summary>
    private static async Task CommitEntryOne(RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink)
    {
        sm.ReplicateLogs([new RaftLog { Id = 1, Term = 1, LogType = "t" }], autoCommit: true, replyCorrelationId: 100);
        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            host.PartitionId, OperationId: 1, Term: -1, MinLogIndex: -1, MaxLogIndex: 1,
            WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success));

        (ulong _, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 100);
        await sm.CompleteAppendLogsAsync("node-b", proposeReply.TicketId, RaftOperationStatus.Success, committedIndex: 0, responseTerm: 1);
        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            host.PartitionId, OperationId: 2, Term: -1, MinLogIndex: -1, MaxLogIndex: 1,
            WALWriteOperationType.LeaderCommit, RaftOperationStatus.Success));
    }

    // ── immediate completion ──────────────────────────────────────────────────

    [Fact]
    public void CoveredIndex_CompletesImmediately_OnFollower()
    {
        (RaftPartitionStateMachine sm, _, CapturingReplySink sink) = MakeFollower("node-b");

        // A fresh state machine's applied frontier is -1; a required index of -1 ("nothing
        // committed before the call began") is already covered.
        sm.WaitLocalApplication(requiredIndex: -1, replyCorrelationId: 1);

        (ulong id, RaftResponse response) = Assert.Single(sink.Completed);
        Assert.Equal(1UL, id);
        Assert.Equal(RaftOperationStatus.Success, response.Status);
    }

    // ── parked until the apply path advances the frontier ─────────────────────

    [Fact]
    public async Task LaggingFrontier_Parks_ThenCompletesWhenEntryApplies()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b");

        // The wait is role-agnostic; using the leader harness lets the test drive a real apply.
        // Required index 1 is ahead of the applied frontier (-1): the caller must park.
        sm.WaitLocalApplication(requiredIndex: 1, replyCorrelationId: 7);
        Assert.DoesNotContain(sink.Completed, r => r.Id == 7);

        await CommitEntryOne(sm, host, sink);

        Assert.Contains(sink.Completed, r => r.Id == 7 && r.Response.Status == RaftOperationStatus.Success);
    }

    // ── the follower-side bound: expiry from the tick in a non-leader state ───

    [Fact]
    public async Task ParkedWaiter_OnFollower_ExpiresFromTickWithinTimeout()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeFollower("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        sm.WaitLocalApplication(requiredIndex: 5, replyCorrelationId: 2);
        Assert.Empty(sink.Completed);

        // Below the timeout: the tick must NOT fail the waiter yet.
        host.AdvanceMonotonic(host.Configuration.LeadershipConfirmationTimeout / 2);
        await sm.CheckPartitionLeadershipAsync();
        Assert.Empty(sink.Completed);

        // Past the timeout: the waiter fails from the FOLLOWER tick. Before this feature, expiry
        // only ran in the Leader branch — a follower waiter would have parked forever.
        host.AdvanceMonotonic(host.Configuration.LeadershipConfirmationTimeout);
        await sm.CheckPartitionLeadershipAsync();

        (ulong id, RaftResponse response) = Assert.Single(sink.Completed);
        Assert.Equal(2UL, id);
        Assert.NotEqual(RaftOperationStatus.Success, response.Status);
    }

    // ── leadership transitions fail parked waiters (fail closed) ──────────────

    [Fact]
    public async Task ParkedWaiter_FailsOnLeadershipTransition()
    {
        (RaftPartitionStateMachine sm, _, CapturingReplySink sink) = MakeLeader("node-b", "node-c");

        sm.WaitLocalApplication(requiredIndex: 3, replyCorrelationId: 4);
        Assert.Empty(sink.Completed);

        await sm.StepDownAsync(replyCorrelationId: null);

        Assert.Contains(sink.Completed, r => r.Id == 4 && r.Response.Status == RaftOperationStatus.NodeIsNotLeader);
    }

    // ── the leader's confirmation reply carries the captured read index ───────

    [Fact]
    public async Task LeaderConfirmation_ReplyCarriesCapturedReadIndex()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b");

        // Commit entry 1 so the confirmed frontier is a real, nonzero index.
        await CommitEntryOne(sm, host, sink);

        await sm.ConfirmLeadershipAsync(replyCorrelationId: 9);
        await sm.CompleteAppendLogsAsync(
            "node-b",
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            RaftOperationStatus.Success,
            committedIndex: 1,
            responseTerm: 1);

        // The reply's LogIndex is the round's captured commit frontier — the exact value
        // GetReadIndex forwards to a remote follower's applied-frontier wait.
        (ulong _, RaftResponse response) = Assert.Single(sink.Completed, r => r.Id == 9);
        Assert.Equal(RaftOperationStatus.Success, response.Status);
        Assert.Equal(1L, response.LogIndex);
    }
}
