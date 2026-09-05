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
/// Covers the published leadership lease (<c>TryConfirmLeadershipFast</c>): a quorum-confirmed
/// leadership window that off-thread callers may reuse for one heartbeat interval without an
/// executor round trip. The contract under test:
///
/// <list type="bullet">
///   <item>A lease exists only after a quorum confirmation, and a hit requires the published
///         leader, the Leader role, freshness, and an applied frontier that covers the commit
///         frontier — the same two halves the executor path enforces.</item>
///   <item>Every leadership-loss transition kills the lease before any new operation can read it:
///         explicit step-down, higher-term deposition via append ack, and a step-down notice at
///         an EQUAL term (the path that bypasses <c>FailAllActiveProposalWaiters</c>).</item>
///   <item>Expiry alone also kills a hit — a lease is never reused past its window.</item>
/// </list>
/// </summary>
public class TestLeadershipLease
{
    // ── stubs (same shape as TestReadIndexConfirmation) ───────────────────────

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

        /// <summary>Injectable monotonic clock; null uses the real <see cref="Stopwatch"/>.</summary>
        public long? MonotonicOverride { get; set; }

        public long GetMonotonicTimestamp() => MonotonicOverride ?? Stopwatch.GetTimestamp();

        public void AdvanceMonotonic(TimeSpan by) =>
            MonotonicOverride = (MonotonicOverride ?? Stopwatch.GetTimestamp())
                + (long)(Stopwatch.Frequency * by.TotalSeconds);

        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request) { }

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
        sm.SetLocalCommittedIndexForTesting(-1);
        return (sm, host, sink);
    }

    private static Task Ack(RaftPartitionStateMachine sm, FakePartitionHost host, string endpoint, long responseTerm = 1) =>
        sm.CompleteAppendLogsAsync(
            endpoint,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            RaftOperationStatus.Success,
            -1,
            responseTerm: responseTerm).AsTask();

    /// <summary>Drives a full quorum confirmation so a lease is published. The ack must carry the
    /// leader's CURRENT term — a stale-term ack is fenced off before it can feed the round.</summary>
    private static async Task ConfirmOnce(RaftPartitionStateMachine sm, FakePartitionHost host, ulong correlationId = 1, long term = 1)
    {
        await sm.ConfirmLeadershipAsync(replyCorrelationId: correlationId);
        await Ack(sm, host, "node-b", responseTerm: term);
    }

    // ── lease lifecycle ────────────────────────────────────────────────────────

    [Fact]
    public async Task NoLeaseBeforeFirstConfirmation()
    {
        (RaftPartitionStateMachine sm, _, _) = MakeLeader("node-b", "node-c");

        // A fresh leader has no confirmed quorum round yet: the fast path must miss and route
        // the caller to the executor, never claim confirmation from election alone.
        Assert.False(sm.TryConfirmLeadershipFast());
        await Task.CompletedTask;
    }

    [Fact]
    public async Task QuorumConfirmationPublishesLease_FastPathHits()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b", "node-c");

        await ConfirmOnce(sm, host);
        Assert.Contains(sink.Completed, r => r.Response.Status == RaftOperationStatus.Success);

        Assert.True(sm.TryConfirmLeadershipFast());
        // A hit is a pure read: it must stay repeatable inside the window.
        Assert.True(sm.TryConfirmLeadershipFast());
    }

    [Fact]
    public async Task LeaseExpiresAfterHeartbeatInterval()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        // Just under the window: still fresh.
        host.AdvanceMonotonic(host.Configuration.HeartbeatInterval / 2);
        Assert.True(sm.TryConfirmLeadershipFast());

        // At/past the window: the lease is stale and must miss, even though nothing invalidated it.
        host.AdvanceMonotonic(host.Configuration.HeartbeatInterval);
        Assert.False(sm.TryConfirmLeadershipFast());
    }

    // ── invalidation on leadership-loss transitions ────────────────────────────

    [Fact]
    public async Task ExplicitStepDownKillsLease()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        await sm.StepDownAsync(replyCorrelationId: null);

        Assert.False(sm.TryConfirmLeadershipFast());
    }

    [Fact]
    public async Task HigherTermAppendAckDeposesLeaderAndKillsLease()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        // A same-endpoint ack carrying a higher term is the LeaderInOldTerm repair channel:
        // the leader adopts the term and steps down; the lease must die with the term.
        await Ack(sm, host, "node-b", responseTerm: 2);

        Assert.NotEqual(RaftNodeState.Leader, sm.NodeState);
        Assert.False(sm.TryConfirmLeadershipFast());
    }

    [Fact]
    public async Task EqualTermStepDownNoticeKillsLeaseAndFailsParkedWaiters()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, CapturingReplySink sink) = MakeLeader("node-b", "node-c");

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        // Park a second confirmation waiter behind the frontier so it is alive at notice time:
        // committed frontier above the applied frontier keeps the reply parked.
        sm.SetLocalCommittedIndexForTesting(5);
        await sm.ConfirmLeadershipAsync(replyCorrelationId: 42);
        Assert.DoesNotContain(sink.Completed, r => r.Id == 42);

        // An EQUAL-term step-down notice: the term fence (Math.Max) is a no-op, so only the
        // explicit read-index invalidation on this path protects the lease and the waiters.
        // Endpoint must equal the published leader for the notice to be accepted.
        await sm.ReceiveStepDownNoticeAsync(new StepDownNoticeRequest(
            host.PartitionId,
            term: 1,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            endpoint: "node-a"));

        Assert.False(sm.TryConfirmLeadershipFast());

        // The parked waiter fails fast instead of dying by timeout.
        (ulong id, RaftResponse response) = Assert.Single(sink.Completed, r => r.Id == 42);
        Assert.Equal(42UL, id);
        Assert.NotEqual(RaftOperationStatus.Success, response.Status);
    }

    // ── the two synchronous gates ──────────────────────────────────────────────

    [Fact]
    public async Task UnpublishedLeaderNeverHits()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        // Promotion-barrier shape: role still Leader, but leadership is not published.
        host.Leader = "";
        Assert.False(sm.TryConfirmLeadershipFast());

        host.Leader = "node-a";
        Assert.True(sm.TryConfirmLeadershipFast());
    }

    [Fact]
    public async Task AppliedFrontierBehindCommitFrontierMisses()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        // Commit frontier ahead of the applied frontier: a hit here could serve a read that
        // misses an acknowledged write, so the fast path must miss and defer to the executor
        // path's applied-frontier wait.
        sm.SetLocalCommittedIndexForTesting(5);
        Assert.False(sm.TryConfirmLeadershipFast());

        // Frontier caught up again (fake WAL frontier: both at -1): hits resume.
        sm.SetLocalCommittedIndexForTesting(-1);
        Assert.True(sm.TryConfirmLeadershipFast());
    }

    // ── re-arm after a new stint ───────────────────────────────────────────────

    [Fact]
    public async Task NewLeadershipStintStartsWithoutLease_ReconfirmRepublishes()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        await sm.StepDownAsync(replyCorrelationId: null);
        Assert.False(sm.TryConfirmLeadershipFast());

        // Re-promotion must NOT resurrect the old lease; only a fresh quorum round may.
        sm.SetLeaderForTesting(term: 3);
        sm.SetLocalCommittedIndexForTesting(-1);
        Assert.False(sm.TryConfirmLeadershipFast());

        await ConfirmOnce(sm, host, correlationId: 7, term: 3);
        Assert.True(sm.TryConfirmLeadershipFast());
    }

    // ── allocation contract ────────────────────────────────────────────────────

    /// <summary>
    /// The lease exists to make a confirmed read a zero-allocation field read; this pins that
    /// contract so a future edit cannot quietly reintroduce a per-call allocation. Measured with
    /// the per-thread allocation counter over a synchronous loop (no awaits inside the measured
    /// region, so the thread cannot migrate). The companion benchmark
    /// (`Kommander.MicroBenchmarks/LeadershipLeaseBenchmarks.cs`) reports the same number.
    /// </summary>
    [Fact]
    public async Task FastPathHit_AllocatesNothing()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        Assert.True(sm.TryConfirmLeadershipFast());

        // Warm-up: let any lazy statics (cached delegates, tier-0 helpers) allocate outside the
        // measured region.
        for (int i = 0; i < 1_000; i++)
            sm.TryConfirmLeadershipFast();

        long before = GC.GetAllocatedBytesForCurrentThread();

        bool all = true;
        for (int i = 0; i < 10_000; i++)
            all &= sm.TryConfirmLeadershipFast();

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(all);
        Assert.Equal(0, allocated);
    }
}
