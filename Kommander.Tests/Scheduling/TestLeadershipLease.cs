using System.Diagnostics;
using Kommander;
using Kommander.Consensus;
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
///   <item>The off-thread applied-frontier wait (<c>WaitForAppliedFrontierAsync</c>) parks a reader
///         on its own index: it is woken once, by the advance that covers it, never by advances
///         below it; a lapse, a step-down or the confirmation deadline turns the wait into a miss;
///         a step-down releases parked readers at once; a reader that gives up leaves nothing in
///         the registry.</item>
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

    // ── lease refresh from the ack stream ─────────────────────────────────────

    [Fact]
    public async Task SameTermAppendAcksPublishTheLease_WithoutAReadIndexRound()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        Assert.False(sm.TryConfirmLeadershipFast());

        // One peer ack plus the leader is a majority of three voters: the ordinary replication
        // stream proves the quorum still follows this leader, so the lease is published with no
        // read-index round at all.
        await Ack(sm, host, "node-b");
        Assert.True(sm.TryConfirmLeadershipFast());

        // The lease still lapses after one heartbeat interval without acks...
        host.AdvanceMonotonic(host.Configuration.HeartbeatInterval);
        Assert.False(sm.TryConfirmLeadershipFast());

        // ...and the next same-term ack renews it, so a leader under a steady write stream never lapses.
        await Ack(sm, host, "node-c");
        Assert.True(sm.TryConfirmLeadershipFast());
    }

    [Fact]
    public async Task AckRefreshAnchorsAtTheMajorityThNewestAck()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c", "node-d", "node-e");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        // Five voters: the leader plus two peers make the majority. One peer alone proves nothing.
        await Ack(sm, host, "node-b");
        Assert.False(sm.TryConfirmLeadershipFast());

        // The second peer's ack completes the majority; the lease is anchored at the OLDER of the two
        // acks (node-b's), so a lease that is fresh now lapses one heartbeat interval after that ack.
        host.AdvanceMonotonic(host.Configuration.HeartbeatInterval / 2);
        await Ack(sm, host, "node-c");
        Assert.True(sm.TryConfirmLeadershipFast());

        host.AdvanceMonotonic(host.Configuration.HeartbeatInterval / 2);
        Assert.False(sm.TryConfirmLeadershipFast());
    }

    // ── off-thread applied-frontier wait under a fresh lease ───────────────────

    [Fact]
    public async Task FreshLeaseWithLaggingApply_WaitsForTheFrontierThenHits()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(5);

        // Committed ahead of applied: the synchronous fast path misses, but the lease itself is good
        // and names the commit frontier the reader has to see applied.
        Assert.False(sm.TryConfirmLeadershipFast());
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));
        Assert.Equal(1, term);
        Assert.Equal(5, readIndex);

        Task<bool> wait = sm.WaitForAppliedFrontierAsync(term, readIndex, CancellationToken.None).AsTask();
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(wait.IsCompleted);

        // Applying up to (but not through) the frontier wakes the reader and parks it again.
        sm.SetLastAppliedIndexForTesting(3);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(wait.IsCompleted);

        sm.SetLastAppliedIndexForTesting(5);
        Assert.True(await wait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        // The partial advance to 3 did not touch the reader; only the covering advance signalled it.
        Assert.Equal(1, sm.AppliedFrontierWakeupsForTesting);
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
    }

    [Fact]
    public async Task ParkedReaderIsNotWokenByAdvancesBelowItsIndex()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(10);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));

        Task<bool> wait = sm.WaitForAppliedFrontierAsync(term, readIndex, CancellationToken.None).AsTask();
        Assert.Equal(1, sm.ParkedAppliedFrontierWaitersForTesting);

        // A leader applies a batch one entry at a time. The reader parked on the batch's end must
        // see none of the nine advances below it: no continuation, no timer re-arm.
        for (long applied = 1; applied < 10; applied++)
            sm.SetLastAppliedIndexForTesting(applied);

        Assert.Equal(0, sm.AppliedFrontierWakeupsForTesting);
        Assert.False(wait.IsCompleted);

        sm.SetLastAppliedIndexForTesting(10);
        Assert.True(await wait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal(1, sm.AppliedFrontierWakeupsForTesting);
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
    }

    [Fact]
    public async Task ReadersWithDifferentTargets_CompleteOnTheirOwnAdvance()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);

        // Reader A read the commit frontier at 3; reader B read it later, at 5.
        sm.SetLocalCommittedIndexForTesting(3);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndexA));
        Task<bool> waitA = sm.WaitForAppliedFrontierAsync(term, readIndexA, CancellationToken.None).AsTask();

        sm.SetLocalCommittedIndexForTesting(5);
        Assert.True(sm.TryReadFreshLease(out _, out long readIndexB));
        Task<bool> waitB = sm.WaitForAppliedFrontierAsync(term, readIndexB, CancellationToken.None).AsTask();
        Assert.Equal(2, sm.ParkedAppliedFrontierWaitersForTesting);

        // The intermediate advance covers A exactly, and only A.
        sm.SetLastAppliedIndexForTesting(3);
        Assert.True(await waitA.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal(1, sm.AppliedFrontierWakeupsForTesting);
        Assert.False(waitB.IsCompleted);
        Assert.Equal(1, sm.ParkedAppliedFrontierWaitersForTesting);

        sm.SetLastAppliedIndexForTesting(4);
        Assert.Equal(1, sm.AppliedFrontierWakeupsForTesting);
        Assert.False(waitB.IsCompleted);

        sm.SetLastAppliedIndexForTesting(5);
        Assert.True(await waitB.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal(2, sm.AppliedFrontierWakeupsForTesting);
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
    }

    [Fact]
    public async Task ReadersOnTheSameIndex_ShareOneWaiterAndWakeTogether()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(5);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));

        Task<bool>[] waits = new Task<bool>[8];
        for (int i = 0; i < waits.Length; i++)
            waits[i] = sm.WaitForAppliedFrontierAsync(term, readIndex, CancellationToken.None).AsTask();

        // Eight readers, one distinct index: one registry entry, and one covering advance wakes all.
        Assert.Equal(1, sm.ParkedAppliedFrontierWaitersForTesting);

        sm.SetLastAppliedIndexForTesting(5);
        bool[] results = await Task.WhenAll(waits).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.All(results, Assert.True);
        Assert.Equal(waits.Length, sm.AppliedFrontierWakeupsForTesting);
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
    }

    [Fact]
    public async Task FrontierWaitIsAMissWhenTheLeaseLapsesMeanwhile()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(5);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));

        Task<bool> wait = sm.WaitForAppliedFrontierAsync(term, readIndex, CancellationToken.None).AsTask();

        // The lease lapses while the reader is parked. An advance below its index does not wake it;
        // the covering advance does, and the reader must then hand the decision back to the executor
        // path instead of serving on a stale lease.
        host.AdvanceMonotonic(host.Configuration.HeartbeatInterval);
        sm.SetLastAppliedIndexForTesting(2);
        Assert.False(wait.IsCompleted);
        sm.SetLastAppliedIndexForTesting(5);

        Assert.False(await wait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
    }

    [Fact]
    public async Task StepDownReleasesParkedReadersAtOnce()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        // A deadline far beyond the assertion window: only the release can end the wait in time.
        host.Configuration.LeadershipConfirmationTimeout = TimeSpan.FromSeconds(60);

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(5);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));

        Task<bool> wait = sm.WaitForAppliedFrontierAsync(term, readIndex, CancellationToken.None).AsTask();
        Assert.Equal(1, sm.ParkedAppliedFrontierWaitersForTesting);

        // Nothing applies after the demotion. The reader must not sit on a dead lease until its
        // deadline: the step-down releases it and it answers a miss now.
        await sm.StepDownAsync(replyCorrelationId: null);

        Assert.False(await wait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
        Assert.Equal(0, sm.AppliedFrontierWakeupsForTesting);
    }

    [Fact]
    public async Task CancelledFrontierWait_WithdrawsItsRegistration()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(5);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));

        using CancellationTokenSource cts = new();
        Task<bool> wait = sm.WaitForAppliedFrontierAsync(term, readIndex, cts.Token).AsTask();
        Assert.Equal(1, sm.ParkedAppliedFrontierWaitersForTesting);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
    }

    [Fact]
    public async Task FrontierWaitIsAMissAfterAStepDown_EvenWhenTheFrontierArrives()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(5);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));

        Task<bool> wait = sm.WaitForAppliedFrontierAsync(term, readIndex, CancellationToken.None).AsTask();

        // Demoted while parked: the frontier then arrives as a follower applying the new leader's
        // entries. Covering the index is not enough — the lease is gone, so this is a miss.
        await sm.StepDownAsync(replyCorrelationId: null);
        sm.SetLastAppliedIndexForTesting(5);

        Assert.False(await wait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task FrontierWaitGivesUpAtTheConfirmationTimeout()
    {
        (RaftPartitionStateMachine sm, FakePartitionHost host, _) = MakeLeader("node-b", "node-c");
        host.MonotonicOverride = Stopwatch.GetTimestamp();
        host.Configuration.LeadershipConfirmationTimeout = TimeSpan.FromMilliseconds(200);

        await ConfirmOnce(sm, host);
        sm.SetLocalCommittedIndexForTesting(5);
        Assert.True(sm.TryReadFreshLease(out long term, out long readIndex));

        // Nothing ever applies: the wait is bounded by the confirmation timeout and answers a miss,
        // so a wedged consumer cannot park lease readers forever.
        Assert.False(await sm.WaitForAppliedFrontierAsync(term, readIndex, CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        // The reader that gave up withdrew its registration.
        Assert.Equal(0, sm.ParkedAppliedFrontierWaitersForTesting);
    }

    // ── the applied-frontier waiter registry on its own ─────────────────────────

    [Fact]
    public void Registry_AlreadyCoveredIndex_NeedsNoWaiter()
    {
        RaftPartitionCoreState core = new();
        core.LastAppliedIndex = 5;

        Assert.Null(core.RegisterAppliedFrontierWaiter(5));
        Assert.Null(core.RegisterAppliedFrontierWaiter(3));
        Assert.Equal(0, core.ParkedAppliedFrontierWaiterCount);
    }

    [Fact]
    public void Registry_SameIndexSharesOneEntry_AbandonRemovesItOnlyWhenEmpty()
    {
        RaftPartitionCoreState core = new();

        AppliedFrontierWaiter? first = core.RegisterAppliedFrontierWaiter(7);
        AppliedFrontierWaiter? second = core.RegisterAppliedFrontierWaiter(7);
        Assert.NotNull(first);
        Assert.Same(first, second);
        Assert.Equal(1, core.ParkedAppliedFrontierWaiterCount);
        Assert.Equal(2, first.Readers);

        core.AbandonAppliedFrontierWaiter(first);
        Assert.Equal(1, core.ParkedAppliedFrontierWaiterCount);
        Assert.False(first.Covered.IsCompleted);

        core.AbandonAppliedFrontierWaiter(first);
        Assert.Equal(0, core.ParkedAppliedFrontierWaiterCount);
        Assert.False(first.Covered.IsCompleted);

        // A covering advance after the last reader left has nobody to wake.
        core.LastAppliedIndex = 7;
        Assert.False(first.Covered.IsCompleted);
        Assert.Equal(0, core.AppliedFrontierWakeups);
    }

    [Fact]
    public void Registry_PopsOnlyTheCoveredHeads_InIndexOrder()
    {
        RaftPartitionCoreState core = new();

        // Registered out of order; the registry keeps them sorted.
        AppliedFrontierWaiter? at9 = core.RegisterAppliedFrontierWaiter(9);
        AppliedFrontierWaiter? at2 = core.RegisterAppliedFrontierWaiter(2);
        AppliedFrontierWaiter? at5 = core.RegisterAppliedFrontierWaiter(5);
        Assert.NotNull(at9);
        Assert.NotNull(at2);
        Assert.NotNull(at5);
        Assert.Equal(3, core.ParkedAppliedFrontierWaiterCount);

        core.LastAppliedIndex = 1;
        Assert.False(at2.Covered.IsCompleted);
        Assert.Equal(3, core.ParkedAppliedFrontierWaiterCount);

        core.LastAppliedIndex = 6;
        Assert.True(at2.Covered.IsCompleted);
        Assert.True(at5.Covered.IsCompleted);
        Assert.False(at9.Covered.IsCompleted);
        Assert.Equal(1, core.ParkedAppliedFrontierWaiterCount);
        Assert.Equal(2, core.AppliedFrontierWakeups);

        // Abandoning a waiter that already completed is a harmless no-op.
        core.AbandonAppliedFrontierWaiter(at5);
        Assert.Equal(1, core.ParkedAppliedFrontierWaiterCount);

        core.LastAppliedIndex = 9;
        Assert.True(at9.Covered.IsCompleted);
        Assert.Equal(0, core.ParkedAppliedFrontierWaiterCount);
        Assert.Equal(3, core.AppliedFrontierWakeups);
    }

    [Fact]
    public void Registry_LeaseDeathReleasesEveryWaiter_WithoutCountingAWake()
    {
        RaftPartitionCoreState core = new();
        core.CurrentTerm = 1;
        core.NodeState = RaftNodeState.Leader;
        core.PublishLeadershipLease(term: 1, confirmedTicks: 0);

        AppliedFrontierWaiter? at4 = core.RegisterAppliedFrontierWaiter(4);
        AppliedFrontierWaiter? at8 = core.RegisterAppliedFrontierWaiter(8);
        Assert.NotNull(at4);
        Assert.NotNull(at8);

        // A role change is the structural lease kill; it must also empty the registry.
        core.NodeState = RaftNodeState.Follower;

        Assert.Null(core.LeadershipLease);
        Assert.True(at4.Covered.IsCompleted);
        Assert.True(at8.Covered.IsCompleted);
        Assert.Equal(0, core.ParkedAppliedFrontierWaiterCount);
        Assert.Equal(0, core.AppliedFrontierWakeups);

        // And a term change, with the registry populated again.
        core.NodeState = RaftNodeState.Leader;
        core.PublishLeadershipLease(term: 1, confirmedTicks: 0);
        AppliedFrontierWaiter? again = core.RegisterAppliedFrontierWaiter(12);
        Assert.NotNull(again);

        core.CurrentTerm = 2;
        Assert.True(again.Covered.IsCompleted);
        Assert.Equal(0, core.ParkedAppliedFrontierWaiterCount);
    }

    [Fact]
    public async Task Registry_ConcurrentRegisterAndAdvance_NeverLosesAWake()
    {
        // The store-buffering race the fences in RegisterAppliedFrontierWaiter and the frontier
        // setter exist for. A lost wake shows up as a waiter still parked while the frontier covers
        // it — exactly the state this loop asserts never happens, across many interleavings.
        RaftPartitionCoreState core = new();
        const int rounds = 20_000;

        using Barrier start = new(2);
        long parkedWhileCovered = 0;

        Task advancer = Task.Run(() =>
        {
            for (long i = 1; i <= rounds; i++)
            {
                start.SignalAndWait();
                core.LastAppliedIndex = i;
            }
        }, TestContext.Current.CancellationToken);

        Task registrar = Task.Run(() =>
        {
            for (long i = 1; i <= rounds; i++)
            {
                start.SignalAndWait();
                AppliedFrontierWaiter? waiter = core.RegisterAppliedFrontierWaiter(i);
                if (waiter is null)
                    continue;

                // Either the advance already covered the index (the waiter must be completed by
                // the time the setter returns) or it will shortly. Spin until the frontier covers
                // the index, then the waiter must be completed — never still parked.
                SpinWait spin = new();
                while (core.LastAppliedIndex < i)
                    spin.SpinOnce();

                spin = new();
                while (!waiter.Covered.IsCompleted && spin.Count < 10_000)
                    spin.SpinOnce();

                if (!waiter.Covered.IsCompleted)
                    Interlocked.Increment(ref parkedWhileCovered);
            }
        }, TestContext.Current.CancellationToken);

        await Task.WhenAll(advancer, registrar).WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        Assert.Equal(0, Interlocked.Read(ref parkedWhileCovered));
        Assert.Equal(0, core.ParkedAppliedFrontierWaiterCount);
    }
}
