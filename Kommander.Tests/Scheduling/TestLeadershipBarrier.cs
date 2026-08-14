
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
/// Covers the promotion barrier: a newly elected leader whose WAL holds inherited
/// prior-term entries above the known commit frontier must commit a new-term no-op —
/// which forces the inherited-entry drain — BEFORE publishing leadership. Closes the
/// inherited-entry serving hole: without the barrier, an idle promoted leader served
/// consumer state that was missing entries the previous leader had already acknowledged
/// to clients, and nothing ever triggered the inherited drain until the next write.
///
/// <list type="bullet">
///   <item>Inherited entries reach the consumer via <c>InvokeReplicationReceived</c>
///         before <c>host.Leader</c> is published (the <c>AmILeader</c> gate) — with no
///         write traffic after promotion.</item>
///   <item>The visible node state (<c>GetNodeState</c>, the <c>AmILeader</c> fallback)
///         does not leak <c>Leader</c> while the barrier is pending.</item>
///   <item>Barrier commit failure and barrier timeout revert the node to Follower —
///         promotion stays all-or-nothing.</item>
///   <item>The barrier no-op itself is never delivered to the consumer.</item>
/// </list>
/// </summary>
public class TestLeadershipBarrier
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// IRaftPartitionHost that records, in order, every consumer apply and every
    /// LeaderChanged(self) call, with a configurable barrier timeout.
    /// </summary>
    private sealed class BarrierRecordingHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
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
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [];

        /// <summary>Ordered log of event labels produced by the callbacks.</summary>
        public List<string> EventLog { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string ep, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string ep, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string ep, RaftResponderRequest req) { }

        public Task InvokeLeaderChanged(int p, string leader)
        {
            if (leader == LocalEndpoint)
                EventLog.Add($"LeaderChanged:{leader}");
            return Task.CompletedTask;
        }

        /// <summary>Per-entry consumer apply latency; the storm test uses it to model a slow
        /// state machine so the barrier window is wide, not instantaneous.</summary>
        public TimeSpan ConsumerDelay { get; set; } = TimeSpan.Zero;

        public async Task<bool> InvokeReplicationReceived(int p, RaftLog log)
        {
            if (ConsumerDelay > TimeSpan.Zero)
                await Task.Delay(ConsumerDelay);

            EventLog.Add($"Applied:{log.Id}");
            return true;
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"SystemApplied:{log.Id}");
            return Task.FromResult(true);
        }

        public void InvokeReplicationError(int p, RaftLog log)
        {
            EventLog.Add($"ApplyError:{log.Id}");
        }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode n, SnapshotRequest r, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// WAL facade simulating the inherited-tail promotion scenario: prior-term Proposed
    /// entries sit ABOVE the known commit frontier (their commit broadcast never arrived),
    /// so <c>GetMaxLogAsync</c> leads <c>GetCommitIndex</c> and <c>BecomeLeaderAsync</c>
    /// must arm the barrier. Unlike the stubs in TestLeaderAppliesBeforeLeaderChanged,
    /// <see cref="EnqueuePropose"/> mirrors the real facade by stamping ids and terms on
    /// the proposed entries, so the barrier no-op gets a real position in the log.
    /// </summary>
    private sealed class InheritedTailWalFacade : IRaftWalFacade
    {
        /// <summary>Inherited prior-term Proposed entries (the un-broadcast committed tail).</summary>
        public List<RaftLog> InheritedProposed { get; } = [];

        public List<RaftLog> Committed { get; } = [];

        private long _commitIndex;
        private long _nextId;

        public void Seed(params RaftLog[] inherited)
        {
            InheritedProposed.AddRange(inherited);
            _commitIndex = 0;
            _nextId = inherited.Length > 0 ? inherited.Max(l => l.Id) + 1 : 1;
        }

        /// <summary>
        /// Appends further inherited prior-term Proposed entries at the next log positions WITHOUT
        /// resetting the commit frontier — the mid-life shape the step-down storm exercises: a
        /// re-promoted node that already applied earlier rounds finds a fresh un-broadcast tail
        /// above its (advanced) frontier.
        /// </summary>
        public List<RaftLog> SeedMoreInherited(int count, long term)
        {
            List<RaftLog> added = [];
            for (int i = 0; i < count; i++)
            {
                RaftLog log = new() { Id = _nextId++, Term = term, Type = RaftLogType.Proposed, LogType = "t" };
                InheritedProposed.Add(log);
                added.Add(log);
            }

            return added;
        }

        public long GetCommitIndex() => _commitIndex;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_nextId - 1);

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
            => ValueTask.FromResult(Committed.Where(l => l.Id >= start).Take(max).ToList());

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
            => ValueTask.FromResult(Committed.Concat(InheritedProposed)
                .Where(l => l.Id >= start)
                .OrderBy(l => l.Id)
                .Take(max)
                .ToList());

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            // Mirror RaftWriteAhead.EnqueuePropose: assign contiguous ids and stamp the term.
            foreach (RaftLog log in logs)
            {
                log.Id = _nextId++;
                log.Term = term;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, 1L, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            // Mirror RaftWriteAhead.EnqueueCommit: flip Proposed → Committed and advance the frontier.
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            _commitIndex = maxId;
            Committed.AddRange(logs);

            return new(null!, 2L, WALWriteOperationType.LeaderCommit, (1, logs), logIndex: maxId);
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
            => new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));

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

    private static RaftWalCompletion ProposeCompletion(int partitionId, long logIndex, RaftOperationStatus status = RaftOperationStatus.Success) =>
        new(partitionId, OperationId: 1L, Term: -1L,
            MinLogIndex: -1L, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderPropose, status);

    private static RaftWalCompletion CommitCompletion(int partitionId, long minLogIndex, long maxLogIndex, RaftOperationStatus status = RaftOperationStatus.Success) =>
        new(partitionId, OperationId: 2L, Term: -1L,
            MinLogIndex: minLogIndex, MaxLogIndex: maxLogIndex,
            WALWriteOperationType.LeaderCommit, status);

    private static (BarrierRecordingHost host, InheritedTailWalFacade wal, RaftPartitionStateMachine sm) MakeInheritedTailFixture()
    {
        InheritedTailWalFacade wal = new();
        wal.Seed(
            new RaftLog { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new RaftLog { Id = 2, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new RaftLog { Id = 3, Term = 0, Type = RaftLogType.Proposed, LogType = "t" });

        BarrierRecordingHost host = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        return (host, wal, sm);
    }

    // ── acceptance: inherited entries reach the consumer before AmILeader opens ──

    /// <summary>
    /// The core acceptance test for the barrier: promotion over an inherited
    /// Proposed-but-quorum-committed tail must deliver every inherited entry to the
    /// consumer BEFORE <c>host.Leader</c> (the <c>AmILeader</c> gate) is published —
    /// with NO write traffic after promotion. The internal barrier no-op supplies the
    /// first-current-term commit that the old code waited (possibly forever) for a
    /// client write to provide.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_WithInheritedTail_AppliesInheritedEntriesBeforePublishingLeadership()
    {
        (BarrierRecordingHost host, InheritedTailWalFacade wal, RaftPartitionStateMachine sm) = MakeInheritedTailFixture();

        // Promote. The inherited tail (max log 3 > commit frontier 0) arms the barrier:
        // the promotion call returns with leadership NOT yet published.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));

        // The visible node state must not leak Leader while the barrier is pending —
        // GetNodeState backs the AmILeader fallback path.
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

        // Drive the barrier no-op's WAL propose completion. Single-voter: quorum is self,
        // so this auto-enqueues the commit; then drive the commit completion. This is the
        // consensus machinery advancing on its own — the test issues no client writes.
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));

        // Inherited entries 1..3 reached the consumer, in log order.
        int idx1 = host.EventLog.IndexOf("Applied:1");
        int idx2 = host.EventLog.IndexOf("Applied:2");
        int idx3 = host.EventLog.IndexOf("Applied:3");
        Assert.True(idx1 >= 0 && idx2 >= 0 && idx3 >= 0, $"inherited entries must be applied; events: {string.Join(",", host.EventLog)}");
        Assert.True(idx1 < idx2 && idx2 < idx3, "inherited applies must be in log order");

        // The barrier no-op (id 4) is consensus-internal: never delivered.
        Assert.DoesNotContain("Applied:4", host.EventLog);

        // Leadership published only AFTER the inherited applies.
        int leaderChangedIdx = host.EventLog.IndexOf("LeaderChanged:node-a");
        Assert.True(leaderChangedIdx >= 0, "LeaderChanged(self) must fire once the barrier commits");
        Assert.True(idx3 < leaderChangedIdx,
            $"Applied:3 (idx {idx3}) must precede LeaderChanged (idx {leaderChangedIdx})");

        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    /// <summary>
    /// Idle-window regression guard: while the barrier is pending nothing has published
    /// leadership, and no amount of waiting changes that — only the barrier commit does.
    /// (Before the fix, an idle promoted leader was published immediately and served
    /// stale state indefinitely; now it is unpublished until proven complete.)
    /// </summary>
    [Fact]
    public async Task PromotedLeader_WithInheritedTail_StaysUnpublishedUntilBarrierCommit()
    {
        (BarrierRecordingHost host, InheritedTailWalFacade _, RaftPartitionStateMachine sm) = MakeInheritedTailFixture();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        // Leader ticks (well within the barrier timeout) must neither publish nor revert.
        await sm.CheckPartitionLeadershipAsync();
        await sm.CheckPartitionLeadershipAsync();

        Assert.NotEqual("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));
    }

    /// <summary>
    /// The inherited entries must be durably COMMITTED, not merely applied. The drain treats them
    /// as committed (delivers them, and the leader serves reads from that state), but before this
    /// fix their WAL records stayed <c>Proposed</c> — so the backfill read (which filters
    /// uncommitted entries) could never re-ship them to followers missing the range: the leader
    /// shipped anchored batches that silently skipped the range, followers buffered them above
    /// their gap, and the partition wedged with every frontier frozen one index below the first
    /// missing entry (the Jepsen one-stuck-entry shape). After the barrier commits, the inherited
    /// range must be visible to <see cref="IRaftWalFacade.GetRangeAsync"/> as committed.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_DurablyRecommitsInheritedEntries()
    {
        (BarrierRecordingHost host, InheritedTailWalFacade wal, RaftPartitionStateMachine sm) = MakeInheritedTailFixture();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));
        Assert.Equal("node-a", host.Leader);

        // Every inherited entry's WAL record was upgraded to Committed...
        Assert.All(wal.InheritedProposed, l => Assert.Equal(RaftLogType.Committed, l.Type));

        // ...and the backfill read now returns the range, so a follower missing it can be repaired.
        List<RaftLog> backfillView = await ((IRaftWalFacade)wal).GetRangeAsync(1, 10);
        Assert.Equal([1L, 2L, 3L], backfillView.Where(l => l.Id <= 3).Select(l => l.Id).Order().ToList());
    }

    // ── no-tail fast path: zero added latency ─────────────────────────────────

    /// <summary>
    /// When the WAL tail does not extend past the commit frontier there is provably
    /// nothing inherited (election freshness guarantees the winner holds every
    /// quorum-durable entry), so promotion publishes immediately — no barrier round,
    /// no added failover latency for the common idle case.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_WithNoInheritedTail_PublishesImmediately()
    {
        InheritedTailWalFacade wal = new();   // empty: maxLog == commit frontier
        BarrierRecordingHost host = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Contains("LeaderChanged:node-a", host.EventLog);
    }

    // ── all-or-nothing: barrier failure/timeout reverts to Follower ───────────

    /// <summary>
    /// A failed barrier commit means the leader can never prove its consumer projection
    /// complete: it must revert to Follower with leadership never published, exactly like
    /// the promotion-drain failure path.
    /// </summary>
    [Fact]
    public async Task BarrierCommitFailure_RevertsToFollower_WithoutPublishing()
    {
        (BarrierRecordingHost host, InheritedTailWalFacade _, RaftPartitionStateMachine sm) = MakeInheritedTailFixture();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));

        // Fail the commit fsync.
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4, RaftOperationStatus.Errored));

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
    }

    /// <summary>
    /// A failed barrier propose (the no-op could not even persist locally) must likewise
    /// revert to Follower instead of holding unpublished leadership open forever.
    /// </summary>
    [Fact]
    public async Task BarrierProposeFailure_RevertsToFollower_WithoutPublishing()
    {
        (BarrierRecordingHost host, InheritedTailWalFacade _, RaftPartitionStateMachine sm) = MakeInheritedTailFixture();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4, RaftOperationStatus.Errored));

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
    }

    /// <summary>
    /// Liveness bound: if the barrier never commits (quorum lost right after election),
    /// the leader tick reverts the node to Follower after
    /// <see cref="RaftConfiguration.LeadershipBarrierTimeout"/> — otherwise it would
    /// heartbeat forever without serving, wedging the partition (followers suppressed,
    /// no leader published).
    /// </summary>
    [Fact]
    public async Task BarrierTimeout_RevertsToFollower_ViaLeaderTick()
    {
        (BarrierRecordingHost host, InheritedTailWalFacade _, RaftPartitionStateMachine sm) = MakeInheritedTailFixture();
        host.Config.LeadershipBarrierTimeout = TimeSpan.FromMilliseconds(1);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);   // barrier pending

        await Task.Delay(30, TestContext.Current.CancellationToken);
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);

        // A late commit completion for the dead barrier must not resurrect leadership.
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));
        Assert.NotEqual("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    // ── adversarial: repeated step-down/re-promotion with a slow consumer ──────

    /// <summary>
    /// The Kahuna lock-failover canary shape: the SAME node is promoted, serves, steps down, and is
    /// re-promoted over a fresh inherited tail, many times in a row, with a slow consumer widening
    /// the barrier window. Every round must uphold the barrier invariant — leadership is published
    /// only after that round's inherited entries reached the consumer, in log order — and no
    /// state carried over from the previous tenure (applied frontier, barrier bookkeeping,
    /// proposal maps) may let a later promotion publish early or re-deliver an entry. Leader ticks
    /// are interleaved mid-barrier the way the real executor serializes them. A promoted node
    /// answering from a projection that missed the previous tenure's tail is exactly the
    /// released-lock-resurfaces-as-held failure this guards against.
    /// </summary>
    [Fact]
    public async Task PromotionStepDownStorm_SlowConsumer_NeverPublishesBeforeInheritedApplies()
    {
        InheritedTailWalFacade wal = new();
        wal.Seed();   // empty log, ids start at 1
        BarrierRecordingHost host = new() { ConsumerDelay = TimeSpan.FromMilliseconds(2) };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        HashSet<long> everApplied = [];

        for (int round = 0; round < 12; round++)
        {
            List<RaftLog> inherited = wal.SeedMoreInherited(count: 3, term: 0);
            long barrierId = inherited[^1].Id + 1;
            int eventsBefore = host.EventLog.Count;

            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

            // Barrier armed: nothing published, node state does not leak Leader.
            Assert.NotEqual("node-a", host.Leader);
            Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

            // Leader ticks inside the barrier window must neither publish nor revert.
            await sm.CheckPartitionLeadershipAsync();

            await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: barrierId));
            await sm.CheckPartitionLeadershipAsync();
            await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: barrierId, maxLogIndex: barrierId));

            Assert.Equal("node-a", host.Leader);
            Assert.Equal(RaftNodeState.Leader, sm.NodeState);

            // Within this round's events: every inherited entry applied, in order, strictly
            // before the leadership publish; the barrier no-op never delivered.
            List<string> events = host.EventLog.Skip(eventsBefore).ToList();
            int leaderChangedIdx = events.IndexOf("LeaderChanged:node-a");
            Assert.True(leaderChangedIdx >= 0, $"round {round}: leadership never published; events: {string.Join(",", events)}");

            int previousApplyIdx = -1;
            foreach (RaftLog log in inherited)
            {
                int applyIdx = events.IndexOf($"Applied:{log.Id}");
                Assert.True(applyIdx >= 0, $"round {round}: entry {log.Id} never applied; events: {string.Join(",", events)}");
                Assert.True(applyIdx < leaderChangedIdx,
                    $"round {round}: entry {log.Id} applied at {applyIdx} AFTER publish at {leaderChangedIdx}");
                Assert.True(applyIdx > previousApplyIdx, $"round {round}: entry {log.Id} applied out of order");
                previousApplyIdx = applyIdx;

                // Cross-round exactly-once: no tenure may re-deliver a previous tenure's entries.
                Assert.True(everApplied.Add(log.Id), $"round {round}: entry {log.Id} delivered twice across tenures");
            }

            Assert.DoesNotContain($"Applied:{barrierId}", host.EventLog);

            await sm.StepDownAsync(replyCorrelationId: null);
            Assert.Equal(RaftNodeState.Follower, sm.NodeState);
            Assert.NotEqual("node-a", host.Leader);
        }

        // Nothing was ever delivered twice in the whole storm (any duplicate would have
        // produced two Applied:<id> events for some id).
        List<string> applies = host.EventLog.Where(e => e.StartsWith("Applied:")).ToList();
        Assert.Equal(applies.Count, applies.Distinct().Count());
        Assert.Equal(12 * 3, applies.Count);
    }
}
