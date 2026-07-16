
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
/// Covers the two correctness gaps in the leader-promotion sequence described in
/// spec-apply-before-leader-changed.md:
///
/// <list type="bullet">
///   <item>Case A — committed-but-unapplied follower entries must be drained before
///         <c>InvokeLeaderChanged(self)</c> fires (T1).</item>
///   <item>Case B — entries committed by the leader itself (including inherited entries
///         from a prior term) must reach the leader's consumer via
///         <c>InvokeReplicationReceived</c> (T2).</item>
/// </list>
/// </summary>
public class TestLeaderAppliesBeforeLeaderChanged
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// IRaftPartitionHost that records, in order, every InvokeReplicationReceived
    /// and InvokeLeaderChanged(self) call. Used to assert event ordering.
    /// </summary>
    private sealed class OrderRecordingHost : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
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
        public RaftConfiguration Configuration => _config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => NodesOverride;
        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = [];

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
            // Only record the self-promotion event, not the "" transitional event.
            if (leader == LocalEndpoint)
                EventLog.Add($"LeaderChanged:{leader}");
            return Task.CompletedTask;
        }

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
    /// WAL facade backed by a pre-seeded list of committed entries. GetRangeAsync
    /// returns the committed entries in the seeded list that fall within the requested
    /// range; GetCommitIndex returns the highest seeded id.
    /// </summary>
    private sealed class SeededWalFacade : IRaftWalFacade
    {
        private readonly List<RaftLog> _committed;

        public SeededWalFacade(IEnumerable<RaftLog> committedEntries)
        {
            _committed = committedEntries
                .Select(l => new RaftLog { Id = l.Id, Type = RaftLogType.Committed, Term = l.Term, LogType = l.LogType, LogData = l.LogData })
                .OrderBy(l => l.Id)
                .ToList();
        }

        public long GetCommitIndex() => _committed.Count > 0 ? _committed[^1].Id : -1;

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
        {
            List<RaftLog> result = _committed
                .Where(l => l.Id >= start && l.Type == RaftLogType.Committed)
                .Take(max)
                .ToList();
            return ValueTask.FromResult(result);
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(GetCommitIndex());
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp t, bool autoCommit)
            => new(_ => { }, 1L, WALWriteOperationType.LeaderPropose, (1, logs));

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
            => new(_ => { }, 2L, WALWriteOperationType.LeaderCommit, (1, logs));

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
        {
            // SeededWalFacade only has committed entries; unfiltered == filtered.
            List<RaftLog> result = _committed
                .Where(l => l.Id >= start)
                .Take(max)
                .ToList();
            return ValueTask.FromResult(result);
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
            => new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1)
            => logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);

        public void NotifyCommitted() { }
    }

    /// <summary>
    /// Minimal no-op WAL for tests that drive commits manually without seeded entries.
    /// The commit facade used for T2 needs to call back with a completion, so we use
    /// an Action-based callback instead.
    ///
    /// <para>Set <see cref="InheritedProposed"/> before promotion to simulate Proposed
    /// entries from a prior term that were never committed (WalSingleFsyncCommit scenario).
    /// <see cref="GetRangeAllTypesAsync"/> returns these so the inherited-entry drain in
    /// <c>CompleteLeaderCommit</c> can deliver them to the consumer.</para>
    /// </summary>
    private sealed class CallbackWalFacade : IRaftWalFacade
    {
        public int CommitCallCount;
        public List<RaftLog> LastCommittedLogs { get; } = [];

        /// <summary>
        /// Inherited Proposed entries from a prior term. Returned by
        /// <see cref="GetRangeAllTypesAsync"/> so <c>DrainInheritedAppliesAsync</c> can
        /// find them; invisible to the committed-only <see cref="GetRangeAsync"/>.
        /// </summary>
        public List<RaftLog> InheritedProposed { get; } = [];

        public Action<RaftWalCompletion>? OnCommitEnqueued { get; set; }

        private long _commitIndex;

        public long GetCommitIndex() => _commitIndex;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
        {
            // Return inherited Proposed entries (if any) alongside committed entries.
            List<RaftLog> committed = LastCommittedLogs.Where(l => l.Id >= start).Take(max).ToList();
            List<RaftLog> proposed = InheritedProposed.Where(l => l.Id >= start).Take(max).ToList();

            // Merge, sort by id, take up to max.
            List<RaftLog> result = committed.Concat(proposed)
                .OrderBy(l => l.Id)
                .Take(max)
                .ToList();
            return ValueTask.FromResult(result);
        }

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            WALWriteOperation op = new(null!, 1L, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term, logIndex: maxId);
            return op;
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            Interlocked.Increment(ref CommitCallCount);

            // Mirror what RaftWriteAhead.EnqueueCommit does: mutate log types to Committed
            // so that ApplyLogToConsumerAsync's type guard fires correctly.
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
                else if (log.Type == RaftLogType.ProposedCheckpoint)
                    log.Type = RaftLogType.CommittedCheckpoint;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            long minId = logs.Count > 0 ? logs.Min(l => l.Id) : 0;
            _commitIndex = maxId;
            LastCommittedLogs.AddRange(logs);

            WALWriteOperation op = new(null!, 2L, WALWriteOperationType.LeaderCommit, (1, logs), logIndex: maxId);

            // Invoke the callback synchronously so tests can drive the completion
            // without needing a real WAL scheduler.
            OnCommitEnqueued?.Invoke(new RaftWalCompletion(
                PartitionId: 1,
                OperationId: 2L,
                Term: -1L,
                MinLogIndex: minId,
                MaxLogIndex: maxId,
                OperationType: WALWriteOperationType.LeaderCommit,
                Status: RaftOperationStatus.Success));

            return op;
        }

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

    private static RaftWalCompletion MakeProposeCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 1L, Term: -1L,
            MinLogIndex: -1L, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success);

    private static RaftWalCompletion MakeCommitCompletion(int partitionId, long minLogIndex, long maxLogIndex) =>
        new(partitionId, OperationId: 2L, Term: -1L,
            MinLogIndex: minLogIndex, MaxLogIndex: maxLogIndex,
            WALWriteOperationType.LeaderCommit, RaftOperationStatus.Success);

    // ── T1 tests ───────────────────────────────────────────────────────────────

    /// <summary>
    /// A single-node partition whose WAL already contains committed entries
    /// (simulating a follower that had appended and committed before winning election)
    /// must deliver those entries to the consumer before firing
    /// <c>InvokeLeaderChanged(self)</c>.
    /// </summary>
    [Fact]
    public async Task NewLeader_FiresLeaderChangedOnlyAfterCommittedAppliesDrain()
    {
        // Seed the WAL with three committed entries that have not been applied yet.
        List<RaftLog> seeded =
        [
            new() { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ];

        OrderRecordingHost host = new() { NodesOverride = [] };   // single-voter
        SeededWalFacade wal = new(seeded);
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // ForceLeaderForTestingAsync uses the single-node promotion path:
        // BecomeLeaderAsync() (drain) → InvokeLeaderChanged(self)
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        // All three entries must appear before LeaderChanged:node-a.
        int leaderChangedIdx = host.EventLog.IndexOf("LeaderChanged:node-a");
        Assert.True(leaderChangedIdx >= 0, "LeaderChanged(self) must have fired");

        for (int id = 1; id <= 3; id++)
        {
            int applyIdx = host.EventLog.IndexOf($"Applied:{id}");
            Assert.True(applyIdx >= 0, $"Applied:{id} must have fired");
            Assert.True(applyIdx < leaderChangedIdx,
                $"Applied:{id} (idx {applyIdx}) must precede LeaderChanged (idx {leaderChangedIdx})");
        }
    }

    /// <summary>
    /// After the drain-before-leader-changed promotion, host.Leader must be set
    /// to the local endpoint — AmILeader observes true only after applies are done.
    /// </summary>
    [Fact]
    public async Task NewLeader_HostLeaderIsSetAfterDrain()
    {
        List<RaftLog> seeded =
        [
            new() { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ];

        OrderRecordingHost host = new() { NodesOverride = [] };
        SeededWalFacade wal = new(seeded);
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // Before promotion: not leader.
        Assert.NotEqual("node-a", host.Leader);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        // After promotion: leader is set and applies were recorded.
        Assert.Equal("node-a", host.Leader);
        Assert.Contains("Applied:1", host.EventLog);
        Assert.Contains("Applied:2", host.EventLog);
    }

    /// <summary>
    /// B6 regression: after a restart replays the committed log, a subsequent promotion must NOT
    /// deliver the retained committed entries to the consumer a SECOND time. WAL restore already
    /// delivered the committed prefix (via <c>InvokeLogRestored</c>) and <c>CompleteRestoreAsync</c>
    /// now seeds <c>lastAppliedIndex</c> to the commit frontier. Before the fix that cursor stayed at
    /// -1, so <c>BecomeLeaderAsync</c> re-drained the whole log from index 0 on promotion —
    /// double-delivering every committed entry and holding the serial executor for the full replay
    /// before the first heartbeat. Contrast with <see cref="NewLeader_HostLeaderIsSetAfterDrain"/>,
    /// which uses the same seeded WAL WITHOUT a restore and therefore DOES apply the entries on
    /// promotion — the two tests bracket the fix.
    /// </summary>
    [Fact]
    public async Task RestoredNode_Promotion_DoesNotReapplyRetainedCommittedLog()
    {
        List<RaftLog> seeded =
        [
            new() { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new() { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ];

        OrderRecordingHost host = new() { NodesOverride = [] };   // single-voter
        SeededWalFacade wal = new(seeded);
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // Simulate restart replay: restore seeds the applied cursor to the commit frontier (3). The
        // real restore also delivers the prefix via a separate InvokeLogRestored path; this harness's
        // SeededWalFacade.CompleteRestoreAsync is a no-op, so no "Applied:" events fire here — what we
        // assert is that promotion does not now re-deliver them.
        await sm.CompleteRestoreAsync(seeded);
        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        // Promotion advertises leadership, but the retained committed entries are NOT re-delivered:
        // the restore-seeded cursor made the drain a no-op.
        Assert.Equal("node-a", host.Leader);
        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));
    }

    /// <summary>
    /// When the WAL has no committed entries (fresh cluster), the promotion drain is
    /// a no-op and the partition is still advertised as the serving leader.
    /// </summary>
    [Fact]
    public async Task NewLeader_WithNoCommittedEntries_BecomesLeaderWithoutApplies()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        SeededWalFacade wal = new([]);    // empty WAL
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        Assert.Equal("node-a", host.Leader);
        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));
        Assert.Contains("LeaderChanged:node-a", host.EventLog);
    }

    // ── T2 tests ───────────────────────────────────────────────────────────────

    /// <summary>
    /// A leader that commits entries via the leader path must deliver those entries
    /// to its own consumer state machine via InvokeReplicationReceived — not only to
    /// followers. This covers both normal leader-committed writes and inherited entries
    /// from a prior term that are committed after promotion.
    /// </summary>
    [Fact]
    public async Task LeaderCommit_DeliversEntriesToLeaderConsumer()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        CapturingReplySink sink = new();
        CallbackWalFacade wal = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // Become leader on a single-voter partition.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();   // discard drain events (empty WAL, no applies)

        // Propose a log entry.
        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);

        // Drive propose WAL completion.
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        HLCTimestamp ticketId = proposeReply.TicketId;

        // Manually drive commit (single-voter: Completed on propose quorum).
        await sm.CommitLogsAsync(ticketId, replyCorrelationId: 2);

        // Drive commit WAL completion.
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 1, maxLogIndex: 1));

        // Assert: the leader's consumer received an apply for log id 1.
        Assert.Contains("Applied:1", host.EventLog);
    }

    /// <summary>
    /// The leader must apply each committed entry exactly once: once via the leader
    /// commit path (CompleteLeaderCommit). Entries should not be double-applied when
    /// later promoted. Verifies the single-consumer-apply invariant for leader-committed
    /// entries.
    /// </summary>
    [Fact]
    public async Task LeaderCommit_AppliesEntryExactlyOnce()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        CapturingReplySink sink = new();
        CallbackWalFacade wal = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));
        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);

        await sm.CommitLogsAsync(proposeReply.TicketId, replyCorrelationId: 2);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 1, maxLogIndex: 1));

        // Exactly one apply for id 1 — no duplicates.
        int applyCount = host.EventLog.Count(e => e == "Applied:1");
        Assert.Equal(1, applyCount);
    }

    /// <summary>
    /// An autoCommit proposal commits in a single WAL round (propose + commit coalesced).
    /// The leader's consumer must still receive the apply via CompleteLeaderCommit.
    /// </summary>
    [Fact]
    public async Task LeaderAutoCommit_DeliversEntriesToLeaderConsumer()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        CapturingReplySink sink = new();
        CallbackWalFacade wal = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        // autoCommit: true — propose and commit coalesced.
        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: true, replyCorrelationId: 1);

        // Single-voter: propose quorum is immediate; drive propose WAL completion which
        // also triggers commit for autoCommit proposals.
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        // The propose completion drives CommitLogs automatically for autoCommit.
        // Drive the commit WAL completion next.
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 1, maxLogIndex: 1));

        Assert.Contains("Applied:1", host.EventLog);
    }

    // ── T2 inherited-entry tests ───────────────────────────────────────────────

    /// <summary>
    /// A node holds Proposed entries from a prior term at promotion time (the classic
    /// "uncommitted entries inherited from a crashed/replaced leader" scenario).
    /// When the new leader commits its first current-term entry, the inherited entries
    /// must be applied to the leader's consumer BEFORE the current-term entry, in log order.
    ///
    /// <para>This covers the WalSingleFsyncCommit path where lazy commit markers may have
    /// been lost and inherited entries remain as Proposed in the WAL.</para>
    /// </summary>
    [Fact]
    public async Task PromotedLeader_AppliesEntriesInheritedUncommittedFromPriorTerm()
    {
        // WAL has three Proposed entries from term 0 (inherited from a prior leader).
        // No committed entries → commit frontier = -1 / 0.
        CallbackWalFacade wal = new();
        wal.InheritedProposed.AddRange([
            new() { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new() { Id = 2, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new() { Id = 3, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
        ]);

        OrderRecordingHost host = new() { NodesOverride = [] };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // Promote: term 0 → 1. Drain committed entries (none): no-op.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        // Propose and commit entry [4] in current term (1).
        List<RaftLog> logs = [new() { Id = 4, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 4));

        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        await sm.CommitLogsAsync(proposeReply.TicketId, replyCorrelationId: 2);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));

        // All four entries must have been delivered to the consumer.
        Assert.Contains("Applied:1", host.EventLog);
        Assert.Contains("Applied:2", host.EventLog);
        Assert.Contains("Applied:3", host.EventLog);
        Assert.Contains("Applied:4", host.EventLog);

        // Inherited entries must precede the current-term entry.
        int idx1 = host.EventLog.IndexOf("Applied:1");
        int idx2 = host.EventLog.IndexOf("Applied:2");
        int idx3 = host.EventLog.IndexOf("Applied:3");
        int idx4 = host.EventLog.IndexOf("Applied:4");
        Assert.True(idx1 < idx2 && idx2 < idx3 && idx3 < idx4,
            $"Applies must be in log order: got indices {idx1},{idx2},{idx3},{idx4}");
    }

    /// <summary>
    /// Each inherited entry must be applied exactly once on the leader.  No double-apply
    /// even if DrainInheritedAppliesAsync and the normal CompleteLeaderCommit apply loop
    /// both run for the same logical gap.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_InheritedEntries_AppliedExactlyOnce()
    {
        CallbackWalFacade wal = new();
        wal.InheritedProposed.AddRange([
            new() { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new() { Id = 2, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
        ]);

        OrderRecordingHost host = new() { NodesOverride = [] };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        List<RaftLog> logs = [new() { Id = 3, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 3));

        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        await sm.CommitLogsAsync(proposeReply.TicketId, replyCorrelationId: 2);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 3, maxLogIndex: 3));

        // Each inherited entry must appear exactly once.
        Assert.Equal(1, host.EventLog.Count(e => e == "Applied:1"));
        Assert.Equal(1, host.EventLog.Count(e => e == "Applied:2"));
        Assert.Equal(1, host.EventLog.Count(e => e == "Applied:3"));
    }

    /// <summary>
    /// Current-term Proposed entries that have NOT yet been committed (still in-flight)
    /// must NOT be applied during the inherited-entry drain.  Only prior-term Proposed
    /// entries are eligible.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_DoesNotApply_CurrentTermInFlightProposals()
    {
        // Inherited entries from term 0, current leader term will be 1.
        CallbackWalFacade wal = new();
        wal.InheritedProposed.AddRange([
            new() { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            // Entry 2 will be proposed in current term (1) but NOT committed.
            new() { Id = 2, Term = 1, Type = RaftLogType.Proposed, LogType = "t" },
        ]);

        OrderRecordingHost host = new() { NodesOverride = [] };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        // Commit entry 3, creating a gap 1–2. Entry 2 is current-term (term=1), in-flight.
        List<RaftLog> logs = [new() { Id = 3, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 3));

        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        await sm.CommitLogsAsync(proposeReply.TicketId, replyCorrelationId: 2);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 3, maxLogIndex: 3));

        // Only entry 1 (term 0 < currentTerm 1) should have been applied from the gap.
        // Entry 2 (term 1 == currentTerm) must NOT be applied.
        Assert.Contains("Applied:1", host.EventLog);
        Assert.DoesNotContain("Applied:2", host.EventLog);
        Assert.Contains("Applied:3", host.EventLog);
    }

    // ── System-partition self-apply tests ──────────────────────────────────────
    //
    // CompleteLeaderCommit now delivers InvokeSystemReplicationReceived for the
    // leader's own committed system entries (new invariant). These tests confirm:
    //   (a) routing: P0 + LogType==_RaftSystem → InvokeSystemReplicationReceived
    //   (b) T1 drain on P0 fires SystemApplied before LeaderChanged
    //   (c) inherited P0 system entries are delivered on first current-term commit
    //   (d) non-system entries on P0 still route to InvokeReplicationReceived
    // The apply callbacks are monotonic-version-guarded in the system coordinator
    // so idempotent replay is safe; these tests provide explicit sign-off on the
    // contract change introduced by CompleteLeaderCommit's self-apply loop.

    /// <summary>
    /// A leader on the system partition (P0) committing a <c>_RaftSystem</c> log entry
    /// must deliver it via <c>InvokeSystemReplicationReceived</c>, not the consumer path.
    /// </summary>
    [Fact]
    public async Task SystemPartition_LeaderCommit_RoutesToSystemReplicationReceived()
    {
        // PartitionId = 0 (system partition)
        OrderRecordingHost host = new() { PartitionId = RaftSystemConfig.SystemPartition, NodesOverride = [] };
        CallbackWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        // Propose a system log entry (LogType == "_RaftSystem").
        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = RaftSystemConfig.RaftLogType }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));

        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        await sm.CommitLogsAsync(proposeReply.TicketId, replyCorrelationId: 2);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 1, maxLogIndex: 1));

        // Must route to system callback, not consumer callback.
        Assert.Contains("SystemApplied:1", host.EventLog);
        Assert.DoesNotContain("Applied:1", host.EventLog);
    }

    /// <summary>
    /// The T1 drain on the system partition must apply committed system entries via
    /// <c>InvokeSystemReplicationReceived</c> before <c>InvokeLeaderChanged(self)</c> fires.
    /// </summary>
    [Fact]
    public async Task SystemPartition_T1Drain_SystemEntriesFireBeforeLeaderChanged()
    {
        List<RaftLog> seeded =
        [
            new() { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = RaftSystemConfig.RaftLogType },
            new() { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = RaftSystemConfig.RaftLogType },
        ];

        OrderRecordingHost host = new() { PartitionId = RaftSystemConfig.SystemPartition, NodesOverride = [] };
        SeededWalFacade wal = new(seeded);
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        int leaderChangedIdx = host.EventLog.IndexOf("LeaderChanged:node-a");
        Assert.True(leaderChangedIdx >= 0, "LeaderChanged(self) must have fired");

        for (int id = 1; id <= 2; id++)
        {
            int applyIdx = host.EventLog.IndexOf($"SystemApplied:{id}");
            Assert.True(applyIdx >= 0, $"SystemApplied:{id} must have fired");
            Assert.True(applyIdx < leaderChangedIdx,
                $"SystemApplied:{id} (idx {applyIdx}) must precede LeaderChanged (idx {leaderChangedIdx})");
        }
    }

    /// <summary>
    /// A system-partition leader that inherits uncommitted <c>_RaftSystem</c> entries from
    /// a prior term must deliver them via <c>InvokeSystemReplicationReceived</c> when the
    /// first current-term entry commits — exactly once.
    /// </summary>
    [Fact]
    public async Task SystemPartition_InheritedEntries_RoutesToSystemReplicationReceived()
    {
        CallbackWalFacade wal = new();
        wal.InheritedProposed.AddRange([
            new() { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = RaftSystemConfig.RaftLogType },
            new() { Id = 2, Term = 0, Type = RaftLogType.Proposed, LogType = RaftSystemConfig.RaftLogType },
        ]);

        OrderRecordingHost host = new() { PartitionId = RaftSystemConfig.SystemPartition, NodesOverride = [] };
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        // Commit entry [3] in current term.
        List<RaftLog> logs = [new() { Id = 3, Term = 1, LogType = RaftSystemConfig.RaftLogType }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 3));
        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        await sm.CommitLogsAsync(proposeReply.TicketId, replyCorrelationId: 2);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 3, maxLogIndex: 3));

        // Inherited entries [1, 2] and new entry [3] must all be delivered via system callback.
        Assert.Contains("SystemApplied:1", host.EventLog);
        Assert.Contains("SystemApplied:2", host.EventLog);
        Assert.Contains("SystemApplied:3", host.EventLog);
        // No spillover to the consumer callback.
        Assert.DoesNotContain("Applied:1", host.EventLog);
        Assert.DoesNotContain("Applied:2", host.EventLog);
        Assert.DoesNotContain("Applied:3", host.EventLog);
        // Exactly once each.
        Assert.Equal(1, host.EventLog.Count(e => e == "SystemApplied:1"));
        Assert.Equal(1, host.EventLog.Count(e => e == "SystemApplied:2"));
        Assert.Equal(1, host.EventLog.Count(e => e == "SystemApplied:3"));
    }

    /// <summary>
    /// On P0, only entries whose <c>LogType == RaftSystemConfig.RaftLogType</c> route to
    /// <c>InvokeSystemReplicationReceived</c>; other log types on P0 still route to
    /// the consumer <c>InvokeReplicationReceived</c>.
    /// </summary>
    [Fact]
    public async Task SystemPartition_NonSystemLogType_RoutesToConsumerNotSystem()
    {
        OrderRecordingHost host = new() { PartitionId = RaftSystemConfig.SystemPartition, NodesOverride = [] };
        CallbackWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        // Use a non-system log type on the system partition.
        List<RaftLog> logs = [new() { Id = 1, Term = 1, LogType = "consumer-data" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: 1);
        await sm.CompleteWalOperationAsync(MakeProposeCompletion(host.PartitionId, logIndex: 1));
        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == 1);
        await sm.CommitLogsAsync(proposeReply.TicketId, replyCorrelationId: 2);
        await sm.CompleteWalOperationAsync(MakeCommitCompletion(host.PartitionId, minLogIndex: 1, maxLogIndex: 1));

        // Must route to consumer, not system.
        Assert.Contains("Applied:1", host.EventLog);
        Assert.DoesNotContain("SystemApplied:1", host.EventLog);
    }

    // ── Atomicity: a failed promotion drain must revert to Follower ───────────────

    /// <summary>
    /// WAL facade whose promotion drain fails: <see cref="GetCommitIndex"/> reports a
    /// non-empty committed range so <c>BecomeLeaderAsync</c> runs the drain, but
    /// <see cref="GetRangeAsync"/> throws a WAL-level error (simulating ReadScheduler
    /// backpressure / shutdown). Consumer callbacks are never reached.
    /// </summary>
    private sealed class ThrowingDrainWalFacade : IRaftWalFacade
    {
        public long GetCommitIndex() => 3;

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) =>
            throw new InvalidOperationException("simulated WAL read failure during promotion drain");

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(3L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp t, bool autoCommit)
            => new(_ => { }, 1L, WALWriteOperationType.LeaderPropose, (1, logs));
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
            => new(_ => { }, 2L, WALWriteOperationType.LeaderCommit, (1, logs));
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
            => new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1)
            => logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);
        public void NotifyCommitted() { }
    }

    /// <summary>
    /// If the promotion drain throws (WAL read backpressure / shutdown), the node must
    /// revert to <see cref="RaftNodeState.Follower"/> and never advertise itself as leader —
    /// <c>host.Leader</c> stays unset, so the <c>AmILeader</c> gate remains closed. The
    /// exception propagates so the cluster re-elects in the next term. Guards against a
    /// half-promoted state where <c>nodeState == Leader</c> but the drain never completed.
    /// </summary>
    [Fact]
    public async Task PromotionDrainFailure_RevertsToFollower_AndDoesNotAdvertiseLeader()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        ThrowingDrainWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => sm.ForceLeaderForTestingAsync(replyCorrelationId: null));

        // Reverted — not a half-promoted leader.
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        // host.Leader never set to self, so AmILeader stays closed.
        Assert.NotEqual("node-a", host.Leader);
        // No leader-changed(self) fired and no committed applies leaked to the consumer.
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));
    }
}
