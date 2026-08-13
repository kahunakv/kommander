
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
/// Regression tests for the leader-only applied-sequence hole found by the Jepsen
/// <c>log-append</c> workload: with pipelined proposals, quorum acks complete in network
/// order, so a later proposal's commit completion can arrive while an earlier proposal is
/// still in flight. The old <c>CompleteLeaderCommit</c> gap drain advanced the applied
/// cursor over the in-flight (current-term Proposed) entry without delivering it; when
/// that entry's own commit later completed, the exactly-once guard suppressed its apply —
/// a committed, client-acknowledged write permanently missing from the leader's consumer
/// projection while every follower applied it (Log Matching violation).
///
/// <para>The fix defers the out-of-order batch until the in-flight entry resolves (commit
/// or rollback) and then flushes deferred batches in log order. These tests drive WAL
/// completions out of order by hand and assert exactly-once, in-order delivery with no
/// holes.</para>
/// </summary>
public class TestLeaderOutOfOrderCommitApplies
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// IRaftPartitionHost that records every InvokeReplicationReceived call, in order.
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

        /// <summary>Ordered log of apply events produced by the consumer callbacks.</summary>
        public List<string> EventLog { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string ep, int p, HLCTimestamp t) { }
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
    /// WAL facade for pipelined-proposal tests. Unlike <c>CallbackWalFacade</c> in
    /// <see cref="TestLeaderAppliesBeforeLeaderChanged"/>, it assigns a unique operation id
    /// to every enqueue (so several proposals can be pending at once, exactly like the real
    /// WAL scheduler) and retains every entry it has seen — Proposed entries included — so
    /// <c>DrainInheritedAppliesAsync</c>'s <c>GetRangeAllTypesAsync</c> read sees the same
    /// in-flight entries a real backend would. Entry types are mutated in place on
    /// commit/rollback, mirroring <c>RaftWriteAhead.EnqueueCommit</c>/<c>EnqueueRollback</c>.
    /// The test drives completions by hand, in whatever order the scenario needs.
    /// </summary>
    private sealed class PipelinedWalFacade : IRaftWalFacade
    {
        private long _nextOperationId;
        private long _commitIndex;
        private readonly SortedDictionary<long, RaftLog> _entries = [];

        /// <summary>Operation id assigned to the most recent enqueue, for driving its completion.</summary>
        public long LastOperationId => _nextOperationId;

        public long GetCommitIndex() => _commitIndex;

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
            => ValueTask.FromResult(_entries.Values
                .Where(l => l.Id >= start && l.Type == RaftLogType.Committed)
                .Take(max)
                .ToList());

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
            => ValueTask.FromResult(_entries.Values
                .Where(l => l.Id >= start)
                .Take(max)
                .ToList());

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

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
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderCommit,
                (1, logs), logIndex: maxId);
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
            return new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderRollback,
                (1, logs), logIndex: maxId);
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

    /// <summary>
    /// Proposes a single-entry batch and drives its propose completion, returning the
    /// proposal ticket needed to commit or roll it back later.
    /// </summary>
    private static async Task<HLCTimestamp> ProposeEntryAsync(
        RaftPartitionStateMachine sm, PipelinedWalFacade wal, CapturingReplySink sink,
        long id, ulong correlationId)
    {
        List<RaftLog> logs = [new() { Id = id, Term = 1, LogType = "t" }];
        sm.ReplicateLogs(logs, autoCommit: false, replyCorrelationId: correlationId);

        long proposeOpId = wal.LastOperationId;
        await sm.CompleteWalOperationAsync(MakeCompletion(proposeOpId, WALWriteOperationType.LeaderPropose, minLogIndex: -1, maxLogIndex: id));

        (_, RaftResponse proposeReply) = Assert.Single(sink.Completed, r => r.Id == correlationId);
        return proposeReply.TicketId;
    }

    // ── tests ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// The direct Jepsen regression: entry 1's proposal is slow, entry 2's commits first.
    /// Entry 2's apply must be withheld while entry 1 is in flight, and once entry 1's
    /// commit completes BOTH entries must reach the consumer, in log order, exactly once.
    /// Before the fix, entry 2's commit advanced the applied cursor over the in-flight
    /// entry 1, and entry 1 was never delivered at all — the leader-only hole.
    /// </summary>
    [Fact]
    public async Task OutOfOrderCommitCompletions_ApplyInLogOrder_NoHole()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        PipelinedWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        // Two pipelined proposals: both propose-complete before either commits.
        HLCTimestamp ticket1 = await ProposeEntryAsync(sm, wal, sink, id: 1, correlationId: 1);
        HLCTimestamp ticket2 = await ProposeEntryAsync(sm, wal, sink, id: 2, correlationId: 2);

        // Entry 2's quorum completes first: its commit completion arrives while entry 1
        // is still Proposed in the WAL.
        await sm.CommitLogsAsync(ticket2, replyCorrelationId: 3);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 2, maxLogIndex: 2));

        // Entry 2 must be withheld: applying it now would advance the cursor over the
        // in-flight entry 1 and permanently suppress entry 1's own apply.
        Assert.DoesNotContain("Applied:2", host.EventLog);
        Assert.DoesNotContain("Applied:1", host.EventLog);

        // The slow proposal finally commits.
        await sm.CommitLogsAsync(ticket1, replyCorrelationId: 4);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 1, maxLogIndex: 1));

        // No hole: both entries delivered, exactly once each, in log order.
        Assert.Equal(1, host.EventLog.Count(e => e == "Applied:1"));
        Assert.Equal(1, host.EventLog.Count(e => e == "Applied:2"));
        Assert.True(host.EventLog.IndexOf("Applied:1") < host.EventLog.IndexOf("Applied:2"),
            $"Applies must be in log order; got: {string.Join(",", host.EventLog)}");
    }

    /// <summary>
    /// Three pipelined proposals whose commit completions arrive fully reversed (3, 2, 1).
    /// Batches 3 and 2 each defer behind the still-in-flight entry 1; entry 1's commit
    /// must flush the whole deferred chain in log order.
    /// </summary>
    [Fact]
    public async Task ReversedCommitCompletionOrder_FlushesDeferredChainInOrder()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        PipelinedWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        HLCTimestamp ticket1 = await ProposeEntryAsync(sm, wal, sink, id: 1, correlationId: 1);
        HLCTimestamp ticket2 = await ProposeEntryAsync(sm, wal, sink, id: 2, correlationId: 2);
        HLCTimestamp ticket3 = await ProposeEntryAsync(sm, wal, sink, id: 3, correlationId: 3);

        await sm.CommitLogsAsync(ticket3, replyCorrelationId: 4);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 3, maxLogIndex: 3));

        await sm.CommitLogsAsync(ticket2, replyCorrelationId: 5);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 2, maxLogIndex: 2));

        // Nothing may reach the consumer while entry 1 is still in flight.
        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));

        await sm.CommitLogsAsync(ticket1, replyCorrelationId: 6);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 1, maxLogIndex: 1));

        // The whole chain flushes: 1, 2, 3 — exactly once each, in order.
        Assert.Equal(["Applied:1", "Applied:2", "Applied:3"], host.EventLog);
    }

    /// <summary>
    /// A rollback must resolve the gap the same way a commit does: entry 2's commit defers
    /// behind in-flight entry 1; when entry 1 is rolled back, the rollback completion
    /// advances the applied cursor over the rolled-back id (no delivery — rolled-back
    /// entries never reach the consumer) and flushes the deferred batch.
    /// </summary>
    [Fact]
    public async Task RollbackOfBlockingProposal_FlushesDeferredBatch()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        PipelinedWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        HLCTimestamp ticket1 = await ProposeEntryAsync(sm, wal, sink, id: 1, correlationId: 1);
        HLCTimestamp ticket2 = await ProposeEntryAsync(sm, wal, sink, id: 2, correlationId: 2);

        await sm.CommitLogsAsync(ticket2, replyCorrelationId: 3);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 2, maxLogIndex: 2));

        Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));

        // Entry 1 fails and is rolled back instead of committing.
        await sm.RollbackLogsAsync(ticket1, replyCorrelationId: 4);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderRollback, minLogIndex: 1, maxLogIndex: 1));

        // The rolled-back entry is never delivered; the deferred entry 2 flushes exactly once.
        Assert.DoesNotContain("Applied:1", host.EventLog);
        Assert.Equal(1, host.EventLog.Count(e => e == "Applied:2"));
    }

    /// <summary>
    /// The steady-state fast path is unchanged: commits completing in log order apply
    /// immediately, in order, exactly once — no deferral involved.
    /// </summary>
    [Fact]
    public async Task InOrderCommitCompletions_ApplyImmediately()
    {
        OrderRecordingHost host = new() { NodesOverride = [] };
        PipelinedWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        host.EventLog.Clear();

        HLCTimestamp ticket1 = await ProposeEntryAsync(sm, wal, sink, id: 1, correlationId: 1);
        HLCTimestamp ticket2 = await ProposeEntryAsync(sm, wal, sink, id: 2, correlationId: 2);

        await sm.CommitLogsAsync(ticket1, replyCorrelationId: 3);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 1, maxLogIndex: 1));
        Assert.Equal(["Applied:1"], host.EventLog);

        await sm.CommitLogsAsync(ticket2, replyCorrelationId: 4);
        await sm.CompleteWalOperationAsync(MakeCompletion(wal.LastOperationId, WALWriteOperationType.LeaderCommit, minLogIndex: 2, maxLogIndex: 2));
        Assert.Equal(["Applied:1", "Applied:2"], host.EventLog);
    }
}
