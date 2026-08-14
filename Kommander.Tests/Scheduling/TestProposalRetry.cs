using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// A Raft leader retries replication until it succeeds; Kommander's live-propose broadcast fired
/// exactly once, and the backfill path re-ships only committed entries — so a propose lost to a
/// fault window had no second chance. The proposal could then never reach quorum, nothing ever
/// resolved it (no rollback, no sweep), its WAL rows stayed durably <c>Proposed</c>, and every
/// drain correctly blocked at the first such entry: the partition wedged one index below it, on
/// every replica, forever (the Jepsen one-stuck-entry shape — including the <c>boundary=1</c>
/// case where a partition's very first proposal loses quorum and nothing ever commits at all).
///
/// <para>
/// The fix is the canonical-Raft one — retry, never abandon: the heartbeat tick re-sends every
/// unresolved proposal to the voters that have not acknowledged it, and a follower that already
/// holds a re-sent batch re-acks it (durability-gated) so a lost ACK is also repaired. Rollback
/// was rejected as unsafe: a peer may hold the entry durably, win a later election on it, and its
/// promotion barrier commits it — a leader that had meanwhile rolled it back would have forked
/// the committed history.
/// </para>
/// </summary>
public class TestProposalRetry
{
    private const string VoterA   = "follower-a:9001";
    private const string VoterB   = "follower-b:9002";
    private const string LeaderEp = "old-leader:9005";

    /// <summary>
    /// The reproduction and the heal in one arc: a proposal whose broadcast is lost is re-sent on
    /// the heartbeat tick with its ORIGINAL ticket timestamp (so late acks credit the original
    /// quorum), a single late ack then completes quorum, and the entry commits.
    /// </summary>
    [Fact]
    public async Task Leader_RetriesUnresolvedProposal_UntilQuorumCommitsIt()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ProposeWal wal) = BuildLeader();

        (RaftOperationStatus status, HLCTimestamp ticket) = sm.ReplicateLogs(
            [new RaftLog { LogType = "t", LogData = [1] }], autoCommit: true);
        Assert.Equal(RaftOperationStatus.Pending, status);

        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 1));

        // The original one-shot broadcast went to both voters…
        Assert.Equal(2, EntryBatches(host).Count);

        // …and was "lost": no acks arrive. The next heartbeat tick must re-send the proposal,
        // stamped with the original ticket so a late ack still credits it.
        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        List<RaftResponderRequest> resent = EntryBatches(host);
        Assert.Equal(2, resent.Count);
        Assert.All(resent, r => Assert.Equal(ticket, r.AppendLogsRequest!.Time));
        Assert.All(resent, r => Assert.Single(r.AppendLogsRequest!.Logs!));

        // One late ack completes the quorum (leader + VoterA of 3 voters) and auto-commits.
        await sm.CompleteAppendLogsAsync(VoterA, ticket, RaftOperationStatus.Success, committedIndex: 0);
        Assert.Contains(wal.Committed, l => l.Id == 1);

        // Resolved proposals are not re-sent. (Batches under OTHER timestamps are legitimate —
        // e.g. the idle-tail backfill re-shipping the committed entry to a lagging follower; the
        // proposal retry is identified by carrying the original ticket timestamp.)
        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.DoesNotContain(EntryBatches(host), r => r.AppendLogsRequest!.Time == ticket);
    }

    /// <summary>
    /// Control: a proposal that reached quorum on its original broadcast is never re-sent — the
    /// retry must not turn every write into duplicate traffic.
    /// </summary>
    [Fact]
    public async Task Leader_DoesNotRetryAQuorumCompletedProposal()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ProposeWal wal) = BuildLeader();

        (_, HLCTimestamp ticket) = sm.ReplicateLogs(
            [new RaftLog { LogType = "t", LogData = [1] }], autoCommit: true);
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 1));
        await sm.CompleteAppendLogsAsync(VoterA, ticket, RaftOperationStatus.Success, committedIndex: 0);
        Assert.Contains(wal.Committed, l => l.Id == 1);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.DoesNotContain(EntryBatches(host), r => r.AppendLogsRequest!.Time == ticket);
    }

    /// <summary>
    /// The lost-ACK leg: a follower that already durably holds a re-sent batch must re-acknowledge
    /// it (the WAL plans nothing for a duplicate, and before this fix the follower stayed silent —
    /// so a proposal whose acks were lost could never be credited, however often it was re-sent).
    /// </summary>
    [Fact]
    public async Task Follower_ReAcksADuplicateBatch_WhenAlreadyDurable()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, DuplicateWal _) = await BuildFollower(durableMax: 5);

        HLCTimestamp batchTime = host.HybridLogicalClock.TrySendOrLocalEvent(2);
        host.Requests.Clear();
        await sm.AppendLogsAsync(LeaderEp, 1, batchTime,
            [new RaftLog { Id = 5, Term = 1, Type = RaftLogType.Proposed, LogType = "t" }]);

        CompleteAppendLogsRequest? ack = host.Requests
            .Select(r => r.CompleteAppendLogsRequest)
            .FirstOrDefault(r => r is not null);
        Assert.NotNull(ack);
        Assert.Equal(RaftOperationStatus.Success, ack!.Status);
        Assert.Equal(batchTime, ack.Time);
    }

    /// <summary>
    /// The durability gate on the re-ack: while the original append is still queued (the batch's
    /// max id is not yet durable in the backend), the duplicate must stay silent — the original
    /// write's own completion carries the ack, and an early re-ack would claim a durability the
    /// disk does not have (the single-fsync ticket releases on propose-quorum-DURABLE).
    /// </summary>
    [Fact]
    public async Task Follower_StaysSilentOnADuplicateBatch_StillInTheWriteQueue()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, DuplicateWal _) = await BuildFollower(durableMax: 4);

        host.Requests.Clear();
        await sm.AppendLogsAsync(LeaderEp, 1, host.HybridLogicalClock.TrySendOrLocalEvent(2),
            [new RaftLog { Id = 5, Term = 1, Type = RaftLogType.Proposed, LogType = "t" }]);

        Assert.DoesNotContain(host.Requests, r => r.CompleteAppendLogsRequest is not null);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static (RaftPartitionStateMachine, CapturingHost, ProposeWal) BuildLeader()
    {
        CapturingHost host = new();
        ProposeWal wal = new();

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 1);

        return (sm, host, wal);
    }

    private static async Task<(RaftPartitionStateMachine, CapturingHost, DuplicateWal)> BuildFollower(long durableMax)
    {
        CapturingHost host = new();
        DuplicateWal wal = new(durableMax);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        return (sm, host, wal);
    }

    private static RaftWalCompletion ProposeCompletion(int partitionId, long logIndex) =>
        new(partitionId, OperationId: 1L, Term: -1L,
            MinLogIndex: -1L, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderPropose, RaftOperationStatus.Success);

    private static List<RaftResponderRequest> EntryBatches(CapturingHost host) =>
        host.Requests.Where(r => r.AppendLogsRequest?.Logs is { Count: > 0 }).ToList();

    // ── stubs ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Leader-side WAL: stamps proposed ids like the real facade and records commits, so the
    /// retry arc (propose → lose the broadcast → retry → late ack → commit) is observable.
    /// </summary>
    private sealed class ProposeWal : IRaftWalFacade
    {
        public List<RaftLog> Committed { get; } = [];
        private long _nextId = 1;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(_nextId - 1);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) =>
            ValueTask.FromResult(Committed.Where(l => l.Id >= start).Take(max).ToList());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit)
        {
            foreach (RaftLog log in logs)
            {
                log.Id = _nextId++;
                log.Term = term;
            }
            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, 1L, WALWriteOperationType.LeaderPropose, (1, logs), ts, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
            }
            Committed.AddRange(logs);
            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, 2L, WALWriteOperationType.LeaderCommit, (1, logs), logIndex: maxId);
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);
        public void NotifyCommitted() { }
    }

    /// <summary>
    /// Follower-side WAL that treats every incoming batch as a duplicate (plans nothing), with a
    /// configurable durable max so both sides of the re-ack durability gate are testable.
    /// </summary>
    private sealed class DuplicateWal : IRaftWalFacade
    {
        private readonly long durableMax;

        public DuplicateWal(long durableMax) => this.durableMax = durableMax;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(durableMax);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
        public long GetCommitIndex() => durableMax;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) =>
            new(_ => { }, 1L, WALWriteOperationType.LeaderPropose, (1, logs));
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(_ => { }, 2L, WALWriteOperationType.LeaderCommit, (1, logs));
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));

        /// <summary>Duplicate: everything already present — plan nothing.</summary>
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1) => null;

        public void NotifyCommitted() { }
    }

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1, BackfillThreshold = 10,
            HeartbeatInterval = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) => Requests.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(true));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
