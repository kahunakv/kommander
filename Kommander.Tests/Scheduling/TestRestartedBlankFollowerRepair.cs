using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// A member that crashes and restarts with an empty log under the same endpoint never leaves the
/// committed roster, so nothing resets the progress the leader recorded for its previous
/// incarnation. The leader must learn the regression from the peer's own rejections instead.
///
/// <para>Before this was handled, the stale presence frontier of the old incarnation raised every
/// anchored hole repair above what the blank peer held: the leader shipped a batch anchored at the
/// old frontier, the peer rejected it reporting a log through 0, the leader re-anchored at the old
/// frontier again, and so on several times per second with no warning. The peer converged only
/// when SWIM evicted the dead incarnation and re-admitted the live one, minutes later (a Kahuna
/// embedded-cluster restart test hit its three-minute deadline on that path).</para>
/// </summary>
public class TestRestartedBlankFollowerRepair
{
    private const string PeerB = "node-b";
    private const string PeerC = "node-c";

    private const long CommitFrontier = 10;   // committed prefix 1..10
    private const long InheritedTail = 12;    // uncommitted term-1 tail 11..12, so a repair above the prefix has entries to ship

    /// <summary>
    /// The peer acked the whole committed prefix with a presence report, then restarted empty and
    /// rejected the next append reporting a log through 0. The repair must start at entry 1,
    /// anchored at 0, not at the old incarnation's presence frontier.
    /// </summary>
    [Fact]
    public async Task FollowerRestartedEmpty_RepairIsAnchoredAtZero()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        await AckCommittedPrefix(sm, host, PeerB);
        await RejectAsEmptyLog(sm, host, PeerB);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest repair = SingleBatchTo(host, PeerB);
        Assert.Equal(0, repair.PrevLogIndex);
        Assert.Equal(1, repair.Logs!.Min(l => l.Id));
    }

    /// <summary>
    /// The lowered records must not stick once the peer has caught up: a Success ack at the
    /// committed prefix restores its progress, and the next heartbeat ships nothing anchored below it.
    /// </summary>
    [Fact]
    public async Task FollowerRestartedEmpty_ThenCaughtUp_IsNotRepairedAgain()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        await AckCommittedPrefix(sm, host, PeerB);
        await RejectAsEmptyLog(sm, host, PeerB);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(0, SingleBatchTo(host, PeerB).PrevLogIndex);

        await AckCommittedPrefix(sm, host, PeerB);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.Outbound,
            r => r.Node?.Endpoint == PeerB && r.AppendLogsRequest?.Logs is { Count: > 0 } && r.AppendLogsRequest.PrevLogIndex < CommitFrontier);
    }

    /// <summary>
    /// A rejection that reports the position the leader already recorded is an ordinary hole
    /// report, not a regression: the presence anchor stays, and the repair starts right above it.
    /// </summary>
    [Fact]
    public async Task RejectionAtTheRecordedPresence_KeepsThePresenceAnchor()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        await AckCommittedPrefix(sm, host, PeerB);

        await sm.CompleteAppendLogsAsync(PeerB, host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.LogMismatch, CommitFrontier,
            presentIndex: CommitFrontier, presentTerm: 1);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest repair = SingleBatchTo(host, PeerB);
        Assert.Equal(CommitFrontier, repair.PrevLogIndex);
        Assert.Equal(CommitFrontier + 1, repair.Logs!.Min(l => l.Id));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a leader whose log is committed through <see cref="CommitFrontier"/> and holds a
    /// contiguous uncommitted term-1 tail through <see cref="InheritedTail"/>. Its promotion arms a
    /// barrier at the next id; leadership stays unpublished while the barrier waits for quorum,
    /// which is enough for the heartbeat round to run its repairs.
    /// </summary>
    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildLeader()
    {
        TailWal wal = new();
        for (long id = 1; id <= InheritedTail; id++)
            wal.Entries.Add(new RaftLog
            {
                Id = id, Term = 1, LogType = "t",
                Type = id <= CommitFrontier ? RaftLogType.Committed : RaftLogType.Proposed,
            });

        CapturingHost host = new() { Nodes = [new RaftNode(PeerB), new RaftNode(PeerC)] };
        RaftPartitionStateMachine sm = new(host, wal, new NullSink(), NullLogger<IRaft>.Instance);
        sm.MarkRestoredForTesting();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync(PeerB, sm.CurrentTerm, remoteMaxLogId: CommitFrontier);
        await sm.ReceivedVoteAsync(PeerC, sm.CurrentTerm, remoteMaxLogId: CommitFrontier);

        Assert.Equal(InheritedTail + 1, wal.GetPresentIndex());   // the barrier no-op

        return (sm, host);
    }

    /// <summary>A Success ack for the whole committed prefix, carrying the presence report a healthy peer sends.</summary>
    private static Task AckCommittedPrefix(RaftPartitionStateMachine sm, CapturingHost host, string peer) =>
        sm.CompleteAppendLogsAsync(peer, host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.Success, CommitFrontier,
            presentIndex: CommitFrontier, presentTerm: 1).AsTask();

    /// <summary>
    /// The rejection a peer that restarted with an empty log sends to any anchored append: a log
    /// through 0, and no presence report (an empty log reports the proto defaults).
    /// </summary>
    private static Task RejectAsEmptyLog(RaftPartitionStateMachine sm, CapturingHost host, string peer) =>
        sm.CompleteAppendLogsAsync(peer, host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.LogMismatch, 0,
            presentIndex: 0, presentTerm: 0).AsTask();

    private static AppendLogsRequest SingleBatchTo(CapturingHost host, string endpoint) =>
        Assert.Single(host.Outbound,
            r => r.Node?.Endpoint == endpoint && r.AppendLogsRequest?.Logs is { Count: > 0 }).AppendLogsRequest!;

    // ── stubs ────────────────────────────────────────────────────────────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost", Port = 8001, InitialPartitions = 1,
            StartElectionTimeout = 50, EndElectionTimeout = 100,
            HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
            LeadershipBarrierTimeout = TimeSpan.FromSeconds(30),
        };

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];
        public List<RaftResponderRequest> Outbound { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) => Outbound.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>In-memory log, contiguous from 1, so the presence frontier is the max id.</summary>
    private sealed class TailWal : IRaftWalFacade
    {
        public List<RaftLog> Entries { get; } = [];

        private long nextId = 1;

        private long MaxId => Entries.Count == 0 ? 0 : Entries.Max(l => l.Id);

        public long GetCommitIndex() => CommitFrontier;
        public long GetPresentIndex() => MaxId;
        public long GetPresentTerm() => Entries.Count == 0 ? 0 : Entries.MaxBy(l => l.Id)!.Term;
        public void SeedProposeAllocator(long id) => nextId = id;

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(MaxId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) =>
            ValueTask.FromResult(Entries.FirstOrDefault(l => l.Id == logIndex)?.Term ?? -1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) =>
            ValueTask.FromResult(Entries
                .Where(l => l.Id >= start && l.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)
                .OrderBy(l => l.Id).Take(max).ToList());

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max) =>
            ValueTask.FromResult(Entries.Where(l => l.Id >= start).OrderBy(l => l.Id).Take(max).ToList());

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            foreach (RaftLog log in logs)
            {
                log.Id = nextId++;
                log.Term = term;
                log.Type = RaftLogType.Proposed;
                Entries.Add(log);
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, 1L, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(_ => { }, 2L, WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public void NotifyCommitted() { }
    }

    private sealed class NullSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
