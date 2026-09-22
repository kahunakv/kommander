using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// A hole inside a new leader's UNCOMMITTED inherited tail must be repaired from the follower's
/// contiguous presence frontier, not from its commit frontier.
///
/// <para>CamusDB Caraxes fault soak fs4 (Kommander 1.7.7): a slow disk left all three voters with
/// the same hole above 48,266,403 in an inherited tail that started at 48,266,276. The one
/// contiguous node kept winning and arming a promotion barrier. Both followers withheld their
/// barrier acks over the hole and reported LogMismatch anchored at 48,266,403. The heartbeat
/// clamped that anchor to their commit frontier, 48,266,275, which cannot pass the uncommitted
/// tail until the barrier commits. So every repair re-shipped the same 128 entries the followers
/// already held (627 times), the barrier timed out every cycle, and the partition had no leader
/// for 59 minutes.</para>
///
/// <para>The leader now anchors the repair at the follower's reported presence frontier when its
/// own entry there carries the term the follower reported. Otherwise the commit-frontier clamp
/// stands.</para>
/// </summary>
public class TestInheritedTailHoleRepair
{
    private const string PeerB = "node-b";
    private const string PeerC = "node-c";

    private const long CommitFrontier = 10;   // committed prefix 1..10
    private const long InheritedTail = 30;    // Proposed, term 1: 11..30
    private const long FollowerPresent = 20;  // follower holds 11..20, misses 21, holds 22..

    /// <summary>
    /// The fs4 shape: the follower's commit frontier is pinned at the committed prefix, and it
    /// reports presence through 20 with the term this leader holds at 20. The repair must start
    /// at 21, the first entry the follower is missing.
    /// </summary>
    [Fact]
    public async Task HoleInUncommittedInheritedTail_RepairIsAnchoredAtVerifiedPresence()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeaderWithInheritedTail();

        await ReportHole(sm, host, PeerB, presentIndex: FollowerPresent, presentTerm: 1);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest repair = SingleBatchTo(host, PeerB);
        Assert.Equal(FollowerPresent, repair.PrevLogIndex);
        Assert.Equal(FollowerPresent + 1, repair.Logs!.Min(l => l.Id));
    }

    /// <summary>
    /// A presence report whose term disagrees with this leader's log at that index is not a
    /// valid anchor. That is the AppendEntries consistency check, and the repair must fall back
    /// to the committed prefix, which cannot diverge.
    /// </summary>
    [Fact]
    public async Task PresenceTermMismatch_RepairFallsBackToCommitFrontier()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeaderWithInheritedTail();

        await ReportHole(sm, host, PeerB, presentIndex: FollowerPresent, presentTerm: 7);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(CommitFrontier, SingleBatchTo(host, PeerB).PrevLogIndex);
    }

    /// <summary>
    /// A peer that predates the presence report sends proto defaults (0, 0). The behavior must be
    /// the same as before the report existed.
    /// </summary>
    [Fact]
    public async Task PeerWithoutPresenceReport_RepairKeepsTheCommitFrontierClamp()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeaderWithInheritedTail();

        await ReportHole(sm, host, PeerB, presentIndex: 0, presentTerm: 0);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(CommitFrontier, SingleBatchTo(host, PeerB).PrevLogIndex);
    }

    /// <summary>
    /// A presence report at or past this leader's own tail leaves nothing to ship from there. An
    /// empty read would count as a compaction-floor refusal and escalate to a snapshot, so the
    /// clamp stands.
    /// </summary>
    [Fact]
    public async Task PresenceAtOrPastLeaderTail_IsNotUsedAsAnAnchor()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeaderWithInheritedTail();

        // The barrier no-op sits at InheritedTail + 1, so this is the leader's own tail.
        await ReportHole(sm, host, PeerB, presentIndex: InheritedTail + 1, presentTerm: 1);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(CommitFrontier, SingleBatchTo(host, PeerB).PrevLogIndex);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a leader whose log is committed through <see cref="CommitFrontier"/> and holds a
    /// contiguous uncommitted term-1 tail through <see cref="InheritedTail"/>. Its promotion arms
    /// a barrier, and leadership stays unpublished while the barrier waits for quorum, as in fs4.
    /// </summary>
    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildLeaderWithInheritedTail()
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

        // Promoted but unpublished: the barrier is pending, and an unpublished leader reads as
        // Candidate off-thread (RaftPartitionStateMachine.NodeState).
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Equal("", host.Leader);
        Assert.Equal(InheritedTail + 1, wal.GetPresentIndex());   // the barrier no-op

        return (sm, host);
    }

    /// <summary>
    /// The follower's side of fs4: a Success ack carrying its pinned commit frontier, then the
    /// over-gap LogMismatch anchored at its presence frontier, both carrying the presence report.
    /// </summary>
    private static async Task ReportHole(RaftPartitionStateMachine sm, CapturingHost host, string peer, long presentIndex, long presentTerm)
    {
        await sm.CompleteAppendLogsAsync(peer, host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.Success, CommitFrontier,
            presentIndex: presentIndex, presentTerm: presentTerm);

        await sm.CompleteAppendLogsAsync(peer, host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.LogMismatch, FollowerPresent,
            presentIndex: presentIndex, presentTerm: presentTerm);
    }

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
            // Long enough that the barrier does not time out and revert inside a test.
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

    /// <summary>
    /// In-memory log with presence tracking: contiguous from 1, so the presence frontier is the
    /// max id. Proposals append at the allocator, so the promotion barrier lands in the log and
    /// is shipped by backfill like any other entry.
    /// </summary>
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
