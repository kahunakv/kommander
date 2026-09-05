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
/// A confirmed snapshot install must advance the leader's full replication progress for the
/// seeded follower — not only its recorded commit frontier.
///
/// <para>
/// The regression this pins (Caraxes <c>bank-optimistic-2h-f</c>, Kommander 1.2.11): on the
/// legacy two-fsync path every heartbeat ack reports <c>committedIndex = -1</c> ("no report"),
/// so no ack ever advances <c>matchIndex</c>/<c>nextIndex</c>. The install completion advanced
/// only <c>lastCommitIndexes</c>, while <c>TrySendBackfillBatchAsync</c> anchors at
/// <c>nextIndex</c> whenever it sits at or below the leader's committed index. A
/// <c>nextIndex</c> pinned below the WAL compaction floor therefore re-anchored every backfill
/// at an index the WAL could no longer serve: the batch was refused, the refusal re-escalated
/// to another snapshot, the install succeeded, and nothing the leader consults ever moved —
/// 76 installs in 13 minutes against one follower, with the tracked anchor frozen while the
/// follower fell monotonically further behind.
/// </para>
/// </summary>
public class TestSnapshotInstallProgress
{
    private const string VoterA = "follower-a:9001";
    private const string VoterB = "follower-b:9002";

    private const long LeaderCommit = 500;
    private const long Floor = 300; // WAL compaction floor: entries 1..300 are gone

    /// <summary>
    /// The full loop shape. A follower whose send cursor sits below the compaction floor gets its
    /// backfill refused (nothing ships). After the leader confirms a snapshot install at the
    /// floor, the next heartbeat must ship a contiguous batch anchored at the boundary — even
    /// though the follower's acks (legacy two-fsync path) never report a frontier. On 1.2.11 the
    /// second round refused again at the same frozen anchor, forever.
    /// </summary>
    [Fact]
    public async Task ConfirmedInstall_AdvancesTheBackfillAnchorPastTheFloor()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        // The follower last legitimately reported frontier 199 — below the floor. This also sets
        // matchIndex = 199 and nextIndex = 200, the cursor the backfill path anchors at.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 199);

        // Round 1: the anchored read starts at 200 but the WAL's first entry is 301 — the batch
        // is refused and nothing ships. This is the state that escalates to a snapshot.
        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(0, BatchesTo(host, VoterA));

        // The background snapshot task confirmed an install at the floor.
        sm.CompleteSnapshotInstalled(VoterA, Floor);

        // Legacy heartbeat ack: committedIndex = -1 carries no frontier and must disturb nothing.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, -1);
        Assert.Equal(Floor, sm.GetFollowerCommittedIndex(VoterA));

        // Round 2: the anchor must now sit at the installed boundary, so a contiguous batch
        // starting at Floor + 1 ships and no refusal episode re-opens.
        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(FirstShippedId(host, VoterA) == Floor + 1,
            "a confirmed snapshot install must advance the leader's send cursor to the installed " +
            "boundary; a nextIndex left below the compaction floor re-anchors every backfill at an " +
            "unservable index and the snapshot rescue loops forever (frozen-anchor finding)");
        Assert.Empty(sm.GetBackfillStatuses());
    }

    /// <summary>
    /// A stale install confirmation must never drag progress backwards: if acks already advanced
    /// the peer past the snapshot index, the completion is a no-op and the next batch stays
    /// anchored at the higher cursor.
    /// </summary>
    [Fact]
    public async Task StaleInstallConfirmation_NeverRegressesProgress()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        // The peer already reported real progress above the floor.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 400);

        // A stale completion (an install that raced newer acks) confirms a lower boundary.
        sm.CompleteSnapshotInstalled(VoterA, Floor);

        Assert.Equal(400, sm.GetFollowerCommittedIndex(VoterA));

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(FirstShippedId(host, VoterA) == 401,
            "a stale install confirmation must not lower matchIndex/nextIndex; the batch must " +
            "stay anchored at the newer acked position");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildLeader()
    {
        CapturingHost host = new();
        FloorWal wal = new(LeaderCommit, Floor);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        long term = sm.CurrentTerm;
        await sm.ReceivedVoteAsync(VoterA, term, LeaderCommit);
        await sm.ReceivedVoteAsync(VoterB, term, LeaderCommit);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        return (sm, host);
    }

    private static ValueTask Ack(RaftPartitionStateMachine sm, CapturingHost host, string endpoint,
                                 RaftOperationStatus status, long committedIndex) =>
        sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                   status, committedIndex);

    private static int BatchesTo(CapturingHost host, string endpoint) =>
        host.Requests.Count(r => r.Node?.Endpoint == endpoint
                                 && r.AppendLogsRequest?.Logs is { Count: > 0 });

    /// <summary>First log id of the first entry-carrying batch captured for the peer, or -1.</summary>
    private static long FirstShippedId(CapturingHost host, string endpoint) =>
        host.Requests
            .Where(r => r.Node?.Endpoint == endpoint && r.AppendLogsRequest?.Logs is { Count: > 0 })
            .Select(r => r.AppendLogsRequest!.Logs![0].Id)
            .DefaultIfEmpty(-1)
            .First();

    // ── stubs (same shape as TestAckFrontierSemantics) ───────────────────────

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
            HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
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

    /// <summary>
    /// WAL stub with a compaction floor: entries at or below <c>floor</c> are gone, so an anchored
    /// read that starts at or below it comes back beginning at <c>floor + 1</c> — the
    /// non-contiguous shape the backfill path refuses.
    /// </summary>
    private sealed class FloorWal : IRaftWalFacade
    {
        private readonly long commitIndex;
        private readonly long floor;

        public FloorWal(long commitIndex, long floor)
        {
            this.commitIndex = commitIndex;
            this.floor = floor;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            List<RaftLog> batch = [];
            long first = Math.Max(startLogIndex, floor + 1);
            for (long id = first; id < first + 3 && id <= commitIndex; id++)
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "test" });

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(floor);
        public long GetCommitIndex() => commitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
