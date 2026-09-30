using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// The follower's side of the replicated live-replica retention floor: what it publishes to its own
/// WAL from an AppendLogs, and from whom.
///
/// <para>The leader holds its log for a replica that is behind and sends the floor with every
/// AppendLogs; a follower holds its own compaction at the same place, so a successor leader can
/// still serve that replica by backfill. The leader's side — what floor it computes and sends — is
/// pinned in <see cref="TestSnapshotRescueConvergence"/>, and the two together across a real leader
/// change in <c>TestStalledFollowerRetention</c>.</para>
/// </summary>
public class TestReplicatedRetentionFloor
{
    private const string Leader = "leader:9000";

    [Fact]
    public async Task AcceptedAppendLogs_PublishesTheLeadersFloorAndBudget_ToTheFollowersWal()
    {
        (RaftPartitionStateMachine sm, FollowerHost host, FloorRecordingWal wal) = await BuildFollowerAsync();

        await sm.AppendLogsAsync(Leader, term: 1, Timestamp(host), logs: null, retentionFloor: 200, retentionBudget: 1_000);

        Assert.Equal(1, wal.PublishCalls);
        Assert.Equal(200, wal.PublishedFloor);
        Assert.Equal(1_000, wal.PublishedBudget);

        // Every accepted AppendLogs refreshes it, so the WAL's staleness window measures the time
        // since the leader last beat and nothing else.
        await sm.AppendLogsAsync(Leader, term: 1, Timestamp(host), logs: null, retentionFloor: 350, retentionBudget: 2_000);

        Assert.Equal(2, wal.PublishCalls);
        Assert.Equal(350, wal.PublishedFloor);
        Assert.Equal(2_000, wal.PublishedBudget);
    }

    /// <summary>
    /// Zero is no statement: a leader that predates the field, or one that has not computed a floor
    /// in its term yet. The follower keeps what it has. Treating zero as "no constraint" would let a
    /// freshly elected leader's first message release the hold its predecessor had established.
    /// </summary>
    [Fact]
    public async Task FloorOfZero_LeavesThePreviousFloorInPlace()
    {
        (RaftPartitionStateMachine sm, FollowerHost host, FloorRecordingWal wal) = await BuildFollowerAsync();

        await sm.AppendLogsAsync(Leader, term: 1, Timestamp(host), logs: null);
        Assert.Equal(0, wal.PublishCalls);

        await sm.AppendLogsAsync(Leader, term: 1, Timestamp(host), logs: null, retentionFloor: 200, retentionBudget: 1_000);
        await sm.AppendLogsAsync(Leader, term: 1, Timestamp(host), logs: null);

        Assert.Equal(1, wal.PublishCalls);
        Assert.Equal(200, wal.PublishedFloor);
    }

    [Fact]
    public async Task NoConstraint_IsPublishedAsSuch()
    {
        (RaftPartitionStateMachine sm, FollowerHost host, FloorRecordingWal wal) = await BuildFollowerAsync();

        await sm.AppendLogsAsync(Leader, term: 1, Timestamp(host), logs: null, retentionFloor: 200, retentionBudget: 1_000);
        await sm.AppendLogsAsync(Leader, term: 1, Timestamp(host), logs: null, retentionFloor: long.MaxValue, retentionBudget: 1_000);

        Assert.Equal(long.MaxValue, wal.PublishedFloor);
    }

    /// <summary>
    /// Only the term's accepted leader sets the floor. A deposed leader's late message, or one from
    /// an endpoint that is not a member, is refused before it can move this node's retention.
    /// </summary>
    [Fact]
    public async Task AppendLogsThatIsNotFromTheTermsLeader_DoesNotTouchTheFloor()
    {
        (RaftPartitionStateMachine sm, FollowerHost host, FloorRecordingWal wal) = await BuildFollowerAsync();

        await sm.AppendLogsAsync(Leader, term: 5, Timestamp(host), logs: null, retentionFloor: 200, retentionBudget: 1_000);
        Assert.Equal(200, wal.PublishedFloor);

        // A leader of an earlier term.
        await sm.AppendLogsAsync("deposed:9001", term: 3, Timestamp(host), logs: null, retentionFloor: long.MaxValue, retentionBudget: 0);
        Assert.Equal(200, wal.PublishedFloor);

        // An endpoint outside the roster.
        host.NonMember = "stranger:9002";
        await sm.AppendLogsAsync("stranger:9002", term: 6, Timestamp(host), logs: null, retentionFloor: long.MaxValue, retentionBudget: 0);
        Assert.Equal(200, wal.PublishedFloor);

        Assert.Equal(1, wal.PublishCalls);
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private static HLCTimestamp Timestamp(FollowerHost host) => host.HybridLogicalClock.TrySendOrLocalEvent(2);

    private static async Task<(RaftPartitionStateMachine, FollowerHost, FloorRecordingWal)> BuildFollowerAsync()
    {
        FollowerHost host = new();
        FloorRecordingWal wal = new();

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);

        return (sm, host, wal);
    }

    private sealed class FollowerHost : IRaftPartitionHost
    {
        public string? NonMember;

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "follower:9001";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public bool IsMember(string endpoint) => endpoint != NonMember;
        public RaftConfiguration Configuration { get; } = new() { StartElectionTimeout = 5_000, EndElectionTimeout = 10_000 };
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new(TestReplicatedRetentionFloor.Leader)];

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>An empty WAL that records the live-replica retention floor published to it.</summary>
    private sealed class FloorRecordingWal : IRaftWalFacade
    {
        public long PublishedFloor = -1;
        public long PublishedBudget = -1;
        public int PublishCalls;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        public void SetLiveReplicaRetentionFloor(long floor) => SetLiveReplicaRetentionFloor(floor, 0);

        public void SetLiveReplicaRetentionFloor(long floor, long budget)
        {
            PublishedFloor = floor;
            PublishedBudget = budget;
            PublishCalls++;
        }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
