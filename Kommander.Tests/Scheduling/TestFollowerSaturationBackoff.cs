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
/// A leader must stop sending entry-carrying batches to a follower that reported
/// <see cref="RaftOperationStatus.FollowerWalSaturated"/>, for
/// <see cref="RaftConfiguration.FollowerSaturationBackoff"/>.
///
/// <para>
/// Answering a saturated follower is not, by itself, backpressure. A follower whose WAL queue is
/// full rejects every batch it is sent, so if the leader re-sends immediately the queue never gets
/// an interval in which to drain and the follower never converges — a measured run had an idle,
/// fully healed cluster accelerating to ~7,800 rejected batches a minute while no replica advanced
/// a single log index. The status has to change what the leader *does*.
/// </para>
/// </summary>
public class TestFollowerSaturationBackoff
{
    private const string SaturatedPeer = "follower-a:9001";
    private const string HealthyPeer   = "follower-b:9002";

    /// <summary>
    /// The saturated peer stops receiving entries while the healthy one keeps receiving them —
    /// the backoff must be per-peer, or one slow follower would stall catch-up for the cluster.
    /// </summary>
    [Fact]
    public async Task SaturatedFollower_StopsReceivingBatches_WhileOthersContinue()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        // Both followers are far enough behind to be actively backfilled.
        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.Success, committedIndex: 5);
        await Ack(sm, host, HealthyPeer,   RaftOperationStatus.Success, committedIndex: 5);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(2, BatchCount(host));

        // The saturated peer now refuses.
        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.FollowerWalSaturated, committedIndex: 5);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, BatchCountTo(host, SaturatedPeer));
        Assert.True(BatchCountTo(host, HealthyPeer) > 0, "the healthy follower must keep catching up");
    }

    /// <summary>
    /// Heartbeats to the saturated peer must continue — they carry no entries, they are what holds
    /// leadership, and suppressing them would trade a lagging follower for an election.
    /// </summary>
    [Fact]
    public async Task SaturatedFollower_StillReceivesHeartbeats()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.FollowerWalSaturated, committedIndex: 5);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest[] toPeer = RequestsTo(host, SaturatedPeer);

        Assert.NotEmpty(toPeer);
        Assert.All(toPeer, r => Assert.True(r.Logs is null or { Count: 0 },
            "a paused peer may be heartbeated, but must not be sent entries"));
    }

    /// <summary>
    /// The pause is a delay, not a ban: once the window elapses the peer is backfilled again.
    /// A backoff that never expired would convert a transient queue overflow into a permanently
    /// abandoned replica — the same outcome it exists to prevent.
    /// </summary>
    [Fact]
    public async Task SaturatedFollower_ResumesAfterTheWindowElapses()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.Success, committedIndex: 5);
        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.FollowerWalSaturated, committedIndex: 5);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(0, BatchCountTo(host, SaturatedPeer));

        // Zero window: every subsequent round is past the deadline.
        host.Configuration.FollowerSaturationBackoff = TimeSpan.Zero;
        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.FollowerWalSaturated, committedIndex: 5);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.True(BatchCountTo(host, SaturatedPeer) > 0, "the peer must be retried once the window passes");
    }

    /// <summary>
    /// Other failure statuses must not pause the peer. <see cref="RaftOperationStatus.LogMismatch"/>
    /// in particular is resolved by re-sending at an earlier anchor, so backing off on it would
    /// delay the very repair it is asking for.
    /// </summary>
    [Fact]
    public async Task OtherFailureStatuses_DoNotPauseTheFollower()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader();

        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.Success, committedIndex: 5);
        await Ack(sm, host, SaturatedPeer, RaftOperationStatus.ReplicationFailed, committedIndex: 5);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(BatchCountTo(host, SaturatedPeer) > 0);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildLeader()
    {
        CapturingHost host = new();
        StubWal wal = new(commitIndex: 100);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 1);

        return (sm, host);
    }

    private static Task Ack(RaftPartitionStateMachine sm, CapturingHost host,
                            string endpoint, RaftOperationStatus status, long committedIndex) =>
        sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                   status, committedIndex).AsTask();

    private static AppendLogsRequest[] RequestsTo(CapturingHost host, string endpoint) =>
        host.Requests
            .Where(r => r.Node?.Endpoint == endpoint && r.AppendLogsRequest is not null)
            .Select(r => r.AppendLogsRequest!)
            .ToArray();

    private static int BatchCountTo(CapturingHost host, string endpoint) =>
        RequestsTo(host, endpoint).Count(r => r.Logs is { Count: > 0 });

    private static int BatchCount(CapturingHost host) =>
        host.Requests.Count(r => r.AppendLogsRequest?.Logs is { Count: > 0 });

    // ── stubs ────────────────────────────────────────────────────────────────

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
        public IReadOnlyList<RaftNode> Nodes => [new(SaturatedPeer), new(HealthyPeer)];
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

    /// <summary>A WAL that always has entries available from the requested anchor.</summary>
    private sealed class StubWal : IRaftWalFacade
    {
        private readonly long commitIndex;

        public StubWal(long commitIndex) => this.commitIndex = commitIndex;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            List<RaftLog> batch = [];
            for (long id = startLogIndex; id < startLogIndex + 3 && id <= commitIndex; id++)
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "test" });

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
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
