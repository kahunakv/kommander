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
/// A leader must stop reading and shipping backfill batches to a follower whose outbound
/// transport queue is over its byte budget (<see cref="IRaftPartitionHost.IsOutboundQueueSaturated"/>).
///
/// <para>
/// This gate differs from the ack-driven <c>FollowerWalSaturated</c> pause in one decisive way: it
/// needs no reply from the peer. A follower that stopped draining entirely (SIGSTOP pause, dead
/// network) never acks, so the ack-driven pause never engages — and before this gate existed, every
/// heartbeat read a fresh full batch from the WAL and stacked it onto the peer's queue behind all
/// the previous ones. The Caraxes run Q leader retained ~830 MiB of such batches and aborted on
/// memory exhaustion.
/// </para>
/// </summary>
public class TestOutboundQueueSaturation
{
    private const string StalledPeer = "follower-a:9001";
    private const string HealthyPeer = "follower-b:9002";

    /// <summary>
    /// The stalled peer receives no entry-carrying batches while the healthy one keeps catching
    /// up — the gate must be per-peer, or one frozen follower would stall the cluster.
    /// </summary>
    [Fact]
    public async Task SaturatedPeer_ReceivesNoBatches_WhileOthersContinue()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CountingWal _) = await BuildLeader();

        await Ack(sm, host, StalledPeer, committedIndex: 5);
        await Ack(sm, host, HealthyPeer, committedIndex: 5);

        host.SaturatedEndpoints.Add(StalledPeer);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, BatchCountTo(host, StalledPeer));
        Assert.True(BatchCountTo(host, HealthyPeer) > 0, "the healthy follower must keep catching up");
    }

    /// <summary>
    /// Heartbeats to the stalled peer must continue: they carry no payload (the transport never
    /// drops them either) and they are what holds leadership.
    /// </summary>
    [Fact]
    public async Task SaturatedPeer_StillReceivesHeartbeats()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CountingWal _) = await BuildLeader();

        await Ack(sm, host, StalledPeer, committedIndex: 5);
        host.SaturatedEndpoints.Add(StalledPeer);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest[] toPeer = RequestsTo(host, StalledPeer);

        Assert.NotEmpty(toPeer);
        Assert.All(toPeer, r => Assert.True(r.Logs is null or { Count: 0 },
            "a saturated peer may be heartbeated, but must not be sent entries"));
    }

    /// <summary>
    /// The point of gating BEFORE the WAL read: a round in which every behind follower is
    /// saturated must not read a single range from the WAL. Materializing a batch per heartbeat
    /// for a peer that cannot receive it was the leader's largest recurring allocation while the
    /// run Q follower was frozen.
    /// </summary>
    [Fact]
    public async Task SaturatedRound_PerformsNoWalRangeRead()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CountingWal wal) = await BuildLeader();

        await Ack(sm, host, StalledPeer, committedIndex: 5);
        await Ack(sm, host, HealthyPeer, committedIndex: 5);

        host.SaturatedEndpoints.Add(StalledPeer);
        host.SaturatedEndpoints.Add(HealthyPeer);

        wal.RangeReads = 0;
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, wal.RangeReads);
    }

    /// <summary>
    /// The gate is a live query, not a latch: once the transport reports the queue drained, the
    /// next round resumes backfill with no further ceremony.
    /// </summary>
    [Fact]
    public async Task Backfill_Resumes_WhenTheQueueDrains()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CountingWal _) = await BuildLeader();

        await Ack(sm, host, StalledPeer, committedIndex: 5);
        host.SaturatedEndpoints.Add(StalledPeer);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(0, BatchCountTo(host, StalledPeer));

        host.SaturatedEndpoints.Clear();

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.True(BatchCountTo(host, StalledPeer) > 0, "backfill must resume once the queue drains");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(RaftPartitionStateMachine, CapturingHost, CountingWal)> BuildLeader()
    {
        CapturingHost host = new();
        CountingWal wal = new(commitIndex: 100);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 1);

        return (sm, host, wal);
    }

    private static Task Ack(RaftPartitionStateMachine sm, CapturingHost host,
                            string endpoint, long committedIndex) =>
        sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                   RaftOperationStatus.Success, committedIndex).AsTask();

    private static AppendLogsRequest[] RequestsTo(CapturingHost host, string endpoint) =>
        host.Requests
            .Where(r => r.Node?.Endpoint == endpoint && r.AppendLogsRequest is not null)
            .Select(r => r.AppendLogsRequest!)
            .ToArray();

    private static int BatchCountTo(CapturingHost host, string endpoint) =>
        RequestsTo(host, endpoint).Count(r => r.Logs is { Count: > 0 });

    // ── stubs ────────────────────────────────────────────────────────────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        /// <summary>Endpoints the fake transport reports as over their outbound byte budget.</summary>
        public HashSet<string> SaturatedEndpoints { get; } = [];

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
        public IReadOnlyList<RaftNode> Nodes => [new(StalledPeer), new(HealthyPeer)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public bool IsOutboundQueueSaturated(string endpoint) => SaturatedEndpoints.Contains(endpoint);

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

    /// <summary>A WAL with entries always available that counts its range reads.</summary>
    private sealed class CountingWal : IRaftWalFacade
    {
        private readonly long commitIndex;

        /// <summary>Number of range reads performed; reset by tests that assert on a single round.</summary>
        public int RangeReads;

        public CountingWal(long commitIndex) => this.commitIndex = commitIndex;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            RangeReads++;

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
        public void TryComplete(ulong correlationId, RaftOperationStatus status) { }

        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
