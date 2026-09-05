
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Tests.Scheduler;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Unit tests for the leader-side quiescence path.
/// Verifies that an idle leader sends a quiesce marker to followers after QuiesceAfter has
/// elapsed, that heartbeats are suppressed once quiesced, and that new proposals un-quiesce
/// and resume normal heartbeating.
/// Uses <see cref="RaftPartitionStateMachine.SetLeaderForTesting"/> to inject Leader state
/// and a custom <see cref="CapturingTestHost"/> that records the full outbound requests.
/// </summary>
public class TestQuiescedLeader
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static CapturingTestHost MakeHost()
    {
        CapturingTestHost host = new();
        host.Configuration.EnableQuiescence = true;
        // Both intervals are set to zero so the quiesce check fires immediately
        // on the first CheckPartitionLeadershipAsync call after SetLeaderForTesting.
        host.Configuration.HeartbeatInterval = TimeSpan.Zero;
        host.Configuration.RecentHeartbeat = TimeSpan.Zero;
        host.Configuration.QuiesceAfter = TimeSpan.Zero;
        host.Configuration.StartElectionTimeout = 500;
        host.Configuration.EndElectionTimeout = 1000;
        return host;
    }

    private static async Task<RaftPartitionStateMachine> MakeLeaderAsync(
        CapturingTestHost host, MinimalWalFacadeL wal, MinimalReplySinkL sink)
    {
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        host.ClearObservations();
        sm.SetLeaderForTesting(term: 1);
        return sm;
    }

    /// <summary>
    /// Simulates the peer's Success ack of an (empty) heartbeat, which records its commit frontier
    /// in the leader's progress table. Quiescing requires first contact from every peer — a peer
    /// with no recorded frontier counts as lagging unconditionally, because a quiesced leader is
    /// the only catch-up path a silent peer has (see <c>HeartbeatDriver.HasLaggingPeer</c>).
    /// </summary>
    private static ValueTask AckHeartbeatAsync(
        RaftPartitionStateMachine sm, CapturingTestHost host, string endpoint, long committedIndex = 0) =>
        sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.Success, committedIndex);

    // ── Idle leader quiesces and sends quiesce marker ─────────────────────────

    [Fact]
    public async Task Leader_IdlePastQuiesceAfter_SendsQuiesceMarker()
    {
        CapturingTestHost host = MakeHost();
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();
        RaftPartitionStateMachine sm = await MakeLeaderAsync(host, wal, sink);

        // SetLeaderForTesting delegates to BecomeLeader(), which seeds lastProposalAt at the
        // election timestamp — the same path real election wins take.  With HeartbeatInterval=0
        // and QuiesceAfter=0, the idle threshold is met immediately on the first leadership check
        // once the peer has made first contact (an uncontacted peer blocks quiescence).
        await AckHeartbeatAsync(sm, host, host.Peer);
        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest? quiesceMsg = host.CapturedAppendLogs
            .FirstOrDefault(r => r.Quiesce);
        Assert.NotNull(quiesceMsg);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    // ── Leader without proposal history does NOT quiesce ──────────────────────

    [Fact]
    public async Task Leader_ZeroLastProposalAt_DoesNotQuiesce()
    {
        CapturingTestHost host = MakeHost();
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();

        // Build an SM that is in Leader state but whose lastProposalAt is HLCTimestamp.Zero,
        // representing a node that somehow entered leader state without going through BecomeLeader.
        // The guard must block quiescence in this case.
        RaftPartitionStateMachine sm = await MakeLeaderAsync(host, wal, sink);
        sm.SetLeaderForTesting(term: 1);     // BecomeLeader → lastProposalAt seeded
        await AckHeartbeatAsync(sm, host, host.Peer); // first contact — isolate the guard under test
        sm.ClearLastProposalAtForTesting();  // force back to Zero to assert the guard
        host.ClearObservations();

        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.CapturedAppendLogs, r => r.Quiesce);
    }

    // ── After quiescing, the leader stops sending normal heartbeats ───────────

    [Fact]
    public async Task Leader_AfterQuiesce_HeartbeatsAreSuppressed()
    {
        CapturingTestHost host = MakeHost();
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();
        RaftPartitionStateMachine sm = await MakeLeaderAsync(host, wal, sink);

        // First tick after the peer's first contact quiesces the leader.
        await AckHeartbeatAsync(sm, host, host.Peer);
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.CapturedAppendLogs, r => r.Quiesce);

        host.ClearObservations();

        // Subsequent ticks should not send any AppendLogs (heartbeats suppressed).
        for (int i = 0; i < 5; i++)
            await sm.CheckPartitionLeadershipAsync();

        Assert.Empty(host.CapturedAppendLogs);
    }

    // ── EnableQuiescence=false: leader never quiesces ─────────────────────────

    [Fact]
    public async Task Leader_QuiescenceDisabled_NeverQuiesces()
    {
        CapturingTestHost host = MakeHost();
        host.Configuration.EnableQuiescence = false;
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();
        RaftPartitionStateMachine sm = await MakeLeaderAsync(host, wal, sink);

        await sm.CheckPartitionLeadershipAsync();

        // All outbound AppendLogs should be normal heartbeats (Quiesce=false).
        Assert.All(host.CapturedAppendLogs, r => Assert.False(r.Quiesce));
        Assert.NotEmpty(host.CapturedAppendLogs); // at least one heartbeat went out
    }

    // ── Quiesced leader with a lagging peer wakes on the periodic tick ─────────

    [Fact]
    public async Task Leader_Quiesced_WithLaggingPeer_WakesAndHeartbeats()
    {
        CapturingTestHost host = MakeHost();
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();
        RaftPartitionStateMachine sm = await MakeLeaderAsync(host, wal, sink);

        // First tick after the peer's first contact quiesces the leader: the partition is empty
        // (committed frontier 0), the contacted peer reported frontier 0, so nothing lags and
        // heartbeats are suppressed.
        await AckHeartbeatAsync(sm, host, host.Peer);
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.CapturedAppendLogs, r => r.Quiesce);
        host.ClearObservations();

        // The leader's frontier advances past the peer's recorded one AFTER we quiesced. Pre-vote
        // is side-effect-free (Raft §9.6) and does not wake a quiesced leader, so the re-arm must
        // come from the periodic tick's HasLaggingPeer() gate — otherwise node-b is stranded with
        // no catch-up path on an idle partition (heartbeats host the only backfill/idle-tail
        // re-ship).
        sm.SetLocalCommittedIndexForTesting(5);

        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);           // still leader
        Assert.Contains(host.CapturedAppendLogs, r => !r.Quiesce);  // un-quiesced: forced heartbeat resumes catch-up
    }

    // ── Quiesced leader wakes when a never-contacted peer appears ─────────────

    /// <summary>
    /// The Jepsen <c>register/placement</c> wedge: a placement <c>AddReplica</c> materializes a
    /// Learner on an idle, never-written range whose leader already quiesced. The new peer has no
    /// recorded progress and the partition's committed frontier is 0 — the old
    /// <c>LocalCommittedIndex &lt;= 0</c> short-circuit in <c>HasLaggingPeer</c> made the peer
    /// invisible, so the leader never heartbeat it, its lag was never measurable, and the range
    /// stayed transitional until the decommission drain timed out. A never-contacted peer must
    /// count as lagging even at frontier 0, so the tick re-arms heartbeats.
    /// </summary>
    [Fact]
    public async Task Leader_Quiesced_NewPeerAppears_WakesAndHeartbeatsIt()
    {
        CapturingTestHost host = MakeHost();
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();
        RaftPartitionStateMachine sm = await MakeLeaderAsync(host, wal, sink);

        await AckHeartbeatAsync(sm, host, host.Peer);
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.CapturedAppendLogs, r => r.Quiesce);
        host.ClearObservations();

        // A committed map application adds a learner replica to this range's peer set.
        host.PeerNodes.Add(new RaftNode("node-c"));

        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Contains(host.CapturedAppendLogs, r => !r.Quiesce);
        Assert.Contains("node-c", host.CapturedEndpoints); // the new peer itself was contacted
    }

    // ── Empty partition does not quiesce before first contact ─────────────────

    /// <summary>
    /// Quiescing requires evidence of convergence from every peer, even on a partition with
    /// nothing committed: an uncontacted peer might be a fresh learner whose promotion depends on
    /// the leader's progress table gaining an entry for it. The leader keeps heartbeating until
    /// the peer acks once; the empty partition then quiesces on the next tick (previous tests).
    /// </summary>
    [Fact]
    public async Task Leader_EmptyPartition_UncontactedPeer_DoesNotQuiesce()
    {
        CapturingTestHost host = MakeHost();
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();
        RaftPartitionStateMachine sm = await MakeLeaderAsync(host, wal, sink);

        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.CapturedAppendLogs, r => r.Quiesce);
        Assert.Contains(host.CapturedAppendLogs, r => !r.Quiesce); // still probing the silent peer
    }

    // ── Follower receiving quiesce-flagged AppendLogs becomes quiesced ────────

    [Fact]
    public async Task Follower_ReceivingQuiesceMarker_BecomesQuiesced()
    {
        CapturingTestHost host = MakeHost();
        using MinimalWalFacadeL wal = new();
        MinimalReplySinkL sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        host.ClearObservations();

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);
        await sm.AppendLogsAsync("node-b", term: 1, timestamp: ts, logs: null, quiesce: true);

        // Follower is now quiesced — the next leadership check with SWIM Alive should be a no-op.
        host.LivenessTable.MarkAlive("node-b", incarnation: 1);
        sm.SetQuiescedForTesting(true, leaderEndpoint: "node-b", term: 1);

        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.DoesNotContain(RaftResponderRequestType.RequestVotes, host.CapturedTypes);
    }

    // ── Test doubles ─────────────────────────────────────────────────────────

    private sealed class CapturingTestHost : IRaftPartitionHost
    {
        public string Peer { get; } = "node-b";

        public int PartitionId => 1;

        public string Leader { get; set; } = "";

        public string LocalEndpoint => "node-a";

        public int LocalNodeId => 1;

        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;

        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();

        /// <summary>Mutable so a test can simulate a committed map application growing the
        /// peer set mid-run (a placement AddReplica materializing a learner).</summary>
        public List<RaftNode> PeerNodes { get; }

        public IReadOnlyList<RaftNode> Nodes => PeerNodes;

        public CapturingTestHost() => PeerNodes = [new(Peer)];

        public LivenessTable LivenessTable { get; } = new();

        public MemberLivenessState GetNodeLiveness(string endpoint) => LivenessTable.GetState(endpoint);

        public List<AppendLogsRequest> CapturedAppendLogs { get; } = [];
        public List<RaftResponderRequestType> CapturedTypes { get; } = [];
        public List<string> CapturedEndpoints { get; } = [];

        public void ClearObservations()
        {
            CapturedAppendLogs.Clear();
            CapturedTypes.Clear();
            CapturedEndpoints.Clear();
        }

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;



        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request)
        {
            CapturedTypes.Add(request.Type);
            CapturedEndpoints.Add(endpoint);
            if (request.AppendLogsRequest is not null)
                CapturedAppendLogs.Add(request.AppendLogsRequest);
        }

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;

        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    internal sealed class MinimalWalFacadeL : IRaftWalFacade, IDisposable
    {
        private readonly FakeWAL wal = new();

        public void Dispose() => wal.Dispose();

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);

        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);

        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);

        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, 1, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 1, WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 1, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class MinimalReplySinkL : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
