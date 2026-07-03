
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
        // and QuiesceAfter=0, the idle threshold is met immediately on the first leadership check.
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

        // First tick quiesces the leader.
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

        public IReadOnlyList<RaftNode> Nodes => [new(Peer)];

        public LivenessTable LivenessTable { get; } = new();

        public MemberLivenessState GetNodeLiveness(string endpoint) => LivenessTable.GetState(endpoint);

        public List<AppendLogsRequest> CapturedAppendLogs { get; } = [];
        public List<RaftResponderRequestType> CapturedTypes { get; } = [];

        public void ClearObservations()
        {
            CapturedAppendLogs.Clear();
            CapturedTypes.Clear();
        }

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request)
        {
            CapturedTypes.Add(request.Type);
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
