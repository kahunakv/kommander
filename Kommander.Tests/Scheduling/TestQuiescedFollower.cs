
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
/// Unit tests for the quiesced-follower election gate.
/// Exercises the SWIM-based failover path without any heartbeat suppression
/// (the leader-side quiesce path that suppresses heartbeats is not involved here).
/// Uses <see cref="RaftPartitionStateMachine.SetQuiescedForTesting"/> to inject the
/// quiesced state and drives <see cref="LivenessTable"/> directly.
/// </summary>
public class TestQuiescedFollower
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static QuiescedTestHost MakeHost(string leaderEndpoint = "node-b")
    {
        QuiescedTestHost host = new() { Peer = leaderEndpoint };
        host.Configuration.EnableQuiescence = true;
        host.Configuration.PingInterval = TimeSpan.FromMilliseconds(200);
        host.Configuration.StartElectionTimeout = 500;
        host.Configuration.EndElectionTimeout = 1000;
        return host;
    }

    private static async Task<RaftPartitionStateMachine> MakeFollowerAsync(
        QuiescedTestHost host, MinimalWalFacade wal, MinimalReplySink sink)
    {
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        host.ClearObservations();
        return sm;
    }

    // ── Quiesced follower stays calm while leader node is Alive ───────────────

    [Fact]
    public async Task QuiescedFollower_LeaderAlive_DoesNotStartPreVote()
    {
        QuiescedTestHost host = MakeHost();
        using MinimalWalFacade wal = new();
        MinimalReplySink sink = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, wal, sink);

        host.LivenessTable.MarkAlive("node-b", incarnation: 1);
        sm.SetQuiescedForTesting(true, leaderEndpoint: "node-b", term: sm.CurrentTerm);

        for (int i = 0; i < 10; i++)
            await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.DoesNotContain(host.EnqueuedResponses, r =>
            r == RaftResponderRequestType.RequestVotes);
    }

    // ── Quiesced follower un-quiesces and pre-votes when leader goes Suspect ──

    [Fact]
    public async Task QuiescedFollower_LeaderSuspect_TriggersPreVote()
    {
        QuiescedTestHost host = MakeHost();
        using MinimalWalFacade wal = new();
        MinimalReplySink sink = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, wal, sink);

        host.LivenessTable.MarkAlive("node-b", incarnation: 1);
        sm.SetQuiescedForTesting(true, leaderEndpoint: "node-b", term: sm.CurrentTerm);

        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);

        host.LivenessTable.MarkSuspect("node-b");
        await sm.CheckPartitionLeadershipAsync();

        // State stays Follower while the pre-vote round is open (no reply arrives from node-b
        // in this unit test). The important assertion is that a RequestVotes (pre-vote probe)
        // was enqueued, proving the quiesced branch triggered an election attempt.
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Contains(host.EnqueuedResponses, r =>
            r == RaftResponderRequestType.RequestVotes);
    }

    // ── Quiesced follower un-quiesces and pre-votes when leader goes Dead ─────

    [Fact]
    public async Task QuiescedFollower_LeaderDead_TriggersPreVote()
    {
        QuiescedTestHost host = MakeHost();
        using MinimalWalFacade wal = new();
        MinimalReplySink sink = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, wal, sink);

        host.LivenessTable.MarkAlive("node-b", incarnation: 1);
        sm.SetQuiescedForTesting(true, leaderEndpoint: "node-b", term: sm.CurrentTerm);

        host.LivenessTable.MarkSuspect("node-b");
        host.LivenessTable.AdvanceExpiry(DateTimeOffset.UtcNow.AddSeconds(30), suspicionTimeout: TimeSpan.Zero);
        Assert.Equal(MemberLivenessState.Dead, host.GetNodeLiveness("node-b"));

        await sm.CheckPartitionLeadershipAsync();

        // Same reasoning as the Suspect case: pre-vote probe was sent; state stays Follower
        // until a pre-vote quorum arrives.
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Contains(host.EnqueuedResponses, r =>
            r == RaftResponderRequestType.RequestVotes);
    }

    // ── EnableQuiescence=false: quiesced flag is never consulted ─────────────

    [Fact]
    public async Task QuiescedFollower_QuiescenceDisabled_IgnoresQuiescedFlag()
    {
        QuiescedTestHost host = MakeHost();
        host.Configuration.EnableQuiescence = false;
        using MinimalWalFacade wal = new();
        MinimalReplySink sink = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, wal, sink);

        // Leader is Suspect but quiescence is off — quiesced branch is skipped.
        // The normal timer branch applies: lastHeartbeat was just refreshed, so
        // (now - lastHB) < electionTimeout → follower stays calm.
        host.LivenessTable.MarkSuspect("node-b");
        sm.SetQuiescedForTesting(true, leaderEndpoint: "node-b", term: sm.CurrentTerm);

        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.DoesNotContain(host.EnqueuedResponses, r =>
            r == RaftResponderRequestType.RequestVotes);
    }

    // ── AppendLogs clears quiesced flag ──────────────────────────────────────

    [Fact]
    public async Task AppendLogs_ClearsQuiescedState()
    {
        QuiescedTestHost host = MakeHost();
        using MinimalWalFacade wal = new();
        MinimalReplySink sink = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, wal, sink);

        host.LivenessTable.MarkAlive("node-b", incarnation: 1);
        sm.SetQuiescedForTesting(true, leaderEndpoint: "node-b", term: sm.CurrentTerm);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(1);
        await sm.AppendLogsAsync("node-b", term: sm.CurrentTerm, timestamp: ts, logs: null);

        // Quiesced is now false; lastHeartbeat was refreshed — normal timer returns early.
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.DoesNotContain(host.EnqueuedResponses, r =>
            r == RaftResponderRequestType.RequestVotes);
    }

    // ── Test doubles ─────────────────────────────────────────────────────────

    private sealed class QuiescedTestHost : IRaftPartitionHost
    {
        public string Peer { get; init; } = "node-b";

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

        public List<RaftResponderRequestType> EnqueuedResponses { get; } = [];

        public void ClearObservations() => EnqueuedResponses.Clear();

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request) =>
            EnqueuedResponses.Add(request.Type);

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    internal sealed class MinimalWalFacade : IRaftWalFacade, IDisposable
    {
        private readonly FakeWAL wal = new();

        public void Dispose() => wal.Dispose();

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);

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

    private sealed class MinimalReplySink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
