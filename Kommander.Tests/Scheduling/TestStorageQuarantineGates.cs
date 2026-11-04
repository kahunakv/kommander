using System.Diagnostics;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// The consensus consequences of a storage verdict, pinned on the bare state machine with fakes:
/// a restore that found acknowledged entries missing withholds votes, pre-votes and candidacy until
/// the presence frontier reaches the watermark again; an application floor below the compaction
/// floor withholds candidacy and asks the leader for a snapshot while still voting; a fail-stopped
/// engine steps a leader down and withholds everything. The health probe reports each.
/// </summary>
public sealed class TestStorageQuarantineGates
{
    private const int Partition = 1;

    private static readonly RaftNode Candidate = new("node-b");

    // ── Lost acknowledged entries ────────────────────────────────────────────────────────

    [Fact]
    public async Task LostAcknowledgedEntries_DeniesVotesAndPreVotes_UntilThePresenceFrontierReachesTheWatermark()
    {
        FakePartitionHost host = new() { Nodes = [Candidate] };
        FakeWalFacade wal = new()
        {
            PresentIndex = 7,
            Verdict = new RaftRestoreStorageVerdict(RaftPartitionStorageState.LostAcknowledgedEntries, AcknowledgedThrough: 10, PresentThrough: 7, DurablyApplied: -1, CompactionFloor: 0),
        };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.CompleteRestoreAsync([]);

        RaftPartitionStorageHealth health = sm.GetStorageHealth();
        Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, health.State);
        Assert.Equal(10, health.AcknowledgedThrough);
        Assert.Equal(7, health.PresentThrough);
        Assert.True(health.WithholdsCandidacy);

        // A pre-vote for a fresher candidate is denied, and so is the real vote (the term is still adopted).
        await sm.VoteAsync(Candidate, voteTerm: 1, remoteMaxLogId: 50, host.HybridLogicalClock.SendOrLocalEvent(2), preVote: true, remoteLastLogTerm: 1);
        await sm.VoteAsync(Candidate, voteTerm: 1, remoteMaxLogId: 50, host.HybridLogicalClock.SendOrLocalEvent(2), preVote: false, remoteLastLogTerm: 1);
        Assert.DoesNotContain(host.Sent, r => r.Type == RaftResponderRequestType.Vote);
        Assert.Equal(1, sm.CurrentTerm);

        // The leader replicated past the watermark: the gates open on the live frontier ...
        wal.PresentIndex = 10;
        await sm.VoteAsync(Candidate, voteTerm: 2, remoteMaxLogId: 50, host.HybridLogicalClock.SendOrLocalEvent(2), preVote: false, remoteLastLogTerm: 1);
        Assert.Contains(host.Sent, r => r.Type == RaftResponderRequestType.Vote && r.VoteRequest?.Term == 2);

        // ... and the tick flips the state for the health probe.
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftPartitionStorageState.Healthy, sm.GetStorageHealth().State);
    }

    [Fact]
    public async Task LostAcknowledgedEntries_WithholdsCandidacy_SingleVoterNeverSelfElects()
    {
        FakePartitionHost quarantinedHost = new() { Nodes = [] };
        FakeWalFacade quarantinedWal = new()
        {
            PresentIndex = 3,
            Verdict = new RaftRestoreStorageVerdict(RaftPartitionStorageState.LostAcknowledgedEntries, 5, 3, -1, 0),
        };
        RaftPartitionStateMachine quarantined = new(quarantinedHost, quarantinedWal, new CapturingReplySink(), NullLogger<IRaft>.Instance);
        await quarantined.CompleteRestoreAsync([]);

        // No peers and an expired election timer: a healthy single voter elects itself on this tick.
        FakePartitionHost healthyHost = new() { Nodes = [] };
        RaftPartitionStateMachine healthy = new(healthyHost, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);
        await healthy.CompleteRestoreAsync([]);

        for (int tick = 0; tick < 5; tick++)
        {
            await quarantined.CheckPartitionLeadershipAsync();
            await healthy.CheckPartitionLeadershipAsync();
        }

        Assert.Equal(RaftNodeState.Leader, healthy.NodeState);
        Assert.Equal(RaftNodeState.Follower, quarantined.NodeState);
        Assert.Equal(0, quarantined.CurrentTerm);
        Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, quarantined.GetStorageHealth().State);

        // Once repaired, the same node campaigns: it leaves Follower for a new term (the fake WAL never
        // completes the promotion barrier's write, so the win is reported as Candidate until it does).
        quarantinedWal.PresentIndex = 5;
        for (int tick = 0; tick < 5 && quarantined.NodeState == RaftNodeState.Follower; tick++)
            await quarantined.CheckPartitionLeadershipAsync();

        Assert.NotEqual(RaftNodeState.Follower, quarantined.NodeState);
        Assert.Equal(1, quarantined.CurrentTerm);
        Assert.Equal(RaftPartitionStorageState.Healthy, quarantined.GetStorageHealth().State);
    }

    // ── Application floor below the compaction floor ─────────────────────────────────────

    [Fact]
    public async Task ApplicationFloorBelowCompaction_WithholdsCandidacy_KeepsVoting_AndAsksTheLeaderForASnapshot()
    {
        FakePartitionHost host = new() { Nodes = [Candidate], Leader = Candidate.Endpoint };
        FakeWalFacade wal = new()
        {
            PresentIndex = 15,
            Verdict = new RaftRestoreStorageVerdict(RaftPartitionStorageState.ApplicationFloorBelowCompaction, -1, 15, DurablyApplied: 3, CompactionFloor: 11, ApplicationReplayRefused: true),
        };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.CompleteRestoreAsync([]);

        Assert.Equal(RaftPartitionStorageState.ApplicationFloorBelowCompaction, sm.GetStorageHealth().State);

        // Its Raft log is complete: it still votes.
        await sm.VoteAsync(Candidate, voteTerm: 1, remoteMaxLogId: 50, host.HybridLogicalClock.SendOrLocalEvent(2), preVote: false, remoteLastLogTerm: 1);
        Assert.Contains(host.Sent, r => r.Type == RaftResponderRequestType.Vote);

        // The tick asks the known leader for a whole-partition snapshot and holds applies meanwhile.
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Sent, r => r.Type == RaftResponderRequestType.Reseed);
        Assert.True(sm.IsReseedPending);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    // ── Engine fail-stop ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EngineFailStop_StepsALeaderDown_AndWithholdsEverything()
    {
        FakePartitionHost host = new() { Nodes = [] };
        FakeWalFacade wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.CompleteRestoreAsync([]);
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 1);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        wal.FailStopReason = "Corruption: block checksum mismatch: stored = 1, computed = 2 in /data/wal/000012.sst offset 0 size 4096";

        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal("", host.Leader);

        // The loss is reported through the host's leader-changed callback in production; this fake
        // host does not call back, so the next tick's backstop reports it.
        await sm.CheckPartitionLeadershipAsync();
        Assert.Single(host.LeadershipLost);

        RaftPartitionStorageHealth health = sm.GetStorageHealth();
        Assert.Equal(RaftPartitionStorageState.EngineFailed, health.State);
        Assert.Contains("000012.sst", health.Reason);

        // Neither votes nor campaigns again, however many ticks pass.
        host.Nodes = [Candidate];
        await sm.VoteAsync(Candidate, voteTerm: sm.CurrentTerm + 1, remoteMaxLogId: 50, host.HybridLogicalClock.SendOrLocalEvent(2), preVote: false, remoteLastLogTerm: 1);
        Assert.DoesNotContain(host.Sent, r => r.Type == RaftResponderRequestType.Vote);

        host.Nodes = [];
        for (int tick = 0; tick < 5; tick++)
            await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    // ── Fakes ────────────────────────────────────────────────────────────────────────────

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        public int PartitionId => Partition;
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
            EnableQuiescence = false,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public long GetMonotonicTimestamp() => Stopwatch.GetTimestamp();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];
        public List<(int Partition, long Term)> LeadershipLost { get; } = [];

        /// <summary>Every outbound message the state machine asked the host to send.</summary>
        public List<RaftResponderRequest> Sent { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => Sent.Add(request);

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task InvokeLeadershipLost(int partitionId, long term)
        {
            LeadershipLost.Add((partitionId, term));
            return Task.CompletedTask;
        }

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer { get; } = new NoopPartitionTransfer();

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new SnapshotResponse(false));
    }

    private sealed class NoopPartitionTransfer : IRaftPartitionStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream());

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FakeWalFacade : IRaftWalFacade
    {
        /// <summary>The contiguous presence frontier the fake advertises (-1 = untracked, the interface default).</summary>
        public long PresentIndex { get; set; } = -1;

        public RaftRestoreStorageVerdict Verdict { get; set; } = RaftRestoreStorageVerdict.None;

        public string? FailStopReason { get; set; }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            IReadOnlyList<RaftLog> none = [];
            return ValueTask.FromResult(none);
        }

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(Math.Max(PresentIndex, 0));
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public long GetPresentIndex() => PresentIndex;
        public long GetPresentTerm() => PresentIndex >= 0 ? 1 : -1;
        public RaftRestoreStorageVerdict GetRestoreStorageVerdict() => Verdict;
        public string? GetStorageFailStopReason() => FailStopReason;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, 1, WALWriteOperationType.LeaderPropose, (Partition, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 2, WALWriteOperationType.LeaderCommit, (Partition, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 3, WALWriteOperationType.LeaderRollback, (Partition, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }

        public ValueTask<(RaftOperationStatus Status, bool SuffixTruncated)> InstallSnapshotBoundaryAsync(long snapshotIndex, long lastIncludedTerm) =>
            ValueTask.FromResult((RaftOperationStatus.Success, false));
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) =>
            Completed.Add((correlationId, response));
    }
}
