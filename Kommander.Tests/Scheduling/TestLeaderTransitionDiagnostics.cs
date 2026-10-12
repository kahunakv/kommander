using System.Diagnostics;
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Leader-side transitions and the diagnostics they owe the operator: a promotion barrier whose
/// no-op never commits must revert to Follower naming the peers whose disks are stalled and whose
/// logs are contiguous only below the leader's tail; a step-down must hand the partition to the
/// most advanced peer and never to itself; a (re)admitted member's replication progress must be
/// reset and a quiesced leader woken by the reset; and a follower's re-seed request must be
/// refused without a leader, held to a bound with one, and dropped by a leader that owes the
/// requester nothing.
/// </summary>
public class TestLeaderTransitionDiagnostics
{
    private const string Voter = "node-b:9001";
    private const string Learner = "node-c:9002";
    private const string Self = "node-a:9000";

    // ── promotion-barrier timeout diagnostics ────────────────────────────────

    /// <summary>
    /// A leader whose promotion barrier cannot gather quorum reverts to Follower once the bound
    /// elapses, and the revert line names the cause per peer: the voter reporting a durable-write
    /// stall (with the age it reported) and the hole below this leader's tail that withholds the
    /// voter's barrier ack (its contiguous frontier versus the leader's, and its commit frontier).
    /// </summary>
    [Fact]
    public async Task BarrierTimeout_WithStalledAndHoledVoter_NamesBothInTheRevertReason()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> messages) = Build([Voter], maxLog: 5, commitIndex: 2);

        await ElectThroughFullVoteAsync(sm, host, Voter);
        // While the barrier is pending the leader is Leader only internally; observers see
        // Candidate until the barrier no-op publishes leadership.
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

        await sm.CompleteAppendLogsAsync(
            Voter, Timestamp(host), RaftOperationStatus.Success,
            committedIndex: 2, responseTerm: sm.CurrentTerm,
            durableIndex: 1, walStallMs: 5_000, presentIndex: 3, presentTerm: 1);

        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(250));
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        string revert = Assert.Single(messages, m => m.Contains("Reverting unpublished promotion"));
        Assert.Contains("barrier commit timed out", revert);
        Assert.Contains("durable-write stall: node-b:9001 (voter): oldest pending WAL write 5000 ms", revert);
        Assert.Contains("below this leader's tail: node-b:9001 (voter): contiguous through 3 of 5, commit frontier 2", revert);
    }

    /// <summary>
    /// The same revert on a leader whose only peer is a Learner replica labels it "(learner)" —
    /// the quorum-denominator fact an operator needs when the barrier cannot commit because no
    /// voter besides the leader exists.
    /// </summary>
    [Fact]
    public async Task BarrierTimeout_WithStalledLearner_LabelsItLearnerInTheRevertReason()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> messages) = Build([Learner], maxLog: 5, commitIndex: 2);
        host.NonVoterEndpoints.Add(Learner);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        await sm.CompleteAppendLogsAsync(
            Learner, Timestamp(host), RaftOperationStatus.Success,
            committedIndex: 2, responseTerm: sm.CurrentTerm,
            durableIndex: 1, walStallMs: 6_000, presentIndex: 2, presentTerm: 1);

        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(250));
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        string revert = Assert.Single(messages, m => m.Contains("Reverting unpublished promotion"));
        Assert.Contains("durable-write stall: node-c:9002 (learner): oldest pending WAL write 6000 ms", revert);
        Assert.Contains("below this leader's tail: node-c:9002 (learner): contiguous through 2 of 5", revert);
    }

    /// <summary>
    /// With no peer reporting anything, both diagnostics read "none" — an honest answer that says
    /// "quorum was lost outright", not a blank that hides which list was consulted.
    /// </summary>
    [Fact]
    public async Task BarrierTimeout_WithNoPeerReports_SaysNoneForBothLists()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> messages) = Build([], maxLog: 5, commitIndex: 2);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(250));
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        string revert = Assert.Single(messages, m => m.Contains("Reverting unpublished promotion"));
        Assert.Contains("durable-write stall: none", revert);
        Assert.Contains("below this leader's tail: none", revert);
    }

    // ── step-down target selection ───────────────────────────────────────────

    /// <summary>
    /// A step-down must notify the peer with the most advanced log — and never this node itself,
    /// even when its own endpoint appears in the peer list: the notice tells the successor to
    /// campaign, and pointing it at the departing leader would be a no-op round-trip.
    /// </summary>
    [Fact]
    public async Task StepDown_NotifiesTheMostAdvancedPeer_NeverItself()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> _) = Build([Self, Voter, Learner], maxLog: 5, commitIndex: 2);
        sm.SetLeaderForTesting(term: 2);
        sm.SetStartCommitIndexForTesting(Voter, 4);
        host.Outbound.Clear();

        await sm.StepDownAsync(replyCorrelationId: null);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        RaftResponderRequest notice = Assert.Single(host.Outbound, m => m.Type == RaftResponderRequestType.StepDownNotice);
        Assert.Equal(Voter, notice.Node!.Endpoint);
    }

    // ── resetting a (re)admitted member's progress ───────────────────────────

    /// <summary>
    /// A member (re)admitted to the roster must have its retained replication progress dropped:
    /// progress from before the (re)admission would read the member as caught-up, keep a quiesced
    /// leader quiesced, and starve it of the heartbeats that are its only catch-up path. The reset
    /// wakes the quiesced leader immediately and names the reset at Information.
    /// </summary>
    [Fact]
    public async Task ReadmittedMember_ProgressReset_WakesAQuiescedLeader()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> messages) = Build([Voter], maxLog: 5, commitIndex: 2);
        sm.SetLeaderForTesting(term: 1);
        sm.SeedOptimisticProgressForTesting(Voter, leaderMaxLog: 5);
        sm.SetQuiescedForTesting(true);
        Assert.True((await sm.GetPartitionView()).Quiesced);

        sm.ResetFollowerProgress(Voter);

        Assert.False((await sm.GetPartitionView()).Quiesced, "a member with no recorded progress counts as lagging, which re-arms heartbeats");
        Assert.Contains(messages, m => m.Contains("Reset replication progress") && m.Contains(Voter));
    }

    // ── follower re-seed requests ────────────────────────────────────────────

    /// <summary>
    /// A follower with no leader to ask cannot request a re-seed: the call is refused with the
    /// errored verdict rather than queued against nobody.
    /// </summary>
    [Fact]
    public async Task ReseedRequest_WithoutAKnownLeader_IsRefusedErrored()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> _) = Build([Voter], maxLog: 0, commitIndex: 0);
        host.Leader = "";

        await sm.RequestReseedAsync(replyCorrelationId: 7);

        Assert.Equal(RaftOperationStatus.Errored, Assert.Single(host.Sink.Completed, r => r.Id == 7).Response.Status);
        Assert.False(sm.IsReseedPending);
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.Reseed);
    }

    /// <summary>
    /// With a leader known, the re-seed request ships to it, committed applies are held while the
    /// install is pending, and the hold is released once the bound elapses without an install —
    /// a leader that never serves the snapshot cannot leave the follower held forever.
    /// </summary>
    [Fact]
    public async Task ReseedRequest_WithKnownLeader_HoldsApplies_ThenAbandonsAtTheBound()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> messages) = Build([Voter], maxLog: 0, commitIndex: 0);
        host.Leader = Voter;

        await sm.RequestReseedAsync(replyCorrelationId: 8);

        Assert.Equal(RaftOperationStatus.Success, Assert.Single(host.Sink.Completed, r => r.Id == 8).Response.Status);
        Assert.True(sm.IsReseedPending);
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.Reseed && m.Node!.Endpoint == Voter);

        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(400));
        await sm.CheckPartitionLeadershipAsync();

        Assert.False(sm.IsReseedPending, "the bound elapsed without an install; the hold must release");
        Assert.Contains(messages, m => m.Contains("expired"));
    }

    /// <summary>
    /// A leader drops re-seed requests it can never serve: one from an endpoint that is not a
    /// peer of the partition, and one from a real peer when no state transfer is registered here.
    /// Neither may start a snapshot transfer.
    /// </summary>
    [Fact]
    public async Task ReseedRequest_OnLeader_FromUnservableRequesters_StartsNoSnapshotTransfer()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _, List<string> _) = Build([Voter], maxLog: 0, commitIndex: 0);
        sm.SetLeaderForTesting(term: 1);

        await sm.ReceiveReseedRequestAsync("ghost.nowhere:1", term: 1);
        await sm.ReceiveReseedRequestAsync(Voter, term: 1);

        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(400));
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, host.SnapshotSends);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static HLCTimestamp Timestamp(CapturingHost host) =>
        host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

    private static long ToTicks(TimeSpan delta) => (long)(delta.TotalSeconds * Stopwatch.Frequency);

    /// <summary>
    /// Drives a real pre-vote → election → quorum promotion so the leader holds a pending barrier
    /// against a live voter peer, exactly as after a genuine election win.
    /// </summary>
    private static async Task ElectThroughFullVoteAsync(RaftPartitionStateMachine sm, CapturingHost host, string voter)
    {
        await sm.CheckPartitionLeadershipAsync();

        long preVoteTerm = sm.CurrentTerm + 1;
        await sm.ReceivedVoteAsync(voter, voteTerm: preVoteTerm, remoteMaxLogId: 5, preVote: true, remoteLastLogTerm: 1);

        long electionTerm = sm.CurrentTerm;
        await sm.ReceivedVoteAsync(voter, voteTerm: electionTerm, remoteMaxLogId: 5, preVote: false, remoteLastLogTerm: 1);
    }

    private static (RaftPartitionStateMachine Sm, CapturingHost Host, ControllableWal Wal, List<string> Messages) Build(
        string[] peers, long maxLog, long commitIndex)
    {
        CapturingHost host = new() { Nodes = peers.Select(p => new RaftNode(p)).ToList() };
        ControllableWal wal = new() { MaxLog = maxLog, CommitIndex = commitIndex, PresentId = maxLog > 0 ? maxLog : -1 };
        ILogger<IRaft> logger = CapturingLoggerFactory.Create<IRaft>(out List<string> messages);
        RaftPartitionStateMachine sm = new(host, wal, host.Sink, logger);
        sm.MarkRestoredForTesting();
        sm.SetPostToExecutor(_ => { });
        return (sm, host, wal, messages);
    }

    // ── stubs ────────────────────────────────────────────────────────────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            StartElectionTimeout = 100,
            EndElectionTimeout = 101,
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            LeadershipBarrierTimeout = TimeSpan.FromMilliseconds(200),
            ReseedRequestTimeout = TimeSpan.FromMilliseconds(300),
            EnableCheckQuorum = false,
        };

        public HashSet<string> NonVoterEndpoints { get; } = [];

        public CapturingReplySink Sink { get; } = new();

        public int SnapshotSends { get; private set; }

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => !NonVoterEndpoints.Contains(endpoint);
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public List<RaftResponderRequest> Outbound { get; } = [];

        /// <summary>Monotonic tick source frozen at a fixed base, advanced only by the test.</summary>
        public long? MonotonicTicks { get; set; } = Stopwatch.GetTimestamp();

        public long GetMonotonicTimestamp() => MonotonicTicks!.Value;

        public void AdvanceMonotonic(TimeSpan delta) => MonotonicTicks += ToTicks(delta);

        public long GetLastNodeActivityTicks(string endpoint, int partitionId) => 0;
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => Outbound.Add(request);

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            SnapshotSends++;
            return Task.FromResult(new SnapshotResponse(false));
        }
    }

    /// <summary>
    /// WAL facade presenting a contiguous inherited tail above the commit frontier — the log shape
    /// that arms the promotion barrier — and nothing else: reads return no entries and no write
    /// ever completes, so a barrier propose stays pending exactly as under lost quorum.
    /// </summary>
    private sealed class ControllableWal : IRaftWalFacade
    {
        private long _nextOperationId;

        public long MaxLog { get; init; }

        public long CommitIndex { get; init; }

        public long PresentId { get; init; } = -1;

        public long GetPresentIndex() => PresentId;
        public long GetPresentTerm() => 1;
        public bool HasPresenceGap() => false;
        public long GetCommitIndex() => CommitIndex;
        public double GetOldestPendingWriteAgeMs() => 0;

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(MaxLog);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) => ValueTask.FromResult(new List<RaftLog>());

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, Interlocked.Increment(ref _nextOperationId), WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) => Completed.Add((correlationId, response));
    }
}
