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
/// A quiesced follower trusts SWIM for its leader, and SWIM cannot tell a leader from a node that
/// restarted or stepped down. These tests pin the three parts that close the gap, on the bare state
/// machine with fakes:
/// <list type="bullet">
///   <item>a follower whose candidacy is held still tells its peers that it has no leader (the
///   leaderless notice);</item>
///   <item>a vote-path message from the leader a follower remembers vacates that leadership, so the
///   follower leaves quiescence whether or not it grants;</item>
///   <item>a quiesce marker that arrives after the sender stated it leads nothing is refused.</item>
/// </list>
/// The incident: Jepsen lost-tail run 45829910335, partition 2 leaderless for 5.5 minutes.
/// </summary>
public sealed class TestLeaderlessNotice
{
    private const int Partition = 1;

    private static readonly RaftNode PeerB = new("node-b");

    private static readonly RaftNode PeerC = new("node-c");

    // ── The sender: a held candidacy still announces itself ──────────────────────────────

    [Fact]
    public async Task StorageQuarantine_HeldCandidate_SendsANoticeToEveryVoter_AndOpensNoRound()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new()
        {
            PresentIndex = 7,
            Verdict = new RaftRestoreStorageVerdict(RaftPartitionStorageState.LostAcknowledgedEntries, AcknowledgedThrough: 10, PresentThrough: 7, DurablyApplied: -1, CompactionFloor: 0),
        };
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, wal);

        host.AdvancePastElectionTimeout();
        await sm.CheckPartitionLeadershipAsync();

        List<RequestVotesRequest> notices = host.SentVoteRequests;
        Assert.Equal(2, notices.Count);
        Assert.All(notices, n =>
        {
            Assert.True(n.NoticeOnly);
            Assert.True(n.PreVote);
            Assert.Equal(sm.CurrentTerm + 1, n.Term);
            Assert.Equal(7, n.MaxLogId);
        });
        Assert.Equal(["node-b", "node-c"], host.Sent.Where(r => r.RequestVotesRequest is not null).Select(r => r.Node!.Endpoint).Order());

        // No round is open: a pre-grant from a peer that predates the flag promotes nothing.
        await sm.ReceivedVoteAsync(PeerB.Endpoint, voteTerm: 1, remoteMaxLogId: 7, preVote: true, remoteLastLogTerm: 1);
        await sm.ReceivedVoteAsync(PeerC.Endpoint, voteTerm: 1, remoteMaxLogId: 7, preVote: true, remoteLastLogTerm: 1);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(0, sm.CurrentTerm);
    }

    [Fact]
    public async Task HeldCandidate_SendsOneNoticePerElectionTimeout_NotOnePerTick()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new()
        {
            PresentIndex = 7,
            Verdict = new RaftRestoreStorageVerdict(RaftPartitionStorageState.LostAcknowledgedEntries, 10, 7, -1, 0),
        };
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, wal);

        host.AdvancePastElectionTimeout();
        for (int tick = 0; tick < 5; tick++)
            await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(2, host.SentVoteRequests.Count);

        host.AdvancePastElectionTimeout();
        for (int tick = 0; tick < 5; tick++)
            await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(4, host.SentVoteRequests.Count);
        Assert.All(host.SentVoteRequests, n => Assert.True(n.NoticeOnly));
    }

    [Fact]
    public async Task CandidacyWithheldByTheApplication_SendsANotice()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());
        sm.SetCandidacyWithheld(true);

        host.AdvancePastElectionTimeout();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(2, host.SentVoteRequests.Count);
        Assert.All(host.SentVoteRequests, n => Assert.True(n.NoticeOnly && n.PreVote));
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    [Fact]
    public async Task HealthyFollower_SendsAnOrdinaryPreVote_NotANotice()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());

        host.AdvancePastElectionTimeout();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(2, host.SentVoteRequests.Count);
        Assert.All(host.SentVoteRequests, n => Assert.True(n.PreVote && !n.NoticeOnly));
    }

    // ── The receiver: a follower that remembers the sender as its leader ─────────────────

    [Fact]
    public async Task QuiescedFollower_NoticeFromItsLeader_LeavesQuiescence_SendsNoGrant_ThenGrantsAHealthyCandidate()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());
        await QuiesceUnderAsync(sm, host, PeerB, term: 1);

        // While quiesced, a healthy candidate is denied: SWIM reports the leader's node Alive.
        await sm.VoteAsync(PeerC, voteTerm: 2, remoteMaxLogId: 0, host.Now(), preVote: true);
        Assert.Empty(host.SentVotes);

        // The leader restarted into a held candidacy and says so.
        await sm.VoteAsync(PeerB, voteTerm: 2, remoteMaxLogId: 0, host.Now(), preVote: true, noticeOnly: true);

        Assert.False(sm.Quiesced);
        Assert.Empty(host.SentVotes);
        Assert.Equal(1, sm.CurrentTerm);

        // Out of quiescence the ordinary rule applies: one election timeout of silence, then grant.
        host.AdvancePastElectionTimeout();
        await sm.VoteAsync(PeerC, voteTerm: 2, remoteMaxLogId: 0, host.Now(), preVote: true);

        VoteRequest grant = Assert.Single(host.SentVotes);
        Assert.True(grant.PreVote);
        Assert.Equal(2, grant.Term);
    }

    [Fact]
    public async Task QuiescedFollower_NoticeFromItsLeader_CampaignsOnTheNextTick()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());
        await QuiesceUnderAsync(sm, host, PeerB, term: 1);

        // Quiesced and calm, however much time passes.
        host.AdvancePastElectionTimeout();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Empty(host.SentVoteRequests);

        await sm.VoteAsync(PeerB, voteTerm: 2, remoteMaxLogId: 0, host.Now(), preVote: true, noticeOnly: true);
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(2, host.SentVoteRequests.Count);
        Assert.All(host.SentVoteRequests, n => Assert.True(n.PreVote && !n.NoticeOnly && n.Term == 2));
    }

    [Fact]
    public async Task StaleQuiesceMarker_AfterTheLeaderStatedItLeadsNothing_IsRefused()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());

        // The follower knows node-b as the leader of term 1 and is awake.
        await sm.AppendLogsAsync(PeerB.Endpoint, term: 1, host.Now(), logs: null);
        Assert.Equal(PeerB.Endpoint, host.Leader);
        int acksBefore = host.SentAcks;

        // node-b restarts and announces itself; then the marker its dead process left in a send
        // buffer arrives.
        await sm.VoteAsync(PeerB, voteTerm: 2, remoteMaxLogId: 0, host.Now(), preVote: true, noticeOnly: true);
        await sm.AppendLogsAsync(PeerB.Endpoint, term: 1, host.Now(), logs: null, quiesce: true);

        Assert.False(sm.Quiesced);
        Assert.Equal(acksBefore, host.SentAcks);

        // The fence is bounded by the term: a later leadership of the same node is accepted.
        await sm.AppendLogsAsync(PeerB.Endpoint, term: 2, host.Now(), logs: null, quiesce: true);

        Assert.True(sm.Quiesced);
        Assert.Equal(2, sm.CurrentTerm);
        Assert.Equal(acksBefore + 1, host.SentAcks);
    }

    [Fact]
    public async Task LateHeartbeat_OfAVacatedLeadership_IsStillProcessed_OnlyTheMarkerIsRefused()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());
        await sm.AppendLogsAsync(PeerB.Endpoint, term: 1, host.Now(), logs: null);
        await sm.VoteAsync(PeerB, voteTerm: 2, remoteMaxLogId: 0, host.Now(), preVote: true, noticeOnly: true);
        int acksBefore = host.SentAcks;

        // A late ordinary heartbeat has a bounded effect, so Raft's usual tolerance applies: it is
        // acknowledged, and it never quiesces.
        await sm.AppendLogsAsync(PeerB.Endpoint, term: 1, host.Now(), logs: null);

        Assert.False(sm.Quiesced);
        Assert.Equal(acksBefore + 1, host.SentAcks);

        // The marker is the one message that is refused.
        await sm.AppendLogsAsync(PeerB.Endpoint, term: 1, host.Now(), logs: null, quiesce: true);

        Assert.False(sm.Quiesced);
        Assert.Equal(acksBefore + 1, host.SentAcks);
    }

    [Fact]
    public async Task DelayedProbeOfTheTermTheLeaderWon_VacatesNothing()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());
        await QuiesceUnderAsync(sm, host, PeerB, term: 1);
        int acksBefore = host.SentAcks;

        // A pre-vote for term 1 left node-b before it won term 1. It says nothing about term 1.
        await sm.VoteAsync(PeerB, voteTerm: 1, remoteMaxLogId: 0, host.Now(), preVote: true);

        Assert.True(sm.Quiesced);

        await sm.AppendLogsAsync(PeerB.Endpoint, term: 1, host.Now(), logs: null, quiesce: true);
        Assert.True(sm.Quiesced);
        Assert.Equal(acksBefore + 1, host.SentAcks);
    }

    [Fact]
    public async Task QuiescedFollower_DeniesItsRestartedLeaderForAnOutdatedLog_ButLeavesQuiescence()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade { PresentIndex = 10 });
        await QuiesceUnderAsync(sm, host, PeerB, term: 1);

        // The restarted leader lost its tail: its probe is denied on freshness.
        await sm.VoteAsync(PeerB, voteTerm: 2, remoteMaxLogId: 5, host.Now(), preVote: true, remoteLastLogTerm: 1);

        Assert.Empty(host.SentVotes);
        Assert.False(sm.Quiesced);

        // The follower holds the fresher log, so it is the one that campaigns.
        host.AdvancePastElectionTimeout();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.SentVoteRequests, n => n.PreVote && !n.NoticeOnly && n.Term == 2);
    }

    [Fact]
    public async Task QuiescedFollower_WithNoLeaderRecordedForItsTerm_LeavesQuiescenceOnTheTick()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade { PresentIndex = 10 });
        await QuiesceUnderAsync(sm, host, PeerB, term: 1);

        // A real vote request moves the term; the candidate's log is behind, so no vote is cast
        // and no leader is recorded for term 2.
        await sm.VoteAsync(PeerC, voteTerm: 2, remoteMaxLogId: 5, host.Now(), preVote: false, remoteLastLogTerm: 1);
        Assert.Equal(2, sm.CurrentTerm);
        Assert.Empty(host.SentVotes);
        Assert.True(sm.Quiesced);

        await sm.CheckPartitionLeadershipAsync();

        Assert.False(sm.Quiesced);
    }

    // ── The receiver: a quiesced leader ──────────────────────────────────────────────────

    [Fact]
    public async Task QuiescedLeader_LeaderlessNotice_Wakes_AndSendsNoGrant()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());
        sm.SetLeaderForTesting(term: 1);
        sm.SetQuiescedForTesting(true);
        Assert.True(sm.Quiesced);

        await sm.VoteAsync(PeerB, voteTerm: 2, remoteMaxLogId: 0, host.Now(), preVote: true, noticeOnly: true);

        Assert.False(sm.Quiesced);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal(1, sm.CurrentTerm);
        Assert.Empty(host.SentVotes);
    }

    [Fact]
    public async Task Notice_IsNeverTreatedAsARealVoteRequest()
    {
        FakePartitionHost host = new();
        RaftPartitionStateMachine sm = await MakeFollowerAsync(host, new FakeWalFacade());

        // Malformed: the notice flag without the pre-vote flag. The term must not be adopted.
        await sm.VoteAsync(PeerB, voteTerm: 9, remoteMaxLogId: 0, host.Now(), preVote: false, noticeOnly: true);

        Assert.Equal(0, sm.CurrentTerm);
        Assert.Empty(host.SentVotes);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────────────

    private static async Task<RaftPartitionStateMachine> MakeFollowerAsync(FakePartitionHost host, FakeWalFacade wal)
    {
        RaftPartitionStateMachine sm = new(host, wal, new NullReplySink(), NullLogger<IRaft>.Instance);
        await sm.CompleteRestoreAsync(await sm.StartRestoreAsync());
        host.Sent.Clear();
        return sm;
    }

    /// <summary>Makes <paramref name="leader"/> the leader of <paramref name="term"/> and delivers its quiesce marker.</summary>
    private static async Task QuiesceUnderAsync(RaftPartitionStateMachine sm, FakePartitionHost host, RaftNode leader, long term)
    {
        await sm.AppendLogsAsync(leader.Endpoint, term, host.Now(), logs: null, quiesce: true);

        Assert.True(sm.Quiesced);
        Assert.Equal(leader.Endpoint, host.Leader);
        Assert.Equal(term, sm.CurrentTerm);
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
            EnableQuiescence = true,
            StartElectionTimeout = 500,
            EndElectionTimeout = 1000,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();

        /// <summary>Pinned and advanced by hand, so no test waits on the wall clock.</summary>
        public long MonotonicTicks { get; set; } = Stopwatch.GetTimestamp();

        public long GetMonotonicTimestamp() => MonotonicTicks;

        /// <summary>Moves the clock past the widest election timeout the configuration can draw.</summary>
        public void AdvancePastElectionTimeout() =>
            MonotonicTicks += Stopwatch.Frequency * (Configuration.EndElectionTimeout + 100) / 1000;

        public HLCTimestamp Now() => HybridLogicalClock.SendOrLocalEvent(2);

        public IReadOnlyList<RaftNode> Nodes { get; set; } = [PeerB, PeerC];

        /// <summary>Every outbound message the state machine asked the host to send.</summary>
        public List<RaftResponderRequest> Sent { get; } = [];

        public List<RequestVotesRequest> SentVoteRequests =>
            Sent.Where(r => r.RequestVotesRequest is not null).Select(r => r.RequestVotesRequest!).ToList();

        public List<VoteRequest> SentVotes =>
            Sent.Where(r => r.Type == RaftResponderRequestType.Vote).Select(r => r.VoteRequest!).ToList();

        public int SentAcks => Sent.Count(r => r.Type == RaftResponderRequestType.CompleteAppendLogs);

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => Sent.Add(request);

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new SnapshotResponse(false));
    }

    private sealed class FakeWalFacade : IRaftWalFacade
    {
        /// <summary>The contiguous presence frontier the fake advertises (-1 = untracked, the interface default).</summary>
        public long PresentIndex { get; set; } = -1;

        public RaftRestoreStorageVerdict Verdict { get; set; } = RaftRestoreStorageVerdict.None;

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

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, 1, WALWriteOperationType.LeaderPropose, (Partition, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 2, WALWriteOperationType.LeaderCommit, (Partition, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 3, WALWriteOperationType.LeaderRollback, (Partition, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class NullReplySink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
