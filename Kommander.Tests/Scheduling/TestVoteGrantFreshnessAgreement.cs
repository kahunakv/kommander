using System.Diagnostics;
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// The two sides of a vote must judge log freshness by one rule.
///
/// <para><b>The defect (CamusDB fault soak fs11, Kommander 1.8.2).</b> A voter grants by Raft §5.4.1:
/// last log term first, then index. The candidate tallying the grant re-checked it by index alone
/// and discarded any grant from a voter whose index was higher. A deposed leader holds exactly such
/// a log — an older-term tail of unreplicated proposals, longer than the new leader's log — and it
/// correctly grants to a candidate holding one entry of the newer term. The candidate threw the
/// grant away every round; each grant re-armed the voter's candidacy cooldown, so the voter never
/// campaigned either; and with the third node paused the partition had no leader for the whole
/// pause (eight terms, 30 s at 0 ops/s).</para>
///
/// <para>Two fixes, tested here:</para>
/// <list type="bullet">
///   <item>The candidate's fence uses the same lexicographic (term, index) rule as the voter, with
///         the roles mirrored: a grant is discarded only when the GRANTER's log is strictly fresher
///         than ours, which is the one case the voter should have denied.</item>
///   <item>A grant re-arms the cooldown only when the previous grant produced a leader. Granting
///         again to a candidate that never led leaves the cooldown anchored at the first grant, so
///         the voter's own election timer can fire.</item>
/// </list>
/// </summary>
public class TestVoteGrantFreshnessAgreement
{
    private const string PeerB = "node-b";
    private const string PeerC = "node-c";

    // ── the candidate counts a grant the voter correctly gave ─────────────────

    /// <summary>
    /// The fs11 shape, small. The candidate's log ends at (term 3, index 9); the voter's at
    /// (term 2, index 12): an older term with a longer tail. The voter is behind by §5.4.1 and
    /// grants. The candidate must count the grant and win — on 1.8.2 it discarded it because 12 > 9.
    /// </summary>
    [Fact]
    public async Task Candidate_CountsGrantFromVoterWithOlderTermAndLongerTail()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 3, lastIndex: 9);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

        await sm.ReceivedVoteAsync(PeerB, sm.CurrentTerm, remoteMaxLogId: 12, preVote: false, remoteLastLogTerm: 2);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal("node-a", host.Leader);
        Assert.Contains("LeaderChanged:node-a", host.EventLog);
    }

    /// <summary>The ordinary case still counts: same last term, the voter's index lower.</summary>
    [Fact]
    public async Task Candidate_CountsGrantFromVoterWithSameTermAndLowerIndex()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 3, lastIndex: 9);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync(PeerB, sm.CurrentTerm, remoteMaxLogId: 7, preVote: false, remoteLastLogTerm: 3);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal("node-a", host.Leader);
    }

    // ── the fence still holds where the voter should have denied ─────────────

    /// <summary>
    /// Mirror image: our log ends at (term 2, index 12), the granter's at (term 3, index 9). The
    /// granter is fresher by term, so by §5.4.1 it should have denied us; the grant is discarded and
    /// the node stays a candidate. This is the case the old index-only fence could not see either
    /// way (it would have COUNTED this grant, because 12 &gt; 9).
    /// </summary>
    [Fact]
    public async Task Candidate_DiscardsGrantFromVoterAheadByTerm()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 2, lastIndex: 12);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync(PeerB, sm.CurrentTerm, remoteMaxLogId: 9, preVote: false, remoteLastLogTerm: 3);

        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Equal("", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
    }

    /// <summary>
    /// A granter predating the last-log-term field advertises term 0. The fence then falls back to
    /// the index-only comparison — the legacy behaviour, so mixed-version clusters keep working
    /// exactly as before.
    /// </summary>
    [Fact]
    public async Task Candidate_DiscardsLegacyGrantWithHigherIndexAndNoTerm()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 3, lastIndex: 9);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync(PeerB, sm.CurrentTerm, remoteMaxLogId: 12, preVote: false, remoteLastLogTerm: 0);

        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Equal("", host.Leader);
    }

    // ── the voter's side of the same pair ─────────────────────────────────────

    /// <summary>
    /// The grant the candidate above receives really is what a voter with the older-term, longer
    /// tail sends: it grants, and the grant carries its own (term, index) so the candidate can apply
    /// the same rule.
    /// </summary>
    [Fact]
    public async Task Voter_WithOlderTermAndLongerTail_GrantsAndAdvertisesItsPosition()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 2, lastIndex: 12);
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        await sm.VoteAsync(new RaftNode(PeerB), voteTerm: 5, remoteMaxLogId: 9, ts, preVote: false, remoteLastLogTerm: 3);

        RaftResponderRequest grant = Assert.Single(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
        Assert.NotNull(grant.VoteRequest);
        Assert.Equal(12, grant.VoteRequest!.MaxLogId);
        Assert.Equal(2, grant.VoteRequest.LastLogTerm);
        Assert.Equal(5, grant.VoteRequest.Term);
    }

    // ── a fruitless grant does not re-arm the cooldown ────────────────────────

    /// <summary>
    /// The voter grants to B in term 5, hears no leader, and grants to B again in term 6. On 1.8.2
    /// the second grant restarted the 2 × ElectionTimeout cooldown, so at one election timeout after
    /// it the voter still could not campaign — and B, re-campaigning inside that window every time,
    /// kept it there indefinitely. Now the cooldown stays anchored at the first grant and the voter's
    /// election timer runs its pre-vote.
    /// </summary>
    [Fact]
    public async Task Voter_RepeatGrantWithNoLeaderInBetween_DoesNotRearmTheCooldown()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 2, lastIndex: 12);
        host.AdvanceMonotonic(TimeSpan.Zero); // freeze the clock
        TimeSpan electionTimeout = TimeSpan.FromMilliseconds(host.Config.EndElectionTimeout);

        await Grant(sm, host, PeerB, term: 5);

        // Past the first cooldown; B has not led. B asks again.
        host.AdvanceMonotonic(electionTimeout * 2.5);
        await Grant(sm, host, PeerB, term: 6);

        // One election timeout after the second grant: within 2 × ElectionTimeout of it, but well
        // past 2 × ElectionTimeout since the first.
        host.AdvanceMonotonic(electionTimeout * 1.5);
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        List<RaftResponderRequest> probes = host.Outbound.Where(m => m.Type == RaftResponderRequestType.RequestVotes).ToList();
        Assert.Equal(2, probes.Count); // one per voter peer
        Assert.All(probes, probe => Assert.True(probe.RequestVotesRequest!.PreVote, "the voter's own election timer should open a pre-vote round"));
    }

    /// <summary>
    /// Control for the test above: after a single grant the cooldown is armed as before, so one
    /// election timeout later the voter still yields to the candidate it backed.
    /// </summary>
    [Fact]
    public async Task Voter_FirstGrant_StillArmsTheCooldown()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 2, lastIndex: 12);
        host.AdvanceMonotonic(TimeSpan.Zero);
        TimeSpan electionTimeout = TimeSpan.FromMilliseconds(host.Config.EndElectionTimeout);

        await Grant(sm, host, PeerB, term: 5);

        host.AdvanceMonotonic(electionTimeout * 1.5);
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// The anchor is about leaders, not candidates: once this node itself has led (or heard a
    /// leader) since its last grant, the next grant is a first grant again and arms the cooldown.
    /// Leading is the transition available to a unit test; an accepted append refreshes the same
    /// contact tick.
    /// </summary>
    [Fact]
    public async Task Voter_GrantAfterALeaderWasHeard_ArmsTheCooldownAgain()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, _) = BuildCandidate(lastTerm: 2, lastIndex: 12);
        host.AdvanceMonotonic(TimeSpan.Zero);
        TimeSpan electionTimeout = TimeSpan.FromMilliseconds(host.Config.EndElectionTimeout);

        await Grant(sm, host, PeerB, term: 5);

        // Leadership contact in between: this node wins term 6 itself and then steps down for a
        // higher-term vote request from B.
        host.AdvanceMonotonic(electionTimeout * 2.5);
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync(PeerB, sm.CurrentTerm, remoteMaxLogId: 12, preVote: false, remoteLastLogTerm: 2);
        await sm.ReceivedVoteAsync(PeerC, sm.CurrentTerm, remoteMaxLogId: 12, preVote: false, remoteLastLogTerm: 2);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        host.AdvanceMonotonic(electionTimeout * 2.5);
        await Grant(sm, host, PeerB, term: sm.CurrentTerm + 1);

        host.AdvanceMonotonic(electionTimeout * 1.5);
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The election timer is reset when the vote is RESERVED, not when its durable reply leaves.
    /// A voter whose timer kept running through the vote's fsync opened a pre-vote for the next term
    /// on its next tick and deposed the candidate it had just backed (the RecoveryReSupplyClusterTests
    /// GA failure). The write is left pending here to hold the window open; the tail of the test is the
    /// positive control that this harness does campaign once both the timer and the cooldown expire.
    /// </summary>
    [Fact]
    public async Task Voter_WithQueuedVoteGrant_DoesNotCampaignWhileTheVoteIsBeingPersisted()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ContiguousWal wal) = BuildCandidate(lastTerm: 2, lastIndex: 12);
        wal.QueueHardState = true;
        host.AdvanceMonotonic(TimeSpan.Zero); // freeze the clock
        TimeSpan electionTimeout = TimeSpan.FromMilliseconds(host.Config.EndElectionTimeout);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode(PeerB), voteTerm: 5, remoteMaxLogId: 9, ts, preVote: false, remoteLastLogTerm: 3);

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.Vote); // still being persisted
        Assert.NotEmpty(wal.QueuedHardStateOperationIds);

        // One tick inside the fsync window: the timer must read "just granted", so no pre-vote.
        host.AdvanceMonotonic(electionTimeout * 0.5);
        await sm.CheckPartitionLeadershipAsync();
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // Past the timer but inside the 2 × timeout cooldown armed by the same reservation: still no pre-vote.
        host.AdvanceMonotonic(electionTimeout);
        await sm.CheckPartitionLeadershipAsync();
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // The write completes: the grant leaves now, for the term it was reserved in.
        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            host.PartitionId, OperationId: wal.QueuedHardStateOperationIds[^1], Term: 5, MinLogIndex: -1, MaxLogIndex: -1,
            WALWriteOperationType.HardState, RaftOperationStatus.Success));
        RaftResponderRequest grant = Assert.Single(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
        Assert.Equal(5, grant.VoteRequest!.Term);

        // Positive control: once the cooldown has run out with no leader heard, this voter campaigns.
        host.AdvanceMonotonic(electionTimeout);
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// A write the storage engine rejects withholds the vote (the reservation is released so the
    /// candidate's retry can be granted) but keeps the timer reset: the cost is at most one election
    /// timeout before this node may campaign, which is the safe direction.
    /// </summary>
    [Fact]
    public async Task Voter_WithRejectedVoteWrite_WithholdsTheVoteButKeepsTheTimerReset()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ContiguousWal wal) = BuildCandidate(lastTerm: 2, lastIndex: 12);
        wal.QueueHardState = true;
        host.AdvanceMonotonic(TimeSpan.Zero);
        TimeSpan electionTimeout = TimeSpan.FromMilliseconds(host.Config.EndElectionTimeout);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode(PeerB), voteTerm: 5, remoteMaxLogId: 9, ts, preVote: false, remoteLastLogTerm: 3);

        await sm.CompleteWalOperationAsync(new RaftWalCompletion(
            host.PartitionId, OperationId: wal.QueuedHardStateOperationIds[^1], Term: 5, MinLogIndex: -1, MaxLogIndex: -1,
            WALWriteOperationType.HardState, RaftOperationStatus.Errored));
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);

        host.AdvanceMonotonic(electionTimeout * 0.5);
        await sm.CheckPartitionLeadershipAsync();
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // The candidate asks again and is granted this time.
        wal.QueueHardState = false;
        await sm.VoteAsync(new RaftNode(PeerB), voteTerm: 5, remoteMaxLogId: 9, ts, preVote: false, remoteLastLogTerm: 3);
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    private static async Task Grant(RaftPartitionStateMachine sm, CapturingHost host, string candidate, long term)
    {
        host.Outbound.Clear();
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        // The candidate is fresher by term than this node's (term 2, index 12), so the grant is due.
        await sm.VoteAsync(new RaftNode(candidate), term, remoteMaxLogId: 9, ts, preVote: false, remoteLastLogTerm: 3);
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    /// <summary>
    /// A node-a whose committed, contiguous log runs from 1 to <paramref name="lastIndex"/>, with
    /// the last entry at <paramref name="lastTerm"/> and everything before it one term earlier. Two
    /// voter peers, so quorum is two and one counted grant wins.
    /// </summary>
    private static (RaftPartitionStateMachine, CapturingHost, ContiguousWal) BuildCandidate(long lastTerm, long lastIndex)
    {
        ContiguousWal wal = new();
        for (long id = 1; id <= lastIndex; id++)
        {
            wal.Entries.Add(new RaftLog
            {
                Id = id,
                Term = id == lastIndex ? lastTerm : lastTerm - 1,
                LogType = "t",
                Type = RaftLogType.Committed,
            });
        }

        wal.SeedProposeAllocator(lastIndex + 1);

        CapturingHost host = new() { Nodes = [new RaftNode(PeerB), new RaftNode(PeerC)] };
        RaftPartitionStateMachine sm = new(host, wal, new NullSink(), NullLogger<IRaft>.Instance);
        sm.MarkRestoredForTesting();

        return (sm, host, wal);
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            // A one-millisecond band, so "one election timeout" is a definite quantity for the
            // cooldown tests rather than a draw from a range.
            StartElectionTimeout = 100,
            EndElectionTimeout = 101,
            LeadershipBarrierTimeout = TimeSpan.FromMilliseconds(300),
        };

        public int PartitionId { get; init; } = 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public List<string> EventLog { get; } = [];

        public List<RaftResponderRequest> Outbound { get; } = [];

        public long? MonotonicTicks { get; set; }

        public long GetMonotonicTimestamp() => MonotonicTicks ?? Stopwatch.GetTimestamp();

        public void AdvanceMonotonic(TimeSpan delta) =>
            MonotonicTicks = (MonotonicTicks ?? Stopwatch.GetTimestamp()) + (long)(delta.TotalSeconds * Stopwatch.Frequency);

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string ep, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string ep, RaftResponderRequest req) => Outbound.Add(req);

        public Task InvokeLeaderChanged(int p, string leader)
        {
            if (leader == LocalEndpoint)
                EventLog.Add($"LeaderChanged:{leader}");
            return Task.CompletedTask;
        }

        public Task<bool> InvokeReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"Applied:{log.Id}");
            return Task.FromResult(true);
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode n, SnapshotRequest r, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// In-memory log, contiguous from 1, committed through its max: the presence frontier is the max
    /// id and its term the last entry's. Proposals append at the allocator so a promotion barrier
    /// lands like any other entry.
    /// </summary>
    private sealed class ContiguousWal : IRaftWalFacade
    {
        public List<RaftLog> Entries { get; } = [];

        private long nextId = 1;

        private long MaxId => Entries.Count == 0 ? 0 : Entries.Max(l => l.Id);

        public long GetCommitIndex() => Entries.Where(l => l.Type is RaftLogType.Committed).Select(l => l.Id).DefaultIfEmpty(0).Max();
        public long GetPresentIndex() => MaxId;
        public long GetPresentTerm() => Entries.Count == 0 ? 0 : Entries.MaxBy(l => l.Id)!.Term;
        public void SeedProposeAllocator(long id) => nextId = id;

        /// <summary>
        /// When set, hard-state writes are queued (as the production facade does) instead of being
        /// persisted inline, so a test can hold a vote's fsync window open and complete it explicitly
        /// through <see cref="RaftPartitionStateMachine.CompleteWalOperationAsync"/>.
        /// </summary>
        public bool QueueHardState { get; set; }

        public List<long> QueuedHardStateOperationIds { get; } = [];

        private long nextHardStateOperationId = 100;

        public WALWriteOperation? TryEnqueueHardState(long currentTerm, string? votedFor)
        {
            if (!QueueHardState)
                return null;

            long id = nextHardStateOperationId++;
            QueuedHardStateOperationIds.Add(id);
            return new(_ => { }, id, WALWriteOperationType.HardState, (1, []), term: currentTerm, votedFor: votedFor);
        }

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(MaxId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(GetPresentTerm());
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) =>
            ValueTask.FromResult(Entries.FirstOrDefault(l => l.Id == logIndex)?.Term ?? -1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max) =>
            ValueTask.FromResult(Entries
                .Where(l => l.Id >= start && l.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)
                .OrderBy(l => l.Id).Take(max).ToList());

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max) =>
            ValueTask.FromResult(Entries.Where(l => l.Id >= start).OrderBy(l => l.Id).Take(max).ToList());

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            foreach (RaftLog log in logs)
            {
                log.Id = nextId++;
                log.Term = term;
                log.Type = RaftLogType.Proposed;
                Entries.Add(log);
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            return new(null!, 1L, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
            }

            return new(_ => { }, 2L, WALWriteOperationType.LeaderCommit, (1, logs));
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;

        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId)
        {
            Entries.RemoveAll(l => l.Id > afterLogId);
            return ValueTask.FromResult(MaxId);
        }

        public void NotifyCommitted() { }
    }

    private sealed class NullSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
