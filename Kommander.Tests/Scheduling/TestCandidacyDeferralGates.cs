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
/// The gates that can stop a node from campaigning before Raft is even consulted: a stalled local
/// WAL write, an application-withheld candidacy, and the holey-log deference to a fresher voter.
///
/// <para>All three share one contract — while the gate holds, the election-timer tick must produce
/// no <c>RequestVotes</c> at all, and the moment it clears the very next tick campaigns. The
/// holey-log deference additionally must be evidence-gated: only a peer whose freshness
/// advertisement is recent (TTL) and whose node is Alive per SWIM may hold the round, and the
/// deferral counter must reset whenever the evidence lapses so the bound counts consecutive —
/// not lifetime — rounds.</para>
///
/// <para>The expected-leader freshness back-off is covered here too: zero activity, activity older
/// than one election timeout, and fresh activity that refreshes the follower's heartbeat anchor
/// forward to the genuine receipt tick — never to "now", which would extend the follower election
/// gate beyond what the leader actually said.</para>
/// </summary>
public class TestCandidacyDeferralGates
{
    private const string Peer = "node-b";

    // ── stalled local WAL write ───────────────────────────────────────────────

    /// <summary>
    /// A follower whose oldest pending WAL write is unanswered past the configured bound must not
    /// campaign: it would win on log freshness (a deposed leader's log is the freshest by
    /// construction) and then hold the partition again with a disk that is not answering. Unlike
    /// the gap deferral this one is unbounded by design — the condition is a local fact that clears
    /// the moment the write completes.
    /// </summary>
    [Fact]
    public async Task StalledLocalWalWrite_WithholdsCandidacy_BeyondAnyRoundCount_UntilTheWriteCompletes()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = Build(peers: [Peer]);
        host.Config.WalStallStepDownTimeout = TimeSpan.FromMilliseconds(200);
        wal.OldestPendingWriteAgeMs = 0;

        // Control: an unstalled follower opens a pre-vote round on the tick.
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // Stalled past the bound: no RequestVotes leaves the node, on every round — the count is
        // deliberately not bounded, because a healthy peer must be able to win any of these terms.
        wal.OldestPendingWriteAgeMs = 250;
        for (int round = 0; round < 12; round++)
        {
            host.Outbound.Clear();
            await sm.CheckPartitionLeadershipAsync();
            Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        }

        // The write completes: the very next tick campaigns.
        wal.OldestPendingWriteAgeMs = 0;
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// A stalled write below the configured bound must NOT defer — the gate is a disk-failure
    /// signal, not another timer, and an ordinary queued write must not delay failover.
    /// </summary>
    [Fact]
    public async Task PendingWriteYoungerThanTheBound_DoesNotDeferCandidacy()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = Build(peers: [Peer]);
        host.Config.WalStallStepDownTimeout = TimeSpan.FromMilliseconds(200);
        wal.OldestPendingWriteAgeMs = 150;

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// The application withheld this replica's candidacy: its projection is incomplete and a term
    /// it won would be served from that state. Suppression lasts exactly as long as the
    /// application's word — every round, unbounded — and the first tick after release campaigns.
    /// </summary>
    [Fact]
    public async Task WithheldCandidacy_SuppressesEveryRound_UntilTheApplicationReleasesIt()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _) = Build(peers: [Peer]);

        sm.SetCandidacyWithheld(true);
        Assert.True(sm.IsCandidacyWithheld);

        for (int round = 0; round < 12; round++)
        {
            host.Outbound.Clear();
            await sm.CheckPartitionLeadershipAsync();
            Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        }

        sm.SetCandidacyWithheld(false);
        Assert.False(sm.IsCandidacyWithheld);

        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    // ── holey-log deference is evidence-gated ────────────────────────────────

    /// <summary>
    /// A fresher voter's advertisement only counts while the voter is Alive per SWIM: a Dead (or
    /// departed) peer cannot win the term and backfill this node, so deferring to it would only
    /// suppress the nodes that could actually lead. The gapped node campaigns immediately.
    /// </summary>
    [Fact]
    public async Task GappedFollower_FresherVoterNotAlive_CampaignsImmediately()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = Build(peers: [Peer]);
        wal.HasPresenceGapValue = true;
        wal.PresentId = 3;

        // The peer advertises a fresher contiguous position on its pre-vote probe; the probe is
        // recorded as freshness evidence whether it is granted or denied.
        await sm.VoteAsync(new RaftNode(Peer), voteTerm: 2, remoteMaxLogId: 9, timestamp: Timestamp(host), preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        host.Liveness[Peer] = MemberLivenessState.Dead;

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// The advertisement also expires: a fresher position observed by this node more than the
    /// freshness TTL ago describes a peer this node may no longer even reach, and must not hold
    /// the deferral open forever.
    /// </summary>
    [Fact]
    public async Task GappedFresherVoterAdvertisement_PastTheTtl_CampaignsImmediately()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = Build(peers: [Peer]);
        wal.HasPresenceGapValue = true;
        wal.PresentId = 3;

        await sm.VoteAsync(new RaftNode(Peer), voteTerm: 2, remoteMaxLogId: 9, timestamp: Timestamp(host), preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        // Peer stays Alive; only its advertisement ages out.
        host.AdvanceMonotonic(TimeSpan.FromSeconds(31));

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// The deferral bound counts CONSECUTIVE rounds, not lifetime rounds: whenever the evidence
    /// lapses (here: the fresher voter goes Dead) the counter resets, so a voter that flaps can
    /// defer this node again for a full bound each time it is Alive. A counter that never reset
    /// would campaign on the eleventh Alive-round overall, silencing the deference while the
    /// fresher peer still needs the quiet to win its own term.
    /// </summary>
    [Fact]
    public async Task GappedFollower_DeferralCounterResets_WhenTheEvidenceLapses()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = Build(peers: [Peer]);
        wal.HasPresenceGapValue = true;
        wal.PresentId = 3;

        await sm.VoteAsync(new RaftNode(Peer), voteTerm: 2, remoteMaxLogId: 9, timestamp: Timestamp(host), preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        // Five deferral rounds while the fresher voter is Alive.
        for (int round = 0; round < 5; round++)
        {
            await sm.CheckPartitionLeadershipAsync();
            Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        }

        // The evidence lapses for one round: the voter goes Dead and this node campaigns...
        host.Liveness[Peer] = MemberLivenessState.Dead;
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // ...and returns: a reset counter defers for a full fresh bound —
        host.Liveness[Peer] = MemberLivenessState.Alive;
        for (int round = 0; round < 10; round++)
        {
            host.Outbound.Clear();
            await sm.CheckPartitionLeadershipAsync();
            Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        }

        // — before the eleventh consecutive Alive-round campaigns.
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    // ── the expected-leader freshness back-off ───────────────────────────────

    /// <summary>
    /// A node that has committed to a leader this term but never recorded activity from it must
    /// not treat the leadership as fresh: the back-off requires a genuine receipt tick, and zero
    /// means "never heard".
    /// </summary>
    [Fact]
    public async Task ExpectedLeaderWithNoRecordedActivity_Campaigns()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _) = Build(peers: [Peer]);
        await GrantVoteToPeerAsync(sm, host);

        // Past the recent-vote cooldown, with the leader's activity still unrecorded (tick 0).
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(sm.ElectionTimeout.TotalMilliseconds * 2 + 50));

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// Recorded activity older than one election timeout is stale: a leader this silent has
    /// forfeited the back-off and the follower must campaign.
    /// </summary>
    [Fact]
    public async Task ExpectedLeaderActivityOlderThanTheElectionTimeout_Campaigns()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _) = Build(peers: [Peer]);
        await GrantVoteToPeerAsync(sm, host);

        long now = host.MonotonicTicks!.Value;
        host.ActivityTicks[Peer] = now - ToTicks(TimeSpan.FromMilliseconds(sm.ElectionTimeout.TotalMilliseconds + 20));
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(sm.ElectionTimeout.TotalMilliseconds * 2 + 50));

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// Fresh expected-leader activity suppresses the election round, and once that activity is
    /// older than a full election timeout the follower must open a campaign — the back-off tracks
    /// the leader's actual last contact, not the tick that observed it.
    /// </summary>
    [Fact]
    public async Task FreshExpectedLeaderActivity_SuppressesThen_CampaignsWhenItStales()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _) = Build(peers: [Peer]);
        await GrantVoteToPeerAsync(sm, host);
        double timeoutMs = sm.ElectionTimeout.TotalMilliseconds;

        // The leader was heard from just now.
        host.ActivityTicks[Peer] = host.MonotonicTicks!.Value;

        await sm.CheckPartitionLeadershipAsync();
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // Two full timeouts past the receipt: beyond the gate under any anchoring, so the
        // follower must campaign.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(timeoutMs * 2 + 50));

        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static HLCTimestamp Timestamp(CapturingHost host) =>
        host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

    /// <summary>
    /// Grants a real vote to the peer so the expected-leader record and the recent-vote cooldown
    /// are both armed, and freezes a deterministic election timeout for the test to measure
    /// against (the grant re-draws it, so it must be read afterwards).
    /// </summary>
    private static async Task GrantVoteToPeerAsync(RaftPartitionStateMachine sm, CapturingHost host)
    {
        await sm.VoteAsync(new RaftNode(Peer), voteTerm: 2, remoteMaxLogId: 5, timestamp: Timestamp(host), preVote: false, remoteLastLogTerm: 1);
        host.Outbound.Clear();
    }

    private static long ToTicks(TimeSpan delta) => (long)(delta.TotalSeconds * Stopwatch.Frequency);

    private static (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) Build(string[] peers)
    {
        CapturingHost host = new() { Nodes = peers.Select(p => new RaftNode(p)).ToList() };
        ControllableWal wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        sm.MarkRestoredForTesting();
        sm.SetPostToExecutor(_ => { });
        return (sm, host, wal);
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
        };

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public List<RaftResponderRequest> Outbound { get; } = [];

        public Dictionary<string, MemberLivenessState> Liveness { get; } = [];

        /// <summary>Monotonic tick source frozen at a fixed base, advanced only by the test.</summary>
        public long? MonotonicTicks { get; set; } = Stopwatch.GetTimestamp();

        public long GetMonotonicTimestamp() => MonotonicTicks!.Value;

        public void AdvanceMonotonic(TimeSpan delta) => MonotonicTicks += ToTicks(delta);

        /// <summary>Receipt tick of the last recorded activity per endpoint; absent = never heard.</summary>
        public Dictionary<string, long> ActivityTicks { get; } = [];

        public long GetLastNodeActivityTicks(string endpoint, int partitionId) =>
            ActivityTicks.GetValueOrDefault(endpoint);

        public MemberLivenessState GetNodeLiveness(string endpoint) =>
            Liveness.TryGetValue(endpoint, out MemberLivenessState state) ? state : MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => Outbound.Add(request);

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;

        public Task InvokeLeadershipLost(int partitionId, long term) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// WAL facade whose presence gap, contiguous frontier and oldest-pending-write age are set by
    /// the test — the three local signals the candidacy gates read.
    /// </summary>
    private sealed class ControllableWal : IRaftWalFacade
    {
        private long _nextOperationId;

        public bool HasPresenceGapValue { get; set; }

        public long PresentId { get; set; } = -1;

        public double OldestPendingWriteAgeMs { get; set; }

        public bool HasPresenceGap() => HasPresenceGapValue;
        public long GetPresentIndex() => PresentId;
        public long GetPresentTerm() => 1;
        public double GetOldestPendingWriteAgeMs() => OldestPendingWriteAgeMs;

        public long GetCommitIndex() => 0;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
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

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
