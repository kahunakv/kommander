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
/// The election back-off that leans on evidence about OTHER nodes: fresh activity from the
/// expected leader suppresses a campaign even once the recent-vote cooldown has lapsed (and the
/// follower's heartbeat anchor moves forward to the genuine receipt tick, never beyond it), and
/// the holey-log deference counts freshness advertisements from VOTERS only — a learner or
/// removing replica can never win the term, so deferring to it would only silence the nodes
/// that could lead.
/// </summary>
public class TestExpectedLeaderBackoff
{
    private const string Voter = "node-b:9001";
    private const string Learner = "node-c:9002";

    // ── fresh expected-leader activity ────────────────────────────────────────

    /// <summary>
    /// A follower that granted a vote an election-timeout-and-a-half ago (past the 2× timeout
    /// recent-vote cooldown) must still not campaign while it heard from the leader it installed
    /// less than one election timeout ago: the back-off tracks the leader's actual last contact.
    /// Once that contact is older than a full timeout, the very next tick campaigns.
    /// </summary>
    [Fact]
    public async Task FreshLeaderActivity_PastTheVoteCooldown_SuppressesCampaign_ThenYieldsWhenItStales()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _) = Build([Voter]);
        await GrantVoteToPeerAsync(sm, host, Voter);
        double timeoutMs = sm.ElectionTimeout.TotalMilliseconds;

        // Past the 2× timeout cooldown (draw is within [100,101] ms), with leader activity 50 ms old.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(timeoutMs * 2 + 50));
        host.ActivityTicks[Voter] = host.MonotonicTicks!.Value - ToTicks(TimeSpan.FromMilliseconds(50));

        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // The anchor moved forward to the leader's genuine receipt tick; two full timeouts past
        // that contact are beyond the gate under any anchoring, so the follower must campaign.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(timeoutMs * 2 + 50));
        host.Outbound.Clear();

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    // ── holey-log deference counts voters only ───────────────────────────────

    /// <summary>
    /// A fresher contiguous position advertised by a NON-voter (a Learner or Removing replica of
    /// this range) must not hold a gapped node's candidacy: that peer cannot win the term and
    /// backfill the hole, so deferring to it would only suppress the nodes that can. The same
    /// advertisement from a committed voter does hold the round.
    /// </summary>
    [Fact]
    public async Task HoleyLogDeference_IgnoresFresherAdvertisementsFromNonVoters()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = Build([Voter, Learner]);
        host.NonVoterEndpoints.Add(Learner);
        wal.HasPresenceGapValue = true;
        wal.PresentId = 3;

        // The learner's pre-vote probe advertises position 9; the probe is recorded as freshness
        // evidence even though the pre-vote itself is denied to a non-voter.
        await sm.VoteAsync(new RaftNode(Learner), voteTerm: 2, remoteMaxLogId: 9, timestamp: Timestamp(host), preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);

        // Control: the same advertisement from the voter does defer the gapped node.
        await sm.VoteAsync(new RaftNode(Voter), voteTerm: 2, remoteMaxLogId: 9, timestamp: Timestamp(host), preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// While the deference holds, the round is named at Information with the contiguous frontier
    /// and the bound, so a cluster stuck behind the gate can be diagnosed from the log rather
    /// than reproduced.
    /// </summary>
    [Fact]
    public async Task HoleyLogDeferenceNamesTheHoleAndTheBoundInTheLog()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = BuildWithCapturedLog([Voter], out List<string> messages);
        wal.HasPresenceGapValue = true;
        wal.PresentId = 3;

        await sm.VoteAsync(new RaftNode(Voter), voteTerm: 2, remoteMaxLogId: 9, timestamp: Timestamp(host), preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        Assert.Contains(messages, m => m.Contains("Deferring candidacy") && m.Contains("contiguous through 3"));
    }

    /// <summary>
    /// A voter without a leader for longer than three election timeouts explains itself at Warning:
    /// the pre-vote rounds it starts, the reason it does not campaign, and the pre-votes it denies.
    /// Before that bound the same events stay at Information, so a routine failover logs nothing
    /// new. The CamusDB fault soak of 2026-10-30 stayed leaderless for 245 s with two voters alive
    /// and a Warning-level log that held only transport errors.
    /// </summary>
    [Fact]
    public async Task LeaderlessBeyondThreeTimeouts_NamesTheRoundsTheHeldCandidacyAndTheDenialsAtWarning()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = BuildWithCapturedLog([Voter], out List<string> messages);
        wal.PresentId = 5;
        int endTimeoutMs = host.Config.EndElectionTimeout;

        // First trigger: no leader was ever heard, so the episode is anchored here — Information only.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(endTimeoutMs + 50));
        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        Assert.DoesNotContain(messages, m => m.Contains("No leader for"));

        // Past three timeouts without a leader: the round start is a Warning that says how long.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(endTimeoutMs * 3 + 50));
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        Assert.Contains(messages, m => m.Contains("No leader for") && m.Contains("starting pre-vote round"));

        // A peer whose log is behind is denied, and the denial names the reason.
        await sm.VoteAsync(new RaftNode(Voter), voteTerm: 2, remoteMaxLogId: 1, timestamp: Timestamp(host), preVote: true, remoteLastLogTerm: 1);

        Assert.Contains(messages, m => m.Contains("No leader for") && m.Contains("denying pre-vote") && m.Contains("outdated log"));

        // The application withholds candidacy: the held round names that too, every round.
        sm.SetCandidacyWithheld(true);
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(endTimeoutMs + 50));
        host.Outbound.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        Assert.Contains(messages, m => m.Contains("No leader for") && m.Contains("not campaigning") && m.Contains("withheld"));
    }

    /// <summary>
    /// The application's candidacy word is a plain volatile flag: setting it to the value it
    /// already holds changes nothing, and the latest call wins.
    /// </summary>
    [Fact]
    public void CandidacyWithheld_ReflectsTheLatestApplicationWord()
    {
        (RaftPartitionStateMachine sm, _, _) = Build([Voter]);

        sm.SetCandidacyWithheld(false);
        Assert.False(sm.IsCandidacyWithheld);

        sm.SetCandidacyWithheld(true);
        Assert.True(sm.IsCandidacyWithheld);

        sm.SetCandidacyWithheld(true);
        sm.SetCandidacyWithheld(false);
        Assert.False(sm.IsCandidacyWithheld);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static HLCTimestamp Timestamp(CapturingHost host) =>
        host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

    private static async Task GrantVoteToPeerAsync(RaftPartitionStateMachine sm, CapturingHost host, string peer)
    {
        await sm.VoteAsync(new RaftNode(peer), voteTerm: 2, remoteMaxLogId: 5, timestamp: Timestamp(host), preVote: false, remoteLastLogTerm: 1);
        host.Outbound.Clear();
    }

    private static long ToTicks(TimeSpan delta) => (long)(delta.TotalSeconds * Stopwatch.Frequency);

    private static (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) Build(string[] peers) =>
        BuildWithCapturedLog(peers, out _);

    private static (RaftPartitionStateMachine Sm, CapturingHost Host, ControllableWal Wal) BuildWithCapturedLog(
        string[] peers, out List<string> messages)
    {
        CapturingHost host = new() { Nodes = peers.Select(p => new RaftNode(p)).ToList() };
        ControllableWal wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), CapturingLoggerFactory.Create<IRaft>(out messages));
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

        public HashSet<string> NonVoterEndpoints { get; } = [];

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

        /// <summary>Receipt tick of the last recorded activity per endpoint; absent = never heard.</summary>
        public Dictionary<string, long> ActivityTicks { get; } = [];

        public long GetLastNodeActivityTicks(string endpoint, int partitionId) =>
            ActivityTicks.GetValueOrDefault(endpoint);

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
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// WAL facade whose presence gap and contiguous frontier are set by the test — the two local
    /// signals the holey-log deference reads.
    /// </summary>
    private sealed class ControllableWal : IRaftWalFacade
    {
        private long _nextOperationId;

        public bool HasPresenceGapValue { get; set; }

        public long PresentId { get; set; } = -1;

        public bool HasPresenceGap() => HasPresenceGapValue;
        public long GetPresentIndex() => PresentId;
        public long GetPresentTerm() => 1;

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

/// <summary>
/// Logger factory whose loggers capture every formatted Information-or-higher message, for tests
/// that assert on diagnostics lines the state machine emits.
/// </summary>
internal static class CapturingLoggerFactory
{
    public static ILogger<T> Create<T>(out List<string> messages)
    {
        messages = [];
        List<string> sink = messages;
        return LoggerFactory.Create(b => b
                .SetMinimumLevel(LogLevel.Information)
                .AddProvider(new Provider(sink)))
            .CreateLogger<T>();
    }

    private sealed class Provider(List<string> messages) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new Logger(messages);

        public void Dispose() { }

        private sealed class Logger(List<string> messages) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

            public void Log<TState>(
                LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Information)
                    messages.Add(formatter(state, exception));
            }
        }
    }
}
