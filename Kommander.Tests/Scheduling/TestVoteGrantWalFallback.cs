using System.Diagnostics;
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// What a voter does around granting a vote when storage misbehaves or the candidate keeps
/// asking: a WAL scheduler that refuses the queued hard-state write must fall back to the
/// synchronous persist and still send the grant (a withheld vote costs an election round), and a
/// candidate that re-asks in a higher term after a grant that produced no leader must NOT re-arm
/// the voter's 2× timeout cooldown — otherwise a failing candidate silences the voters it needs.
/// </summary>
public class TestVoteGrantWalFallback
{
    private const string Peer = "node-b:9001";

    /// <summary>
    /// When the WAL scheduler refuses to queue the hard-state write (its backpressure budget is
    /// spent), the vote grant falls back to the synchronous persist. The grant must still leave,
    /// and it must carry exactly the (term, candidate) pair the durable write recorded.
    /// </summary>
    [Fact]
    public async Task WalQueueBackpressureOnVoteGrant_FallsBackToSynchronousPersist_AndStillGrants()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) = Build();
        wal.ThrowOnEnqueueHardState = true;

        await sm.VoteAsync(new RaftNode(Peer), voteTerm: 3, remoteMaxLogId: 5, timestamp: Timestamp(host), preVote: false, remoteLastLogTerm: 1);

        Assert.True(
            host.Outbound.Any(m => m.Type == RaftResponderRequestType.Vote && m.VoteRequest is { PreVote: false, Term: 3 }),
            "the grant must still be sent once the write is durable the synchronous way");
        Assert.Contains((3L, Peer), wal.PersistedHardStates);
    }

    /// <summary>
    /// The candidate the voter backed failed to win and asks again in a higher term. The voter
    /// grants again, but its recent-vote cooldown stays anchored at the FIRST grant: re-arming it
    /// on every fruitless re-ask made the voter's silence track the candidate's failures exactly,
    /// and the only other live voter never campaigned. The check lands between the two cooldown
    /// windows — past 2× the timeout measured from the first grant, still inside 2× measured from
    /// the second — so a re-armed anchor fails the test.
    /// </summary>
    [Fact]
    public async Task RepeatGrantWithNoLeaderContact_KeepsTheCooldownAnchorAtTheFirstGrant()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal _) = Build();

        await sm.VoteAsync(new RaftNode(Peer), voteTerm: 2, remoteMaxLogId: 5, timestamp: Timestamp(host), preVote: false, remoteLastLogTerm: 1);

        // No leader is ever heard. The failed candidate re-asks in term 3, 50 ms later.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(50));
        await sm.VoteAsync(new RaftNode(Peer), voteTerm: 3, remoteMaxLogId: 5, timestamp: Timestamp(host), preVote: false, remoteLastLogTerm: 1);
        Assert.True(
            host.Outbound.Any(m => m.Type == RaftResponderRequestType.Vote && m.VoteRequest is { Term: 3 }),
            "the repeat grant is still sent; only the cooldown anchor differs");

        // The timeout draw lives in [100, 101) ms: 225 ms after the first grant is always past
        // 2× it, and 175 ms after the second is always inside 2× of that one.
        host.AdvanceMonotonic(TimeSpan.FromMilliseconds(175));
        host.Outbound.Clear();

        await sm.CheckPartitionLeadershipAsync();

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static HLCTimestamp Timestamp(CapturingHost host) =>
        host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

    private static long ToTicks(TimeSpan delta) => (long)(delta.TotalSeconds * Stopwatch.Frequency);

    private static (RaftPartitionStateMachine sm, CapturingHost host, ControllableWal wal) Build()
    {
        CapturingHost host = new() { Nodes = [new RaftNode(Peer)] };
        ControllableWal wal = new();
        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), CapturingLoggerFactory.Create<IRaft>(out _));
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
        public string LocalEndpoint => "node-a:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
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
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// WAL facade whose hard-state paths are directed by the test: the queued enqueue either
    /// returns nothing or throws the scheduler's backpressure exception, and the synchronous
    /// persist records every (term, votedFor) pair it was asked to make durable.
    /// </summary>
    private sealed class ControllableWal : IRaftWalFacade
    {
        private long _nextOperationId;

        public bool ThrowOnEnqueueHardState { get; set; }

        public bool PersistHardStateResult { get; set; } = true;

        public List<(long Term, string? VotedFor)> PersistedHardStates { get; } = [];

        public WALWriteOperation? TryEnqueueHardState(long currentTerm, string? votedFor)
        {
            if (ThrowOnEnqueueHardState)
                throw new BackpressureExceededException(partitionId: 1, currentDepth: 9999);

            return null;
        }

        public ValueTask<bool> PersistHardStateAsync(long currentTerm, string? votedFor)
        {
            PersistedHardStates.Add((currentTerm, votedFor));
            return ValueTask.FromResult(PersistHardStateResult);
        }

        public long GetCommitIndex() => 0;
        public long GetPresentIndex() => -1;
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
