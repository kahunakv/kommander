using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Raft §5.1 over the append-acknowledgement channel: a response stamped with a term higher than
/// the sender's must depose the sender.
///
/// <para>
/// This is the repair channel for the Jepsen frozen-frontier stall. A follower whose per-partition
/// term was bumped by a failed election rejects every AppendLogs — heartbeats and backfill alike —
/// with <see cref="RaftOperationStatus.LeaderInOldTerm"/>, at the very first guard, before the WAL
/// is touched. Pre-vote (correctly) prevents that node from winning an election against the still-
/// healthy quorum, so its higher term cannot propagate by election either. Unless the leader steps
/// down when the rejection reaches it, the wedge is permanent: the leader ships backfill forever
/// (<c>send=True</c> every round), the follower rejects it forever, and that partition's replica on
/// the bumped node never commits another entry while its log keeps growing — with another partition
/// on the same node converging fine, because terms are per-partition.
/// </para>
///
/// <para>
/// Two halves make the loop close: the follower's old-term rejection must carry its own
/// <c>currentTerm</c> (not an echo of the sender's stale term), and the leader must adopt a higher
/// response term with a durable step-down instead of discarding the ack at the stale-term fence.
/// </para>
/// </summary>
public class TestHigherTermAppendAck
{
    private const string VoterA    = "follower-a:9001";
    private const string VoterB    = "follower-b:9002";
    private const string NonVoter  = "follower-c:9003";
    private const string NonVoter2 = "follower-d:9004";

    /// <summary>
    /// The leader half: a LeaderInOldTerm ack carrying a higher term deposes the leader and the
    /// higher term is adopted, so the next election can converge the partition.
    /// </summary>
    [Fact]
    public async Task Leader_StepsDownAndAdoptsTermOnHigherTermAppendAck()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);
        long term = sm.CurrentTerm;

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.LeaderInOldTerm, committedIndex: -1,
                                         responseTerm: term + 3);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(term + 3, sm.CurrentTerm);
        Assert.Equal("", host.Leader);
    }

    /// <summary>
    /// Control for the fence interplay: a same-term ack still flows through the normal progress
    /// path, and a LOWER stale term still cannot depose anyone. Without this, the step-down could
    /// pass on any change that simply tears down leadership on every non-Success ack.
    /// </summary>
    [Fact]
    public async Task Leader_IsNotDeposedBySameOrLowerTermAcks()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);
        long term = sm.CurrentTerm;

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 383, responseTerm: term);
        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.LeaderInOldTerm, committedIndex: -1,
                                         responseTerm: term - 1 >= 0 ? term - 1 : 0);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal(term, sm.CurrentTerm);
    }

    /// <summary>
    /// The membership fence: an endpoint outside the committed roster must not be able to depose a
    /// leader with a fabricated high term, mirroring the non-member fence on inbound AppendLogs.
    /// </summary>
    [Fact]
    public async Task Leader_IgnoresHigherTermAckFromNonMember()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);
        long term = sm.CurrentTerm;

        host.Members = [VoterA, VoterB, NonVoter, NonVoter2];

        await sm.CompleteAppendLogsAsync("intruder:9999", host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.LeaderInOldTerm, committedIndex: -1,
                                         responseTerm: term + 50);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal(term, sm.CurrentTerm);
    }

    /// <summary>
    /// The follower half: the old-term rejection must report the follower's own (higher) term —
    /// it is the only channel through which the stale leader can learn that term exists.
    /// </summary>
    [Fact]
    public async Task Follower_ReportsItsOwnTermInOldTermRejection()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRestored(commitIndex: 0);

        // Adopt a leader at term 5 via a normal heartbeat.
        await sm.AppendLogsAsync(VoterB, 5, host.HybridLogicalClock.TrySendOrLocalEvent(1), null);
        Assert.Equal(5, sm.CurrentTerm);

        // A deposed leader still sending at term 3 must be rejected with OUR term, not its own.
        host.Requests.Clear();
        await sm.AppendLogsAsync(VoterA, 3, host.HybridLogicalClock.TrySendOrLocalEvent(1), null);

        CompleteAppendLogsRequest? rejection = host.Requests
            .Select(r => r.CompleteAppendLogsRequest)
            .FirstOrDefault(r => r?.Status == RaftOperationStatus.LeaderInOldTerm);

        Assert.NotNull(rejection);
        Assert.Equal(5, rejection!.Term);
    }

    /// <summary>
    /// The loop closed end-to-end at the state-machine level: the wedged follower's rejection,
    /// fed back to the leader, deposes it — the exact exchange that was previously a permanent
    /// send-forever / reject-forever cycle.
    /// </summary>
    [Fact]
    public async Task WedgedFollowerRejection_DeposesTheStaleLeader()
    {
        (RaftPartitionStateMachine leader, CapturingHost leaderHost) = await BuildLeader(commitIndex: 383);
        (RaftPartitionStateMachine follower, CapturingHost followerHost) = await BuildRestored(commitIndex: 383);

        // The follower's term was bumped past the leader's by a failed election during a fault.
        long bumpedTerm = leader.CurrentTerm + 2;
        await follower.AppendLogsAsync(NonVoter, bumpedTerm, followerHost.HybridLogicalClock.TrySendOrLocalEvent(2), null);
        Assert.Equal(bumpedTerm, follower.CurrentTerm);

        // The stale leader ships a backfill heartbeat; the follower rejects with its higher term.
        followerHost.Requests.Clear();
        await follower.AppendLogsAsync(leaderHost.LocalEndpoint, leader.CurrentTerm,
                                       followerHost.HybridLogicalClock.TrySendOrLocalEvent(2), null);

        CompleteAppendLogsRequest? rejection = followerHost.Requests
            .Select(r => r.CompleteAppendLogsRequest)
            .FirstOrDefault(r => r?.Status == RaftOperationStatus.LeaderInOldTerm);
        Assert.NotNull(rejection);

        // Relay the rejection to the leader, as the responder/transport would.
        await leader.CompleteAppendLogsAsync(rejection!.Endpoint, rejection.Time, rejection.Status,
                                             rejection.CommitIndex, rejection.Term);

        Assert.Equal(RaftNodeState.Follower, leader.NodeState);
        Assert.Equal(bumpedTerm, leader.CurrentTerm);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildRestored(long commitIndex)
    {
        CapturingHost host = new();
        StubWal wal = new(commitIndex);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        return (sm, host);
    }

    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildLeader(long commitIndex)
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRestored(commitIndex);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        long term = sm.CurrentTerm;
        await sm.ReceivedVoteAsync(VoterA, term, commitIndex);
        await sm.ReceivedVoteAsync(VoterB, term, commitIndex);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        return (sm, host);
    }

    // ── stubs (same shape as TestAckFrontierSemantics; adds a settable member roster) ─────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        /// <summary>Null = every endpoint is a member (the interface default); set to fence.</summary>
        public HashSet<string>? Members { get; set; }

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public bool IsMember(string endpoint) => Members is null || Members.Contains(endpoint);

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1, BackfillThreshold = 10,
            HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB), new(NonVoter), new(NonVoter2)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) => Requests.Add(r);
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(true));
    }

    private sealed class StubWal : IRaftWalFacade
    {
        private readonly long commitIndex;

        public StubWal(long commitIndex) => this.commitIndex = commitIndex;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            List<RaftLog> batch = [];
            for (long id = startLogIndex; id < startLogIndex + 3 && id <= commitIndex; id++)
                batch.Add(new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "test" });

            return ValueTask.FromResult(batch);
        }

        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(0L);
        public long GetCommitIndex() => commitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
