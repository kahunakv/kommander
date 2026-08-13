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
/// A newly elected leader must not end up believing a follower is further along than it is, in a
/// way no later acknowledgement can correct.
///
/// <para>
/// Two mechanisms meet here. <c>ReceivedVoteAsync</c> seeds <c>lastCommitIndexes[voter]</c> from
/// the vote's <c>remoteMaxLogId</c> — the voter's highest *log* id, which is at or above the
/// frontier it has actually applied. <c>CompleteAppendLogsAsync</c> then updates that same map
/// only when an acknowledgement reports a *higher* value. A follower whose applied frontier is
/// below the seeded log id therefore reports the truth on every heartbeat and is ignored every
/// time, while <c>SendHeartbeat</c> computes <c>followerGap</c> from the seeded over-estimate and
/// concludes there is nothing to send.
/// </para>
///
/// <para>
/// The result is a replica that never receives another entry and never recovers on its own,
/// matching Jepsen runs where four replicas sat 18 entries behind for three minutes on a healed,
/// idle cluster with a working transport and no WAL pressure.
/// </para>
/// </summary>
public class TestBackfillFrontierSeeding
{
    // Five-node partition, as the Jepsen cluster runs: quorum is 3, so a leader is elected on
    // its own vote plus two grants and the remaining two peers never vote at all.
    private const string VoterA   = "follower-a:9001";
    private const string VoterB   = "follower-b:9002";
    private const string NonVoter = "follower-c:9003";
    private const string NonVoter2 = "follower-d:9004";

    /// <summary>
    /// The reproduction: elect a leader on a vote that reports a high max log id, then have that
    /// peer report a materially lower applied frontier, and assert the leader backfills the gap.
    /// </summary>
    [Fact]
    public async Task Leader_BackfillsAPeerWhoseAppliedFrontierIsBelowItsVotedMaxLogId()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRestored(commitIndex: 383);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

        // The voter's log reaches 383 — but a log id is not an applied frontier, and this is the
        // value the leader records as the peer's progress.
        await ElectOn(sm, remoteMaxLogId: 383);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        // The peer now reports what it has actually applied: 18 entries short.
        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 365);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(BatchesTo(host, VoterA) > 0,
            "a peer 18 entries behind must be backfilled; if it is not, its own acknowledgements " +
            "can never change the leader's mind and it stays behind permanently");
    }

    /// <summary>
    /// Control. A peer the leader has no record of is repaired by its very first acknowledgement:
    /// the ack sets <c>lastCommitIndexes</c>, and the next round backfills it.
    /// </summary>
    /// <remarks>
    /// This is what makes the failing test above specific. The repair channel works — an
    /// acknowledgement does update the leader's view and does trigger a backfill — so the defect
    /// is not "acks are ignored" in general. It is that an ack reporting a *lower* frontier than
    /// the recorded one is discarded by the monotonic guard, which is precisely the case a
    /// vote-seeded over-estimate creates and the one case the peer cannot talk its way out of.
    /// </remarks>
    [Fact]
    public async Task Leader_RepairsAnUnseededPeerFromItsFirstAck()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRestored(commitIndex: 383);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await ElectOn(sm, remoteMaxLogId: 383);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        // NonVoter was never seeded, so this ack is the leader's first record of it — and being
        // the first, it is accepted rather than compared against a higher value.
        await sm.CompleteAppendLogsAsync(NonVoter, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 365);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(BatchesTo(host, NonVoter) > 0,
            "an unseeded peer must be caught up once it reports where it is");
    }

    /// <summary>
    /// Control: a peer whose reported frontier is genuinely close to the leader's is correctly left
    /// alone. Without this, the two tests above would pass on any change that simply backfills
    /// everyone every round.
    /// </summary>
    [Fact]
    public async Task Leader_DoesNotBackfillAPeerThatIsCaughtUp()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildRestored(commitIndex: 383);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await ElectOn(sm, remoteMaxLogId: 383);

        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                         RaftOperationStatus.Success, committedIndex: 383);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, BatchesTo(host, VoterA));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// <summary>Grants the two votes that carry this candidate to leader at its current term.</summary>
    private static async Task ElectOn(RaftPartitionStateMachine sm, long remoteMaxLogId)
    {
        long term = sm.CurrentTerm;
        await sm.ReceivedVoteAsync(VoterA, term, remoteMaxLogId);
        await sm.ReceivedVoteAsync(VoterB, term, remoteMaxLogId);
    }

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

    private static int BatchesTo(CapturingHost host, string endpoint) =>
        host.Requests.Count(r => r.Node?.Endpoint == endpoint
                                 && r.AppendLogsRequest?.Logs is { Count: > 0 });

    // ── stubs ────────────────────────────────────────────────────────────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1, BackfillThreshold = 10,
            HeartbeatInterval = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new(VoterA), new(VoterB), new(NonVoter), new(NonVoter2)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
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
