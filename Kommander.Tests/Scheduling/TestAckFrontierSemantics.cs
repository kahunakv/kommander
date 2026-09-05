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
/// A leader's record of a follower's commit frontier must only ever hold values the follower
/// actually reported about its own frontier, and must remain correctable by later reports.
///
/// <para>
/// The <c>committedIndex</c> field of a <c>CompleteAppendLogs</c> acknowledgement is overloaded:
/// on <see cref="RaftOperationStatus.Success"/> it carries the follower's gap-aware committed
/// frontier, but rejection acks (<see cref="RaftOperationStatus.LogMismatch"/>,
/// <see cref="RaftOperationStatus.FollowerWalSaturated"/>) reuse it for the follower's raw max
/// log id — which sits arbitrarily far above the frontier whenever the log has an uncommitted or
/// non-contiguous tail. Folding a rejection's value into <c>lastCommitIndexes</c> pinned an
/// over-estimate that the old monotonic guard let no later truthful (lower) report correct:
/// <c>SendHeartbeat</c> computed <c>followerGap ≈ 0</c> and never backfilled the peer.
/// </para>
///
/// <para>
/// That is the Jepsen stranded-replica shape: a follower whose commit frontier stalls behind a
/// lost commit marker while the unanchored live-propose broadcast keeps growing its log — its
/// max log tracks the leader's tip, so a single rejection ack makes the leader believe the peer
/// is current forever, and the entries keep replicating but are never committed.
/// </para>
/// </summary>
public class TestAckFrontierSemantics
{
    private const string VoterA    = "follower-a:9001";
    private const string VoterB    = "follower-b:9002";
    private const string NonVoter  = "follower-c:9003";
    private const string NonVoter2 = "follower-d:9004";

    /// <summary>
    /// The reproduction. A follower's frontier is stalled at 365 while its log has grown to the
    /// leader's tip (383) via the unanchored live-propose broadcast. A LogMismatch rejection
    /// reports that raw max log; the leader must not mistake it for the peer's committed
    /// frontier, or the following truthful report can never re-open the gap and the heartbeat
    /// round sends nothing forever.
    /// </summary>
    [Fact]
    public async Task Leader_StillBackfillsAfterALogMismatchAckReportedThePeersMaxLog()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        // Truthful self-report: frontier stalled 18 entries behind.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 365);

        // Rejection ack: the committedIndex field here is the follower's raw max log (383),
        // NOT a frontier report — its log grew while its frontier did not.
        await Ack(sm, host, VoterA, RaftOperationStatus.LogMismatch, 383);

        // The follower keeps telling the truth.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 365);

        Assert.Equal(365, sm.GetFollowerCommittedIndex(VoterA));

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(BatchesTo(host, VoterA) > 0,
            "a LogMismatch ack carries the follower's max log id, not its committed frontier; " +
            "recording it as the frontier hides the peer's real gap from the backfill gate and " +
            "strands the replica with a growing log it never commits");
    }

    /// <summary>
    /// Same defect through the other rejection path: a saturation ack also reports the raw max
    /// log. The saturation backoff is zeroed so the assertion sees the very next heartbeat round
    /// rather than the (legitimate) cooldown.
    /// </summary>
    [Fact]
    public async Task Leader_StillBackfillsAfterASaturationAckReportedThePeersMaxLog()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);
        host.Configuration.FollowerSaturationBackoff = TimeSpan.Zero;

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 365);
        await Ack(sm, host, VoterA, RaftOperationStatus.FollowerWalSaturated, 383);
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 365);

        Assert.Equal(365, sm.GetFollowerCommittedIndex(VoterA));

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(BatchesTo(host, VoterA) > 0,
            "a FollowerWalSaturated ack carries the follower's max log id, not its committed " +
            "frontier; a saturated peer is exactly the one whose frontier is most likely stalled, " +
            "so recording its max log as progress strands it once the pressure clears");
    }

    /// <summary>
    /// The frontier record is last-writer-wins: a follower that crash-restarted and reconstructed
    /// a lower frontier must be able to LOWER the leader's record so its gap becomes visible to
    /// the heartbeat backfill gate again, not only to the separate regression-note path.
    /// </summary>
    [Fact]
    public async Task Leader_AcceptsAFrontierRegressionReport()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 383);
        Assert.Equal(383, sm.GetFollowerCommittedIndex(VoterA));

        // Crash-restart: lazy commit markers lost, frontier reconstructed 18 entries lower.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 365);
        Assert.Equal(365, sm.GetFollowerCommittedIndex(VoterA));

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(BatchesTo(host, VoterA) > 0,
            "a lower Success self-report is the follower's word about its own frontier and must " +
            "re-open the gap; discarding it as stale leaves the regressed range unsupplied");
    }

    /// <summary>
    /// The -1 sentinel is ambiguous — a truthful "nothing committed" on the single-fsync path,
    /// but merely "no report" from a legacy-path heartbeat ack — so it seeds an unknown peer
    /// (SendHeartbeat's lag check needs the key present) yet never erases a real frontier.
    /// </summary>
    [Fact]
    public async Task MinusOne_SeedsAnUnknownPeer_ButDoesNotEraseARecordedFrontier()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        // Unknown peer: the -1 ack is its first contact and must enter the map.
        await Ack(sm, host, NonVoter, RaftOperationStatus.Success, -1);
        Assert.Equal(-1, sm.GetFollowerCommittedIndex(NonVoter));

        // Known peer: a later -1 must not clobber the recorded frontier.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 365);
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, -1);
        Assert.Equal(365, sm.GetFollowerCommittedIndex(VoterA));
    }

    /// <summary>
    /// The quiesce gate reads the same frontier map, and quiescing is the most catastrophic
    /// consumer of a polluted record: <c>HasLaggingPeer</c> deciding "nobody lags" stops
    /// heartbeats entirely, which also stops the very acks whose truthful reports could correct
    /// the record — an absolute strand, not just a delayed one. A rejection ack that reported the
    /// stalled peer's max log must therefore not keep a quiesced leader asleep.
    /// </summary>
    [Fact]
    public async Task QuiescedLeader_RearmsWhenAPeersTruthfulFrontierLags()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        // Shrink to the two peers with recorded progress: an unseeded peer counts as lagging by
        // design, which would re-arm heartbeats regardless and mask what this test pins.
        host.Nodes = [new(VoterA), new(VoterB)];

        await Ack(sm, host, VoterB, RaftOperationStatus.Success, 383);   // genuinely caught up
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, 365);   // stalled frontier
        await Ack(sm, host, VoterA, RaftOperationStatus.LogMismatch, 383); // its grown max log

        sm.SetQuiescedForTesting(true);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.True(BatchesTo(host, VoterA) > 0,
            "a quiesced leader must re-arm heartbeats while a peer's self-reported frontier lags; " +
            "believing a rejection ack's max log here silences the partition permanently");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(RaftPartitionStateMachine, CapturingHost)> BuildLeader(long commitIndex)
    {
        CapturingHost host = new();
        StubWal wal = new(commitIndex);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        long term = sm.CurrentTerm;
        await sm.ReceivedVoteAsync(VoterA, term, commitIndex);
        await sm.ReceivedVoteAsync(VoterB, term, commitIndex);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);

        return (sm, host);
    }

    private static ValueTask Ack(RaftPartitionStateMachine sm, CapturingHost host, string endpoint,
                                 RaftOperationStatus status, long committedIndex) =>
        sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                   status, committedIndex);

    private static int BatchesTo(CapturingHost host, string endpoint) =>
        host.Requests.Count(r => r.Node?.Endpoint == endpoint
                                 && r.AppendLogsRequest?.Logs is { Count: > 0 });

    // ── stubs (same shape as TestBackfillFrontierSeeding) ────────────────────

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
