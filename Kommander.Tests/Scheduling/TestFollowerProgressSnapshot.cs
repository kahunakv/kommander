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
/// The leader publishes what each follower's acknowledgement says about the follower's disk — its
/// durable frontier and its pending-write age — as a <see cref="RaftFollowerProgress"/> snapshot that an
/// application can read without an executor round-trip (<see cref="IRaft.GetFollowerProgress"/>).
///
/// <para>Why it matters: a protocol that waits on a follower (Kahuna's replica fence waits for a
/// follower to apply a prepare and attest) has no way to tell, from probe latency alone, that a
/// follower is tens of thousands of entries behind or that its disk is paused — it answers probes
/// promptly from stale memory in both cases (CamusDB leader-kill run lk8: a replica re-attested on three
/// fast probes while 75,000 entries behind, and every commit then waited on it). The leader knows both
/// facts from every ack; the snapshot hands them out. These tests pin what the snapshot carries, that it
/// is a leader-only fact, and that it goes away with the leadership.</para>
/// </summary>
public class TestFollowerProgressSnapshot
{
    private const string VoterA = "follower-a:9001";
    private const string VoterB = "follower-b:9002";

    [Fact]
    public async Task EveryTermValidAck_PublishesTheFollowersDurableFrontierAndStallAge()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 380, durableIndex: 375, walStallMs: 12);

        RaftFollowerProgress progress = Assert.Contains(VoterA, host.Progress);
        Assert.Equal(sm.CurrentTerm, progress.Term);
        Assert.Equal(375, progress.DurableFrontier);
        Assert.Equal(12, progress.WalStallMs);
        Assert.False(progress.WalStalled, "12 ms is below the 500 ms stall threshold");
        Assert.Equal(383, progress.LeaderCommitIndex);
        Assert.Equal(8, progress.EntriesBehind(383));
        Assert.True(progress.ReportedAtTicks > 0);

        // The protocol frontier lags one ack: this ack's own Success advance folds after publication.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 381, durableIndex: 379, walStallMs: 0);
        Assert.Equal(380, host.Progress[VoterA].ProtocolFrontier);
        Assert.Equal(379, host.Progress[VoterA].DurableFrontier);

        // A rejection ack still carries the disk facts and refreshes the snapshot.
        await Ack(sm, host, VoterA, RaftOperationStatus.LogMismatch, committedIndex: 383, durableIndex: 379, walStallMs: 3);
        Assert.Equal(3, host.Progress[VoterA].WalStallMs);
        Assert.Equal(379, host.Progress[VoterA].DurableFrontier);
    }

    [Fact]
    public async Task AnUnreportedDurableFrontier_CountsAsTheWholeLogBehind()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 383, durableIndex: -1, walStallMs: 0);

        RaftFollowerProgress progress = host.Progress[VoterA];
        Assert.Equal(-1, progress.DurableFrontier);
        Assert.Equal(383, progress.EntriesBehind(383));
    }

    [Fact]
    public async Task AStallEpisode_IsFlaggedFromTheThresholdUntilTheAckThatEndsIt()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);
        host.Configuration.WalStallWarnThreshold = TimeSpan.FromMilliseconds(100);

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 300, durableIndex: 300, walStallMs: 40);
        Assert.False(host.Progress[VoterA].WalStalled);

        // The follower's disk stops answering: its protocol frontier keeps climbing (appends are queued),
        // its durable frontier stands still, and the reported age crosses the threshold.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 340, durableIndex: 300, walStallMs: 150);
        Assert.True(host.Progress[VoterA].WalStalled);
        Assert.Equal(150, host.Progress[VoterA].WalStallMs);
        Assert.Equal(300, host.Progress[VoterA].DurableFrontier);

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 383, durableIndex: 300, walStallMs: 2_000);
        Assert.True(host.Progress[VoterA].WalStalled);

        // The disk answers: the first ack below the threshold ends the episode in the snapshot too.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 383, durableIndex: 383, walStallMs: 5);
        Assert.False(host.Progress[VoterA].WalStalled);
        Assert.Equal(383, host.Progress[VoterA].DurableFrontier);
    }

    [Fact]
    public async Task FollowersArePublishedIndependently_AndTheLeaderNeverPublishesItself()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 383, durableIndex: 383, walStallMs: 0);
        await Ack(sm, host, VoterB, RaftOperationStatus.Success, committedIndex: 200, durableIndex: 190, walStallMs: 0);
        await Ack(sm, host, host.LocalEndpoint, RaftOperationStatus.Success, committedIndex: 383, durableIndex: 383, walStallMs: 0);

        Assert.Equal(383, host.Progress[VoterA].DurableFrontier);
        Assert.Equal(190, host.Progress[VoterB].DurableFrontier);
        Assert.DoesNotContain(host.LocalEndpoint, host.Progress.Keys);
    }

    [Fact]
    public async Task SteppingDown_ClearsEveryPublishedSnapshot()
    {
        (RaftPartitionStateMachine sm, CapturingHost host) = await BuildLeader(commitIndex: 383);

        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 383, durableIndex: 383, walStallMs: 0);
        await Ack(sm, host, VoterB, RaftOperationStatus.Success, committedIndex: 383, durableIndex: 383, walStallMs: 0);
        Assert.Equal(2, host.Progress.Count);

        // A higher-term ack demotes the leader; the tracker's reset takes the snapshots with it.
        await sm.CompleteAppendLogsAsync(VoterA, host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.Success, committedIndex: 383, responseTerm: sm.CurrentTerm + 1, durableIndex: 383, walStallMs: 0);

        Assert.NotEqual(RaftNodeState.Leader, sm.NodeState);
        Assert.True(host.ClearedAll);
        Assert.Empty(host.Progress);

        // A follower folds nothing: acks reaching a non-leader publish no snapshot.
        await Ack(sm, host, VoterA, RaftOperationStatus.Success, committedIndex: 383, durableIndex: 383, walStallMs: 0);
        Assert.Empty(host.Progress);
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

        // Nothing is published by the election itself: a vote reports a log id, not a disk frontier.
        Assert.Empty(host.Progress);

        return (sm, host);
    }

    private static ValueTask Ack(RaftPartitionStateMachine sm, CapturingHost host, string endpoint,
                                 RaftOperationStatus status, long committedIndex, long durableIndex, long walStallMs) =>
        sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                                   status, committedIndex, responseTerm: sm.CurrentTerm, durableIndex: durableIndex, walStallMs: walStallMs);

    // ── stubs (same shape as TestAckFrontierSemantics) ───────────────────────

    private sealed class CapturingHost : IRaftPartitionHost
    {
        public ConcurrentDictionary<string, RaftFollowerProgress> Progress { get; } = new(StringComparer.Ordinal);

        public bool ClearedAll { get; private set; }

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
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [new(VoterA), new(VoterB)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(true));

        public void PublishFollowerProgress(RaftFollowerProgress progress) => Progress[progress.Endpoint] = progress;

        public void ClearFollowerProgress(string? endpoint)
        {
            if (endpoint is null)
            {
                ClearedAll = true;
                Progress.Clear();
            }
            else
            {
                Progress.TryRemove(endpoint, out _);
            }
        }
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
