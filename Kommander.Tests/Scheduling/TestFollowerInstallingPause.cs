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
/// The leader's side of <see cref="RaftOperationStatus.FollowerInstallingSnapshot"/>: a follower whose
/// partition executor is held by a snapshot install refuses entry-carrying appends without queueing
/// them, and the leader must treat that refusal as it treats a saturated WAL — pause entry-carrying
/// sends to the peer for <see cref="RaftConfiguration.FollowerSaturationBackoff"/>, keep heartbeating
/// it, leave its cursors alone, and resume once the pause lapses. Re-sending at once is what filled a
/// follower's queue with two million entries during one install (a CamusDB fault soak).
/// </summary>
public class TestFollowerInstallingPause
{
    private const string InstallingPeer = "follower-a:9001";
    private const string HealthyPeer = "follower-b:9002";

    [Fact]
    public async Task InstallingPeer_ReceivesNoEntries_WhileOthersContinue()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CountingWal _) = await BuildLeader();

        await Ack(sm, host, InstallingPeer, RaftOperationStatus.Success, committedIndex: 5);
        await Ack(sm, host, HealthyPeer, RaftOperationStatus.Success, committedIndex: 5);

        // The refusal carries no position: the follower's frontiers are the executor's to read.
        await Ack(sm, host, InstallingPeer, RaftOperationStatus.FollowerInstallingSnapshot, committedIndex: -1);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(0, BatchCountTo(host, InstallingPeer));
        Assert.True(BatchCountTo(host, HealthyPeer) > 0, "the healthy follower must keep catching up");

        // The peer's recorded position survives the refusal: nothing about it was lowered or advanced.
        Assert.Equal(5, sm.GetFollowerCommittedIndex(InstallingPeer));
    }

    [Fact]
    public async Task InstallingPeer_StillReceivesHeartbeats()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CountingWal _) = await BuildLeader();

        await Ack(sm, host, InstallingPeer, RaftOperationStatus.Success, committedIndex: 5);
        await Ack(sm, host, InstallingPeer, RaftOperationStatus.FollowerInstallingSnapshot, committedIndex: -1);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest[] toPeer = RequestsTo(host, InstallingPeer);

        Assert.NotEmpty(toPeer);
        Assert.All(toPeer, r => Assert.True(r.Logs is null or { Count: 0 },
            "an installing peer may be heartbeated, but must not be sent entries"));
    }

    /// <summary>
    /// The pause is renewed by every refusal and lapses on its own: once the backoff has elapsed with no
    /// further refusal (the install ended), the next round ships entries again.
    /// </summary>
    [Fact]
    public async Task Entries_Resume_OnceTheBackoffLapses()
    {
        (RaftPartitionStateMachine sm, CapturingHost host, CountingWal _) = await BuildLeader();

        await Ack(sm, host, InstallingPeer, RaftOperationStatus.Success, committedIndex: 5);
        await Ack(sm, host, InstallingPeer, RaftOperationStatus.FollowerInstallingSnapshot, committedIndex: -1);

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(0, BatchCountTo(host, InstallingPeer));

        host.MonotonicTicks += (long)(host.Configuration.FollowerSaturationBackoff.TotalSeconds * global::System.Diagnostics.Stopwatch.Frequency) + 1;

        host.Requests.Clear();
        await sm.CheckPartitionLeadershipAsync();
        Assert.True(BatchCountTo(host, InstallingPeer) > 0, "entries must resume once the pause lapses");
    }

    // ── harness ──────────────────────────────────────────────────────────────

    private static async Task<(RaftPartitionStateMachine, CapturingHost, CountingWal)> BuildLeader()
    {
        CapturingHost host = new();
        CountingWal wal = new(commitIndex: 100);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 1);

        return (sm, host, wal);
    }

    private static Task Ack(RaftPartitionStateMachine sm, CapturingHost host, string endpoint, RaftOperationStatus status, long committedIndex) =>
        sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1), status, committedIndex).AsTask();

    private static AppendLogsRequest[] RequestsTo(CapturingHost host, string endpoint) =>
        host.Requests
            .Where(r => r.Node?.Endpoint == endpoint && r.AppendLogsRequest is not null)
            .Select(r => r.AppendLogsRequest!)
            .ToArray();

    private static int BatchCountTo(CapturingHost host, string endpoint) =>
        RequestsTo(host, endpoint).Count(r => r.Logs is { Count: > 0 });

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

        /// <summary>Pinned monotonic clock, advanced by the tests that let a pause lapse.</summary>
        public long MonotonicTicks { get; set; } = global::System.Diagnostics.Stopwatch.GetTimestamp();
        public long GetMonotonicTimestamp() => MonotonicTicks;
        public IReadOnlyList<RaftNode> Nodes => [new(InstallingPeer), new(HealthyPeer)];
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

    /// <summary>A WAL with entries always available.</summary>
    private sealed class CountingWal : IRaftWalFacade
    {
        private readonly long commitIndex;

        public CountingWal(long commitIndex) => this.commitIndex = commitIndex;

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
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
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
