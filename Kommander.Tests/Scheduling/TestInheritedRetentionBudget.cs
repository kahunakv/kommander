using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// A node that wins an election publishes, in its first rounds, at least the live-replica retention
/// budget its predecessor last sent it, not the configured minimum. A fresh leader's rate window has
/// no history, so it used to fall back to the minimum for a full window: a replica the predecessor
/// was holding the log for — one mid-install, or just installed — was compacted past in that gap, and
/// re-installed (CamusDB fault soak, 2026-11-04: the successor's first retained entry sat exactly
/// 1,000,000 entries below its checkpoint while the follower needed 125,000 more).
/// </summary>
public class TestInheritedRetentionBudget
{
    private const string PreviousLeader = "leader:9000";

    [Fact]
    public async Task NewLeader_PublishesAtLeastTheBudgetItsPredecessorSent()
    {
        (RaftPartitionStateMachine sm, FollowerHost host, FloorRecordingWal wal) = await BuildFollowerAsync();

        // The predecessor was holding a 5,000-entry depth for some replica.
        await sm.AppendLogsAsync(PreviousLeader, term: 1, Timestamp(host), logs: null, retentionFloor: 200, retentionBudget: 5_000);
        Assert.Equal(5_000, wal.PublishedBudget);

        sm.SetLeaderForTesting(term: 2);
        await sm.CheckPartitionLeadershipAsync();

        // Its own window has no history: the rate budget would be the configured 100.
        Assert.True(wal.PublishCalls >= 2, "the new leader's round publishes a floor of its own");
        Assert.Equal(5_000, wal.PublishedBudget);
        Assert.Equal(5_000, host.LastAppendLogs?.RetentionBudget);
    }

    [Fact]
    public async Task NewLeader_WithNothingInherited_PublishesTheConfiguredMinimum()
    {
        (RaftPartitionStateMachine sm, FollowerHost host, FloorRecordingWal wal) = await BuildFollowerAsync();

        sm.SetLeaderForTesting(term: 2);
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(100, wal.PublishedBudget);
        Assert.Equal(100, host.LastAppendLogs?.RetentionBudget);
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private static HLCTimestamp Timestamp(FollowerHost host) => host.HybridLogicalClock.TrySendOrLocalEvent(2);

    private static async Task<(RaftPartitionStateMachine, FollowerHost, FloorRecordingWal)> BuildFollowerAsync()
    {
        FollowerHost host = new();
        FloorRecordingWal wal = new();

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });

        return (sm, host, wal);
    }

    private sealed class FollowerHost : IRaftPartitionHost
    {
        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "follower:9001";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public bool IsMember(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "follower", Port = 9001, InitialPartitions = 1,
            StartElectionTimeout = 5_000, EndElectionTimeout = 10_000,
            HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
            CompactionLiveReplicaLagBudget = 100,
            EnableCheckQuorum = false,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new(PreviousLeader)];

        /// <summary>Pinned monotonic clock: no elapsed-time gate on the tick fires between the promotion and the round.</summary>
        public long GetMonotonicTimestamp() => 1_000L * global::System.Diagnostics.Stopwatch.Frequency;

        /// <summary>The most recent AppendLogs handed to the transport for the peer.</summary>
        public volatile AppendLogsRequest? LastAppendLogs;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }

        public void EnqueueResponse(string e, RaftResponderRequest r)
        {
            if (r.AppendLogsRequest is { } append)
                LastAppendLogs = append;
        }

        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>An empty WAL that records the live-replica retention floor and budget published to it, and answers the budget back as a fresh one.</summary>
    private sealed class FloorRecordingWal : IRaftWalFacade
    {
        public long PublishedFloor = -1;
        public long PublishedBudget = -1;
        public int PublishCalls;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        public void SetLiveReplicaRetentionFloor(long floor) => SetLiveReplicaRetentionFloor(floor, 0);

        public void SetLiveReplicaRetentionFloor(long floor, long budget)
        {
            PublishedFloor = floor;
            PublishedBudget = budget;
            PublishCalls++;
        }

        public long GetLiveReplicaRetentionBudget() => Math.Max(0, PublishedBudget);

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
