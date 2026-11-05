using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Consensus;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// A leader that sent no snapshot learns a follower's whole-partition install from the follower's own
/// acks (<see cref="CompleteAppendLogsRequest.SeedIndex"/>): the install holding its executor, named
/// on its <see cref="RaftOperationStatus.FollowerInstallingSnapshot"/> refusals, and the boundary it
/// landed, named on its positional acks afterwards. The successor of the leader that sent the install
/// then holds retention from the boundary while the install runs and keeps the lag the install handed
/// the follower exempt from the budget clamp while it closes it — instead of starting from a fresh
/// tracker, clamping the need to the rate budget, and installing the follower again (CamusDB fault
/// soak, 2026-11-05: the successor's first retained entry was the predecessor's third transfer index).
/// </summary>
public class TestReportedInstallHold
{
    private const string Follower = "follower:9001";
    private const long Floor = 50;
    private const long Commit = 100;

    [Fact]
    public async Task InstallingAck_HoldsRetentionFromTheReportedBoundary_OnALeaderThatSentNothing()
    {
        Harness h = await Harness.BuildLeaderAsync();

        await h.Ack(RaftOperationStatus.FollowerInstallingSnapshot, committedIndex: -1, durableIndex: -1, seedIndex: Floor);
        await h.Sm.CheckPartitionLeadershipAsync();

        Assert.Equal(Floor + 1, h.Wal.PublishedReplicaFloor);
        Assert.Equal(Commit - (Floor + 1) + 1, h.Wal.PublishedBudget);
        Assert.Equal(Commit - (Floor + 1) + 1, h.Host.LastAppendLogs!.RetentionBudget);
    }

    [Fact]
    public async Task LandedInstallAck_ArmsTheSeed_SoTheLagStaysExemptUntilCaughtUp()
    {
        Harness h = await Harness.BuildLeaderAsync();

        // The follower reports standing at the boundary it just installed: its need is exempt.
        await h.Ack(RaftOperationStatus.Success, committedIndex: Floor, durableIndex: Floor, seedIndex: Floor);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(Floor + 1, h.Wal.PublishedReplicaFloor);
        Assert.Equal(Commit - (Floor + 1) + 1, h.Wal.PublishedBudget);

        // Halfway up, still naming the boundary: the hold follows the need, still exempt.
        await h.Ack(RaftOperationStatus.Success, committedIndex: 80, durableIndex: 80, seedIndex: Floor);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(81, h.Wal.PublishedReplicaFloor);
        Assert.Equal(Commit - 81 + 1, h.Wal.PublishedBudget);

        // Caught up: nothing is held past the rate budget any more.
        await h.Ack(RaftOperationStatus.Success, committedIndex: Commit, durableIndex: Commit, seedIndex: Floor);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(Commit + 1, h.Wal.PublishedReplicaFloor);
        Assert.Equal(10, h.Wal.PublishedBudget);
    }

    [Fact]
    public async Task PositionAtOrPastTheReportedInstall_EndsTheHold()
    {
        Harness h = await Harness.BuildLeaderAsync();

        await h.Ack(RaftOperationStatus.FollowerInstallingSnapshot, committedIndex: -1, durableIndex: -1, seedIndex: Floor);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(Floor + 1, h.Wal.PublishedReplicaFloor);

        // The install landed and the follower has caught up by log: the hold is over.
        await h.Ack(RaftOperationStatus.Success, committedIndex: Commit, durableIndex: Commit, seedIndex: Floor);
        await h.Sm.CheckPartitionLeadershipAsync();
        Assert.Equal(Commit + 1, h.Wal.PublishedReplicaFloor);
        Assert.Equal(10, h.Wal.PublishedBudget);
    }

    /// <summary>
    /// A follower names its boundary on every ack for a while after the install. The first report arms
    /// the seed; once the seed's grace has lapsed (the record is cleared), the same boundary must not
    /// arm it again, or a caught-up follower would stay exempt from the budget clamp for good. A later
    /// install (a higher boundary) arms it once more.
    /// </summary>
    [Fact]
    public void ReportedBoundary_ArmsTheSeedOnce()
    {
        ReplicationTracker tracker = new(new LeaderHost());

        tracker.NoteReportedSnapshotSeed(Follower, Floor, Commit);
        Assert.True(tracker.TryGetSnapshotSeed(Follower, out ReplicationTracker.SnapshotSeed seed));
        Assert.Equal(Floor, seed.Index);
        Assert.Equal(Commit, seed.LeaderCommitIndex);

        tracker.ClearSnapshotSeed(Follower);
        tracker.NoteReportedSnapshotSeed(Follower, Floor, Commit + 10);
        Assert.False(tracker.TryGetSnapshotSeed(Follower, out _));

        tracker.NoteReportedSnapshotSeed(Follower, Floor + 20, Commit + 10);
        Assert.True(tracker.TryGetSnapshotSeed(Follower, out seed));
        Assert.Equal(Floor + 20, seed.Index);

        // A reported boundary never lowers a record this leader already holds.
        tracker.NoteReportedSnapshotSeed(Follower, Floor + 5, Commit + 10);
        Assert.True(tracker.TryGetSnapshotSeed(Follower, out seed));
        Assert.Equal(Floor + 20, seed.Index);
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private sealed class Harness
    {
        public required RaftPartitionStateMachine Sm { get; init; }
        public required LeaderHost Host { get; init; }
        public required FloorRecordingCheckpointWal Wal { get; init; }

        public static async Task<Harness> BuildLeaderAsync()
        {
            LeaderHost host = new();
            FloorRecordingCheckpointWal wal = new(checkpoint: Floor, commitIndex: Commit);

            RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
            IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
            await sm.CompleteRestoreAsync(logs);

            sm.SetLeaderForTesting(term: 1);

            return new Harness { Sm = sm, Host = host, Wal = wal };
        }

        public Task Ack(RaftOperationStatus status, long committedIndex, long durableIndex, long seedIndex) =>
            Sm.CompleteAppendLogsAsync(Follower, Host.HybridLogicalClock.TrySendOrLocalEvent(1),
                status, committedIndex, durableIndex: durableIndex, seedIndex: seedIndex).AsTask();
    }

    /// <summary>A leader-side host with one follower; no backfill, so the retention round is all that runs.</summary>
    private sealed class LeaderHost : IRaftPartitionHost
    {
        public ConcurrentBag<RaftResponderRequest> Requests { get; } = [];

        /// <summary>The most recent AppendLogs handed to the transport for the follower.</summary>
        public volatile AppendLogsRequest? LastAppendLogs;

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            NodeId = 1, Host = "leader", Port = 9000, InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.Zero, RecentHeartbeat = TimeSpan.Zero,
            BackfillEnabled = false,
            EnableCheckQuorum = false,
            CompactionLiveReplicaLagBudget = 10,
            CompactionLiveReplicaLagWindow = TimeSpan.Zero,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public long GetMonotonicTimestamp() => Stopwatch.GetTimestamp();
        public IReadOnlyList<RaftNode> Nodes => [new(Follower)];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }

        public void EnqueueResponse(string e, RaftResponderRequest r)
        {
            Requests.Add(r);
            if (r.AppendLogsRequest is { } append)
                LastAppendLogs = append;
        }

        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(true));
    }

    /// <summary>A WAL stub with its compaction floor at the checkpoint that records the published retention floor and budget.</summary>
    private sealed class FloorRecordingCheckpointWal : IRaftWalFacade
    {
        private readonly long commitIndex;
        private readonly long checkpoint;

        public long PublishedReplicaFloor = long.MaxValue;
        public long PublishedBudget;

        public FloorRecordingCheckpointWal(long checkpoint, long commitIndex)
        {
            this.checkpoint = checkpoint;
            this.commitIndex = commitIndex;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(checkpoint);
        public long GetCommitIndex() => commitIndex;
        public long GetDurableCommitFrontier() => commitIndex;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool ac) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : MakeNoOp();
        public void NotifyCommitted() { }

        public void SetLiveReplicaRetentionFloor(long floor) => SetLiveReplicaRetentionFloor(floor, 0);

        public void SetLiveReplicaRetentionFloor(long floor, long budget)
        {
            Volatile.Write(ref PublishedReplicaFloor, floor);
            Volatile.Write(ref PublishedBudget, budget);
        }

        private static WALWriteOperation MakeNoOp() =>
            new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
