using System.Collections.Concurrent;
using Kommander;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Regression coverage for open issue C: the ack-driven catch-up paths in
/// <see cref="RaftPartitionStateMachine.CompleteAppendLogsAsync"/> must fall back to a snapshot
/// transfer when the follower's needed range has been compacted past the WAL floor.
/// Previously only the <c>SendHeartbeat</c> path did so; the ack-path re-ship ignored the empty-batch
/// signal and had no fallback, so a below-floor follower on an idle cluster (in particular a learner
/// reached only via ack-driven re-ship, not heartbeats) was stranded permanently.
/// </summary>
public class TestAckPathSnapshotFallback
{
    [Fact]
    public async Task Ack_FollowerBelowCompactionFloor_TriggersSnapshot()
    {
        // Leader committed to 100 but its WAL is compacted (GetRange always empty, checkpoint floor 100).
        FloorHost host = new(commitIndex: 100, checkpointFloor: 100);
        FloorWalFacade wal = new(commitIndex: 100, floor: 100);
        NoopSink sink = new();

        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 1);

        // Follower acks at committed index 5 — far below the leader's frontier (100) and below the
        // compaction floor. The backfill batch will come back empty, so the only repair is a snapshot.
        await sm.CompleteAppendLogsAsync("follower:9001", host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.Success, committedIndex: 5);

        // The snapshot transfer is fired as a detached background task; poll briefly for the send.
        bool sent = await WaitForAsync(() => !host.SnapshotRequests.IsEmpty, timeoutMs: 5_000);

        Assert.True(sent, "a below-floor follower ack must trigger a snapshot transfer");
        Assert.All(host.SnapshotRequests, r => Assert.Equal("follower:9001", r.FollowerEndpoint));
    }

    private static async Task<bool> WaitForAsync(Func<bool> cond, int timeoutMs)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            if (cond()) return true;
            await Task.Delay(25);
        }
        return cond();
    }

    // ── stubs ────────────────────────────────────────────────────────────────

    private sealed class FloorHost : IRaftPartitionHost
    {
        private readonly long commitIndex;

        public FloorHost(long commitIndex, long checkpointFloor)
        {
            this.commitIndex = commitIndex;
            StateMachineTransfer = new EmptyTransfer();
        }

        public ConcurrentQueue<SnapshotRequest> SnapshotRequests { get; } = new();

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "leader:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "leader", Port = 9000, InitialPartitions = 1, BackfillThreshold = 10,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [new("follower:9001")];
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer { get; }
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct)
        {
            SnapshotRequests.Enqueue(request);
            return Task.FromResult(new SnapshotResponse(true));
        }
    }

    private sealed class EmptyTransfer : IRaftStateMachineTransfer
    {
        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0x01]));

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct) =>
            Task.CompletedTask;
    }

    private sealed class FloorWalFacade : IRaftWalFacade
    {
        private readonly long commitIndex;
        private readonly long floor;

        public FloorWalFacade(long commitIndex, long floor)
        {
            this.commitIndex = commitIndex;
            this.floor = floor;
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);
        // Compacted: no entries are retrievable, forcing the empty-batch → snapshot path.
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(floor);
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
