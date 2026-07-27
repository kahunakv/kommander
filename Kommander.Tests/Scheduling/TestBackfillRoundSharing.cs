using System.Collections.Concurrent;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// A heartbeat round must read each distinct backfill range from the WAL once, no matter how many
/// followers are anchored at it.
/// <para>
/// Lagging followers converge from the same point far more often than not (a fresh join, a healed
/// partition, a restart), and the leader previously issued one <c>GetRangeAsync</c> — and therefore one
/// full decode of every entry, payloads included — per follower per heartbeat, then re-encoded the same
/// entries once per follower for gRPC. The per-round memo collapses both into a single read and a single
/// Protobuf build shared by the fan-out, which is what the live-propose path already did.
/// </para>
/// </summary>
public class TestBackfillRoundSharing
{
    [Fact]
    public async Task Heartbeat_TwoFollowersAtSameAnchor_ReadsWalOnce()
    {
        CountingHost host = new();
        CountingWalFacade wal = new(commitIndex: 100);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 1);

        // Both followers report the same committed frontier, so both anchor the next batch at 6.
        // The acks themselves take the eager per-ack catch-up path (which has nothing to share with),
        // so the read counter is only meaningful from the heartbeat onwards.
        foreach (string endpoint in new[] { "follower-a:9001", "follower-b:9002" })
            await sm.CompleteAppendLogsAsync(endpoint, host.HybridLogicalClock.TrySendOrLocalEvent(1),
                RaftOperationStatus.Success, committedIndex: 5);

        wal.ResetCounters();
        host.Requests.Clear();

        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest[] batches = host.Requests
            .Where(r => r.AppendLogsRequest?.Logs is { Count: > 0 })
            .Select(r => r.AppendLogsRequest!)
            .ToArray();

        Assert.Equal(2, batches.Length);
        Assert.Equal(1, wal.GetRangeCalls);

        // Both followers ship the very same materialized batch, at the same Log Matching anchor.
        Assert.Same(batches[0].Logs, batches[1].Logs);
        Assert.All(batches, b => Assert.Equal(5, b.PrevLogIndex));

        // ...and the same gRPC cache, so the Protobuf form is built once for the whole fan-out.
        AppendLogsGrpcLogCache? cache = batches[0].GrpcLogCache;
        Assert.NotNull(cache);
        Assert.Same(cache, batches[1].GrpcLogCache);

        cache!.GetOrCreate(batches[0].Logs!);
        cache.GetOrCreate(batches[1].Logs!);
        Assert.Equal(1, cache.BuildCount);
    }

    /// <summary>
    /// Followers anchored at different indexes must each get their own read — the memo is keyed by the
    /// batch start index, and collapsing distinct ranges would ship a follower entries it cannot match.
    /// </summary>
    [Fact]
    public async Task Heartbeat_FollowersAtDifferentAnchors_ReadWalPerAnchor()
    {
        CountingHost host = new();
        CountingWalFacade wal = new(commitIndex: 100);

        RaftPartitionStateMachine sm = new(host, wal, new NoopSink(), NullLogger<IRaft>.Instance);
        IReadOnlyList<RaftLog> logs = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(logs);
        sm.SetPostToExecutor(_ => { });
        sm.SetLeaderForTesting(term: 1);

        await sm.CompleteAppendLogsAsync("follower-a:9001", host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.Success, committedIndex: 5);
        await sm.CompleteAppendLogsAsync("follower-b:9002", host.HybridLogicalClock.TrySendOrLocalEvent(1),
            RaftOperationStatus.Success, committedIndex: 40);

        wal.ResetCounters();
        host.Requests.Clear();

        await sm.CheckPartitionLeadershipAsync();

        AppendLogsRequest[] batches = host.Requests
            .Where(r => r.AppendLogsRequest?.Logs is { Count: > 0 })
            .Select(r => r.AppendLogsRequest!)
            .ToArray();

        Assert.Equal(2, batches.Length);
        Assert.Equal(2, wal.GetRangeCalls);
        Assert.NotSame(batches[0].Logs, batches[1].Logs);
        Assert.Equal([5L, 40L], batches.Select(b => b.PrevLogIndex).Order());
    }

    // ── stubs ────────────────────────────────────────────────────────────────

    private sealed class CountingHost : IRaftPartitionHost
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
        public IReadOnlyList<RaftNode> Nodes => [new("follower-a:9001"), new("follower-b:9002")];
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

    /// <summary>
    /// A WAL that always has entries available from the requested anchor, and counts range reads so a
    /// test can assert how many the round issued.
    /// </summary>
    private sealed class CountingWalFacade : IRaftWalFacade
    {
        private readonly long commitIndex;
        private int getRangeCalls;

        public CountingWalFacade(long commitIndex) => this.commitIndex = commitIndex;

        public int GetRangeCalls => Volatile.Read(ref getRangeCalls);

        public void ResetCounters() => Volatile.Write(ref getRangeCalls, 0);

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() =>
            ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(commitIndex);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
        {
            Interlocked.Increment(ref getRangeCalls);

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
