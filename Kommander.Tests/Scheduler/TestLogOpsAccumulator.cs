using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Tests for the per-partition log-replication EWMA accumulator
/// (<c>CurrentLogOpsPerSecond()</c>) added to <see cref="RaftPartitionExecutor"/>.
///
/// Verifies that only <c>ReplicateLogs</c> dispatches record to the log accumulator;
/// all other op types (follower-replay <c>AppendLogs</c>, checkpoint, commit, reads)
/// leave it at zero; and that the existing total-ops accumulator is not disturbed.
/// </summary>
public sealed class TestLogOpsAccumulator
{
    // ── Stubs ──────────────────────────────────────────────────────────────

    private sealed class StubHost : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
        };

        public int PartitionId { get; }
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "test-node";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => _config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = Array.Empty<RaftNode>();
        public IReadOnlyList<RaftNode> Nodes => NodesOverride;

        public StubHost(int partitionId = 0) => PartitionId = partitionId;

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) { }
        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(false);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(false);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) => Task.FromResult(new SnapshotResponse(false));
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public List<(string, RaftResponderRequest)> EnqueuedResponses { get; } = [];
    }

    private sealed class StubWal : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => MakeNoOp();
        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class TestReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;
        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    // ── Helper ─────────────────────────────────────────────────────────────

    private static (RaftPartitionExecutor executor, StubHost host) BuildExecutor(int partitionId = 0)
    {
        StubHost host = new(partitionId);
        TestReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, new StubWal(), sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return (executor, host);
    }

    private static async Task DrainAndWaitAsync(RaftPartitionExecutor executor, CancellationToken ct)
        => await executor.DrainAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A dispatched <c>ReplicateLogs</c> op (empty-log fast-path, returns Success
    /// immediately without touching the WAL) must increment the log accumulator.
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_RaisesLogOpsPerSecond()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, _) = BuildExecutor(partitionId: 10);
        await executor.RestoreTask;

        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());

        // Empty log list → state machine returns Success immediately (no WAL enqueue).
        executor.Post(new RaftRequest(RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: true));
        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        Assert.True(executor.CurrentLogOpsPerSecond() > 0.0,
            "Expected CurrentLogOpsPerSecond() > 0 after dispatching ReplicateLogs.");
    }

    /// <summary>
    /// A read-only op (<c>GetNodeState</c>) must leave the log accumulator at zero.
    /// </summary>
    [Fact]
    public async Task GetNodeState_LeavesLogOpsPerSecondAtZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, _) = BuildExecutor(partitionId: 11);
        await executor.RestoreTask;

        executor.Post(new RaftRequest(RaftRequestType.GetNodeState));
        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());
    }

    /// <summary>
    /// Follower-replay via <c>AppendLogs</c> (Replication kind) must NOT increment
    /// the log accumulator, even though it does increment the total-ops accumulator.
    /// This is the direct proof of the "leader-only by type" guarantee: followers receive
    /// <c>AppendLogs</c>, not <c>ReplicateLogs</c>, so no leader guard is needed in code.
    /// </summary>
    [Fact]
    public async Task AppendLogs_IncrementsTotal_ButLeavesLogOpsPerSecondAtZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, host) = BuildExecutor(partitionId: 17);
        await executor.RestoreTask;

        Assert.Equal(0.0, executor.CurrentOpsPerSecond());
        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());

        // AppendLogs with a valid HLC timestamp (ReceiveEvent rejects L==0) and no logs
        // — processes immediately without WAL interaction.
        HLCTimestamp validTs = host.HybridLogicalClock.SendOrLocalEvent(host.LocalNodeId);
        executor.Post(new RaftRequest(
            RaftRequestType.AppendLogs,
            term: 0,
            endpoint: "leader-node",
            timestamp: validTs,
            logs: new List<RaftLog>()));
        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        // AppendLogs is Replication kind → total-ops accumulator records it.
        Assert.True(executor.CurrentOpsPerSecond() > 0.0,
            "AppendLogs (Replication kind) must increment the total-ops accumulator.");
        // But the log-replication EWMA must remain zero.
        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());
    }

    /// <summary>
    /// <c>ReplicateCheckpoint</c> must leave the log accumulator at zero;
    /// only <c>ReplicateLogs</c> is counted.
    /// </summary>
    [Fact]
    public async Task ReplicateCheckpoint_LeavesLogOpsPerSecondAtZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, _) = BuildExecutor(partitionId: 12);
        await executor.RestoreTask;

        executor.Post(new RaftRequest(RaftRequestType.ReplicateCheckpoint));
        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());
    }

    /// <summary>
    /// <c>CommitLogs</c> must leave the log accumulator at zero.
    /// </summary>
    [Fact]
    public async Task CommitLogs_LeavesLogOpsPerSecondAtZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, _) = BuildExecutor(partitionId: 13);
        await executor.RestoreTask;

        executor.Post(new RaftRequest(RaftRequestType.CommitLogs, timestamp: HLCTimestamp.Zero, autoCommit: false));
        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());
    }

    /// <summary>
    /// The batch path (<c>ExecuteBatchAsync</c>) must record <c>ops.Count</c> to the log
    /// accumulator. Uses a <c>DrainAsync</c> barrier to guarantee all <c>ReplicateLogs</c>
    /// ops are queued before the worker wakes, so they land in the same drain cycle and
    /// trigger <c>ExecuteBatchAsync</c> (requires batchEnd ≥ 2). Asserts
    /// <c>TotalBatchesExecuted</c> to confirm the batch path ran, not just the accumulator.
    /// </summary>
    [Fact]
    public async Task BatchReplicateLogs_ExecutesBatchPath_AndRaisesLogOpsPerSecond()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, _) = BuildExecutor(partitionId: 14);
        await executor.RestoreTask;

        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());
        Assert.Equal(0L, executor.TotalBatchesExecuted);

        // Drain first to let the worker settle (no pending ops).
        await DrainAndWaitAsync(executor, ct);

        // Post all ReplicateLogs ops without yielding so they queue before the next drain.
        // The executor's client drain-quantum is 2, so 4 ops guarantee at least one batch
        // of 2 even if two drain cycles are needed (batchEnd ≥ 2 in each).
        const int batchSize = 4;
        for (int i = 0; i < batchSize; i++)
            executor.Post(new RaftRequest(RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: true));

        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        Assert.True(executor.TotalBatchesExecuted > 0,
            $"Expected TotalBatchesExecuted > 0 but was {executor.TotalBatchesExecuted}. " +
            "ExecuteBatchAsync was not called — ops drained single-file instead of being coalesced.");
        Assert.True(executor.CurrentLogOpsPerSecond() > 0.0,
            "Expected CurrentLogOpsPerSecond() > 0 after dispatching batched ReplicateLogs.");
    }

    /// <summary>
    /// The existing total-ops accumulator (<c>CurrentOpsPerSecond()</c>) must still
    /// record <c>ReplicateLogs</c> dispatches — the log accumulator is additive.
    /// </summary>
    [Fact]
    public async Task ReplicateLogs_ExistingTotalOpsAccumulator_StillRecords()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, _) = BuildExecutor(partitionId: 15);
        await executor.RestoreTask;

        Assert.Equal(0.0, executor.CurrentOpsPerSecond());

        executor.Post(new RaftRequest(RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: true));
        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        // ReplicateLogs is Client kind → total-ops accumulator records it unchanged.
        Assert.True(executor.CurrentOpsPerSecond() > 0.0,
            "Expected CurrentOpsPerSecond() > 0 after dispatching ReplicateLogs (total-ops accumulator must be unaffected).");
    }

    /// <summary>
    /// <c>ReplicateCheckpoint</c> and <c>CommitLogs</c> must still record to the
    /// total-ops accumulator (they are Client kind) while leaving the log accumulator at zero.
    /// </summary>
    [Fact]
    public async Task NonReplicateLogs_TotalOpsRecorded_LogOpsZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var (executor, _) = BuildExecutor(partitionId: 16);
        await executor.RestoreTask;

        executor.Post(new RaftRequest(RaftRequestType.ReplicateCheckpoint));
        await DrainAndWaitAsync(executor, ct);

        executor.Stop();

        Assert.True(executor.CurrentOpsPerSecond() > 0.0,
            "ReplicateCheckpoint (Client kind) must increment total-ops accumulator.");
        Assert.Equal(0.0, executor.CurrentLogOpsPerSecond());
    }
}
