
using System.Diagnostics.Metrics;
using Kommander;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Integration tests for the <c>CompleteWalOperationAsync</c> fence gates.
///
/// Each test delivers a <see cref="RaftWalCompletion"/> through
/// <c>executor.Ask(WriteOperationCompleted, …)</c> and verifies that the
/// state machine discards completions that violate a fencing invariant:
///
/// <list type="bullet">
///   <item>Partition mismatch — completion addressed to the wrong partition.</item>
///   <item>Term mismatch — completion from a superseded leadership term.</item>
///   <item>Inverted log range — MinLogIndex &gt; MaxLogIndex in the envelope.</item>
///   <item>Orphan leader completion — OperationId absent from pendingWalOperations.</item>
///   <item>Orphan follower completion — FollowerAppend without a pending entry.</item>
/// </list>
/// </summary>
public sealed class TestWalCompletionFences : IDisposable
{
    // ── Metric listener ────────────────────────────────────────────────────

    private readonly MeterListener _listener = new();
    private readonly Dictionary<string, List<double>> _measurements = [];
    private readonly object _lock = new();

    public TestWalCompletionFences()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName)
                listener.EnableMeasurementEvents(instrument);
        };

        _listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            lock (_lock) Bucket(instrument.Name).Add(value);
        });

        _listener.Start();
    }

    public void Dispose() => _listener.Dispose();

    private List<double> Bucket(string name)
    {
        if (!_measurements.TryGetValue(name, out List<double>? list))
            _measurements[name] = list = [];
        return list;
    }

    private double Sum(string name)
    {
        lock (_lock)
            return _measurements.TryGetValue(name, out List<double>? list) ? list.Sum() : 0;
    }

    // ── Stubs ──────────────────────────────────────────────────────────────

    internal sealed class StubHost : IRaftPartitionHost
    {
        private readonly RaftConfiguration _config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout   = 100,
        };

        public int PartitionId { get; }
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "test-node";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => _config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = Array.Empty<RaftNode>();
        public List<(string, RaftResponderRequest)> EnqueuedResponses { get; } = [];

        public StubHost(int partitionId = 0) => PartitionId = partitionId;

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => EnqueuedResponses.Add((endpoint, request));
        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(false);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(false);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }
        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) => Task.FromResult(new SnapshotResponse(false));
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }

    internal sealed class StubWal : IRaftWalFacade
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
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => MakeNoOp();
        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class RelaySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;
        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    private RaftPartitionExecutor BuildExecutor(int partitionId = 0)
    {
        StubHost host = new(partitionId);
        StubWal wal = new();
        RelaySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return executor;
    }

    private static (RaftPartitionExecutor executor, StubHost host) BuildExecutorWithHost(int partitionId = 0)
    {
        StubHost host = new(partitionId);
        StubWal wal = new();
        RelaySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return (executor, host);
    }

    private static RaftRequest CompletionRequest(RaftWalCompletion completion)
        => new(RaftRequestType.WriteOperationCompleted, completion);

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A completion addressed to a different partition ID is discarded and
    /// <c>raft.stale_completions_total</c> is incremented.
    /// </summary>
    [Fact]
    public async Task PartitionMismatch_CompletionIsDiscardedAndMetricFires()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);

        RaftWalCompletion completion = new(
            PartitionId:   99,   // wrong — executor owns partition 0
            OperationId:   1,
            Term:          -1,
            MinLogIndex:   -1,
            MaxLogIndex:   -1,
            OperationType: WALWriteOperationType.LeaderPropose,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

        _listener.RecordObservableInstruments();
        Assert.True(Sum("raft.stale_completions_total") >= 1,
            "Partition-mismatch fence must increment raft.stale_completions_total");
    }

    /// <summary>
    /// A completion whose term mismatches the current node term is discarded
    /// and <c>raft.stale_completions_total</c> is incremented.
    /// </summary>
    [Fact]
    public async Task TermMismatch_CompletionIsDiscardedAndMetricFires()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);

        RaftWalCompletion completion = new(
            PartitionId:   0,
            OperationId:   1,
            Term:          99,   // mismatched — node is in term 0
            MinLogIndex:   -1,
            MaxLogIndex:   -1,
            OperationType: WALWriteOperationType.LeaderPropose,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

        _listener.RecordObservableInstruments();
        Assert.True(Sum("raft.stale_completions_total") >= 1,
            "Term-mismatch fence must increment raft.stale_completions_total");
    }

    /// <summary>
    /// A completion with MinLogIndex &gt; MaxLogIndex is discarded silently.
    /// No exception must be thrown and the executor must remain responsive after restore.
    /// </summary>
    [Fact]
    public async Task InvertedLogRange_CompletionIsDiscardedWithoutException()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);
        await executor.RestoreTask;

        RaftWalCompletion completion = new(
            PartitionId:   0,
            OperationId:   42,
            Term:          -1,   // bypass term fence
            MinLogIndex:   10,   // inverted: min > max
            MaxLogIndex:   5,
            OperationType: WALWriteOperationType.LeaderPropose,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

        // Executor must still respond to client requests after discarding the stale completion.
        RaftResponse state = await executor.Ask(
            new RaftRequest(RaftRequestType.GetNodeState),
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, state.Status);
    }

    /// <summary>
    /// A LeaderPropose completion for an OperationId that is not in
    /// <c>pendingWalOperations</c> is discarded silently (orphan/superseded).
    /// </summary>
    [Fact]
    public async Task OrphanLeaderPropose_CompletionIsDiscardedWithoutException()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);
        await executor.RestoreTask;

        RaftWalCompletion completion = new(
            PartitionId:   0,
            OperationId:   9999,   // never registered
            Term:          -1,
            MinLogIndex:   -1,
            MaxLogIndex:   -1,
            OperationType: WALWriteOperationType.LeaderPropose,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

        RaftResponse state = await executor.Ask(
            new RaftRequest(RaftRequestType.GetNodeState),
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, state.Status);
    }

    /// <summary>
    /// A LeaderCommit completion for an unregistered OperationId is discarded.
    /// </summary>
    [Fact]
    public async Task OrphanLeaderCommit_CompletionIsDiscardedWithoutException()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);
        await executor.RestoreTask;

        RaftWalCompletion completion = new(
            PartitionId:   0,
            OperationId:   9999,
            Term:          -1,
            MinLogIndex:   -1,
            MaxLogIndex:   -1,
            OperationType: WALWriteOperationType.LeaderCommit,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

        RaftResponse state = await executor.Ask(
            new RaftRequest(RaftRequestType.GetNodeState),
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, state.Status);
    }

    /// <summary>
    /// A FollowerAppend completion without a matching pending entry is discarded.
    /// Every FollowerAppend registers a pending entry that carries Endpoint, Timestamp,
    /// and Logs; an orphan completion must not silently send a response to an empty endpoint.
    /// </summary>
    [Fact]
    public async Task OrphanFollowerAppend_CompletionIsDiscardedNoResponseEnqueued()
    {
        (RaftPartitionExecutor executor, StubHost host) = BuildExecutorWithHost(partitionId: 0);
        using (executor)
        {
            RaftWalCompletion completion = new(
                PartitionId:   0,
                OperationId:   9999,   // not registered
                Term:          -1,
                MinLogIndex:   -1,
                MaxLogIndex:   -1,
                OperationType: WALWriteOperationType.FollowerAppend,
                Status:        RaftOperationStatus.Success
            );

            await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

            Assert.Empty(host.EnqueuedResponses);
        }
    }

    /// <summary>
    /// A LeaderRollback completion for an unregistered OperationId is discarded.
    /// </summary>
    [Fact]
    public async Task OrphanLeaderRollback_CompletionIsDiscardedWithoutException()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);
        await executor.RestoreTask;

        RaftWalCompletion completion = new(
            PartitionId:   0,
            OperationId:   9999,
            Term:          -1,
            MinLogIndex:   -1,
            MaxLogIndex:   -1,
            OperationType: WALWriteOperationType.LeaderRollback,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

        RaftResponse state = await executor.Ask(
            new RaftRequest(RaftRequestType.GetNodeState),
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, state.Status);
    }

    /// <summary>
    /// The no-orphan rule behind the Caraxes run-H stall: a term-fenced propose completion must
    /// FAIL the client reply, never drop it. The pending entry it discards carries the reply
    /// correlation of the <c>Ask(ReplicateLogs)</c> that submitted the propose, and this completion
    /// is that operation's only resolution channel — before the fix, discarding it silently left
    /// the caller awaiting forever (no response, no timeout), which starved a closed-loop workload
    /// cluster-wide after a paused leader resumed into a higher term.
    /// </summary>
    [Fact]
    public async Task TermFencedPropose_FailsTheClientReplyInsteadOfOrphaningIt()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);
        await executor.RestoreTask;

        // Become a single-node leader so ReplicateLogs accepts the propose.
        await executor.Ask(new RaftRequest(RaftRequestType.ForceLeaderForTesting), TestContext.Current.CancellationToken);

        // Submit a propose. The StubWal registers it under OperationId 0 and its reply resolves
        // only through the WAL completion router, so the Ask must still be pending here.
        Task<RaftResponse> proposeAsk = executor.Ask(
            new RaftRequest(RaftRequestType.ReplicateLogs, [new RaftLog { Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }], true),
            TestContext.Current.CancellationToken);

        await executor.Ask(new RaftRequest(RaftRequestType.GetNodeState), TestContext.Current.CancellationToken);
        Assert.False(proposeAsk.IsCompleted, "the propose reply must still be pending before its WAL completion arrives");

        // Deliver the propose's completion stamped with a superseded term: the fence discards the
        // state transition, and must answer the caller with a retryable failure.
        RaftWalCompletion staleCompletion = new(
            PartitionId:   0,
            OperationId:   0,
            Term:          999,   // != the node's current term → term fence
            MinLogIndex:   -1,
            MaxLogIndex:   -1,
            OperationType: WALWriteOperationType.LeaderPropose,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(staleCompletion), TestContext.Current.CancellationToken);

        RaftResponse reply = await proposeAsk.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.NodeIsNotLeader, reply.Status);
    }

    /// <summary>
    /// Same no-orphan rule for the min-log-index cross-check: a mismatched envelope is discarded,
    /// but the pending entry it consumed must still answer the caller.
    /// </summary>
    [Fact]
    public async Task MinLogMismatchPropose_FailsTheClientReplyInsteadOfOrphaningIt()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);
        await executor.RestoreTask;

        await executor.Ask(new RaftRequest(RaftRequestType.ForceLeaderForTesting), TestContext.Current.CancellationToken);

        Task<RaftResponse> proposeAsk = executor.Ask(
            new RaftRequest(RaftRequestType.ReplicateLogs, [new RaftLog { Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }], true),
            TestContext.Current.CancellationToken);

        await executor.Ask(new RaftRequest(RaftRequestType.GetNodeState), TestContext.Current.CancellationToken);
        Assert.False(proposeAsk.IsCompleted, "the propose reply must still be pending before its WAL completion arrives");

        // Term -1 bypasses the term fence; the envelope min (5) mismatches the pending batch's
        // actual min id, so the min-log cross-check discards it — and must answer the caller.
        RaftWalCompletion mismatched = new(
            PartitionId:   0,
            OperationId:   0,
            Term:          -1,
            MinLogIndex:   5,
            MaxLogIndex:   5,
            OperationType: WALWriteOperationType.LeaderPropose,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(mismatched), TestContext.Current.CancellationToken);

        RaftResponse reply = await proposeAsk.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Errored, reply.Status);
    }

    /// <summary>
    /// A Compaction completion with no pending entry is handled gracefully
    /// (Compaction is fire-and-forget — no pending entry is required).
    /// </summary>
    [Fact]
    public async Task CompactionWithoutPending_IsHandledGracefully()
    {
        using RaftPartitionExecutor executor = BuildExecutor(partitionId: 0);
        await executor.RestoreTask;

        RaftWalCompletion completion = new(
            PartitionId:   0,
            OperationId:   9999,
            Term:          -1,
            MinLogIndex:   -1,
            MaxLogIndex:   -1,
            OperationType: WALWriteOperationType.Compaction,
            Status:        RaftOperationStatus.Success
        );

        await executor.Ask(CompletionRequest(completion), TestContext.Current.CancellationToken);

        RaftResponse state = await executor.Ask(
            new RaftRequest(RaftRequestType.GetNodeState),
            TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, state.Status);
    }
}
