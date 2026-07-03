using System.Diagnostics.Metrics;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Verifies that the pre-computed <c>TagList</c> instances in
/// <see cref="RaftPartitionExecutor"/> still emit the correct
/// <c>partition_id</c> and <c>operation_class</c> tag values when a
/// <see cref="MeterListener"/> is attached.
///
/// This is a regression guard: the P6 allocation fix must not change any
/// observable metric value — only how the tags are constructed.
/// </summary>
public sealed class TestExecutorMetricsTags : IDisposable
{
    private readonly MeterListener _listener;
    private readonly List<(string instrument, int partitionId, string opClass)> _captured = [];

    public TestExecutorMetricsTags()
    {
        _listener = new MeterListener();
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName &&
                instrument.Name == "raft.executor.operations_total")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>(OnMeasurement);
        _listener.Start();
    }

    private void OnMeasurement(
        Instrument instrument,
        long measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        int partitionId = -1;
        string opClass = "";
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            if (tag.Key == "partition_id")    partitionId = Convert.ToInt32(tag.Value);
            if (tag.Key == "operation_class") opClass = (string)(tag.Value ?? "");
        }
        lock (_captured)
            _captured.Add((instrument.Name, partitionId, opClass));
    }

    public void Dispose() => _listener.Dispose();

    // ── Stubs (mirrors TestRaftPartitionExecutor helper pattern) ───────────

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
        public StubHost(int partitionId) => PartitionId = partitionId;
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
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) => Task.FromResult(new SnapshotResponse(false));
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
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
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) => NoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => NoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => NoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => NoOp();
        public void NotifyCommitted() { }
        private static WALWriteOperation NoOp() => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class TestReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;
        public void TryComplete(ulong correlationId, RaftResponse response) => Executor?.DeliverReply(correlationId, response);
    }

    private static (RaftPartitionExecutor executor, RaftPartitionStateMachine sm) BuildExecutor(int partitionId)
    {
        StubHost host = new(partitionId);
        TestReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, new StubWal(), sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return (executor, sm);
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="RaftOperationMapper.GetKindLabel"/> must return the same string
    /// as <c>kind.ToString()</c> for every <see cref="RaftOperationKind"/> value.
    /// The pre-computed label must be observationally equivalent to the enum name.
    /// </summary>
    [Fact]
    public void GetKindLabel_MatchesToStringForAllKinds()
    {
        foreach (RaftOperationKind kind in Enum.GetValues<RaftOperationKind>())
            Assert.Equal(kind.ToString(), RaftOperationMapper.GetKindLabel(kind));
    }

    /// <summary>
    /// Processing a Control-class operation must emit
    /// <c>raft.executor.operations_total</c> with <c>partition_id</c> equal to the
    /// executor's partition id and <c>operation_class = "Control"</c>.
    /// </summary>
    [Fact]
    public async Task Control_EmitsCorrectTags()
    {
        const int pid = 55;
        var (executor, _) = BuildExecutor(pid);
        try
        {
            await executor.RestoreTask;
            await executor.Ask(new RaftRequest(RaftRequestType.CheckLeader));

            List<(string, int, string)> controlHits;
            lock (_captured)
                controlHits = _captured
                    .Where(c => c.partitionId == pid && c.opClass == "Control")
                    .ToList();

            Assert.NotEmpty(controlHits);
            Assert.All(controlHits, h =>
            {
                Assert.Equal("raft.executor.operations_total", h.Item1);
                Assert.Equal(pid, h.Item2);
                Assert.Equal("Control", h.Item3);
            });
        }
        finally
        {
            executor.Stop();
        }
    }

    /// <summary>
    /// Two executors with different <c>partitionId</c>s must emit distinct
    /// <c>partition_id</c> tag values — pre-built tags are per-instance, not shared.
    /// </summary>
    [Fact]
    public async Task TwoExecutors_EmitDistinctPartitionIds()
    {
        const int pid1 = 201;
        const int pid2 = 202;
        var (ex1, _) = BuildExecutor(pid1);
        var (ex2, _) = BuildExecutor(pid2);
        try
        {
            await Task.WhenAll(ex1.RestoreTask, ex2.RestoreTask);
            await Task.WhenAll(
                ex1.Ask(new RaftRequest(RaftRequestType.CheckLeader)),
                ex2.Ask(new RaftRequest(RaftRequestType.CheckLeader)));

            bool sawPid1, sawPid2;
            lock (_captured)
            {
                sawPid1 = _captured.Any(c => c.partitionId == pid1);
                sawPid2 = _captured.Any(c => c.partitionId == pid2);
            }

            Assert.True(sawPid1, "Expected metrics for partition 201");
            Assert.True(sawPid2, "Expected metrics for partition 202");
        }
        finally
        {
            ex1.Stop();
            ex2.Stop();
        }
    }
}
