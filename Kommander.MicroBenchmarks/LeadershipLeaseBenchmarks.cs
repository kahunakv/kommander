using BenchmarkDotNet.Attributes;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Benchmarks the leadership-confirmation fast paths against the REAL state machine (spec
/// <c>6385886f</c> task M3). Setup drives one genuine quorum confirmation so a lease is published,
/// with a pinned monotonic clock so neither window ever expires mid-run.
///
/// <para>Expected signal:</para>
/// <list type="bullet">
///   <item><c>TryConfirmLeadershipFast</c> hit — <b>0 B</b>. This is the published-lease read that
///         off-thread callers use; any allocation here is a regression.</item>
///   <item>The async hit chain — <b>0 B</b>. Mirrors the exact hit branch added to
///         <c>RaftPartition.ConfirmLeadershipAsync</c> (<c>if (fast) return true;</c> before the
///         first await): a synchronous completion uses the runtime's cached <c>true</c> task, so
///         no state-machine box and no <c>Task</c> escape. (The real partition method is not
///         constructible without a full <c>RaftManager</c>; the branch shape is identical.)</item>
///   <item>Executor-side confirm fast path (baseline) — the per-call cost the lease removes at
///         the state-machine layer: one fresh <c>RaftResponse</c> per reply. The full production
///         round trip also paid the executor <c>Ask</c> (reply source + TCS + correlation-dict
///         insert + async boxes), so the real saving is larger than this baseline shows.</item>
/// </list>
/// </summary>
[Config(typeof(InProcessConfig))]
public class LeadershipLeaseBenchmarks
{
    private RaftPartitionStateMachine _sm = null!;
    private BenchPartitionHost _host = null!;

    [GlobalSetup]
    public void Setup()
    {
        _host = new BenchPartitionHost { Nodes = [new("node-b"), new("node-c")] };
        _sm = new RaftPartitionStateMachine(_host, new BenchWalFacade(), new DiscardReplySink(), NullLogger<IRaft>.Instance);

        _sm.SetLeaderForTesting(term: 1);
        _sm.SetLocalCommittedIndexForTesting(-1);

        // Pin the clock BEFORE the confirmation so the heartbeat-interval freshness window can
        // never expire across benchmark iterations — both fast paths stay on their hit branch.
        _host.MonotonicOverride = global::System.Diagnostics.Stopwatch.GetTimestamp();

        // One real quorum confirmation: open the round, then feed a same-term voter ack
        // (self + node-b = 2 of 3 voters). This publishes the lease.
        _sm.ConfirmLeadershipAsync(replyCorrelationId: 1).GetAwaiter().GetResult();
        _sm.CompleteAppendLogsAsync(
            "node-b",
            _host.HybridLogicalClock.TrySendOrLocalEvent(_host.LocalNodeId),
            RaftOperationStatus.Success,
            -1,
            responseTerm: 1).AsTask().GetAwaiter().GetResult();

        if (!_sm.TryConfirmLeadershipFast())
            throw new InvalidOperationException("Setup failed: no lease was published.");
    }

    /// <summary>The published-lease read itself. Expected: 0 B, a handful of ns.</summary>
    [Benchmark(Description = "lease: TryConfirmLeadershipFast hit (expect 0 B)")]
    public bool Lease_FastPathHit() => _sm.TryConfirmLeadershipFast();

    /// <summary>
    /// The hit branch of <c>RaftPartition.ConfirmLeadershipAsync</c>: an async <c>Task&lt;bool&gt;</c>
    /// method that returns synchronously on a fast-path hit. Expected: 0 B (cached true task).
    /// </summary>
    [Benchmark(Description = "lease: async hit chain, cached true task (expect 0 B)")]
    public Task<bool> Lease_AsyncChainHit() => ConfirmLeadershipHitChain();

    private async Task<bool> ConfirmLeadershipHitChain()
    {
        if (_sm.TryConfirmLeadershipFast())
            return true;

        // Never reached with the pinned clock; present so the method shape (an await after the
        // fast check) matches RaftPartition.ConfirmLeadershipAsync exactly.
        await Task.Yield();
        return false;
    }

    /// <summary>
    /// Baseline: the executor-side confirm fast path (`ReadIndexCoordinator` reuse window). Still
    /// allocates one fresh <c>RaftResponse</c> per reply — the cost every confirm paid per call
    /// before the lease, EXCLUDING the executor Ask round trip on top of it.
    /// </summary>
    [Benchmark(Baseline = true, Description = "baseline: executor-side confirm fast path (RaftResponse per call)")]
    public Task ExecutorSide_ConfirmFastPath() => _sm.ConfirmLeadershipAsync(replyCorrelationId: 1);

    // ── stubs ─────────────────────────────────────────────────────────────────

    private sealed class BenchPartitionHost : IRaftPartitionHost
    {
        public int PartitionId => 1;

        public string Leader { get; set; } = "";

        public string LocalEndpoint => "node-a";

        public int LocalNodeId => 1;

        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;

        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();

        public long? MonotonicOverride { get; set; }

        public long GetMonotonicTimestamp() => MonotonicOverride ?? global::System.Diagnostics.Stopwatch.GetTimestamp();

        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }
        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request) { }

        public Task InvokeLeaderChanged(int partitionId, string leader) => Task.CompletedTask;
        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);
        public void InvokeReplicationError(int partitionId, RaftLog log) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new SnapshotResponse(false));
    }

    private sealed class BenchWalFacade : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            IReadOnlyList<RaftLog> none = [];
            return ValueTask.FromResult(none);
        }

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, 1, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 2, WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 3, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }
    }

    /// <summary>Reply sink that discards responses so the benchmark measures the confirm path,
    /// not a growing capture list.</summary>
    private sealed class DiscardReplySink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
