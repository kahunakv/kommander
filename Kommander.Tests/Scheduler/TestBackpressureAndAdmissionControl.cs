
using System.Collections.Concurrent;
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Acceptance tests for Backpressure and Admission Control.
///
/// Verifies:
/// - Client proposals are rejected with <see cref="RaftOperationStatus.ProposalQueueFull"/>
///   once the per-partition limit is reached.
/// - Control and replication operations are never bounded by the client queue limit.
/// - The queue recovers and accepts new proposals after it drains below capacity.
/// - <see cref="RaftPartitionExecutor.TotalClientRejected"/> and
///   <see cref="RaftPartitionExecutor.ClientQueueDepth"/> expose correct values.
/// - The WAL scheduler rejects writes with <see cref="BackpressureExceededException"/>
///   when the per-partition queue is at capacity.
/// </summary>
public sealed class TestBackpressureAndAdmissionControl
{
    // ── Stubs ──────────────────────────────────────────────────────────────

    private sealed class StubHost : IRaftPartitionHost
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
        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = Array.Empty<RaftNode>();
        public List<(string, RaftResponderRequest)> EnqueuedResponses { get; } = [];
        public IReadOnlyList<RaftNode> Nodes => NodesOverride;

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
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode node, SnapshotRequest request, CancellationToken ct) => Task.FromResult(new SnapshotResponse(false));
        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }

    private sealed class StubWal : IRaftWalFacade
    {
        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);

        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit)
            => MakeNoOp();

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();

        public WALWriteOperation? EnqueueProposeOrCommit(
            List<RaftLog>? logs, HLCTimestamp timestamp = default,
            string? endpoint = null, long term = -1)
            => MakeNoOp();

        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp()
            => new(
                _ => { },
                operationId: 0,
                WALWriteOperationType.LeaderPropose,
                (0, []));
    }

    /// <summary>
    /// Routes state-machine replies back to the executor so Ask() tasks resolve.
    /// This is the same wiring that <see cref="RaftPartition"/> does in production.
    /// </summary>
    private sealed class RelayReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;

        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static RaftPartitionExecutor BuildExecutor(int maxClientQueueDepth, int partitionId = 0)
    {
        StubHost host = new(partitionId);
        StubWal wal = new();
        RelayReplySink sink = new();

        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        RaftPartitionExecutor executor = new(
            sm,
            partitionId,
            slowThresholdMs: 0,
            NullLogger<IRaft>.Instance,
            maxClientQueueDepth: maxClientQueueDepth);

        sink.Executor = executor;
        executor.Start();
        return executor;
    }

    /// <summary>
    /// GetNodeState is a client-class operation replied directly by the executor
    /// (no WAL, no reply-sink routing) — safe to await in tests even when the
    /// node is not a leader.
    /// </summary>
    private static Task<RaftResponse> AskNodeState(RaftPartitionExecutor executor)
        => executor.Ask(new RaftRequest(RaftRequestType.GetNodeState));

    private static RaftRequest MakeCheckLeader()
        => new(RaftRequestType.CheckLeader);

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Posts a large concurrent burst so that the client queue saturates and
    /// <see cref="RaftPartitionExecutor.TotalClientRejected"/> increments.
    /// The burst is large enough to guarantee rejections even on fast machines.
    /// </summary>
    [Fact]
    public async Task ClientQueue_WhenFull_RejectionsAreCountedAndLogged()
    {
        const int cap = 4;
        using RaftPartitionExecutor executor = BuildExecutor(maxClientQueueDepth: cap);

        // Launch many concurrent posts; at least some must hit the cap.
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < 2_000; i++)
                executor.Post(new RaftRequest(RaftRequestType.GetNodeState));
        })));

        Assert.True(executor.TotalClientRejected > 0,
            "TotalClientRejected must be > 0 after a large concurrent burst against a small cap");
    }

    /// <summary>
    /// Ask() returns <see cref="RaftOperationStatus.ProposalQueueFull"/> synchronously
    /// in <c>Enqueue</c> — the TCS is set on the calling thread before the method
    /// returns, so <c>IsCompleted</c> is true immediately after the call.
    ///
    /// We verify the synchronous-completion invariant by:
    ///   1. Filling the queue with a concurrent burst until rejections start.
    ///   2. Verifying the next Ask's task is immediately completed.
    /// </summary>
    [Fact]
    public async Task ClientQueue_WhenFull_AskTaskIsCompletedSynchronously()
    {
        const int cap = 4;
        using RaftPartitionExecutor executor = BuildExecutor(maxClientQueueDepth: cap);

        await executor.RestoreTask;

        // Strategy: post well beyond cap in a tight non-yielding burst from the main
        // thread, then immediately probe with Ask — no background threads needed.
        //
        // Because there are no awaits or Thread.Yield calls inside the inner loop,
        // the current thread holds the CPU for the entire burst.  The worker cannot
        // be scheduled mid-burst, so the queue depth reliably reaches cap before Ask
        // is called.  We repeat in an outer loop on the off-chance the OS preempts
        // us at exactly the wrong microsecond.
        Task<RaftResponse>? rejectedTask = null;

        for (int attempt = 0; attempt < 10_000 && rejectedTask is null; attempt++)
        {
            for (int i = 0; i < cap * 8; i++)
                executor.Post(new RaftRequest(RaftRequestType.GetNodeState));

            Task<RaftResponse> t = AskNodeState(executor);
            if (t.IsCompleted && (await t).Status == RaftOperationStatus.ProposalQueueFull)
                rejectedTask = t;
        }

        Assert.NotNull(rejectedTask);
        Assert.True(rejectedTask!.IsCompleted, "Rejected Ask must complete synchronously (TCS set in Enqueue)");
        RaftResponse rejected = await rejectedTask;
        Assert.Equal(RaftOperationStatus.ProposalQueueFull, rejected.Status);
    }

    /// <summary>
    /// After the executor drains the client queue, new proposals are accepted again.
    /// </summary>
    [Fact]
    public async Task ClientQueue_AfterDrain_AcceptsNewProposals()
    {
        const int cap = 4;
        using RaftPartitionExecutor executor = BuildExecutor(maxClientQueueDepth: cap);

        // Flood until saturated.
        bool saturated = false;
        for (int attempt = 0; attempt < 500 && !saturated; attempt++)
        {
            executor.Post(new RaftRequest(RaftRequestType.GetNodeState));
            if (executor.TotalClientRejected > 0)
                saturated = true;
        }

        Assert.True(saturated, "Expected at least one rejection during flood");

        // Wait for the queue to drain to zero.
        bool drained = await Task.Run(async () =>
        {
            for (int i = 0; i < 200; i++)
            {
                if (executor.ClientQueueDepth == 0)
                    return true;
                await Task.Delay(10);
            }
            return false;
        });

        Assert.True(drained, $"Expected ClientQueueDepth to reach 0; got {executor.ClientQueueDepth}");

        // After draining, a fresh Ask must not be rejected.
        RaftResponse fresh = await AskNodeState(executor);
        Assert.NotEqual(RaftOperationStatus.ProposalQueueFull, fresh.Status);
    }

    /// <summary>
    /// Control-plane operations (CheckLeader) must never be rejected by the client
    /// queue limit, even when the client queue is saturated.
    /// </summary>
    [Fact]
    public async Task ControlOperations_NeverRejectedByClientQueueLimit()
    {
        const int cap = 1;
        using RaftPartitionExecutor executor = BuildExecutor(maxClientQueueDepth: cap);

        // Saturate the client queue.
        bool saturated = false;
        for (int attempt = 0; attempt < 200 && !saturated; attempt++)
        {
            executor.Post(new RaftRequest(RaftRequestType.GetNodeState));
            if (executor.TotalClientRejected > 0)
                saturated = true;
        }

        Assert.True(saturated, "Queue should have saturated");

        // Control op must complete without being ProposalQueueFull.
        RaftResponse controlResp = await executor.Ask(MakeCheckLeader(), TestContext.Current.CancellationToken);
        Assert.NotEqual(RaftOperationStatus.ProposalQueueFull, controlResp.Status);
    }

    /// <summary>
    /// Validates that <see cref="RaftPartitionExecutor.ClientQueueDepth"/> never exceeds
    /// the configured capacity during a concurrent flood of Post operations.
    ///
    /// The increment-first admission pattern is the key correctness property here:
    /// depth is claimed atomically before enqueueing, so it can never creep past the cap
    /// even when many producer threads race simultaneously.
    /// </summary>
    [Fact]
    public async Task ClientQueueDepth_NeverExceedsCapacity_UnderConcurrentFlood()
    {
        const int cap = 8;
        using RaftPartitionExecutor executor = BuildExecutor(maxClientQueueDepth: cap);

        int maxObservedDepth = 0;

        // Flood from 8 threads simultaneously to maximise the chance of concurrent
        // Interlocked.Increment races that the old read-check-increment pattern
        // would have turned into a cap violation.
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < 2_000; i++)
            {
                executor.Post(new RaftRequest(RaftRequestType.GetNodeState));

                int d = executor.ClientQueueDepth;
                int prev = Volatile.Read(ref maxObservedDepth);
                while (d > prev && Interlocked.CompareExchange(ref maxObservedDepth, d, prev) != prev)
                    prev = Volatile.Read(ref maxObservedDepth);
            }
        })));

        // Depth must never exceed the cap — guaranteed by the increment-first pattern.
        Assert.True(maxObservedDepth <= cap,
            $"ClientQueueDepth peaked at {maxObservedDepth}, exceeding capacity {cap}");
    }

    /// <summary>
    /// A client queue capacity of 0 (disabled) lets the queue grow without limit —
    /// no rejections should occur.
    /// </summary>
    [Fact]
    public async Task ClientQueue_WhenCapacityIsZero_IsUnbounded()
    {
        using RaftPartitionExecutor executor = BuildExecutor(maxClientQueueDepth: 0);

        await executor.RestoreTask;

        // Send many proposals as Post — none should be rejected.
        for (int i = 0; i < 200; i++)
            executor.Post(new RaftRequest(RaftRequestType.GetNodeState));

        // Give the executor a moment to try rejecting.
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Equal(0L, executor.TotalClientRejected);
    }

    /// <summary>
    /// The WAL scheduler throws <see cref="BackpressureExceededException"/> when the
    /// per-partition queue is at capacity, carrying the correct partition id and depth.
    /// </summary>
    [Fact]
    public void WalScheduler_WhenQueueFull_ThrowsBackpressureExceededException()
    {
        const int maxDepth = 2;
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxQueueDepthPerPartition: maxDepth);

        scheduler.Start();

        int partitionId = 7;

        WALWriteOperation MakeOp() =>
            new(
                _ => { },
                operationId: 0,
                WALWriteOperationType.LeaderPropose,
                (partitionId, [new RaftLog { Id = 1 }]));

        // Fill the queue.
        for (int i = 0; i < maxDepth; i++)
            scheduler.Enqueue(MakeOp());

        // The next enqueue must throw.
        BackpressureExceededException? caught = null;
        try
        {
            scheduler.Enqueue(MakeOp());
        }
        catch (BackpressureExceededException ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.Equal(partitionId, caught!.PartitionId);
        Assert.True(caught.CurrentDepth >= maxDepth,
            $"Expected CurrentDepth >= {maxDepth}, got {caught.CurrentDepth}");
    }

    /// <summary>
    /// Validates that the three backpressure config properties have the expected
    /// defaults from the plan.
    /// </summary>
    [Fact]
    public void RaftConfiguration_BackpressureDefaults_MatchPlanSpec()
    {
        RaftConfiguration config = new();

        Assert.Equal(2048, config.MaxQueuedClientProposalsPerPartition);
        Assert.Equal(4096, config.MaxWalQueueDepthPerPartition);
        Assert.Equal(256,  config.MaxWalBatchSize);
    }

    /// <summary>
    /// Validates that <see cref="RaftPartitionExecutor.ClientQueueCapacity"/> reflects
    /// the value passed at construction time.
    /// </summary>
    [Fact]
    public async Task ExecutorCapacity_ReflectsConstructedValue()
    {
        const int cap = 42;
        using RaftPartitionExecutor executor = BuildExecutor(maxClientQueueDepth: cap);

        Assert.Equal(cap, executor.ClientQueueCapacity);

        await executor.DrainAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Validates that drain quantum configuration defaults match plan spec.
    /// </summary>
    [Fact]
    public void RaftConfiguration_DrainQuantumDefaults_MatchPlanSpec()
    {
        RaftConfiguration config = new();

        Assert.Equal(8, config.MaxDrainQuantumControl);
        Assert.Equal(4, config.MaxDrainQuantumReplication);
        Assert.Equal(2, config.MaxDrainQuantumClient);
        Assert.Equal(1, config.MaxDrainQuantumMaintenance);
    }

    /// <summary>
    /// Validates that global WAL queue depth defaults to 0 (disabled).
    /// </summary>
    [Fact]
    public void RaftConfiguration_GlobalWalQueueDepth_DefaultsToDisabled()
    {
        RaftConfiguration config = new();

        Assert.Equal(0, config.MaxGlobalWalQueueDepth);
    }

    /// <summary>
    /// Blocking WAL stub: holds Write() until <see cref="Release"/> is called.
    /// This keeps _globalQueueDepth elevated while we assert the backpressure cap,
    /// eliminating the race where a fast worker drains all ops before the 4th Enqueue.
    /// </summary>
    private sealed class BlockingWal : IWAL, IDisposable
    {
        private readonly ManualResetEventSlim _gate = new(initialState: false);

        public void Release() => _gate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            _gate.Wait();
            return RaftOperationStatus.Success;
        }

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public long GetLastCheckpoint(int partitionId) => 0;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries) => (RaftOperationStatus.Success, 0);
        public void Dispose() => _gate.Dispose();
    }

    /// <summary>
    /// The WAL scheduler throws <see cref="BackpressureExceededException"/> when the
    /// global queue cap is reached across partitions.
    ///
    /// Uses a blocking WAL stub to pin _globalQueueDepth at the cap while the
    /// 4th Enqueue runs, avoiding the race where a fast worker drains all ops
    /// before the assertion fires.
    /// </summary>
    [Fact]
    public void WalScheduler_WhenGlobalQueueFull_ThrowsBackpressureExceededException()
    {
        const int globalMaxDepth = 3;
        using BlockingWal wal = new();
        using FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxQueueDepthPerPartition: 10,
            maxBatchSize: 256,
            maxGlobalQueueDepth: globalMaxDepth);

        scheduler.Start();

        // Spread ops across two partitions so the global cap fires, not the per-partition cap.
        WALWriteOperation MakeOp(int partitionId) =>
            new(
                _ => { },
                operationId: 0,
                WALWriteOperationType.LeaderPropose,
                (partitionId, [new RaftLog { Id = 1 }]));

        // Fill to the global cap. The worker will block inside Write(), keeping
        // _globalQueueDepth pinned at globalMaxDepth for the duration of this test.
        for (int i = 0; i < globalMaxDepth; i++)
            scheduler.Enqueue(MakeOp(i % 2));

        // The next enqueue must throw — _globalQueueDepth is still at the cap.
        BackpressureExceededException? caught = null;
        try
        {
            scheduler.Enqueue(MakeOp(0));
        }
        catch (BackpressureExceededException ex)
        {
            caught = ex;
        }
        finally
        {
            wal.Release(); // unblock the worker so the scheduler can shut down cleanly
        }

        Assert.NotNull(caught);
    }

    /// <summary>
    /// Custom drain quanta override defaults — the executor is constructed with
    /// non-default values and the test verifies it starts without error and
    /// drains work normally (no assertion on internal quanta since they are not
    /// exposed publicly, but the smoke test confirms the wiring is correct).
    /// </summary>
    [Fact]
    public async Task ExecutorDrainQuantum_CustomValues_StartAndDrainSuccessfully()
    {
        StubHost host = new(0);
        StubWal wal = new();
        RelayReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        using RaftPartitionExecutor executor = new(
            sm,
            partitionId: 0,
            slowThresholdMs: 0,
            NullLogger<IRaft>.Instance,
            maxClientQueueDepth: 100,
            drainQuantumControl: 16,
            drainQuantumReplication: 8,
            drainQuantumClient: 4,
            drainQuantumMaintenance: 2);

        sink.Executor = executor;
        executor.Start();

        // Post and await a few operations — they should process without error.
        for (int i = 0; i < 10; i++)
            executor.Post(new RaftRequest(RaftRequestType.CheckLeader));

        RaftResponse resp = await executor.Ask(new RaftRequest(RaftRequestType.GetNodeState), TestContext.Current.CancellationToken);
        Assert.NotEqual(RaftOperationStatus.ProposalQueueFull, resp.Status);
    }
}
