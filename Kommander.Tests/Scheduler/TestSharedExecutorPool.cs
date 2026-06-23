
using System.Collections.Concurrent;
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
/// Tests for Phase 1 of the partition-scaling spec: shared executor pool.
///
/// Verifies that:
/// - Config defaults are correct.
/// - Pool mode respects the single-owner invariant (no concurrent execution per partition).
/// - A pool of P threads serves M &gt; P partitions without creating M OS threads.
/// - Post/Ask/DrainAsync work correctly in pool mode.
/// - Stop() in pool mode drains remaining work and unblocks the caller.
/// - EnableSharedExecutorPool=false restores dedicated-thread behaviour.
/// </summary>
public sealed class TestSharedExecutorPool
{
    // ── Stubs (shared with TestRaftPartitionExecutor) ─────────────────────

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
        public List<(string Endpoint, RaftResponderRequest Request)> EnqueuedResponses { get; } = [];
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
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
            => MakeNoOpOperation();

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOpOperation();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOpOperation();

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
            => MakeNoOpOperation();

        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOpOperation()
            => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class TestReplySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;
        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static (RaftPartitionExecutor executor, RaftPartitionStateMachine sm) BuildExecutor(
        RaftExecutorPool pool,
        int partitionId = 0)
    {
        StubHost host = new(partitionId);
        TestReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, new StubWal(), sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId, 0, NullLogger<IRaft>.Instance, pool: pool);
        sink.Executor = executor;
        executor.Start();
        return (executor, sm);
    }

    // ── Config defaults ───────────────────────────────────────────────────

    [Fact]
    public void DefaultConfig_SharedExecutorPoolEnabled() =>
        Assert.True(new RaftConfiguration().EnableSharedExecutorPool);

    [Fact]
    public void DefaultConfig_PoolSizeIsZero() =>
        Assert.Equal(0, new RaftConfiguration().PartitionExecutorPoolSize);

    // ── Pool construction ─────────────────────────────────────────────────

    [Fact]
    public void Pool_PoolSizeZero_UsesProcessorCount()
    {
        using RaftExecutorPool pool = new(0);
        Assert.Equal(Environment.ProcessorCount, pool.PoolSize);
    }

    [Fact]
    public void Pool_ExplicitSize_RespectsValue()
    {
        using RaftExecutorPool pool = new(3);
        Assert.Equal(3, pool.PoolSize);
    }

    [Fact]
    public void Pool_NegativeSize_ClampsToOne()
    {
        using RaftExecutorPool pool = new(-5);
        Assert.Equal(1, pool.PoolSize);
    }

    // ── Thread-count scaling ──────────────────────────────────────────────

    // Reflection handle used by both scaling tests to read the private _worker field,
    // which is null in pool mode and a live Thread in dedicated mode.
    private static readonly global::System.Reflection.FieldInfo s_workerField =
        typeof(RaftPartitionExecutor).GetField("_worker", global::System.Reflection.BindingFlags.NonPublic | global::System.Reflection.BindingFlags.Instance)!;

    private static int CountLiveDedicatedThreads(IEnumerable<RaftPartitionExecutor> executors) =>
        executors.Count(e => s_workerField.GetValue(e) is Thread { IsAlive: true });

    /// <summary>
    /// M partitions running on a pool of P threads must create zero dedicated
    /// "RaftPartitionExecutor-{id}" OS threads. The spec's Done-check is
    /// process thread count ≈ P + overhead, not ≈ M.
    ///
    /// Verified by reflecting on the private <c>_worker</c> field: it is
    /// <see langword="null"/> in pool mode and a live <see cref="Thread"/> in
    /// dedicated mode, so counting live instances is an exact proxy for counting
    /// threads named "RaftPartitionExecutor-*".
    /// </summary>
    [Fact]
    public async Task PoolMode_ManyPartitions_ZeroDedicatedThreadsCreated()
    {
        const int poolSize = 2;
        const int partitionCount = 20;

        using RaftExecutorPool pool = new(poolSize);
        pool.Start();

        var executors = new List<RaftPartitionExecutor>();
        try
        {
            for (int i = 0; i < partitionCount; i++)
            {
                var (ex, _) = BuildExecutor(pool, partitionId: i);
                executors.Add(ex);
            }

            await Task.WhenAll(executors.Select(e => e.RestoreTask)).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // Core invariant: M partitions, 0 dedicated OS threads.
            int dedicated = CountLiveDedicatedThreads(executors);
            Assert.Equal(0, dedicated);

            // Pool itself uses exactly P threads, not M.
            Assert.Equal(poolSize, pool.PoolSize);
        }
        finally
        {
            foreach (RaftPartitionExecutor ex in executors)
            {
                ex.Stop();
                ex.Dispose();
            }
        }
    }

    /// <summary>
    /// Contrast: dedicated-thread mode creates exactly one "RaftPartitionExecutor-{id}"
    /// OS thread per partition. This is the baseline the pool mode eliminates.
    /// </summary>
    [Fact]
    public void DedicatedThreadMode_ManyPartitions_EachOwnsOneDedicatedThread()
    {
        const int partitionCount = 5;

        var executors = new List<RaftPartitionExecutor>();
        try
        {
            for (int i = 0; i < partitionCount; i++)
            {
                StubHost host = new(i);
                TestReplySink sink = new();
                RaftPartitionStateMachine sm = new(host, new StubWal(), sink, NullLogger<IRaft>.Instance);
                RaftPartitionExecutor executor = new(sm, i, 0, NullLogger<IRaft>.Instance); // no pool
                sink.Executor = executor;
                executor.Start();
                executors.Add(executor);
            }

            // Every executor has its own live named thread.
            int dedicated = CountLiveDedicatedThreads(executors);
            Assert.Equal(partitionCount, dedicated);
        }
        finally
        {
            foreach (RaftPartitionExecutor ex in executors)
            {
                ex.Stop();
                ex.Dispose();
            }
        }
    }

    // ── Control-plane not starved (Task 1.3 Done-check) ──────────────────

    /// <summary>
    /// Saturate one partition's client queue with slow proposals and assert that a
    /// different partition's control op still completes within timing bounds.
    ///
    /// This is the Done-check for Task 1.3: the bounded drain quantum (2 client ops
    /// per cycle) releases the pool thread between bursts, so the second pool thread
    /// can always pick up the waiting control op even while partition 0 is saturated.
    ///
    /// Partition 0: leader, slow WAL (5 ms per <c>EnqueuePropose</c>), 50
    /// <c>ReplicateLogs</c> ops → ~250 ms of client-queue work.
    /// Partition 1: instant WAL, <c>CheckLeader</c> posted immediately after the flood.
    /// Expected: partition 1 answers in ≤ one drain cycle (≈ 10 ms nominal; 50 ms CI bound).
    /// </summary>
    [Fact]
    public async Task PoolMode_ControlOpsNotStarvedWhenClientQueueSaturated()
    {
        using RaftExecutorPool pool = new(2);
        pool.Start();

        // Partition 0: slow WAL that sleeps in EnqueuePropose so each ReplicateLogs
        // op keeps the pool thread busy for a measurable window.
        // With quantum=2 and 5 ms/op, each drain cycle costs 10 ms; 50 ops = 25 cycles ≈ 250 ms.
        StubHost host0 = new(0);
        TestReplySink sink0 = new();
        RaftPartitionStateMachine sm0 = new(host0, new SlowWal(delayMs: 5), sink0, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor exec0 = new(sm0, 0, 0, NullLogger<IRaft>.Instance, pool: pool);
        sink0.Executor = exec0;
        exec0.Start();
        await exec0.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Become leader so ReplicateLogs enters EnqueuePropose on the slow WAL.
        // (Non-leader path returns NodeIsNotLeader instantly without touching the WAL.)
        await exec0.Ask(new RaftRequest(RaftRequestType.ForceLeaderForTesting), TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Partition 1: instant WAL — it is the "other partition" whose control op
        // must not be starved by partition 0's sustained client load.
        var (exec1, _) = BuildExecutor(pool, partitionId: 1);
        await exec1.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        try
        {
            // Flood partition 0's client queue.
            List<RaftLog> logs = [new RaftLog { Type = 0 }];
            for (int i = 0; i < 50; i++)
                exec0.Post(new RaftRequest(RaftRequestType.ReplicateLogs, logs, autoCommit: false));

            // Immediately time a control op on a *different* partition.
            // The pool's bounded drain quantum releases the thread between cycles, so
            // the second pool thread (which fails _runLock on partition 0 and parks)
            // must be available to pick up partition 1's CheckLeader within ≤ one cycle.
            global::System.Diagnostics.Stopwatch sw = global::System.Diagnostics.Stopwatch.StartNew();
            await exec1.Ask(new RaftRequest(RaftRequestType.CheckLeader), TestContext.Current.CancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            sw.Stop();

            // Nominal: ≤ 10 ms (one cycle).  50 ms covers slow/loaded CI machines while
            // remaining far below the ≈ 250 ms total load on partition 0.
            Assert.True(sw.ElapsedMilliseconds < 50,
                $"Partition 1 control op took {sw.ElapsedMilliseconds} ms while partition 0 was " +
                $"saturated (~250 ms of client work) — cross-partition starvation suspected.");
        }
        finally
        {
            exec0.Stop();
            exec0.Dispose();
            exec1.Stop();
            exec1.Dispose();
        }
    }

    // ── Post/DrainAsync work in pool mode ─────────────────────────────────

    [Fact]
    public async Task PoolMode_DrainAsync_CompletesAfterRestore()
    {
        using RaftExecutorPool pool = new(2);
        pool.Start();

        var (executor, _) = BuildExecutor(pool, partitionId: 0);
        try
        {
            await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await executor.DrainAsync(TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
        finally
        {
            executor.Stop();
            executor.Dispose();
        }
    }

    // ── Single-owner invariant ─────────────────────────────────────────────

    /// <summary>
    /// Concurrent posts from many tasks must never execute concurrently inside the
    /// state machine for the same partition (single-owner invariant).
    ///
    /// Uses <see cref="RaftRequestType.AppendLogs"/> with non-empty log lists so the
    /// state machine calls <c>EnqueueProposeOrCommit</c> on the instrumented WAL. A
    /// <c>Thread.Sleep(1)</c> inside the hook widens the race window; the
    /// <c>totalCalled &gt; 0</c> guard catches any future regression where the
    /// instrumented path is no longer exercised.
    /// </summary>
    [Fact]
    public async Task PoolMode_SingleOwnerInvariant_NoConcurrentExecutionPerPartition()
    {
        bool concurrentDetected = false;
        int inFlight = 0;
        int totalCalled = 0;

        // Hook fires inside EnqueueProposeOrCommit, which AppendLogsAsync calls
        // whenever it receives a non-empty log list. The 1 ms sleep widens the
        // race window so a broken _runLock would expose the concurrent overlap.
        InstrumentedWal wal = new(() =>
        {
            int v = Interlocked.Increment(ref inFlight);
            if (v > 1) concurrentDetected = true;
            Interlocked.Increment(ref totalCalled);
            Thread.Sleep(1);
            Interlocked.Decrement(ref inFlight);
        });

        using RaftExecutorPool pool = new(4); // 4 threads — more than the 1 partition to stress
        pool.Start();

        StubHost host = new(0);
        TestReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, 0, 0, NullLogger<IRaft>.Instance, pool: pool);
        sink.Executor = executor;
        executor.Start();
        await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        try
        {
            // AppendLogs with non-empty logs → AppendLogsAsync → EnqueueProposeOrCommit.
            // Posting 200 of them from concurrent tasks stresses the _runLock guard.
            const int ops = 200;
            HLCTimestamp ts = new HybridLogicalClock().TrySendOrLocalEvent(1);
            List<RaftLog> logs = [new RaftLog { Type = 0 }];

            Task[] tasks = Enumerable.Range(0, ops)
                .Select(_ => Task.Run(() =>
                    executor.Post(new RaftRequest(
                        RaftRequestType.AppendLogs,
                        term: 0,
                        timestamp: ts,
                        endpoint: "leader-node",
                        logs: logs))))
                .ToArray();

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            await executor.DrainAsync(TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
        finally
        {
            executor.Stop();
            executor.Dispose();
        }

        Assert.True(totalCalled > 0, "EnqueueProposeOrCommit was never called — instrumented path is no longer exercised.");
        Assert.False(concurrentDetected, "Two operations for the same partition ran concurrently — single-owner invariant violated.");
    }

    // ── Stop() unblocks cleanly in pool mode ──────────────────────────────

    [Fact]
    public async Task PoolMode_Stop_UnblocksCleanly()
    {
        using RaftExecutorPool pool = new(2);
        pool.Start();

        var (executor, _) = BuildExecutor(pool, partitionId: 0);
        await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // Post some work then stop — should not deadlock.
        for (int i = 0; i < 10; i++)
            executor.Post(new RaftRequest(RaftRequestType.CheckLeader));

        bool stopped = await Task.Run(executor.Stop, TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken)
            .ContinueWith(t => !t.IsFaulted && !t.IsCanceled);
        Assert.True(stopped, "Stop() deadlocked or threw in pool mode.");

        executor.Dispose();
    }

    // ── Idle-executor stop (regression for the _inQueue/Stop() race) ────────

    /// <summary>
    /// Stop() on a fully-idle executor (restore done, no pending work) must not
    /// deadlock even when it races with the final DrainOnPool that just cleared
    /// _inQueue.
    ///
    /// Regression for: DrainOnPool reads _stopping==false, releases run-lock,
    /// then Stop() sets _stopping=true and its MarkRunnable() CAS fails because
    /// _inQueue is still 1, then DrainOnPool clears _inQueue and exits — leaving
    /// Stop() blocked on _stopTcs with nobody scheduled.
    /// </summary>
    [Fact]
    public async Task PoolMode_Stop_IdleExecutor_DoesNotDeadlock()
    {
        using RaftExecutorPool pool = new(2);
        pool.Start();

        // Run many iterations to expose the narrow timing window.
        for (int i = 0; i < 50; i++)
        {
            var (executor, _) = BuildExecutor(pool, partitionId: i % 8);
            await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Fully drain — executor is now idle with _inQueue potentially == 0 or 1
            // depending on whether the restore DrainOnPool just finished.
            await executor.DrainAsync(TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Stop() on a quiescent executor — must complete promptly.
            bool completed = await Task.Run(executor.Stop, TestContext.Current.CancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
                .ContinueWith(t => !t.IsFaulted && !t.IsCanceled);

            Assert.True(completed, $"Stop() deadlocked on iteration {i}.");
            executor.Dispose();
        }
    }

    // ── Dedicated-thread mode still works (flag=false path) ───────────────

    [Fact]
    public async Task DedicatedThreadMode_DrainAsync_WorksAsExpected()
    {
        // No pool — dedicated thread mode.
        StubHost host = new(0);
        TestReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, new StubWal(), sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, 0, 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();

        try
        {
            await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            executor.Post(new RaftRequest(RaftRequestType.CheckLeader));
            await executor.DrainAsync(TestContext.Current.CancellationToken).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
        finally
        {
            executor.Stop();
            executor.Dispose();
        }
    }

    // ── WAL helpers ───────────────────────────────────────────────────────

    // Delays EnqueuePropose by a fixed interval so pool threads are occupied
    // for a measurable window — used to simulate a saturated client queue.
    private sealed class SlowWal : IRaftWalFacade
    {
        private readonly int _delayMs;
        public SlowWal(int delayMs) => _delayMs = delayMs;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            Thread.Sleep(_delayMs);
            return MakeNoOp();
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => MakeNoOp();
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp()
            => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class InstrumentedWal : IRaftWalFacade
    {
        private readonly Action _onPropose;

        public InstrumentedWal(Action onPropose) => _onPropose = onPropose;

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync() => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(0L);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
            => MakeNoOp();

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();

        // AppendLogsAsync calls this method for non-empty log lists — the instrumented path.
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
        {
            _onPropose();
            return MakeNoOp();
        }
        public void NotifyCommitted() { }

        private static WALWriteOperation MakeNoOp()
            => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }
}
