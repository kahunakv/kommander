using Kommander.Data;
using Kommander.Scheduling;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Tests for the externally driven scheduling modes that
/// <see cref="RaftConfiguration.EnableInternalSchedulingThreads"/> switches on.
///
/// <para><b>Why these exist.</b> Each mode removes a component's own threads so a driver can run
/// its work on one thread and know exactly when it happened. That is what makes a simulated run
/// reproducible. A mode that quietly stopped executing work, or that kept its threads anyway,
/// would produce a run that looks deterministic and is not, so each mode is tested for the two
/// things that matter: the work still happens, and it happens on the calling thread.</para>
///
/// <para>Production leaves the switch on, so none of these modes runs in a deployed node.</para>
/// </summary>
public sealed class TestExternallyDrivenScheduling
{
    // ── Read scheduler: inline ────────────────────────────────────────────

    /// <summary>
    /// An inline read is finished before <c>EnqueueTask</c> returns.
    ///
    /// <para>This is the property the whole mode exists for. A partition executor blocks on the
    /// reads it issues, so a read that completed later would deadlock a single-threaded driver:
    /// the driver would be waiting for work only it could run.</para>
    /// </summary>
    [Fact]
    public async Task InlineReadScheduler_CompletesOnTheCallingThread()
    {
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, inlineExecution: true);
        scheduler.Start();

        int callingThread = Environment.CurrentManagedThreadId;
        int observedThread = 0;

        Task<int> result = scheduler.EnqueueTask(partitionId: 1, () =>
        {
            observedThread = Environment.CurrentManagedThreadId;
            return 42;
        });

        Assert.True(result.IsCompletedSuccessfully, "the task was not already completed on return.");
        Assert.Equal(42, await result);
        Assert.Equal(callingThread, observedThread);
    }

    /// <summary>Submission order still decides execution order.</summary>
    [Fact]
    public void InlineReadScheduler_PreservesSubmissionOrder()
    {
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, inlineExecution: true);
        scheduler.Start();

        List<int> executed = [];

        for (int i = 0; i < 10; i++)
        {
            int value = i;
            scheduler.EnqueueTask(partitionId: 1, () =>
            {
                executed.Add(value);
                return value;
            });
        }

        Assert.Equal(Enumerable.Range(0, 10), executed);
    }

    /// <summary>
    /// A read enqueued from inside a running read still completes.
    ///
    /// <para>The consensus path does this constantly — an applier reads a batch, then reads the
    /// next one from the continuation — so the mode is worthless if re-entry strands the inner
    /// read or overflows the stack.</para>
    /// </summary>
    [Fact]
    public async Task InlineReadScheduler_HandlesReentrantEnqueue()
    {
        using FairReadScheduler scheduler = new(NullLogger<IRaft>.Instance, inlineExecution: true);
        scheduler.Start();

        Task<int>? inner = null;

        Task<int> outer = scheduler.EnqueueTask(partitionId: 1, () =>
        {
            inner = scheduler.EnqueueTask(partitionId: 2, () => 7);
            return 1;
        });

        Assert.True(outer.IsCompletedSuccessfully);
        Assert.NotNull(inner);
        Assert.True(inner!.IsCompletedSuccessfully);
        Assert.Equal(7, await inner);
    }

    // ── Write-ahead-log scheduler: manual ─────────────────────────────────

    /// <summary>
    /// A manual write is durable, and its completion callback delivered, before <c>Enqueue</c>
    /// returns. The callback is what the partition executor waits for, so a deferred one would
    /// deadlock the same way a deferred read would.
    /// </summary>
    [Fact]
    public void ManualWalScheduler_WritesOnTheCallingThread()
    {
        using FairWalScheduler scheduler = new(
            new NoOpWal(), NullLogger<IRaft>.Instance, manualExecution: true);
        scheduler.Start();

        int callingThread = Environment.CurrentManagedThreadId;
        int completionThread = 0;
        RaftOperationStatus status = RaftOperationStatus.Errored;

        scheduler.Enqueue(MakeOperation(partitionId: 1, operationId: 1, completion =>
        {
            completionThread = Environment.CurrentManagedThreadId;
            status = completion.Status;
        }));

        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(callingThread, completionThread);
    }

    /// <summary>An idle manual scheduler has nothing to pump.</summary>
    [Fact]
    public void ManualWalScheduler_PumpsNothingWhenIdle()
    {
        using FairWalScheduler scheduler = new(
            new NoOpWal(), NullLogger<IRaft>.Instance, manualExecution: true);
        scheduler.Start();

        Assert.False(scheduler.PumpOnce());
        Assert.Equal(0, scheduler.PumpUntilIdle());
    }

    /// <summary>
    /// Pumping a scheduler that owns worker threads is refused. Two drainers on one partition
    /// would break the single-executor discipline the worker threads rely on, and a silent
    /// no-op would hide the caller's mistake.
    /// </summary>
    [Fact]
    public void ThreadedWalScheduler_RefusesToBePumped()
    {
        using FairWalScheduler scheduler = new(new NoOpWal(), NullLogger<IRaft>.Instance, workerCount: 1);

        Assert.Throws<InvalidOperationException>(() => scheduler.PumpOnce());
    }

    // ── Executor pool: manual ─────────────────────────────────────────────

    /// <summary>An idle manual pool has nothing to drain, and starting it creates no threads.</summary>
    [Fact]
    public void ManualExecutorPool_StartsNoThreadsAndPumpsNothingWhenIdle()
    {
        using RaftExecutorPool pool = new(poolSize: 4, manualExecution: true);
        pool.Start();

        Assert.Equal(0, pool.PoolSize);
        Assert.False(pool.PumpOnce());
        Assert.Equal(0, pool.PumpUntilIdle());
    }

    /// <summary>Pumping a pool that owns worker threads is refused, for the same reason.</summary>
    [Fact]
    public void ThreadedExecutorPool_RefusesToBePumped()
    {
        using RaftExecutorPool pool = new(poolSize: 2);

        Assert.Throws<InvalidOperationException>(() => pool.PumpOnce());
    }

    // ── Configuration ─────────────────────────────────────────────────────

    /// <summary>
    /// Externally driven scheduling needs the shared executor pool. A partition executor on its
    /// own dedicated thread has nothing to pump, so the combination would start a node that then
    /// does nothing at all — a failure worth catching at startup rather than in production.
    /// </summary>
    [Fact]
    public void Validate_RejectsExternalSchedulingWithoutTheSharedPool()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 9100,
            NodeId = 1,
            EnableInternalSchedulingThreads = false,
            EnableSharedExecutorPool = false,
        };

        RaftException error = Assert.Throws<RaftException>(configuration.Validate);
        Assert.Contains("EnableSharedExecutorPool", error.Message, StringComparison.Ordinal);
    }

    /// <summary>A missing tick source stops every elapsed-time gate, so it is refused at startup.</summary>
    [Fact]
    public void Validate_RejectsANullTickSource()
    {
        RaftConfiguration configuration = new()
        {
            Host = "localhost",
            Port = 9101,
            NodeId = 1,
            TickSource = null!,
        };

        RaftException error = Assert.Throws<RaftException>(configuration.Validate);
        Assert.Contains("TickSource", error.Message, StringComparison.Ordinal);
    }

    /// <summary>The defaults keep every scheduler on its own threads.</summary>
    [Fact]
    public void Defaults_KeepInternalThreadsAndTheSystemTickSource()
    {
        RaftConfiguration configuration = new();

        Assert.True(configuration.EnableInternalSchedulingThreads);
        Assert.True(configuration.EnableInternalTimers);
        Assert.Same(Kommander.Time.SystemMonotonicTickSource.Instance, configuration.TickSource);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static WALWriteOperation MakeOperation(
        int partitionId,
        long operationId,
        Action<RaftWalCompletion> onComplete) =>
        new(
            onComplete,
            operationId,
            WALWriteOperationType.FollowerAppend,
            (partitionId, new List<RaftLog>
            {
                new() { Id = operationId, Term = 1, Type = RaftLogType.Proposed },
            }));

    /// <summary>Storage that accepts every write and holds nothing.</summary>
    private sealed class NoOpWal : IWAL
    {
        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => RaftOperationStatus.Success;

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public long GetLastCheckpoint(int partitionId) => -1;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) =>
            (RaftOperationStatus.Success, afterLogId);
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
            int partitionId, long lastCheckpoint, int count, int? maxTotalEntries = null) =>
            (RaftOperationStatus.Success, 0);
        public void Dispose() { }
    }
}
