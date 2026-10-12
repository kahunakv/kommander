using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Shutdown, linger and metadata-failure edges of <see cref="FairWalScheduler"/>:
///
/// <list type="bullet">
///   <item>Cancelling the scheduler while a worker is inside the deferred group-commit linger must
///         still process the batch the worker had already gathered — cancellation ends the WAIT,
///         never the write.</item>
///   <item>The shutdown drain never touches a partition another worker is writing (the
///         single-writer-per-partition invariant): the queued tail of an in-flight partition is
///         completed by its owning worker's own re-schedule, in submission order.</item>
///   <item>A metadata write (hard state / HLC floor) whose engine call throws degrades to
///         <see cref="RaftOperationStatus.Errored"/> on that operation's completion without
///         killing the worker loop — the next operation still processes.</item>
///   <item>An evidence-gated linger with no arrival inside the window flushes alone instead of
///         holding the write hostage to an arrival that never comes.</item>
///   <item>Lifecycle: Enqueue before Start and after Stop are refused.</item>
/// </list>
/// </summary>
public sealed class TestFairWalSchedulerShutdownEdges
{
    // ── helpers ───────────────────────────────────────────────────────────────

    private static WALWriteOperation MakeOp(
        int partitionId,
        long operationId,
        Action<RaftWalCompletion> onComplete) =>
        new(onComplete, operationId, WALWriteOperationType.LeaderPropose, (partitionId, [new RaftLog { Id = operationId }]));

    private static WALWriteOperation MakeHardStateOp(
        int partitionId, long operationId, long term, string votedFor, Action<RaftWalCompletion> onComplete) =>
        new(onComplete, operationId, WALWriteOperationType.HardState, (partitionId, []), term: term, votedFor: votedFor);

    private static WALWriteOperation MakeHlcFloorOp(
        int partitionId, long operationId, long value, Action<RaftWalCompletion> onComplete) =>
        new(onComplete, operationId, WALWriteOperationType.HlcFloor, (partitionId, []), metadataValue: value);

    private static async Task WaitUntilAsync(Func<bool> predicate, int timeoutMs, string what)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
                return;

            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.True(predicate(), what);
    }

    // ── cancellation during the linger ───────────────────────────────────────

    /// <summary>
    /// A worker mid-linger when Stop fires must break out of the WAIT and still run the group
    /// batch it had gathered, and every operation accepted before Stop must complete before the
    /// workers exit — including the tail that was queued behind a write held inside the engine.
    /// </summary>
    [Fact]
    public async Task CancelDuringLinger_StillProcessesTheGatheredBatchAndTheHeldTail()
    {
        const int lingerMs = 5_000;
        GatedWal wal = new();
        ConcurrentQueue<(int Partition, long OpId)> completed = new();

        using FairWalScheduler scheduler = new(
            wal, NullLogger<IRaft>.Instance, workerCount: 2, groupCommitLingerMs: lingerMs);
        scheduler.Start();

        void OnComplete(RaftWalCompletion c) => completed.Enqueue((c.PartitionId, c.OperationId));

        // Worker 1 takes partition 2's first op and blocks inside the engine's Write.
        scheduler.Enqueue(MakeOp(2, 21, OnComplete));
        await WaitUntilAsync(() => wal.BlockedWriters == 1, 5_000, "the first worker must be held inside Write");

        // Partition 2's second op queues behind the in-flight write; partition 1's op is taken by
        // the free worker, whose sweep finds nothing ready but whose evidence gate sees the queued
        // op and enters the timed wait.
        scheduler.Enqueue(MakeOp(2, 22, OnComplete));
        scheduler.Enqueue(MakeOp(1, 11, OnComplete));
        await WaitUntilAsync(() => scheduler.TotalLingerWaits >= 1, 5_000, "the free worker must enter the linger wait");

        // Shutdown while the worker is parked in that wait, then let the engine answer.
        Task stopTask = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);
        wal.Release();

        await stopTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        (int Partition, long OpId)[] order = completed.ToArray();
        Assert.Equal(3, order.Length);
        Assert.Contains((2, 21L), order);
        Assert.Contains((2, 22L), order);
        Assert.Contains((1, 11L), order);

        // Per-partition FIFO survived the shutdown: 21 was submitted before 22.
        int first = Array.FindIndex(order, e => e.OpId == 21);
        int second = Array.FindIndex(order, e => e.OpId == 22);
        Assert.True(first < second, $"partition 2 FIFO violated across shutdown: {order[0]} then {order[1]} then {order[2]}");
    }

    // ── the shutdown drain respects the single-writer invariant ─────────────

    /// <summary>
    /// While a partition's write is in flight inside the engine, the shutdown drain must skip it:
    /// draining its queued tail here would let two workers complete the same partition
    /// concurrently and deliver its completions out of submission order. The owning worker
    /// re-schedules the partition when its write finishes, and the drain that follows completes
    /// the tail — 1 before 2, exactly once each.
    /// </summary>
    [Fact]
    public async Task ShutdownDrain_NeverTouchesAnInFlightPartition_ItsOwnWorkerCompletesTheTail()
    {
        GatedWal wal = new();
        ConcurrentQueue<long> completed = new();

        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 2);
        scheduler.Start();

        void OnComplete(RaftWalCompletion c) => completed.Enqueue(c.OperationId);

        scheduler.Enqueue(MakeOp(1, 1, OnComplete));
        await WaitUntilAsync(() => wal.BlockedWriters == 1, 5_000, "the worker must be held inside Write");

        // This op is queued behind the in-flight write: the partition is InFlight and NOT in the
        // ready-queue, so only the shutdown drain's partition sweep could (wrongly) pick it up.
        scheduler.Enqueue(MakeOp(1, 2, OnComplete));

        Task stopTask = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        // Let the cancelling worker run its drain pass while the write is still held, then answer.
        await Task.Delay(50, TestContext.Current.CancellationToken);
        wal.Release();

        await stopTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        long[] order = completed.ToArray();
        Assert.Equal([1L, 2L], order);
    }

    // ── a throwing metadata write must not kill the worker ──────────────────

    /// <summary>
    /// A hard-state or HLC-floor write whose engine call throws completes with
    /// <see cref="RaftOperationStatus.Errored"/> (the caller must learn the term/vote is not
    /// durable) and the worker loop survives to process the next operation normally.
    /// </summary>
    [Fact]
    public async Task MetadataWrite_Throwing_CompletesErrored_AndDoesNotKillTheWorkerLoop()
    {
        ThrowingMetadataWal wal = new();

        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        TaskCompletionSource<RaftWalCompletion> hardStateDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeHardStateOp(3, operationId: 41, term: 7, votedFor: "localhost:9999", c => hardStateDone.TrySetResult(c)));

        RaftWalCompletion hardState = await hardStateDone.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Errored, hardState.Status);
        Assert.Equal(WALWriteOperationType.HardState, hardState.OperationType);
        Assert.Equal(7, hardState.Term);

        TaskCompletionSource<RaftWalCompletion> hlcFloorDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeHlcFloorOp(3, operationId: 42, value: 123_456, c => hlcFloorDone.TrySetResult(c)));

        RaftWalCompletion hlcFloor = await hlcFloorDone.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Errored, hlcFloor.Status);
        Assert.Equal(WALWriteOperationType.HlcFloor, hlcFloor.OperationType);
        Assert.Equal(123_456, hlcFloor.MetadataValue);

        // The worker loop survived: an ordinary log write still completes with Success.
        TaskCompletionSource<RaftWalCompletion> proposeDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeOp(4, operationId: 43, c => proposeDone.TrySetResult(c)));

        RaftWalCompletion propose = await proposeDone.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(RaftOperationStatus.Success, propose.Status);
        Assert.Equal(1, scheduler.TotalBatchesWritten);
    }

    // ── the linger window is a bound, not a block ────────────────────────────

    /// <summary>
    /// When the awaited arrival does not materialize inside the window, the linger must give up
    /// and flush the gathered batch alone: the window bounds how long a write with quorum evidence
    /// waits for coalescing, and an arrival that never comes must never hold it hostage.
    /// </summary>
    [Fact]
    public async Task Linger_NoArrivalWithinTheWindow_FlushesAlone()
    {
        const int lingerMs = 250;
        FirstWriteGatedWal wal = new();

        using FairWalScheduler scheduler = new(
            wal, NullLogger<IRaft>.Instance, workerCount: 2, groupCommitLingerMs: lingerMs);
        scheduler.Start();

        // The same evidence shape as the gather test, but the engine's gate on the FIRST write is
        // never opened during the window, so the expected arrival (partition 2's re-schedule)
        // cannot happen. Later writes pass straight through, so partition 1's completion can only
        // be produced by the linger window expiring and the worker flushing alone.
        scheduler.Enqueue(MakeOp(2, 21, _ => { }));
        await WaitUntilAsync(() => wal.FirstWriteHeld, 5_000, "the first worker must be held inside Write");

        scheduler.Enqueue(MakeOp(2, 22, _ => { }));

        TaskCompletionSource<long> flushed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeOp(1, 11, c => flushed.TrySetResult(c.OperationId)));

        long opId = await flushed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(11, opId);
        Assert.True(scheduler.TotalLingerWaits >= 1, "the evidence gate must have admitted the timed wait");

        wal.Release();
        scheduler.Stop();

        Assert.Equal(0, scheduler.GetPartitionDepth(1));
        Assert.Equal(0, scheduler.GetPartitionDepth(2));
    }

    // ── lifecycle guards ─────────────────────────────────────────────────────

    [Fact]
    public void Enqueue_BeforeStart_AndAfterStop_IsRefused()
    {
        ImmediateWal wal = new();

        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);

        Assert.Throws<InvalidOperationException>(() => scheduler.Enqueue(MakeOp(1, 1, _ => { })));

        scheduler.Start();
        scheduler.Stop();

        Assert.Throws<InvalidOperationException>(() => scheduler.Enqueue(MakeOp(1, 2, _ => { })));
    }

    // ── fake WALs ────────────────────────────────────────────────────────────

    /// <summary>
    /// A WAL whose FIRST write call blocks until released; every later write passes through. Used
    /// to hold one partition's write in flight while letting a flushed batch complete.
    /// </summary>
    private sealed class FirstWriteGatedWal : IWAL
    {
        private readonly ManualResetEventSlim _firstWriteStarted = new(false);
        private readonly ManualResetEventSlim _releaseGate = new(false);
        private int _writeCount;

        /// <summary>True once the first Write call has started and is being held.</summary>
        public bool FirstWriteHeld => _firstWriteStarted.IsSet && !_releaseGate.IsSet;

        public void Release() => _releaseGate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            if (Interlocked.Increment(ref _writeCount) == 1)
            {
                _firstWriteStarted.Set();
                _releaseGate.Wait();
            }

            return RaftOperationStatus.Success;
        }

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public long GetLastCheckpoint(int partitionId) => -1;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public (RaftOperationStatus, long) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus, int) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose()
        {
            _firstWriteStarted.Dispose();
            _releaseGate.Dispose();
        }
    }

    /// <summary>A WAL whose writes block until released, counting the writers held inside.</summary>
    private sealed class GatedWal : IWAL, IDisposable
    {
        private readonly ManualResetEventSlim _gate = new(false);
        private int _blockedWriters;

        public int BlockedWriters => Volatile.Read(ref _blockedWriters);

        public void Release() => _gate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            Interlocked.Increment(ref _blockedWriters);
            try
            {
                _gate.Wait();
            }
            finally
            {
                Interlocked.Decrement(ref _blockedWriters);
            }

            return RaftOperationStatus.Success;
        }

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public long GetLastCheckpoint(int partitionId) => -1;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public (RaftOperationStatus, long) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus, int) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() => _gate.Dispose();
    }

    /// <summary>A WAL whose metadata primitives fail like a failing device.</summary>
    private sealed class ThrowingMetadataWal : IWAL
    {
        public bool PersistHardState(int partitionId, long currentTerm, string? votedFor)
            => throw new InvalidOperationException("Simulated metadata device failure.");

        public bool PersistHlcFloor(int partitionId, long floorL)
            => throw new InvalidOperationException("Simulated metadata device failure.");

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
        public (RaftOperationStatus, long) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus, int) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() { }
    }

    /// <summary>A plain succeeding WAL for lifecycle tests.</summary>
    private sealed class ImmediateWal : IWAL
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
        public (RaftOperationStatus, long) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus, int) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() { }
    }
}
