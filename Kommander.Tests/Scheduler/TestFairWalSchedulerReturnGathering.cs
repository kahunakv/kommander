#if !KOMMANDER_THREAD_FREE
// Return gathering needs worker threads, which the thread-free build does not have.
using System.Diagnostics;
using Kommander.Data;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// On a store that runs one synced write at a time, partitions written in a closed loop split into
/// two groups that take turns: a partition comes back a moment after the synced write that carried
/// it ends, by which time the next synced write has started without it, so it waits for that write
/// and pays its own after. Every round then costs two syncs. The scheduler removes the split: it
/// starts no synced write while another is in progress, and after one ends it waits, for a bounded
/// window, for the partitions that write carried and that are expected back.
///
/// <para>These tests drive that with a store that says it serializes its synced writes. They use a
/// window far longer than any test step, so that only the event under test can end a wait, and they
/// pin the window by seeding the measured sync duration.</para>
/// </summary>
public sealed class TestFairWalSchedulerReturnGathering
{
    private const int Loop = 1;
    private const int Other = 2;
    private const int Third = 3;

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(20);

    /// <summary>A window no test step outlasts: a wait under it ends only when its condition does.</summary>
    private static readonly TimeSpan LongWindow = TimeSpan.FromSeconds(60);

    /// <summary>A seeded sync duration that leaves the configured bound as the window in use (half of it is far above any bound here).</summary>
    private static readonly TimeSpan SlowSync = TimeSpan.FromHours(1);

    private static FairWalScheduler NewScheduler(
        IWAL wal,
        TimeSpan? window,
        TimeSpan? syncAverage,
        bool manualExecution = false,
        int workerCount = 4)
    {
        FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: workerCount,
            lazyCommitMarkers: true,
            manualExecution: manualExecution,
            markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs,
            returnWindow: window);

        if (syncAverage is { } average)
            scheduler.SeedSyncWriteAverageForTesting(average);

        scheduler.Start();
        return scheduler;
    }

    // ── Who is expected back ───────────────────────────────────────────────

    /// <summary>
    /// A partition is expected back only once it has come back at once: its first synced write tells
    /// nothing, and the return that follows is the evidence. From then on every synced write of it
    /// marks it as expected.
    /// </summary>
    [Fact]
    public async Task Partition_IsExpectedBack_OnlyAfterItReturnedAtOnce()
    {
        using SerializedWal wal = new();
        using FairWalScheduler scheduler = NewScheduler(wal, LongWindow, SlowSync);

        await Propose(scheduler, Loop, 1).Completed.WaitAsync(Wait);
        Assert.Equal(0, scheduler.TotalReturnsAwaited);

        // Back inside the window: the next synced write of the partition expects it back.
        await Propose(scheduler, Loop, 2).Completed.WaitAsync(Wait);
        Assert.Equal(1, scheduler.TotalReturnsAwaited);

        await Propose(scheduler, Loop, 3).Completed.WaitAsync(Wait);
        Assert.Equal(2, scheduler.TotalReturnsAwaited);

        // A partition alone never waits for itself: it is back as soon as its write is enqueued.
        Assert.Equal(0, scheduler.TotalReturnGathers);
        Assert.Equal(0, scheduler.TotalReturnsMissed);
        Assert.Equal(3, wal.Calls.Count);
    }

    /// <summary>
    /// The next synced write is held for a partition that is expected back, and written when it is:
    /// one engine write for both, where an immediate write would have left the returning partition to
    /// wait for it and then pay a sync of its own.
    /// </summary>
    [Fact]
    public async Task ExpectedPartition_HoldsTheNextSyncedWrite_UntilItIsBack()
    {
        using SerializedWal wal = new();
        using FairWalScheduler scheduler = NewScheduler(wal, LongWindow, SlowSync);

        scheduler.ExpectReturnsForTesting(Loop);
        await Propose(scheduler, Loop, 1).Completed.WaitAsync(Wait);
        Assert.Equal(1, scheduler.TotalReturnsAwaited);

        int writesBefore = wal.Started;

        Operation other = Propose(scheduler, Other, 100);
        await WaitUntil(() => scheduler.TotalReturnGathers == 1);

        // Held: not written, not complete, still counted as pending.
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(other.Completed.IsCompleted);
        Assert.Equal(writesBefore, wal.Started);
        Assert.Equal(1, scheduler.GetPartitionDepth(Other));

        Operation back = Propose(scheduler, Loop, 2);

        RaftWalCompletion otherDone = await other.Completed.WaitAsync(Wait);
        RaftWalCompletion backDone = await back.Completed.WaitAsync(Wait);

        WriteCall shared = Assert.Single(wal.Calls.Skip(writesBefore));
        Assert.True(shared.Sync);
        Assert.Equal(
            [(Loop, 2L, RaftLogType.Proposed), (Other, 100L, RaftLogType.Proposed)],
            shared.Rows.OrderBy(row => row.PartitionId).ToList());

        Assert.Equal(RaftOperationStatus.Success, otherDone.Status);
        Assert.Equal(RaftOperationStatus.Success, backDone.Status);
        Assert.True(otherDone.Synced);
        Assert.Equal(0, scheduler.TotalReturnsMissed);
        Assert.Equal(0, scheduler.GetPartitionDepth(Other));
    }

    /// <summary>
    /// A batch that is ready while a synced write is in progress is not sent to the engine, where it
    /// would only wait for that sync with its content fixed. When the write ends, the batch waits for
    /// the partition that write carried, and the two share one engine write.
    /// </summary>
    [Fact]
    public async Task ReadyBatch_WaitsForTheSyncedWriteInProgress_AndThenForItsPartition()
    {
        using SerializedWal wal = new();
        using FairWalScheduler scheduler = NewScheduler(wal, LongWindow, SlowSync);

        scheduler.ExpectReturnsForTesting(Loop);

        using Gate gate = wal.Block(Loop);
        Operation first = Propose(scheduler, Loop, 1);
        await gate.Entered.WaitAsync(Wait);

        Operation other = Propose(scheduler, Other, 100);
        await WaitUntil(() => scheduler.TotalReturnGathers == 1);

        // Only the write in progress has reached the store.
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal(1, wal.Started);
        Assert.False(other.Completed.IsCompleted);

        gate.Open();
        await first.Completed.WaitAsync(Wait);
        await WaitUntil(() => scheduler.TotalReturnsAwaited == 1);

        // The write ended; its partition is expected back, so the batch is still held.
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal(1, wal.Started);
        Assert.False(other.Completed.IsCompleted);

        Operation back = Propose(scheduler, Loop, 2);

        await other.Completed.WaitAsync(Wait);
        await back.Completed.WaitAsync(Wait);

        Assert.Equal(2, wal.Calls.Count);
        Assert.Equal(
            [(Loop, 2L, RaftLogType.Proposed), (Other, 100L, RaftLogType.Proposed)],
            wal.Calls[1].Rows.OrderBy(row => row.PartitionId).ToList());

        // One hold covered both waits.
        Assert.Equal(1, scheduler.TotalReturnGathers);
    }

    /// <summary>
    /// A synced write in progress that nobody is expected back from holds no batch: the ready
    /// partitions go to the store at once, as they did without gathering, and wait there.
    /// </summary>
    [Fact]
    public async Task ReadyBatch_IsNotHeld_ForAWriteNobodyIsExpectedBackFrom()
    {
        using SerializedWal wal = new();
        using FairWalScheduler scheduler = NewScheduler(wal, LongWindow, SlowSync);

        using Gate gate = wal.Block(Loop);
        Operation first = Propose(scheduler, Loop, 1);
        await gate.Entered.WaitAsync(Wait);

        Operation other = Propose(scheduler, Other, 100);

        // The second write reached the store while the first is still inside it.
        await WaitUntil(() => wal.Started == 2);
        Assert.Equal(0, scheduler.TotalReturnGathers);
        Assert.False(other.Completed.IsCompleted);

        gate.Open();

        await first.Completed.WaitAsync(Wait);
        await other.Completed.WaitAsync(Wait);

        Assert.Equal(2, wal.Calls.Count);
        Assert.Equal(0, scheduler.TotalReturnGathers);
        Assert.Equal(0, scheduler.TotalReturnsAwaited);
    }

    /// <summary>
    /// Several writers on one partition are gathered like several partitions. The partition is back
    /// when as many operations returned as its last synced write carried: the held batch does not go
    /// when the first writer is back, which would leave the other to the write after.
    /// </summary>
    [Fact]
    public async Task SeveralWritersOnOnePartition_HoldTheWriteUntilAllOfThemAreBack()
    {
        using SerializedWal wal = new();

        // One worker, so that while it is inside a held write the two operations below queue up
        // and are then drained together, as one synced write of the partition.
        using FairWalScheduler scheduler = NewScheduler(wal, LongWindow, SlowSync, workerCount: 1);

        scheduler.ExpectReturnsForTesting(Loop);

        using Gate gate = wal.Block(Third);
        Operation plug = Propose(scheduler, Third, 900);
        await gate.Entered.WaitAsync(Wait);

        Operation firstWriter = Enqueue(scheduler, Loop, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        Operation secondWriter = Enqueue(scheduler, Loop, 2, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 2);

        gate.Open();
        await plug.Completed.WaitAsync(Wait);
        await firstWriter.Completed.WaitAsync(Wait);
        await secondWriter.Completed.WaitAsync(Wait);

        Assert.Equal(2, wal.Calls.Count);
        Assert.Equal([(Loop, 1L, RaftLogType.Proposed), (Loop, 2L, RaftLogType.Proposed)], wal.Calls[1].Rows);
        Assert.Equal(1, scheduler.TotalReturnsAwaited);

        Operation other = Propose(scheduler, Other, 100);
        await WaitUntil(() => scheduler.TotalReturnGathers == 1);

        // One writer is back: the partition is not, and the batch stays held.
        Operation firstBack = Enqueue(scheduler, Loop, 3, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 3);

        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal(2, wal.Started);
        Assert.False(other.Completed.IsCompleted);
        Assert.False(firstBack.Completed.IsCompleted);

        Operation secondBack = Enqueue(scheduler, Loop, 4, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 4);

        await other.Completed.WaitAsync(Wait);
        await firstBack.Completed.WaitAsync(Wait);
        await secondBack.Completed.WaitAsync(Wait);

        Assert.Equal(3, wal.Calls.Count);
        Assert.Equal(
            [(Loop, 3L, RaftLogType.Proposed), (Loop, 4L, RaftLogType.Proposed), (Other, 100L, RaftLogType.Proposed)],
            wal.Calls[2].Rows.OrderBy(row => row.PartitionId).ThenBy(row => row.Id).ToList());
        Assert.Equal(0, scheduler.TotalReturnsMissed);
    }

    // ── Markers ────────────────────────────────────────────────────────────

    /// <summary>
    /// The commit markers of a partition that is expected back are not written on their own. The
    /// partition's next propose carries them: one engine write, markers first, both operations
    /// complete from it in submission order, as synced.
    /// </summary>
    [Fact]
    public async Task MarkersOfAnExpectedPartition_RideItsNextPropose()
    {
        using SerializedWal wal = new();
        using FairWalScheduler scheduler = NewScheduler(wal, LongWindow, SlowSync);

        scheduler.ExpectReturnsForTesting(Loop);
        await Propose(scheduler, Loop, 1).Completed.WaitAsync(Wait);

        int writesBefore = wal.Started;
        List<long> completionOrder = [];

        Operation marker = Enqueue(scheduler, Loop, 2, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, 1, completionOrder);
        await WaitUntil(() => scheduler.TotalReturnGathers == 1);

        // A marker is not the partition's return: the propose is.
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(marker.Completed.IsCompleted);
        Assert.Equal(writesBefore, wal.Started);

        Operation propose = Enqueue(scheduler, Loop, 3, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 2, completionOrder);

        RaftWalCompletion markerDone = await marker.Completed.WaitAsync(Wait);
        RaftWalCompletion proposeDone = await propose.Completed.WaitAsync(Wait);

        WriteCall carried = Assert.Single(wal.Calls.Skip(writesBefore));
        Assert.True(carried.Sync);
        Assert.Equal([(Loop, 1L, RaftLogType.Committed), (Loop, 2L, RaftLogType.Proposed)], carried.Rows);

        Assert.Equal(RaftOperationStatus.Success, markerDone.Status);
        Assert.Equal(RaftOperationStatus.Success, proposeDone.Status);
        Assert.True(markerDone.Synced);

        lock (completionOrder)
            Assert.Equal([2L, 3L], completionOrder);

        Assert.Equal(0, scheduler.TotalMarkerBatchesDeferred);
        Assert.Equal(0, scheduler.GetPartitionDepth(Loop));
    }

    // ── A wrong guess ──────────────────────────────────────────────────────

    /// <summary>
    /// A partition that is expected back and does not come costs the held batch one window, no more,
    /// and is then not expected: its later synced writes hold nobody until it has shown again that it
    /// returns at once.
    /// </summary>
    [Fact]
    public async Task ExpectedPartitionThatDoesNotComeBack_CostsOneWindow_AndIsNotExpectedNext()
    {
        TimeSpan window = TimeSpan.FromMilliseconds(250);

        using SerializedWal wal = new();
        using FairWalScheduler scheduler = NewScheduler(wal, window, SlowSync);

        scheduler.ExpectReturnsForTesting(Loop);
        await Propose(scheduler, Loop, 1).Completed.WaitAsync(Wait);
        Assert.Equal(1, scheduler.TotalReturnsAwaited);

        Stopwatch held = Stopwatch.StartNew();
        RaftWalCompletion otherDone = await Propose(scheduler, Other, 100).Completed.WaitAsync(Wait);
        held.Stop();

        Assert.Equal(RaftOperationStatus.Success, otherDone.Status);
        Assert.Equal(1, scheduler.TotalReturnGathers);
        Assert.Equal(1, scheduler.TotalReturnsMissed);

        // It waited for most of the window (the window started when the first write ended, a
        // moment before this stopwatch), and the window ended the wait.
        Assert.True(held.Elapsed >= TimeSpan.FromMilliseconds(100), $"Written after {held.Elapsed.TotalMilliseconds:0} ms: not held.");
        Assert.True(held.Elapsed < Wait / 2, $"Written after {held.Elapsed.TotalMilliseconds:0} ms: the window did not end the wait.");

        // The partition comes back late. Its write completes, and it is not expected back after it,
        // nor after the next one: a miss doubles the returns it has to show first.
        await Propose(scheduler, Loop, 2).Completed.WaitAsync(Wait);
        await Propose(scheduler, Loop, 3).Completed.WaitAsync(Wait);

        Assert.Equal(1, scheduler.TotalReturnsAwaited);

        // So a batch of another partition is not held for it.
        await Propose(scheduler, Third, 200).Completed.WaitAsync(Wait);
        Assert.Equal(1, scheduler.TotalReturnGathers);
    }

    // ── Shutdown ───────────────────────────────────────────────────────────

    /// <summary>A stop request ends a hold: the held batch is written and the scheduler stops.</summary>
    [Fact]
    public async Task Stop_WhileABatchIsHeld_WritesItAndReturns()
    {
        using SerializedWal wal = new();
        using FairWalScheduler scheduler = NewScheduler(wal, LongWindow, SlowSync);

        scheduler.ExpectReturnsForTesting(Loop);
        await Propose(scheduler, Loop, 1).Completed.WaitAsync(Wait);

        Operation other = Propose(scheduler, Other, 100);
        await WaitUntil(() => scheduler.TotalReturnGathers == 1);
        Assert.False(other.Completed.IsCompleted);

        await Task.Run(scheduler.Stop, TestContext.Current.CancellationToken).WaitAsync(Wait, TestContext.Current.CancellationToken);

        RaftWalCompletion otherDone = await other.Completed.WaitAsync(Wait);
        Assert.Equal(RaftOperationStatus.Success, otherDone.Status);
        Assert.Equal(0, scheduler.GetPartitionDepth(Other));
    }

    // ── Where it does not apply ────────────────────────────────────────────

    public enum Off
    {
        /// <summary>The store syncs its partitions independently, or has no sync to share.</summary>
        StoreDoesNotSerialize,

        /// <summary>No window was given.</summary>
        NoWindow,

        /// <summary>A sync so cheap that half of it is below the smallest useful window.</summary>
        CheapSync,

        /// <summary>No synced write was measured yet.</summary>
        NothingMeasured,

        /// <summary>The caller drives every write; nothing may wait on the clock.</summary>
        ManualExecution,
    }

    /// <summary>
    /// Nothing is held where holding cannot pay: the batch of another partition is written at once
    /// although a partition with the history of a closed loop has just been written.
    /// </summary>
    [Theory]
    [InlineData(Off.StoreDoesNotSerialize)]
    [InlineData(Off.NoWindow)]
    [InlineData(Off.CheapSync)]
    [InlineData(Off.NothingMeasured)]
    [InlineData(Off.ManualExecution)]
    public async Task NothingIsHeld_WhereGatheringDoesNotApply(Off reason)
    {
        using SerializedWal wal = new() { Serializes = reason != Off.StoreDoesNotSerialize };

        using FairWalScheduler scheduler = NewScheduler(
            wal,
            window: reason == Off.NoWindow ? null : LongWindow,
            syncAverage: reason switch
            {
                Off.CheapSync => TimeSpan.FromMicroseconds(150),
                Off.NothingMeasured => TimeSpan.Zero,
                _ => SlowSync,
            },
            manualExecution: reason == Off.ManualExecution);

        scheduler.ExpectReturnsForTesting(Loop);

        await Propose(scheduler, Loop, 1).Completed.WaitAsync(Wait);
        await Propose(scheduler, Other, 100).Completed.WaitAsync(Wait);
        await Propose(scheduler, Loop, 2).Completed.WaitAsync(Wait);

        Assert.Equal(3, wal.Calls.Count);
        Assert.Equal(0, scheduler.TotalReturnGathers);
        Assert.Equal(0, scheduler.TotalReturnsAwaited);
        Assert.Equal(0, scheduler.TotalReturnsMissed);
    }

    // ── Under load ─────────────────────────────────────────────────────────

    /// <summary>
    /// Several partitions commit back to back, each sending its next propose when the previous one
    /// completed, while every sync takes real time on a store that runs one at a time. Every
    /// operation completes, each partition's completions arrive in submission order, the store ends
    /// with every row committed, and the gathering was in use.
    /// </summary>
    [Fact]
    public async Task ClosedLoopPartitions_CompleteEverythingInOrder()
    {
        const int partitions = 6;
        const int rounds = 120;

        using SerializedWal wal = new() { SyncDelay = TimeSpan.FromMilliseconds(2) };

        // The window the manager gives, and a sync duration measured from the store.
        using FairWalScheduler scheduler = NewScheduler(wal, FairWalScheduler.DefaultReturnWindow, syncAverage: null);

        Task[] writers = new Task[partitions];

        for (int p = 0; p < partitions; p++)
        {
            int partitionId = 10 + p;

            writers[p] = Task.Run(async () =>
            {
                List<long> completionOrder = [];
                List<long> expectedOrder = [];
                Task<RaftWalCompletion>? lastMarker = null;
                long operationId = 0;

                for (long id = 1; id <= rounds; id++)
                {
                    Operation propose = Enqueue(scheduler, partitionId, ++operationId, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, id, completionOrder);
                    expectedOrder.Add(operationId);
                    Assert.Equal(RaftOperationStatus.Success, (await propose.Completed.WaitAsync(Wait)).Status);

                    // As on the fast path: the marker is enqueued when the propose is durable, and the
                    // next propose follows without waiting for it.
                    Operation marker = Enqueue(scheduler, partitionId, ++operationId, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, id, completionOrder);
                    expectedOrder.Add(operationId);
                    lastMarker = marker.Completed;
                }

                Assert.Equal(RaftOperationStatus.Success, (await lastMarker!.WaitAsync(Wait)).Status);

                lock (completionOrder)
                    Assert.Equal(expectedOrder, completionOrder);
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(writers).WaitAsync(TimeSpan.FromSeconds(120), TestContext.Current.CancellationToken);

        for (int p = 0; p < partitions; p++)
        {
            List<RaftLog> stored = wal.ReadLogs(10 + p);
            Assert.Equal(rounds, stored.Count);
            Assert.All(stored, log => Assert.Equal(RaftLogType.Committed, log.Type));
            Assert.Equal(0, scheduler.GetPartitionDepth(10 + p));
        }

        Assert.True(scheduler.SyncWriteAverageMs >= 1, $"The sync duration was not measured ({scheduler.SyncWriteAverageMs:0.000} ms).");
        Assert.True(scheduler.TotalReturnsAwaited > 0, "No partition was ever expected back.");
        Assert.True(scheduler.TotalReturnGathers > 0, "No batch was ever held.");
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private sealed record Operation(Task<RaftWalCompletion> Completed);

    private static Operation Propose(FairWalScheduler scheduler, int partitionId, long logId) =>
        Enqueue(scheduler, partitionId, logId, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, logId);

    private static Operation Enqueue(
        FairWalScheduler scheduler,
        int partitionId,
        long operationId,
        WALWriteOperationType type,
        RaftLogType logType,
        long logId,
        List<long>? completionOrder = null)
    {
        TaskCompletionSource<RaftWalCompletion> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        scheduler.Enqueue(new WALWriteOperation(
            completion =>
            {
                if (completionOrder is not null)
                {
                    lock (completionOrder)
                        completionOrder.Add(completion.OperationId);
                }

                completed.TrySetResult(completion);
            },
            operationId,
            type,
            (partitionId, [new RaftLog { Id = logId, Term = 1, Type = logType, LogType = "test", LogData = [] }]),
            logIndex: logId));

        return new(completed.Task);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        long deadline = Environment.TickCount64 + (long)Wait.TotalMilliseconds;

        while (!condition())
        {
            Assert.True(Environment.TickCount64 < deadline, "the condition did not hold in time");
            await Task.Delay(1, TestContext.Current.CancellationToken);
        }
    }

    private sealed record WriteCall(bool Sync, List<(int PartitionId, long Id, RaftLogType Type)> Rows);

    /// <summary>Holds the synced writes of one partition open until <see cref="Open"/>.</summary>
    private sealed class Gate : IDisposable
    {
        private readonly ManualResetEventSlim released = new(false);
        private readonly TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completes when a synced write of the partition is inside the store, held.</summary>
        public Task Entered => entered.Task;

        public void Open() => released.Set();

        public void Hold()
        {
            entered.TrySetResult();
            released.Wait();
        }

        public void Dispose()
        {
            released.Set();
            released.Dispose();
        }
    }

    /// <summary>
    /// An in-memory store that behaves like an engine with one log: it runs one write at a time, says
    /// so (<see cref="IWAL.SerializesSyncedWrites"/>), records every write with its sync flag, counts
    /// the writes that reached it, and can hold the synced writes of chosen partitions open.
    /// </summary>
    private sealed class SerializedWal : IWAL, IDisposable
    {
        private readonly IWAL inner = new InMemoryWAL(NullLogger<IRaft>.Instance);
        private readonly object sync = new();
        private readonly object engine = new();
        private readonly List<WriteCall> calls = [];
        private readonly Dictionary<int, Gate> gates = [];
        private int started;

        /// <summary>What the store answers to <see cref="IWAL.SerializesSyncedWrites"/>.</summary>
        public bool Serializes { get; init; } = true;

        /// <summary>Time every synced write spends in the store.</summary>
        public TimeSpan SyncDelay { get; init; }

        public bool SerializesSyncedWrites => Serializes;

        /// <summary>Writes that reached the store, finished or not.</summary>
        public int Started => Volatile.Read(ref started);

        /// <summary>Writes the store finished, in order.</summary>
        public List<WriteCall> Calls
        {
            get
            {
                lock (sync)
                    return [.. calls];
            }
        }

        public Gate Block(int partitionId)
        {
            Gate gate = new();

            lock (sync)
                gates[partitionId] = gate;

            return gate;
        }

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
        {
            Interlocked.Increment(ref started);

            List<(int, long, RaftLogType)> rows = [];
            Gate? gate = null;

            lock (this.sync)
            {
                foreach ((int partitionId, List<RaftLog> partitionLogs) in logs)
                {
                    foreach (RaftLog log in partitionLogs)
                        rows.Add((partitionId, log.Id, log.Type));

                    if (sync && gate is null && gates.Remove(partitionId, out Gate? found))
                        gate = found;
                }
            }

            // One write at a time, like the write thread of an engine with one log.
            lock (engine)
            {
                gate?.Hold();

                if (sync && SyncDelay > TimeSpan.Zero)
                    Thread.Sleep(SyncDelay);

                RaftOperationStatus status = inner.Write(logs, sync);

                lock (this.sync)
                    calls.Add(new(sync, rows));

                return status;
            }
        }

        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public long GetTermAt(int partitionId, long logIndex) => inner.GetTermAt(partitionId, logIndex);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) => inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => inner.TruncateProposedLogsAfter(partitionId, afterLogId);

        public void Dispose() => inner.Dispose();
    }
}
#endif
