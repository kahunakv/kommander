using Kommander.Data;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// With the single-fsync fast path on, a batch that holds only commit markers is written sync-off.
/// The storage engine serializes its writers, so such a write issued while another partition's
/// synced write is in progress waits for that sync, and the partition's next propose waits behind
/// it. The scheduler therefore holds a marker batch back while a sync is in progress, and writes it
/// with the partition's next batch, or on its own once the engine is free or the batch has waited
/// long enough.
///
/// <para>These tests hold a synced write of one partition open and drive another partition
/// around it. They pin what the deferral must keep: order inside the partition, completion only
/// after the write that carries the rows, and a write for every held batch.</para>
/// </summary>
public sealed class TestFairWalSchedulerMarkerDeferral
{
    private const int Busy = 2;
    private const int OtherBusy = 3;
    private const int Lane = 1;

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Markers enqueued while another partition syncs are not written on their own. The partition's
    /// next propose carries them: one engine write, markers first, and both operations complete
    /// from that write, in submission order, as synced.
    /// </summary>
    [Fact]
    public async Task MarkersBehindAnotherPartitionsSync_RideTheirPartitionsNextPropose()
    {
        using GatedRecordingWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: true, markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs);
        scheduler.Start();

        await Enqueue(scheduler, Lane, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1).Completed.WaitAsync(Wait);

        using Gate gate = wal.Block(Busy);
        Operation busy = Enqueue(scheduler, Busy, 100, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        await gate.Entered.WaitAsync(Wait);

        int writesBefore = wal.Calls.Count;
        List<long> completionOrder = [];

        Operation marker = Enqueue(scheduler, Lane, 2, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, 1, completionOrder);
        await WaitUntil(() => scheduler.TotalMarkerBatchesDeferred == 1);

        // Held: not written, not complete, still counted as pending.
        Assert.False(marker.Completed.IsCompleted);
        Assert.Equal(writesBefore, wal.Calls.Count);
        Assert.Equal(1, scheduler.GetPartitionDepth(Lane));

        Operation propose = Enqueue(scheduler, Lane, 3, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 2, completionOrder);

        RaftWalCompletion markerDone = await marker.Completed.WaitAsync(Wait);
        RaftWalCompletion proposeDone = await propose.Completed.WaitAsync(Wait);

        // The other partition is still inside its sync: the lane did not wait for it.
        Assert.False(busy.Completed.IsCompleted);

        WriteCall carried = Assert.Single(wal.Calls.Skip(writesBefore));
        Assert.True(carried.Sync);
        Assert.Equal([(Lane, 1L, RaftLogType.Committed), (Lane, 2L, RaftLogType.Proposed)], carried.Rows);

        Assert.Equal(RaftOperationStatus.Success, markerDone.Status);
        Assert.Equal(RaftOperationStatus.Success, proposeDone.Status);
        Assert.True(markerDone.Synced);
        Assert.Equal([2L, 3L], completionOrder);
        Assert.Equal(0, scheduler.GetPartitionDepth(Lane));

        gate.Open();
        await busy.Completed.WaitAsync(Wait);
    }

    /// <summary>
    /// A held batch whose partition sends nothing more is written when the sync it stepped around
    /// ends: on its own, sync-off, as it would have been on an idle engine.
    /// </summary>
    [Fact]
    public async Task HeldMarkers_AreWrittenWhenTheSyncEnds()
    {
        using GatedRecordingWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: true, markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs);
        scheduler.Start();

        await Enqueue(scheduler, Lane, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1).Completed.WaitAsync(Wait);

        using Gate gate = wal.Block(Busy);
        Operation busy = Enqueue(scheduler, Busy, 100, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        await gate.Entered.WaitAsync(Wait);

        Operation marker = Enqueue(scheduler, Lane, 2, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, 1);
        await WaitUntil(() => scheduler.TotalMarkerBatchesDeferred == 1);
        Assert.False(marker.Completed.IsCompleted);

        gate.Open();

        RaftWalCompletion markerDone = await marker.Completed.WaitAsync(Wait);
        await busy.Completed.WaitAsync(Wait);

        Assert.Equal(RaftOperationStatus.Success, markerDone.Status);
        Assert.False(markerDone.Synced);

        WriteCall last = wal.Calls[^1];
        Assert.False(last.Sync);
        Assert.Equal([(Lane, 1L, RaftLogType.Committed)], last.Rows);
        Assert.Equal(0, scheduler.GetPartitionDepth(Lane));
    }

    /// <summary>
    /// The wait of a held batch is bounded while syncs keep coming: once it is older than the bound,
    /// the next write that ends releases it, although another sync is still in progress.
    /// </summary>
    [Fact]
    public async Task HeldMarkersPastTheBound_AreWrittenWhileAnotherSyncIsInProgress()
    {
        using GatedRecordingWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: true, markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs);
        scheduler.Start();

        await Enqueue(scheduler, Lane, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1).Completed.WaitAsync(Wait);

        using Gate first = wal.Block(Busy);
        using Gate second = wal.Block(OtherBusy);

        Operation busy = Enqueue(scheduler, Busy, 100, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        await first.Entered.WaitAsync(Wait);
        Operation otherBusy = Enqueue(scheduler, OtherBusy, 200, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        await second.Entered.WaitAsync(Wait);

        Operation marker = Enqueue(scheduler, Lane, 2, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, 1);
        await WaitUntil(() => scheduler.TotalMarkerBatchesDeferred == 1);

        await Task.Delay(FairWalScheduler.DefaultMarkerDeferralMs * 10, TestContext.Current.CancellationToken);
        Assert.False(marker.Completed.IsCompleted);

        first.Open();

        RaftWalCompletion markerDone = await marker.Completed.WaitAsync(Wait);
        Assert.Equal(RaftOperationStatus.Success, markerDone.Status);
        Assert.False(otherBusy.Completed.IsCompleted);

        second.Open();
        await busy.Completed.WaitAsync(Wait);
        await otherBusy.Completed.WaitAsync(Wait);
    }

    /// <summary>
    /// A stop writes what is held: every operation the scheduler accepted completes before its
    /// workers exit.
    /// </summary>
    [Fact]
    public async Task Stop_WritesHeldMarkers()
    {
        using GatedRecordingWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: true, markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs);
        scheduler.Start();

        await Enqueue(scheduler, Lane, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1).Completed.WaitAsync(Wait);

        using Gate gate = wal.Block(Busy);
        Operation busy = Enqueue(scheduler, Busy, 100, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        await gate.Entered.WaitAsync(Wait);

        Operation marker = Enqueue(scheduler, Lane, 2, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, 1);
        await WaitUntil(() => scheduler.TotalMarkerBatchesDeferred == 1);

        Task stop = Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);
        gate.Open();
        await stop.WaitAsync(Wait);

        Assert.Equal(RaftOperationStatus.Success, (await marker.Completed.WaitAsync(Wait)).Status);
        Assert.Equal(RaftOperationStatus.Success, (await busy.Completed.WaitAsync(Wait)).Status);
        Assert.Contains(wal.Calls, call => call.Rows.Contains((Lane, 1L, RaftLogType.Committed)));
    }

    /// <summary>
    /// The deferral is off unless the scheduler is given a bound: a marker batch behind another
    /// partition's sync is then written at once, as it always was.
    /// </summary>
    [Fact]
    public async Task WithoutABound_MarkersAreNeverHeld()
    {
        using GatedRecordingWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: true);
        scheduler.Start();

        await Enqueue(scheduler, Lane, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1).Completed.WaitAsync(Wait);

        using Gate gate = wal.Block(Busy);
        Operation busy = Enqueue(scheduler, Busy, 100, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        await gate.Entered.WaitAsync(Wait);

        RaftWalCompletion markerDone =
            await Enqueue(scheduler, Lane, 2, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, 1).Completed.WaitAsync(Wait);

        Assert.Equal(RaftOperationStatus.Success, markerDone.Status);
        Assert.False(markerDone.Synced);
        Assert.Equal(0, scheduler.TotalMarkerBatchesDeferred);
        Assert.False(busy.Completed.IsCompleted);

        gate.Open();
        await busy.Completed.WaitAsync(Wait);
    }

    /// <summary>With no sync in progress the marker write is cheap, and it is written at once.</summary>
    [Fact]
    public async Task NoSyncInProgress_MarkersAreWrittenAtOnce()
    {
        using GatedRecordingWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: true, markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs);
        scheduler.Start();

        await Enqueue(scheduler, Lane, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1).Completed.WaitAsync(Wait);

        RaftWalCompletion markerDone =
            await Enqueue(scheduler, Lane, 2, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, 1).Completed.WaitAsync(Wait);

        Assert.Equal(RaftOperationStatus.Success, markerDone.Status);
        Assert.False(markerDone.Synced);
        Assert.Equal(0, scheduler.TotalMarkerBatchesDeferred);
        Assert.False(wal.Calls[^1].Sync);
    }

    /// <summary>
    /// What is not a lazy marker batch is never held, whatever the engine is doing: a commit that
    /// must sync (its row is a first durable write), a commit that carries a checkpoint, and every
    /// commit when the fast path is off.
    /// </summary>
    [Theory]
    [InlineData(true, true, RaftLogType.Committed)]
    [InlineData(true, false, RaftLogType.CommittedCheckpoint)]
    [InlineData(false, false, RaftLogType.Committed)]
    public async Task CommitsThatMustSync_AreNotHeld(bool lazyCommitMarkers, bool requiresSync, RaftLogType type)
    {
        using GatedRecordingWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: lazyCommitMarkers, markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs);
        scheduler.Start();

        await Enqueue(scheduler, Lane, 1, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1).Completed.WaitAsync(Wait);

        using Gate gate = wal.Block(Busy);
        Operation busy = Enqueue(scheduler, Busy, 100, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, 1);
        await gate.Entered.WaitAsync(Wait);

        RaftWalCompletion done =
            await Enqueue(scheduler, Lane, 2, WALWriteOperationType.LeaderCommit, type, 1, requiresSync: requiresSync).Completed.WaitAsync(Wait);

        Assert.Equal(RaftOperationStatus.Success, done.Status);
        Assert.True(done.Synced);
        Assert.Equal(0, scheduler.TotalMarkerBatchesDeferred);
        Assert.False(busy.Completed.IsCompleted);

        gate.Open();
        await busy.Completed.WaitAsync(Wait);
    }

    /// <summary>
    /// Many partitions commit back to back while every sync takes real time. Every operation
    /// completes, each partition's completions arrive in submission order, and the store ends with
    /// every row committed.
    /// </summary>
    [Fact]
    public async Task ManyPartitionsBackToBack_CompleteEverythingInOrder()
    {
        const int partitions = 6;
        const int rounds = 150;

        using GatedRecordingWal wal = new() { SyncDelay = TimeSpan.FromMilliseconds(1) };
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4, lazyCommitMarkers: true, markerDeferralMs: FairWalScheduler.DefaultMarkerDeferralMs);
        scheduler.Start();

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
                    // The commit of a round is not awaited before the next propose, as on the ticket
                    // release of the fast path: the two are enqueued one after the other.
                    Operation propose = Enqueue(scheduler, partitionId, ++operationId, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed, id, completionOrder);
                    expectedOrder.Add(operationId);
                    Assert.Equal(RaftOperationStatus.Success, (await propose.Completed.WaitAsync(Wait)).Status);

                    Operation marker = Enqueue(scheduler, partitionId, ++operationId, WALWriteOperationType.LeaderCommit, RaftLogType.Committed, id, completionOrder);
                    expectedOrder.Add(operationId);
                    lastMarker = marker.Completed;
                }

                Assert.Equal(RaftOperationStatus.Success, (await lastMarker!.WaitAsync(Wait)).Status);

                lock (completionOrder)
                    Assert.Equal(expectedOrder, completionOrder);
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(writers).WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        for (int p = 0; p < partitions; p++)
        {
            List<RaftLog> stored = wal.ReadLogs(10 + p);
            Assert.Equal(rounds, stored.Count);
            Assert.All(stored, log => Assert.Equal(RaftLogType.Committed, log.Type));
            Assert.Equal(0, scheduler.GetPartitionDepth(10 + p));
        }

        // The load is the one the deferral exists for: it must have been used.
        Assert.True(scheduler.TotalMarkerBatchesDeferred > 0);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private sealed record Operation(Task<RaftWalCompletion> Completed);

    private static Operation Enqueue(
        FairWalScheduler scheduler,
        int partitionId,
        long operationId,
        WALWriteOperationType type,
        RaftLogType logType,
        long logId,
        List<long>? completionOrder = null,
        bool requiresSync = false)
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
            logIndex: logId,
            requiresSync: requiresSync));

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
    /// An in-memory store that records every batch write with its sync flag, and can hold the synced
    /// writes of chosen partitions open, which is how a test keeps a sync "in progress".
    /// </summary>
    private sealed class GatedRecordingWal : IWAL, IDisposable
    {
        private readonly IWAL inner = new InMemoryWAL(NullLogger<IRaft>.Instance);
        private readonly object sync = new();
        private readonly List<WriteCall> calls = [];
        private readonly Dictionary<int, Gate> gates = [];

        /// <summary>Time every synced write spends in the store, so that syncs overlap under load.</summary>
        public TimeSpan SyncDelay { get; init; }

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
            List<(int, long, RaftLogType)> rows = [];
            Gate? gate = null;

            lock (this.sync)
            {
                foreach ((int partitionId, List<RaftLog> partitionLogs) in logs)
                {
                    foreach (RaftLog log in partitionLogs)
                        rows.Add((partitionId, log.Id, log.Type));

                    if (sync && gate is null)
                        gates.TryGetValue(partitionId, out gate);
                }
            }

            gate?.Hold();

            if (sync && SyncDelay > TimeSpan.Zero)
                Thread.Sleep(SyncDelay);

            RaftOperationStatus status = inner.Write(logs, sync);

            lock (this.sync)
                calls.Add(new(sync, rows));

            return status;
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
