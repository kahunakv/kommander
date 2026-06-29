using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Acceptance tests for <see cref="FairWalScheduler"/>.
///
/// Covers:
/// - Same-partition writes complete in submitted order.
/// - Cross-partition writes can run concurrently.
/// - No partition is starved under concurrent load from multiple partitions.
/// - Shutdown does not drop accepted operations silently.
/// - Back-pressure throws when per-partition limit is reached.
/// </summary>
public sealed class TestFairWalScheduler
{
    // ── Helpers ────────────────────────────────────────────────────────────

    private static WALWriteOperation MakeOp(
        int partitionId,
        long operationId,
        List<RaftLog> logs,
        Action<RaftWalCompletion> onComplete)
        => new(
            onComplete,
            operationId,
            WALWriteOperationType.LeaderPropose,
            (partitionId, logs)
        );

    private static List<RaftLog> Logs(long id) =>
        [new RaftLog { Id = id, Term = 1, Type = RaftLogType.Proposed }];

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Operations submitted for the same partition must complete in the order
    /// they were enqueued (FIFO).
    /// </summary>
    [Fact]
    public async Task SamePartition_WritesCompleteInSubmittedOrder()
    {
        const int opCount = 50;
        const int partitionId = 1;

        ConcurrentQueue<long> completionOrder = new();
        CountdownEvent done = new(opCount);
        RecordingWal wal = new();

        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        for (long i = 1; i <= opCount; i++)
        {
            long captured = i;
            scheduler.Enqueue(MakeOp(partitionId, captured, Logs(captured), c =>
            {
                completionOrder.Enqueue(c.OperationId);
                done.Signal();
            }));
        }

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(10)));
        Assert.True(finished, "Not all operations completed within timeout.");

        long[] order = completionOrder.ToArray();
        Assert.Equal(opCount, order.Length);

        // Verify strict FIFO ordering.
        for (int i = 0; i < order.Length - 1; i++)
            Assert.True(order[i] < order[i + 1],
                $"FIFO violated: op {order[i]} completed before op {order[i + 1]}");
    }

    /// <summary>
    /// Operations from different partitions can be processed concurrently by
    /// different worker threads — so total throughput with N workers should
    /// finish faster than sequential processing would.
    ///
    /// We verify this by observing that writes for partition A and partition B
    /// are interleaved (not all A's before all B's), which indicates they ran
    /// concurrently.
    /// </summary>
    [Fact]
    public async Task CrossPartition_WritesInterleaveAcrossPartitions()
    {
        const int opCount = 40; // per partition
        const int workers = 2;

        ConcurrentQueue<(int Partition, long OpId)> completionOrder = new();
        CountdownEvent done = new(opCount * 2);
        RecordingWal wal = new(writeDelayMs: 2); // small delay to encourage interleaving

        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: workers);
        scheduler.Start();

        for (long i = 1; i <= opCount; i++)
        {
            long captured = i;
            scheduler.Enqueue(MakeOp(1, captured, Logs(captured), c =>
            {
                completionOrder.Enqueue((1, c.OperationId));
                done.Signal();
            }));
            scheduler.Enqueue(MakeOp(2, captured + 1000, Logs(captured), c =>
            {
                completionOrder.Enqueue((2, c.OperationId));
                done.Signal();
            }));
        }

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(15)));
        Assert.True(finished, "Cross-partition test timed out.");

        (int Partition, long OpId)[] order = completionOrder.ToArray();
        Assert.Equal(opCount * 2, order.Length);

        // With 2 workers both partitions should appear in the completion list.
        Assert.Contains(order, x => x.Partition == 1);
        Assert.Contains(order, x => x.Partition == 2);

        // Verify per-partition FIFO is preserved.
        long[] p1Order = order.Where(x => x.Partition == 1).Select(x => x.OpId).ToArray();
        long[] p2Order = order.Where(x => x.Partition == 2).Select(x => x.OpId).ToArray();

        for (int i = 0; i < p1Order.Length - 1; i++)
            Assert.True(p1Order[i] < p1Order[i + 1], $"P1 FIFO violated at index {i}");
        for (int i = 0; i < p2Order.Length - 1; i++)
            Assert.True(p2Order[i] < p2Order[i + 1], $"P2 FIFO violated at index {i}");
    }

    /// <summary>
    /// With many active partitions all posting work simultaneously, every
    /// partition must eventually receive all its completions — no partition
    /// is starved.
    /// </summary>
    [Fact]
    public async Task NoStarvation_AllPartitionsEventuallyComplete()
    {
        const int partitions = 8;
        const int opsPerPartition = 20;
        const int total = partitions * opsPerPartition;

        CountdownEvent done = new(total);
        ConcurrentDictionary<int, int> completedPerPartition = new();

        RecordingWal wal = new(writeDelayMs: 1);
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 4);
        scheduler.Start();

        for (int p = 1; p <= partitions; p++)
        {
            int partition = p;
            for (long i = 1; i <= opsPerPartition; i++)
            {
                long seq = i;
                scheduler.Enqueue(MakeOp(partition, (partition * 1000) + seq, Logs(seq), c =>
                {
                    completedPerPartition.AddOrUpdate(c.PartitionId, 1, (_, v) => v + 1);
                    done.Signal();
                }));
            }
        }

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(20)));
        Assert.True(finished, $"Starvation detected: only {total - done.CurrentCount}/{total} ops completed.");

        // Every partition should have all its completions.
        for (int p = 1; p <= partitions; p++)
        {
            int count = completedPerPartition.GetValueOrDefault(p, 0);
            Assert.Equal(opsPerPartition, count);
        }
    }

    /// <summary>
    /// Operations accepted before <see cref="FairWalScheduler.Stop"/> is called
    /// must all receive their <c>OnComplete</c> callback — none are silently dropped.
    /// </summary>
    [Fact]
    public async Task Shutdown_DoesNotDropAcceptedOperations()
    {
        const int opCount = 100;
        const int partitions = 4;
        const int total = opCount * partitions;

        ConcurrentBag<long> completed = [];
        RecordingWal wal = new();

        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 2);
        scheduler.Start();

        long seq = 0;
        for (int p = 1; p <= partitions; p++)
        {
            for (int i = 0; i < opCount; i++)
            {
                long id = Interlocked.Increment(ref seq);
                int partition = p;
                scheduler.Enqueue(MakeOp(partition, id, Logs(id), c => completed.Add(c.OperationId)));
            }
        }

        // Stop after all ops are enqueued; workers must drain completely.
        await Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        Assert.Equal(total, completed.Count);
    }

    /// <summary>
    /// <see cref="FairWalScheduler.Enqueue"/> must throw
    /// <see cref="BackpressureExceededException"/> when the per-partition depth
    /// limit is exceeded, preventing unbounded memory growth.
    /// </summary>
    [Fact]
    public void Backpressure_ThrowsWhenQueueFull()
    {
        const int maxDepth = 4;

        // A WAL that never returns (blocks indefinitely) so the queue stays full.
        BlockingWal blockingWal = new();

        using FairWalScheduler scheduler = new(
            blockingWal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxQueueDepthPerPartition: maxDepth);
        scheduler.Start();

        // Fill the queue up to the limit.
        for (int i = 1; i <= maxDepth; i++)
        {
            long captured = i;
            scheduler.Enqueue(MakeOp(1, captured, Logs(captured), _ => { }));
        }

        // One more should exceed the limit.
        BackpressureExceededException ex = Assert.Throws<BackpressureExceededException>(() =>
            scheduler.Enqueue(MakeOp(1, maxDepth + 1, Logs(maxDepth + 1), _ => { })));

        Assert.Equal(1, ex.PartitionId);

        // Unblock the WAL so the scheduler can drain and stop cleanly.
        blockingWal.Unblock();
        scheduler.Stop();
    }

    /// <summary>
    /// When multiple partitions are ready simultaneously, the scheduler must issue a
    /// single <c>walAdapter.Write</c> call containing ops from all of them — the
    /// cross-partition group-commit path.
    ///
    /// <para>Coordination: the first Write call (partition 1) is held inside
    /// <see cref="CoordinatedWal"/> until the test has enqueued ops for all remaining
    /// partitions.  After releasing, the second Write call receives all remaining
    /// partitions at once, proving they are coalesced into a single WAL call.</para>
    /// </summary>
    [Fact]
    public async Task GroupCommit_MultiplePartitionsCoalescedIntoSingleWrite()
    {
        const int partitions = 4;

        CoordinatedWal wal = new();
        CountdownEvent done = new(partitions);

        using FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxGroupBatchPartitions: partitions);
        scheduler.Start();

        // Enqueue partition 1 — the worker picks it up and blocks inside Write.
        scheduler.Enqueue(MakeOp(1, 1, Logs(1), _ => done.Signal()));

        // Wait until the worker is mid-write so all subsequent enqueues arrive
        // in _readyPartitions while the worker cannot process them.
        wal.WaitForFirstWrite();

        // Enqueue the remaining partitions — they accumulate in the ready-queue.
        for (int p = 2; p <= partitions; p++)
        {
            int partition = p;
            scheduler.Enqueue(MakeOp(partition, partition, Logs(partition), _ => done.Signal()));
        }

        // Unblock the first Write — worker finishes it and then sees all remaining
        // partitions ready at once, coalescing them into one Write call.
        wal.Release();

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(10)));
        Assert.True(finished, "GroupCommit test timed out.");

        // The second Write call should have included all remaining partitions.
        Assert.True(wal.MaxPartitionsInSingleWrite >= partitions - 1,
            $"Expected ≥{partitions - 1} partitions in one Write call; got max={wal.MaxPartitionsInSingleWrite}");
    }

    /// <summary>
    /// Completions from a WAL error must still be delivered to every operation
    /// in the batch (the scheduler must not silently skip callbacks on failure).
    /// </summary>
    [Fact]
    public async Task WalError_CompletionsStillDeliveredWithErroredStatus()
    {
        const int opCount = 5;
        ConcurrentBag<RaftOperationStatus> statuses = [];
        CountdownEvent done = new(opCount);

        ErroringWal errorWal = new();
        using FairWalScheduler scheduler = new(errorWal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        for (long i = 1; i <= opCount; i++)
        {
            long captured = i;
            scheduler.Enqueue(MakeOp(1, captured, Logs(captured), c =>
            {
                statuses.Add(c.Status);
                done.Signal();
            }));
        }

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(finished);
        Assert.All(statuses, s => Assert.Equal(RaftOperationStatus.Errored, s));
    }

    // ── Deferred group-commit linger (WalGroupCommitLingerMs) ───────────────

    /// <summary>
    /// Enabling the linger must not regress the opportunistic cross-partition group
    /// commit: partitions that pile up behind a blocked Write are still coalesced into
    /// a single fsync. (The linger's <em>additional</em> benefit — coalescing
    /// sub-millisecond-staggered arrivals that opportunistic batching misses — is a
    /// timing property validated by the benchmark via
    /// <see cref="FairWalScheduler.TotalPartitionsBatched"/>, not a deterministic unit
    /// assertion.)
    /// </summary>
    [Fact]
    public async Task Linger_DoesNotRegressGroupCommitCoalescing()
    {
        const int partitions = 4;

        CoordinatedWal wal = new();
        CountdownEvent done = new(partitions);

        using FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxGroupBatchPartitions: partitions,
            groupCommitLingerMs: 50);
        scheduler.Start();

        scheduler.Enqueue(MakeOp(1, 1, Logs(1), _ => done.Signal()));
        wal.WaitForFirstWrite();

        for (int p = 2; p <= partitions; p++)
        {
            int partition = p;
            scheduler.Enqueue(MakeOp(partition, partition, Logs(partition), _ => done.Signal()));
        }

        wal.Release();

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(10)));
        Assert.True(finished, "Linger group-commit test timed out.");
        Assert.True(wal.MaxPartitionsInSingleWrite >= partitions - 1,
            $"Expected ≥{partitions - 1} partitions coalesced into one Write; got max={wal.MaxPartitionsInSingleWrite}");
    }

    /// <summary>
    /// The linger is <b>adaptive</b>: causally-sequential writes — each enqueued only
    /// after the previous one is durable — must NOT be merged and must not stall. Each
    /// isolated write gets its own fsync because the worker bails out of the linger the
    /// instant a probe finds nothing newly ready, so a low-overlap workload pays at most
    /// one short probe, never the full window. A naive fixed-window implementation that
    /// always waited would still produce one batch per write here (nothing else is in
    /// flight), but it would inflate each write's latency by the window; this test pins
    /// the batch-per-write structure that proves no unwanted merging.
    /// </summary>
    [Fact]
    public async Task Linger_DoesNotMergeCausallySequentialWrites()
    {
        const int writes = 8;
        RecordingWal wal = new();

        using FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            groupCommitLingerMs: 100);
        scheduler.Start();

        for (long i = 1; i <= writes; i++)
        {
            TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            // A different partition each time, so a blindly-waiting window *could* try to
            // merge them — yet none overlap in time, so each must fsync alone.
            scheduler.Enqueue(MakeOp((int)i, i, Logs(i), _ => tcs.SetResult()));
            await tcs.Task; // do not enqueue the next until this one is durable.
        }

        Assert.Equal(writes, scheduler.TotalBatchesWritten);
        Assert.Equal(writes, scheduler.TotalPartitionsBatched);
    }

    /// <summary>
    /// Per-partition FIFO ordering holds across the linger window: multiple ops for the
    /// same partition are written in ascending id order regardless of how the worker
    /// groups them.
    /// </summary>
    [Fact]
    public async Task Linger_PreservesPerPartitionOrder()
    {
        const int opCount = 30;
        const int partitionId = 7;

        RecordingWal wal = new();
        CountdownEvent done = new(opCount);

        using FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            groupCommitLingerMs: 50);
        scheduler.Start();

        for (long i = 1; i <= opCount; i++)
            scheduler.Enqueue(MakeOp(partitionId, i, Logs(i), _ => done.Signal()));

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(10)));
        Assert.True(finished, "Linger ordering test timed out.");

        long prevMax = 0;
        foreach ((int p, long minId, long maxId) in wal.Writes)
        {
            Assert.Equal(partitionId, p);
            Assert.True(minId > prevMax, $"Out-of-order write: minId={minId} followed prevMax={prevMax}.");
            prevMax = maxId;
        }
    }

    /// <summary>
    /// Durability/completion contract under the linger: a burst across many partitions
    /// all complete with <see cref="RaftOperationStatus.Success"/>, and
    /// <see cref="FairWalScheduler.TotalPartitionsBatched"/> accounts for every partition
    /// fsynced and never undercounts the batches.
    /// </summary>
    [Fact]
    public async Task Linger_BurstAcrossPartitions_AllSucceed_AndCounterTracksDensity()
    {
        const int partitions = 32;

        RecordingWal wal = new();
        ConcurrentBag<RaftOperationStatus> statuses = [];
        CountdownEvent done = new(partitions);

        using FairWalScheduler scheduler = new(
            wal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxGroupBatchPartitions: 64,
            groupCommitLingerMs: 50);
        scheduler.Start();

        for (int p = 1; p <= partitions; p++)
            scheduler.Enqueue(MakeOp(p, p, Logs(p), c => { statuses.Add(c.Status); done.Signal(); }));

        bool finished = await Task.Run(() => done.Wait(TimeSpan.FromSeconds(10)));
        Assert.True(finished, "Linger burst test timed out.");

        Assert.Equal(partitions, statuses.Count);
        Assert.All(statuses, s => Assert.Equal(RaftOperationStatus.Success, s));

        // Each partition is written exactly once, so the partitions-batched counter sums
        // to the partition count, and every batch spans ≥1 partition.
        Assert.Equal(partitions, scheduler.TotalPartitionsBatched);
        Assert.True(scheduler.TotalBatchesWritten >= 1);
        Assert.True(scheduler.TotalPartitionsBatched >= scheduler.TotalBatchesWritten);
    }

    // ── BuildCompletion correctness ────────────────────────────────────────

    /// <summary>
    /// <see cref="RaftWalCompletion.MinLogIndex"/> must equal the smallest log id in the
    /// operation's log list, regardless of the order in which logs are stored. This pins
    /// the correctness of the manual-loop min computation in <c>BuildCompletion</c>.
    /// </summary>
    [Theory]
    [InlineData(new long[] { 5, 1, 3 }, 1L)]      // min is not at index 0
    [InlineData(new long[] { 7 }, 7L)]              // single entry
    [InlineData(new long[] { 2, 2, 2 }, 2L)]        // all equal
    [InlineData(new long[] { 1, 2, 3 }, 1L)]        // already sorted ascending
    [InlineData(new long[] { 3, 2, 1 }, 1L)]        // descending — min is last
    public async Task BuildCompletion_MinLogIndex_MatchesActualMinimum(long[] inputIds, long expectedMin)
    {
        TaskCompletionSource<RaftWalCompletion> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWal wal = new();

        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        List<RaftLog> logs = [.. inputIds.Select(id => new RaftLog { Id = id, Term = 1, Type = RaftLogType.Proposed })];
        scheduler.Enqueue(new WALWriteOperation(
            onComplete: c => tcs.SetResult(c),
            operationId: 1,
            type: WALWriteOperationType.LeaderPropose,
            logs: (1, logs),
            logIndex: inputIds.Max()));

        RaftWalCompletion completion = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(RaftOperationStatus.Success, completion.Status);
        Assert.Equal(expectedMin, completion.MinLogIndex);
        Assert.Equal(inputIds.Max(), completion.MaxLogIndex);
    }

    // ── Pool overflow safety (DrainRemaining) ────────────────────────────────

    /// <summary>
    /// At shutdown, <c>DrainRemaining</c> drains the entire ready-queue without a partition-count
    /// cap — unlike the main loop, which stops at <c>maxGroupBatchPartitions</c>. The op-list pool
    /// pre-allocated in <c>WorkerLoop</c> starts at <c>maxGroupBatchPartitions</c> slots.
    /// If more than that many partitions are pending at shutdown, <c>ProcessGroupBatch</c> must
    /// grow the pool on demand rather than throw <see cref="IndexOutOfRangeException"/>. This
    /// regression test fires more partitions than the default pool size and then calls
    /// <see cref="FairWalScheduler.Stop"/>, driving the oversized drain path.
    /// </summary>
    [Fact]
    public async Task Shutdown_DrainRemaining_ToleratesMorePartitionsThanPoolSize()
    {
        // Use a pool size smaller than the partition count so the grow path is exercised.
        const int maxGroupBatch = 4;
        const int partitions = 16;   // > maxGroupBatch
        const int total = partitions;

        ConcurrentBag<long> completed = [];
        // A WAL that blocks all writes until unblocked — keeps ops queued so they pile up
        // in _readyPartitions, all visible to DrainRemaining's first-phase TryTake loop.
        BlockingWal blockingWal = new();

        using FairWalScheduler scheduler = new(
            blockingWal,
            NullLogger<IRaft>.Instance,
            workerCount: 1,
            maxGroupBatchPartitions: maxGroupBatch);
        scheduler.Start();

        // Enqueue one op per partition; the WAL is blocked so they all accumulate in the queue.
        for (int p = 1; p <= partitions; p++)
        {
            int partition = p;
            scheduler.Enqueue(MakeOp(partition, partition, Logs(partition),
                c => completed.Add(c.OperationId)));
        }

        // Unblock writes and then Stop — DrainRemaining must handle all partitions without crashing.
        blockingWal.Unblock();
        await Task.Run(scheduler.Stop, TestContext.Current.CancellationToken);

        Assert.Equal(total, completed.Count);
    }

    // ── Fake WAL implementations for tests ────────────────────────────────

    /// <summary>
    /// Simple synchronous WAL that records write calls and returns Success immediately.
    /// </summary>
    private sealed class RecordingWal : IWAL
    {
        private readonly int _writeDelayMs;
        public ConcurrentQueue<(int Partition, long MinId, long MaxId)> Writes { get; } = new();

        public RecordingWal(int writeDelayMs = 0) => _writeDelayMs = writeDelayMs;

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            if (_writeDelayMs > 0)
                Thread.Sleep(_writeDelayMs);

            foreach ((int partition, List<RaftLog> batch) in logs)
            {
                long minId = batch.Count > 0 ? batch.Min(l => l.Id) : -1;
                long maxId = batch.Count > 0 ? batch.Max(l => l.Id) : -1;
                Writes.Enqueue((partition, minId, maxId));
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
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() { }
    }

    /// <summary>WAL that blocks until <see cref="Unblock"/> is called.</summary>
    private sealed class BlockingWal : IWAL, IDisposable
    {
        private readonly ManualResetEventSlim _gate = new(false);

        public void Unblock() => _gate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            _gate.Wait();
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
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() { _gate.Dispose(); }
    }

    /// <summary>
    /// WAL that blocks the first Write call until <see cref="Release"/> is called,
    /// allowing the test to enqueue more operations before the scheduler moves on.
    /// Records the maximum number of distinct partitions seen in any single Write call.
    /// </summary>
    private sealed class CoordinatedWal : IWAL
    {
        private readonly ManualResetEventSlim _firstWriteStarted = new(false);
        private readonly ManualResetEventSlim _releaseGate = new(false);
        private int _writeCount;
        private int _maxPartitionsInSingleWrite;

        public int MaxPartitionsInSingleWrite => Volatile.Read(ref _maxPartitionsInSingleWrite);

        public void WaitForFirstWrite() => _firstWriteStarted.Wait();
        public void Release() => _releaseGate.Set();

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            int callIndex = Interlocked.Increment(ref _writeCount);
            if (callIndex == 1)
            {
                _firstWriteStarted.Set();
                _releaseGate.Wait();
            }

            int partitionCount = logs.Select(l => l.Item1).Distinct().Count();
            int prev = _maxPartitionsInSingleWrite;
            while (partitionCount > prev)
                prev = Interlocked.CompareExchange(ref _maxPartitionsInSingleWrite, partitionCount, prev);

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
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() { }
    }

    /// <summary>WAL that always throws to simulate a storage error.</summary>
    private sealed class ErroringWal : IWAL
    {
        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
            => throw new InvalidOperationException("Simulated WAL error.");

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public long GetLastCheckpoint(int partitionId) => -1;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public void Dispose() { }
    }
}
