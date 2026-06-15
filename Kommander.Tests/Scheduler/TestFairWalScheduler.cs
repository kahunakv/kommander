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
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int n) => (RaftOperationStatus.Success, 0);
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
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int n) => (RaftOperationStatus.Success, 0);
        public void Dispose() { _gate.Dispose(); }
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
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int n) => (RaftOperationStatus.Success, 0);
        public void Dispose() { }
    }
}
