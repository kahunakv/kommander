using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Tests for the node-level WAL commit-wait signal that degraded-node leader avoidance depends on.
///
/// <para>Two properties are defended here. First, the signal is recorded <b>once per group batch</b>
/// rather than once per partition in that batch — the fsync is shared, so the batch is the unit at
/// which the device is actually observed. Second, the signal is reported by a node that leads
/// <b>nothing</b>: per-partition figures ride the load report only for led partitions, which makes
/// them useless for judging a node that is a candidate to <i>receive</i> leadership.</para>
/// </summary>
public sealed class TestNodeCommitWaitReporting
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static WALWriteOperation MakeOp(
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

    private static RaftManager MakeManager(InMemoryWAL wal, int port = 9000) =>
        new(
            new RaftConfiguration
            {
                Host = "localhost",
                Port = port,
                InitialPartitions = 0,
                EnableLeaderBalancer = true,
            },
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

    // ── Unknown, not healthy ──────────────────────────────────────────────────

    [Fact]
    public void FreshScheduler_ReportsUnknownRatherThanFast()
    {
        using FairWalScheduler scheduler = new(new NoOpWal(), NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        // A zero sample count is the only thing that distinguishes this from a genuinely fast
        // device. Without it, an unwritten node would look like the best transfer target there is.
        Assert.Equal(0L, scheduler.GetNodeCommitWaitSamples());
        Assert.Equal(0.0, scheduler.GetNodeCommitWaitMs());
        Assert.Equal(0.0, scheduler.GetNodeCommitWaitAgeMs());
    }

    [Fact]
    public void NoWritesYet_ReportIsUnknown()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using RaftManager manager = MakeManager(wal);

        NodeLoadReport report = manager.BuildLocalLoadReport();

        Assert.Equal(0L, report.NodeCommitWaitSamples);
        Assert.Equal(0.0, report.NodeCommitWaitMs);
        Assert.Equal(0L, report.NodeCommitWaitAgeMs);
    }

    // ── One observation per group batch ───────────────────────────────────────

    [Fact]
    public async Task SequentialWrites_RecordOneObservationEach()
    {
        using FairWalScheduler scheduler = new(new NoOpWal(), NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        // Each operation is awaited before the next is submitted, so no two can be batched
        // together and the sample count must match the write count exactly.
        for (long i = 1; i <= 5; i++)
        {
            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            scheduler.Enqueue(MakeOp(1, i, _ => done.TrySetResult()));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        Assert.Equal(5L, scheduler.GetNodeCommitWaitSamples());
        Assert.True(scheduler.GetNodeCommitWaitMs() >= 0.0);
    }

    [Fact]
    public async Task GroupedBatch_RecordsOnceForTheBatchNotOncePerPartition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using GatedWal wal = new();
        using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
        scheduler.Start();

        // Operation 1 seizes the single worker and blocks inside Write.
        TaskCompletionSource first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeOp(1, 1, _ => first.TrySetResult()));
        Assert.True(await wal.WaitForWriteEntry(TimeSpan.FromSeconds(5)), "the worker never entered Write");

        // Three more partitions queue up behind it, so they are all ready together and the worker
        // drains them as a single group batch when it comes back around.
        TaskCompletionSource<int> rest = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int remaining = 3;
        for (long pid = 2; pid <= 4; pid++)
        {
            scheduler.Enqueue(MakeOp((int)pid, pid, _ =>
            {
                if (Interlocked.Decrement(ref remaining) == 0)
                    rest.TrySetResult(0);
            }));
        }

        wal.Release();

        await first.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await rest.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        // Four operations across four partitions, but only two shared fsyncs. Recording per
        // partition instead of per batch would have produced four observations.
        Assert.Equal(2, wal.WriteCount);
        Assert.Equal(2L, scheduler.GetNodeCommitWaitSamples());
    }

    // ── Reported without any leadership ───────────────────────────────────────

    [Fact]
    public async Task NodeThatLeadsNothing_StillReportsItsCommitWait()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using RaftManager manager = MakeManager(wal);

        FairWalScheduler scheduler = (FairWalScheduler)manager.WalScheduler;
        scheduler.Start(); // The manager starts it on join; these tests never join.

        // Follower appends take exactly this path, so a node with no leadership at all still
        // observes its own device.
        for (long i = 1; i <= 3; i++)
        {
            TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            scheduler.Enqueue(MakeOp(7, i, _ => done.TrySetResult()));
            await done.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        }

        NodeLoadReport report = manager.BuildLocalLoadReport();

        Assert.Empty(report.Leaderships);
        Assert.Equal(3L, report.NodeCommitWaitSamples);
        Assert.True(report.NodeCommitWaitMs >= 0.0);
    }

    // ── Age travels with the report ───────────────────────────────────────────

    [Fact]
    public async Task ReportedAge_GrowsWhileTheNodeStaysQuiet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using RaftManager manager = MakeManager(wal);

        FairWalScheduler scheduler = (FairWalScheduler)manager.WalScheduler;
        scheduler.Start(); // The manager starts it on join; these tests never join.

        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Enqueue(MakeOp(7, 1, _ => done.TrySetResult()));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        long earlyAge = manager.BuildLocalLoadReport().NodeCommitWaitAgeMs;

        await Task.Delay(120, ct);

        long laterAge = manager.BuildLocalLoadReport().NodeCommitWaitAgeMs;

        // The EWMA does not decay on its own, so watching this grow is the only way a consumer can
        // tell that the commit-wait figure describes the past rather than the present.
        Assert.True(laterAge > earlyAge,
            $"expected the reported age to grow while the node is quiet: {earlyAge} → {laterAge}");
    }

    // ── Fakes ─────────────────────────────────────────────────────────────────

    /// <summary>WAL that accepts every write immediately and stores nothing.</summary>
    private class NoOpWal : IWAL
    {
        public virtual RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => RaftOperationStatus.Success;

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

    /// <summary>
    /// WAL whose first <c>Write</c> blocks until <see cref="Release"/> is called, so a test can
    /// force several partitions to become ready together and be drained as one group batch.
    /// </summary>
    private sealed class GatedWal : NoOpWal, IDisposable
    {
        private readonly ManualResetEventSlim _entered = new(false);
        private readonly ManualResetEventSlim _release = new(false);
        private readonly ConcurrentQueue<int> _writes = new();
        private int _writeCount;

        public int WriteCount => Volatile.Read(ref _writeCount);

        public override RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            if (Interlocked.Increment(ref _writeCount) == 1)
            {
                _entered.Set();
                _release.Wait(TimeSpan.FromSeconds(10));
            }

            foreach ((int partitionId, List<RaftLog> _) in logs)
                _writes.Enqueue(partitionId);

            return RaftOperationStatus.Success;
        }

        public Task<bool> WaitForWriteEntry(TimeSpan timeout) => Task.Run(() => _entered.Wait(timeout));

        public void Release() => _release.Set();

        public new void Dispose()
        {
            _release.Set();
            _entered.Dispose();
            _release.Dispose();
        }
    }
}
