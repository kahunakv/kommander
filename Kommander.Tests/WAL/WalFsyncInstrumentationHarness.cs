using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Task 1 of the WAL double-fsync spec: measure-first harness. It drives a fixed
/// committed-write count through the real persistent write path
/// (<see cref="FairWalScheduler"/> over <see cref="RocksDbWAL"/> with <c>syncWrites</c>
/// enabled) and reports the three numbers that gate the Task 3 go/no-go decision:
///
/// <list type="number">
///   <item><b>fsyncs-per-committed-write</b> — <see cref="FairWalScheduler.TotalBatchesWritten"/>
///     divided by the committed-write count. The spec predicts ≈ 2.</item>
///   <item><b>per-phase durable latency split</b> — leader propose fsync vs leader commit
///     fsync, from <see cref="WalPhaseInstrumentation"/>. Quorum network wait is excluded
///     here (no network in this harness); the spec derives it end-to-end elsewhere.</item>
///   <item><b>writes-per-partition concurrency</b> — a property of the load shape, reported
///     per configuration the harness drives.</item>
/// </list>
///
/// <para><see cref="FairWalScheduler.TotalBatchesWritten"/> is used directly as the
/// fsync proxy: with <c>syncWrites</c> on, each group-batch <c>db.Write</c> is exactly
/// one fsync, and it is documented as the count most directly correlated with fsync
/// pressure — so no separate fsync counter is added (Task 1 only asks for one if the
/// batch counter is not a clean proxy).</para>
///
/// <para>The structural assertions (enqueues-per-write == 2; serial single-partition
/// batches-per-write == 2) are deterministic. The latency numbers are observational and
/// logged, not asserted, so this harness is not timing-flaky.</para>
/// </summary>
[Collection(WalInstrumentationCollection.Name)]
public sealed class WalFsyncInstrumentationHarness
{
    private readonly ITestOutputHelper output;

    public WalFsyncInstrumentationHarness(ITestOutputHelper output)
    {
        this.output = output;
    }

    /// <summary>
    /// Confirms the instrumentation is genuinely inert when its flag is off: driving the
    /// write path with <see cref="WalPhaseInstrumentation.Enabled"/> = false leaves every
    /// counter at zero.
    /// </summary>
    [Fact]
    public async Task Instrumentation_IsInert_WhenDisabled()
    {
        string path = CreateTempWalPath();
        try
        {
            WalPhaseInstrumentation.Reset();
            WalPhaseInstrumentation.Enabled = false;

            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
            scheduler.Start();

            for (long k = 1; k <= 10; k++)
                await DriveCommittedWriteAsync(scheduler, partitionId: 1, id: k);

            InstrumentationSnapshot snap = WalPhaseInstrumentation.Snapshot();
            Assert.Equal(0, snap.Propose.Enqueued);
            Assert.Equal(0, snap.Commit.Enqueued);
            Assert.Equal(0, snap.Propose.Durable);
            Assert.Equal(0, snap.Commit.Durable);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Serial single-writer, single-partition baseline. With no other partition ready,
    /// cross-partition coalescing cannot engage and each phase is its own group batch, so
    /// fsyncs-per-write is exactly 2 — pinning the spec's predicted baseline and giving the
    /// cleanest per-phase fsync latency split.
    /// </summary>
    [Fact]
    public async Task Report_SerialSinglePartition_BaselineIsTwoFsyncsPerWrite()
    {
        const int writes = 500;
        string path = CreateTempWalPath();
        try
        {
            WalPhaseInstrumentation.Reset();
            WalPhaseInstrumentation.Enabled = true;

            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
            scheduler.Start();

            long batchesBefore = scheduler.TotalBatchesWritten;
            for (long k = 1; k <= writes; k++)
                await DriveCommittedWriteAsync(scheduler, partitionId: 1, id: k);
            long batchesWritten = scheduler.TotalBatchesWritten - batchesBefore;

            InstrumentationSnapshot snap = WalPhaseInstrumentation.Snapshot();

            ReportConfiguration(
                title: "Serial, single writer, single partition (no coalescing possible)",
                writes: writes,
                concurrency: 1,
                partitions: 1,
                batchesWritten: batchesWritten,
                snap: snap);

            // Structural, deterministic: each write fsyncs twice (propose + commit), and with
            // no second partition ready, neither phase coalesces — one batch per phase.
            Assert.Equal(writes, snap.Propose.Durable);
            Assert.Equal(writes, snap.Commit.Durable);
            Assert.Equal(2L * writes, batchesWritten);
        }
        finally
        {
            WalPhaseInstrumentation.Enabled = false;
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Concurrent writers, one partition each. Multiple partitions are ready at once, so the
    /// scheduler's cross-partition group commit can fold several phases into one fsync — the
    /// regime where fsyncs-per-write drops below 2. Reports the achieved ratio; the only
    /// hard assertion is the producer-side invariant (two enqueues per write), since the
    /// coalescing ratio is timing-dependent.
    /// </summary>
    [Fact]
    public async Task Report_ConcurrentMultiPartition_ShowsCoalescing()
    {
        const int concurrency = 64;
        const int writesPerPartition = 32;
        int totalWrites = concurrency * writesPerPartition;

        string path = CreateTempWalPath();
        try
        {
            WalPhaseInstrumentation.Reset();
            WalPhaseInstrumentation.Enabled = true;

            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance);
            scheduler.Start();

            long batchesBefore = scheduler.TotalBatchesWritten;

            Task[] writers = new Task[concurrency];
            for (int w = 0; w < concurrency; w++)
            {
                int partitionId = w + 1;
                writers[w] = Task.Run(async () =>
                {
                    for (long k = 1; k <= writesPerPartition; k++)
                        await DriveCommittedWriteAsync(scheduler, partitionId, k);
                });
            }

            await Task.WhenAll(writers);
            long batchesWritten = scheduler.TotalBatchesWritten - batchesBefore;

            InstrumentationSnapshot snap = WalPhaseInstrumentation.Snapshot();

            ReportConfiguration(
                title: $"Concurrent, {concurrency} writers over {concurrency} partitions (1 write/partition in flight)",
                writes: totalWrites,
                concurrency: concurrency,
                partitions: concurrency,
                batchesWritten: batchesWritten,
                snap: snap);

            Assert.Equal(totalWrites, snap.Propose.Durable);
            Assert.Equal(totalWrites, snap.Commit.Durable);
        }
        finally
        {
            WalPhaseInstrumentation.Enabled = false;
            DeleteTempWalPath(path);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>
    /// Drives one committed write down a single partition's critical path: enqueue the
    /// propose phase, await its durable completion, then enqueue the commit phase and await
    /// its durable completion. Serial-per-write by construction (the commit op is not even
    /// created until the propose is durable), matching the real Raft two-phase structure
    /// minus the cross-node quorum wait.
    /// </summary>
    private static async Task DriveCommittedWriteAsync(FairWalScheduler scheduler, int partitionId, long id)
    {
        await EnqueueAndAwaitAsync(scheduler, partitionId, id, WALWriteOperationType.LeaderPropose, RaftLogType.Proposed);
        await EnqueueAndAwaitAsync(scheduler, partitionId, id, WALWriteOperationType.LeaderCommit, RaftLogType.Committed);
    }

    private static Task EnqueueAndAwaitAsync(
        FairWalScheduler scheduler,
        int partitionId,
        long id,
        WALWriteOperationType type,
        RaftLogType logType)
    {
        TaskCompletionSource<RaftOperationStatus> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        List<RaftLog> logs =
        [
            new RaftLog { Id = id, Term = 1, Type = logType, LogType = $"partition-{partitionId}" }
        ];

        WALWriteOperation op = new(
            c => tcs.TrySetResult(c.Status),
            id,
            type,
            (partitionId, logs),
            logIndex: id);

        scheduler.Enqueue(op);
        return tcs.Task;
    }

    private void ReportConfiguration(
        string title,
        int writes,
        int concurrency,
        int partitions,
        long batchesWritten,
        InstrumentationSnapshot snap)
    {
        double fsyncsPerWrite = writes > 0 ? (double)batchesWritten / writes : 0.0;
        long durablePhases = snap.Propose.Durable + snap.Commit.Durable;
        double durablePerWrite = writes > 0 ? (double)durablePhases / writes : 0.0;

        output.WriteLine($"=== {title} ===");
        output.WriteLine($"committed writes (W)             : {writes}");
        output.WriteLine($"writes-per-partition concurrency : {concurrency} writers over {partitions} partition(s)");
        output.WriteLine($"WAL batches (= fsyncs)           : {batchesWritten}");
        output.WriteLine($"fsyncs-per-committed-write       : {fsyncsPerWrite:F3}  (spec predicts ~2)");
        output.WriteLine($"durable phase-writes-per-write   : {durablePerWrite:F3}  (propose + commit fsync ops)");
        output.WriteLine(
            $"propose fsync  ms  : mean={snap.Propose.MeanMs:F3} p50={snap.Propose.P50Ms:F3} p99={snap.Propose.P99Ms:F3} (n={snap.Propose.Durable})");
        output.WriteLine(
            $"commit  fsync  ms  : mean={snap.Commit.MeanMs:F3} p50={snap.Commit.P50Ms:F3} p99={snap.Commit.P99Ms:F3} (n={snap.Commit.Durable})");
        output.WriteLine("");
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-fsync-harness-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
