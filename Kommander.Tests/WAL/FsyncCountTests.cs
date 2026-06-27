using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Makes fsyncs-per-committed-write a
/// <b>deterministic, assertable</b> quantity, so the fast path's improvement is
/// provable as an exact delta (≈ 2×W → ≈ 1×W) rather than inferred from benchmark noise.
///
/// <para>Determinism is achieved by removing every source of coalescing: a single WAL
/// worker, a single partition, and a strictly serial driver that awaits each phase's
/// durable completion before issuing the next. Under those constraints
/// <see cref="FairWalScheduler"/> emits exactly one group batch (one <c>walAdapter.Write</c>,
/// one fsync on a persistent backend with <c>syncWrites</c> on) per phase, so the durable
/// count is an exact function of the committed-write count, not a timing-dependent one.</para>
///
/// <para>The count is asserted two independent ways: the scheduler's
/// <see cref="FairWalScheduler.TotalBatchesWritten"/> (the fsync proxy) and the
/// <see cref="WalPhaseInstrumentation"/> per-phase durable counters. They must agree at the
/// pinned baseline of two durable writes (propose + commit) per committed write.</para>
///
/// <para>Both persistent backends are pinned (the two-fsync structure lives in the Raft
/// two-phase path, not the storage engine, so RocksDB and SQLite must agree).
/// <see cref="InMemoryWAL"/> is the control: it issues the same two <c>Write</c> calls per
/// write but performs no fsync, which is exactly why the symptom and the fix are
/// persistent-only.</para>
/// </summary>
[Collection(WalInstrumentationCollection.Name)]
public sealed class FsyncCountTests
{
    private const int Writes = 200;

    /// <summary>
    /// Pins the fsync-per-committed-write count as an exact delta across the single-fsync fast path.
    /// With the fast path <b>off</b> each committed write costs exactly two fsyncs (propose +
    /// commit) — the baseline. With it <b>on</b>, the commit-only batch is written sync-off and
    /// rides the next fsync, so the count drops to exactly one fsync per committed write.
    /// <para>
    /// The number of <c>walAdapter.Write</c> calls is identical either way (still two per write —
    /// <see cref="FairWalScheduler.TotalBatchesWritten"/>); only how many of them fsync changes, which is
    /// what <see cref="FairWalScheduler.TotalSyncBatchesWritten"/> isolates. Determinism comes from a
    /// single worker, single partition, and a strictly serial driver, so no coalescing occurs.
    /// </para>
    /// </summary>
    [Theory]
    [InlineData(WalBackend.RocksDb, false)]
    [InlineData(WalBackend.RocksDb, true)]
    [InlineData(WalBackend.Sqlite, false)]
    [InlineData(WalBackend.Sqlite, true)]
    public async Task PersistentBackend_FsyncsPerCommittedWrite_DropFromTwoToOneWithFastPath(WalBackend backend, bool lazyCommit)
    {
        string path = CreateTempWalPath();
        try
        {
            WalPhaseInstrumentation.Reset();
            WalPhaseInstrumentation.Enabled = true;

            using IWAL wal = CreateWal(backend, path, syncWrites: true);
            using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1, lazyCommitMarkers: lazyCommit);
            scheduler.Start();

            long batchesBefore = scheduler.TotalBatchesWritten;
            long syncBefore = scheduler.TotalSyncBatchesWritten;
            for (long id = 1; id <= Writes; id++)
                await DriveCommittedWriteAsync(scheduler, partitionId: 1, id: id);
            long writeCalls = scheduler.TotalBatchesWritten - batchesBefore;
            long fsyncs = scheduler.TotalSyncBatchesWritten - syncBefore;

            InstrumentationSnapshot snap = WalPhaseInstrumentation.Snapshot();

            // Write-call count is unchanged by the fast path: two Writes (propose + commit) per write.
            Assert.Equal(2L * Writes, writeCalls);

            // The fsync count is the exact lever: 2×W off, 1×W on (commit batch rides the next propose).
            long expectedFsyncs = lazyCommit ? Writes : 2L * Writes;
            Assert.Equal(expectedFsyncs, fsyncs);

            // The per-phase op-completion counter is unaffected (it counts completed ops per
            // phase, not fsyncs): both phases still complete W ops regardless of the fast path.
            Assert.Equal(Writes, snap.Propose.Durable);
            Assert.Equal(Writes, snap.Commit.Durable);
        }
        finally
        {
            WalPhaseInstrumentation.Enabled = false;
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Control: <see cref="InMemoryWAL"/> issues the same two <c>Write</c> calls per committed
    /// write (the batching shape is backend-independent), but those calls do not fsync. This
    /// documents that the 2× cost the fast path targets is a property of durable backends only.
    /// </summary>
    [Fact]
    public async Task InMemoryControl_IssuesTwoWritesPerCommittedWrite_ButNoFsyncCost()
    {
        WalPhaseInstrumentation.Reset();
        WalPhaseInstrumentation.Enabled = true;
        try
        {
            using IWAL wal = new InMemoryWAL(NullLogger<IRaft>.Instance);
            using FairWalScheduler scheduler = new(wal, NullLogger<IRaft>.Instance, workerCount: 1);
            scheduler.Start();

            long batchesBefore = scheduler.TotalBatchesWritten;
            for (long id = 1; id <= Writes; id++)
                await DriveCommittedWriteAsync(scheduler, partitionId: 1, id: id);
            long writeCalls = scheduler.TotalBatchesWritten - batchesBefore;

            // Same two-phase structure; the difference vs the persistent backends is that these
            // Write calls are not fsyncs, so the count carries no durability latency.
            Assert.Equal(2L * Writes, writeCalls);
        }
        finally
        {
            WalPhaseInstrumentation.Enabled = false;
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    public enum WalBackend
    {
        RocksDb,
        Sqlite,
    }

    private static IWAL CreateWal(WalBackend backend, string path, bool syncWrites) => backend switch
    {
        WalBackend.RocksDb => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites),
        WalBackend.Sqlite => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites),
        _ => throw new ArgumentOutOfRangeException(nameof(backend)),
    };

    /// <summary>
    /// Drives one committed write's two durable phases serially: enqueue + await the propose
    /// fsync, then enqueue + await the commit fsync. The commit phase is not even issued until
    /// the propose is durable, mirroring the real Raft two-phase critical path minus the
    /// cross-node quorum wait, and guaranteeing the two phases never share a group batch.
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

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-fsync-count-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
