using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using RocksDbSharp;
using Xunit;

namespace Kommander.Tests.WAL;

/// <summary>
/// Pins the write-path cost of <see cref="RocksDbWAL"/> right after a truncating snapshot install —
/// the state a node restarted after SIGKILL under load is left in once its leader re-seeds it.
///
/// <para>The install deletes the suffix above the boundary with one point tombstone per row. Every
/// probe that then walks upward from the boundary has to skip that whole run before the engine can
/// answer, and two probes ran once per row or once per append: the commit-frontier catch-up walk
/// (one seek per commit marker) and the follower's Log Matching max-log read (one reverse seek per
/// AppendEntries). On the Caraxes bank-leader-kill run (~130k tombstones) a group batch of a few
/// thousand markers took minutes of CPU, so no write ever completed, the WAL queue saturated, and
/// the node kept voting for leaders it could not append for. The tests read RocksDB's perf context:
/// <c>internal_delete_skipped_count</c> is the number of tombstones an operation stepped over, and
/// the fix is that the per-row and per-append probes step over none.</para>
/// </summary>
public sealed class TestSnapshotInstallTombstoneRun
{
    private const int Partition = 1;

    // Contiguous committed prefix, an absent span, the boundary, and a large Proposed suffix that
    // the install truncates into a tombstone run.
    private const long CommittedThrough = 2_000;
    private const long Boundary = 10_000;
    private const long SuffixEnd = 40_000;
    private const long BoundaryTerm = 2;

    /// <summary>Index of <c>internal_delete_skipped_count</c> in RocksDB's perf-context metric enum.</summary>
    private const int InternalDeleteSkippedCount = 11;

    private static RaftLog Row(long id, long term, RaftLogType type) => new()
    {
        Id = id,
        Term = term,
        Type = type,
        LogType = "t",
        LogData = [1, 2, 3, 4],
        Time = HLCTimestamp.Zero,
    };

    private static List<RaftLog> Rows(long from, long toInclusive, long term, RaftLogType type)
    {
        List<RaftLog> rows = new((int)(toInclusive - from + 1));
        for (long id = from; id <= toInclusive; id++)
            rows.Add(Row(id, term, type));
        return rows;
    }

    /// <summary>Seeds the shape above and installs the truncating boundary; returns the WAL.</summary>
    private static RocksDbWAL SeedAndInstall(string path)
    {
        RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

        for (long id = 1; id <= CommittedThrough; id += 500)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(id, Math.Min(id + 499, CommittedThrough), 1, RaftLogType.Proposed))]));
        for (long id = 1; id <= CommittedThrough; id += 500)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(id, Math.Min(id + 499, CommittedThrough), 1, RaftLogType.Committed))]));
        Assert.Equal(CommittedThrough, wal.CommitMarkersAbsorbed);

        for (long id = Boundary + 1; id <= SuffixEnd; id += 500)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(id, Math.Min(id + 499, SuffixEnd), 1, RaftLogType.Proposed))]));

        Assert.Equal(SuffixEnd, wal.GetMaxLog(Partition));

        // No row at the boundary and a term that matches nothing: the whole suffix is truncated.
        (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, Boundary, BoundaryTerm, sync: false);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.True(truncated);

        return wal;
    }

    private sealed class PerfContext : IDisposable
    {
        private readonly IntPtr handle;

        public PerfContext()
        {
            Native.Instance.rocksdb_set_perf_level(2); // kEnableCount
            handle = Native.Instance.rocksdb_perfcontext_create();
        }

        public void Reset() => Native.Instance.rocksdb_perfcontext_reset(handle);

        public long DeletesSkipped => (long)Native.Instance.rocksdb_perfcontext_metric(handle, InternalDeleteSkippedCount);

        public void Dispose()
        {
            Native.Instance.rocksdb_perfcontext_destroy(handle);
            Native.Instance.rocksdb_set_perf_level(0);
        }
    }

    [Fact]
    public void CommitMarkersAboveTheBoundary_StepOverNoTombstones()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = SeedAndInstall(path);
            using PerfContext perf = new();

            // The frontier sits far below the boundary (2,000) and the markers far above it: the
            // walk must hop the absent-but-certified span, cross the boundary row, and then stop
            // with a point probe — never a seek into the tombstone run.
            for (int round = 0; round < 3; round++)
            {
                long first = Boundary + 5_000 + round * 100;
                perf.Reset();
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(first, first + 49, BoundaryTerm, RaftLogType.Committed))]));
                Assert.Equal(0, perf.DeletesSkipped);
            }

            // The single-entry fast path takes the same walk.
            perf.Reset();
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 9_000, BoundaryTerm, RaftLogType.Committed)])]));
            Assert.Equal(0, perf.DeletesSkipped);

            // The rows landed as full Committed rows (nothing to absorb into: no Proposed row existed).
            List<RaftLog> written = wal.ReadLogsRange(Partition, Boundary + 5_000, 50);
            Assert.Equal(50, written.Count);
            Assert.All(written, log => Assert.Equal(RaftLogType.Committed, log.Type));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The bounded walk must still do everything the old one did: hop the certified absent span,
    /// cross the boundary row, and absorb a contiguous marker for a Proposed row right above it.
    /// </summary>
    [Fact]
    public void FrontierStillCatchesUpAcrossTheBoundary_AndAbsorbsTheNextMarker()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = SeedAndInstall(path);
            long absorbedBefore = wal.CommitMarkersAbsorbed;

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 1, BoundaryTerm, RaftLogType.Proposed)])]));

            using PerfContext perf = new();
            perf.Reset();
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 1, BoundaryTerm, RaftLogType.Committed)])]));
            Assert.Equal(0, perf.DeletesSkipped);
            Assert.Equal(absorbedBefore + 1, wal.CommitMarkersAbsorbed);

            RaftLog row = Assert.Single(wal.ReadLogsRange(Partition, Boundary + 1, 1));
            Assert.Equal(RaftLogType.Committed, row.Type);

            // A Proposed row inside the certified span stops the walk exactly as before.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 2, BoundaryTerm, RaftLogType.Proposed), Row(Boundary + 4, BoundaryTerm, RaftLogType.Proposed)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 4, BoundaryTerm, RaftLogType.Committed)])]));
            Assert.Equal(absorbedBefore + 1, wal.CommitMarkersAbsorbed);
            Assert.Equal(RaftLogType.Proposed, Assert.Single(wal.ReadLogsRange(Partition, Boundary + 2, 1)).Type);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void CheckpointAboveTheBoundary_VerifiesItsPrefixWithoutSteppingPastIt()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = SeedAndInstall(path);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(Boundary + 1, Boundary + 4, BoundaryTerm, RaftLogType.Proposed))]));

            using PerfContext perf = new();
            perf.Reset();
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 5, BoundaryTerm, RaftLogType.CommittedCheckpoint)])]));

            // The verification is bounded at the checkpoint itself: the one tombstone at the
            // checkpoint's own id (the row lands after the check) is inside the bound, the tens of
            // thousands above it are not.
            Assert.InRange(perf.DeletesSkipped, 0, 1);
            Assert.Equal(Boundary + 5, wal.GetLastCheckpoint(Partition));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void MaxLog_AfterTheInstall_IsAnsweredFromMemory_AndStaysExact()
    {
        string path = CreateTempWalPath();

        try
        {
            using (RocksDbWAL wal = SeedAndInstall(path))
            {
                using PerfContext perf = new();

                // The first read after the install pays the reverse seek across the run once; every
                // later read is a cache hit.
                Assert.Equal(Boundary, wal.GetMaxLog(Partition));
                perf.Reset();
                Assert.Equal(Boundary, wal.GetMaxLog(Partition));
                Assert.Equal(0, perf.DeletesSkipped);

                // Appends raise it.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(Boundary + 200, Boundary + 209, BoundaryTerm, RaftLogType.Proposed))]));
                Assert.Equal(Boundary + 209, wal.GetMaxLog(Partition));

                // A proposed-tail sweep that removes the top rows and leaves none above the cut.
                Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, Boundary + 204));
                Assert.Equal(Boundary + 204, wal.GetMaxLog(Partition));

                // A sweep with a resolved survivor above the cut keeps that survivor as the max.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Row(Boundary + 300, BoundaryTerm, RaftLogType.Committed)])]));
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(Boundary + 301, Boundary + 303, BoundaryTerm, RaftLogType.Proposed))]));
                Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, Boundary + 250));
                Assert.Equal(Boundary + 300, wal.GetMaxLog(Partition));

                // A hard truncation.
                Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, Boundary + 202));
                Assert.Equal(Boundary + 202, wal.GetMaxLog(Partition));

                // A non-truncating boundary install above the tail raises to its boundary row.
                (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, Boundary + 500, -1, sync: false);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.False(truncated);
                Assert.Equal(Boundary + 500, wal.GetMaxLog(Partition));
            }

            // The cache is per engine instance: a reopen recomputes from disk.
            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                Assert.Equal(Boundary + 500, reopened.GetMaxLog(Partition));

                Assert.Equal(RaftOperationStatus.Success, reopened.DeletePartitionWAL(Partition));
                Assert.Equal(0, reopened.GetMaxLog(Partition));
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), "kommander-tombstone-run-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
