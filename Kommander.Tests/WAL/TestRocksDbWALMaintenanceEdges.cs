using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Boundary conditions of <see cref="RocksDbWAL"/>'s maintenance mutations — the shapes the happy
/// path never produces: a snapshot boundary installed at (or beyond) the exact end of the log, the
/// same boundary installed twice, mutations aimed at partitions that were never written, and
/// mutations on two partitions that share one shard column family. Every one of these must be a
/// defined, idempotent, neighbour-safe outcome: maintenance runs on the read scheduler while
/// appends and other partitions' traffic are live, so an edge case that throws, double-applies,
/// or walks past its partition's key range corrupts unrelated state.
/// </summary>
public sealed class TestRocksDbWALMaintenanceEdges
{
    /// <summary>
    /// A boundary install at the exact last entry with a DIFFERENT term has an empty suffix to
    /// remove, so it must report <c>SuffixTruncated = false</c> — yet the boundary row itself is
    /// rewritten with the snapshot's last-included term and checkpoint type (the row is an upsert
    /// target even when nothing above it is deleted).
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_AtTheExactLastEntry_WithADifferentTerm_RewritesTheBoundaryRowWithoutReportingTruncation()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1), Row(2), Row(3), Row(4), Row(5)])]));

            (RaftOperationStatus status, bool suffixTruncated) =
                wal.InstallSnapshotBoundary(1, snapshotIndex: 5, lastIncludedTerm: 9, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(suffixTruncated, "nothing exists above the last entry, so no suffix was removed");

            // The boundary row carries the snapshot's term and checkpoint type now.
            Assert.Equal(9, wal.GetTermAt(1, 5));
            RaftLog boundary = wal.ReadLogsRange(1, 5).Single();
            Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
            Assert.Equal(9, boundary.Term);

            // The rest of the log is untouched and the recorded checkpoint resets to the boundary.
            Assert.Equal([1L, 2L, 3L, 4L, 5L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(1));
            Assert.Equal(5, wal.GetLastCheckpoint(1));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Installing the same boundary twice must be idempotent in both directions: re-installing a
    /// boundary whose term now matches reports no truncation and changes nothing, and the same
    /// holds after the first install DID truncate the suffix. A snapshot is re-delivered on
    /// retry, so the second install may neither re-delete nor resurrect anything.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_InstallingTheSameBoundaryTwice_ChangesNothing()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            // Retained-suffix shape: matching term at the boundary keeps the suffix.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1), Row(2), Row(3), Row(4), Row(5)])]));

            (RaftOperationStatus status, bool truncated) =
                wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 1, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);

            (status, truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 1, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);

            Assert.Equal([1L, 2L, 3L, 4L, 5L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(1));
            Assert.Equal(3, wal.GetLastCheckpoint(1));

            // Truncating shape on a second partition: the first install removes the suffix, the
            // re-install of the identical boundary is a full no-op.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(2, [Row(1), Row(2), Row(3), Row(4)])]));

            (status, truncated) = wal.InstallSnapshotBoundary(2, snapshotIndex: 2, lastIncludedTerm: 7, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(truncated);
            Assert.Equal([1L, 2L], wal.ReadLogsRange(2, 1).Select(l => l.Id));

            (status, truncated) = wal.InstallSnapshotBoundary(2, snapshotIndex: 2, lastIncludedTerm: 7, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);

            Assert.Equal([1L, 2L], wal.ReadLogsRange(2, 1).Select(l => l.Id));
            Assert.Equal(7, wal.GetTermAt(2, 2));
            Assert.Equal(2, wal.GetMaxLog(2));
            Assert.Equal(2, wal.GetLastCheckpoint(2));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Every maintenance mutation aimed at a partition that was never written succeeds as a no-op
    /// and leaves no rows behind. A boundary install on a never-written partition is the one that
    /// does write: it creates the boundary row and the recorded checkpoint, and a subsequent
    /// partition wipe removes both so the next occupant of the id inherits no replay floor.
    /// </summary>
    [Fact]
    public void MaintenanceOps_OnPartitionsThatWereNeverWritten_AreSafeNoOps()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(30, afterLogId: 5));
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(30, afterLogId: 5));
            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(30));
            Assert.Equal(0, wal.GetMaxLog(30));
            Assert.Empty(wal.ReadLogs(30));
            Assert.Equal(-1, wal.GetLastCheckpoint(30));

            // The boundary install is the one maintenance op that materializes state on a fresh
            // partition: the checkpoint row itself and the recorded floor.
            (RaftOperationStatus status, bool suffixTruncated) =
                wal.InstallSnapshotBoundary(31, snapshotIndex: 7, lastIncludedTerm: 3, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(suffixTruncated);

            RaftLog boundary = Assert.Single(wal.ReadLogsRange(31, 1));
            Assert.Equal(7, boundary.Id);
            Assert.Equal(3, boundary.Term);
            Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
            Assert.Equal(7, wal.GetMaxLog(31));
            Assert.Equal(7, wal.GetLastCheckpoint(31));

            // The wipe removes the row and the floor together.
            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(31));
            Assert.Equal(0, wal.GetMaxLog(31));
            Assert.Empty(wal.ReadLogsRange(31, 1));
            Assert.Equal(-1, wal.GetLastCheckpoint(31));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Partitions 0 and 8 share shard column family 0 (ids congruent modulo the shard count), so
    /// every suffix scan, key-range delete and cache invalidation of one partition's maintenance
    /// runs among the other's live rows. A truncation, a truncating boundary install and a full
    /// partition wipe on one side must leave the neighbour's rows, max and recorded floor intact.
    /// </summary>
    [Fact]
    public void MaintenanceOps_OnAShardSharedByTwoPartitions_LeaveTheNeighbourUntouched()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(0, [Row(1), Row(2), Row(3), Row(4), Row(5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(8, [Row(1), Row(2), Row(3), Row(4), Row(5)])]));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(0, afterLogId: 2));
            Assert.Equal([1L, 2L], wal.ReadLogsRange(0, 1).Select(l => l.Id));
            Assert.Equal([1L, 2L, 3L, 4L, 5L], wal.ReadLogsRange(8, 1).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(8));

            (RaftOperationStatus status, bool suffixTruncated) =
                wal.InstallSnapshotBoundary(0, snapshotIndex: 1, lastIncludedTerm: 99, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(suffixTruncated);

            Assert.Equal([1L], wal.ReadLogsRange(0, 1).Select(l => l.Id));
            Assert.Equal(99, wal.GetTermAt(0, 1));
            Assert.Equal(1, wal.GetLastCheckpoint(0));
            Assert.Equal([1L, 2L, 3L, 4L, 5L], wal.ReadLogsRange(8, 1).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(8));
            Assert.Equal(1, wal.GetTermAt(8, 5));
            Assert.Equal(-1, wal.GetLastCheckpoint(8));

            // The wipe uses a range delete bounded at the partition's own key span; the neighbour
            // lives in the same column family and must survive it untouched.
            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(0));
            Assert.Equal(0, wal.GetMaxLog(0));
            Assert.Equal(-1, wal.GetLastCheckpoint(0));
            Assert.Equal([1L, 2L, 3L, 4L, 5L], wal.ReadLogsRange(8, 1).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(8));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Re-running a truncation at the same boundary is a no-op (the second call finds no suffix
    /// and no frontier to clamp), a cut below the first entry empties the partition, and the
    /// emptied partition's max and term fall back to 0. Truncation is idempotent by contract
    /// because the Raft layer re-issues it on retry.
    /// </summary>
    [Fact]
    public void TruncateLogsAfter_RepeatedAndBelowTheFirstEntry_IsIdempotentAndEmpties()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1), Row(2), Row(3), Row(4), Row(5)])]));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 3));
            Assert.Equal([1L, 2L, 3L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
            Assert.Equal(3, wal.GetMaxLog(1));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 3));
            Assert.Equal([1L, 2L, 3L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
            Assert.Equal(3, wal.GetMaxLog(1));

            // A cut below the first entry removes everything.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 0));
            Assert.Empty(wal.ReadLogsRange(1, 1));
            Assert.Equal(0, wal.GetMaxLog(1));
            Assert.Equal(0, wal.GetCurrentTerm(1));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 0));
            Assert.Empty(wal.ReadLogsRange(1, 1));
            Assert.Equal(0, wal.GetMaxLog(1));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    private static RaftLog Row(long id, long term = 1) => new()
    {
        Id = id,
        Term = term,
        Type = RaftLogType.Committed,
        LogType = "maintenance-edge",
    };

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-rocksdb-maint-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
