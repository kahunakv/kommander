using Kommander.Data;
using Kommander.WAL;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The disposal, failure and checkpoint-recomputation edges of <see cref="SqliteWAL"/>'s mutation
/// surface: every status-returning operation on a disposed WAL reports its failure value instead
/// of throwing on a closed connection, a statement that SQLite refuses (injected with a trigger,
/// the engine's own fault injection) rolls back its whole transaction and leaves the shard
/// usable, a truncation whose cut removes the recorded last-checkpoint recomputes the surviving
/// floor from the surviving rows (or clears it), and a shard-count that disagrees with the one
/// pinned into the data directory fails fast instead of silently remapping partitions to
/// different shard files.
/// </summary>
public sealed class TestSqliteWALDisposalAndBoundaryMutations
{
    /// <summary>
    /// The class contract promises status-returning calls report their failure value once the WAL
    /// is disposed (<see cref="RaftOperationStatus.Errored"/>, false, or 0) while reads throw
    /// <see cref="ObjectDisposedException"/> — a host can still be reading through
    /// <c>WalAdapter</c> while <c>RaftManager</c> tears the WAL down.
    /// </summary>
    [Fact]
    public void OperationsAfterDispose_ReportTheirFailureValueInsteadOfThrowing()
    {
        string path = CreateTempWalPath();

        try
        {
            SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(5, [Row(1)])]));
            wal.Dispose();

            // Status-returning mutations: Errored, never an exception from the closed connection.
            Assert.Equal(RaftOperationStatus.Errored, wal.Write([(5, [Row(2)])]));
            Assert.Equal(RaftOperationStatus.Errored, wal.TruncateLogsAfter(5, afterLogId: 0));
            Assert.Equal((RaftOperationStatus.Errored, 0L), wal.TruncateLogsAfterAndGetMax(5, afterLogId: 0));
            Assert.Equal(RaftOperationStatus.Errored, wal.TruncateProposedLogsAfter(5, afterLogId: 0));
            Assert.Equal(RaftOperationStatus.Errored, wal.DeletePartitionWAL(5));
            Assert.Equal((RaftOperationStatus.Errored, 0), wal.CompactLogsOlderThan(5, lastCheckpoint: 10, compactNumberEntries: 10));
            Assert.Equal((RaftOperationStatus.Errored, false), wal.InstallSnapshotBoundary(5, snapshotIndex: 1, lastIncludedTerm: 1, sync: true));

            // Reads and metadata report disposal loudly so a late caller cannot mistake it for
            // "no data": the documented split is throw for reads, failure value for statuses.
            Assert.Equal(0, wal.GetMaxLog(5));
            Assert.Throws<ObjectDisposedException>(() => wal.ReadLogs(5));
            Assert.Throws<ObjectDisposedException>(() => wal.ReadLogsRange(5, 1));
            Assert.Throws<ObjectDisposedException>(() => wal.GetMetaData("shard_count"));
            Assert.False(wal.SetMetaData("late-key", "late-value"));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A partition wipe for a shard that was never opened and whose file was never created is a
    /// success without touching the filesystem: no shard database may be materialized just to
    /// discover there is nothing to delete.
    /// </summary>
    [Fact]
    public void DeletePartitionWAL_OnAShardThatWasNeverCreated_SucceedsWithoutCreatingTheFile()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(42));

            Assert.False(File.Exists($"{path}/raft_shard0_wal.db"),
                "a no-op wipe must not materialize a shard database");
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A wipe issued by a reopened instance that has not yet opened the shard must open the
    /// existing file to perform the delete: the rows and the recorded checkpoint the previous
    /// process wrote are gone afterwards, and the next occupant of the id starts clean.
    /// </summary>
    [Fact]
    public void DeletePartitionWAL_AfterReopen_OpensTheExistingShardFileToPerformTheDelete()
    {
        string path = CreateTempWalPath();

        try
        {
            using (SqliteWAL first = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1))
            {
                Assert.Equal(RaftOperationStatus.Success, first.Write([(7, [Row(1), Row(2), Checkpoint(3)])]));
            }

            using (SqliteWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 0))
            {
                // No read on partition 7 has run yet, so the shard connection is not open.
                Assert.Equal(RaftOperationStatus.Success, reopened.DeletePartitionWAL(7));
                Assert.Empty(reopened.ReadLogs(7));
                Assert.Equal(0, reopened.GetMaxLog(7));
                Assert.Equal(-1, reopened.GetLastCheckpoint(7));
            }

            using SqliteWAL second = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 0);
            Assert.Empty(second.ReadLogs(7));
            Assert.Equal(-1, second.GetLastCheckpoint(7));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A wipe whose DELETE statement fails mid-transaction reports Errored and rolls back: every
    /// row and the recorded checkpoint survive, and after the fault is cleared the wipe succeeds.
    /// </summary>
    [Fact]
    public void DeletePartitionWAL_StatementFailure_RollsBackAndLeavesThePartitionIntact()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(3, [Row(1), Row(2), Checkpoint(3)])]));

            // Reads anchor at the checkpoint floor, so the surviving-rows view is floor-relative:
            // capture it before the failed wipe and compare after.
            List<long> viewBefore = wal.ReadLogs(3).Select(l => l.Id).ToList();

            using (SqliteConnection injector = new($"Data Source={path}/raft_shard0_wal.db;Pooling=False"))
            {
                injector.Open();
                using SqliteCommand createTrigger = new(
                    "CREATE TRIGGER block_log_deletes BEFORE DELETE ON logs " +
                    "BEGIN SELECT RAISE(ABORT, 'wal-test injected delete failure'); END",
                    injector);
                createTrigger.ExecuteNonQuery();

                Assert.Equal(RaftOperationStatus.Errored, wal.DeletePartitionWAL(3));

                using SqliteCommand dropTrigger = new("DROP TRIGGER block_log_deletes", injector);
                dropTrigger.ExecuteNonQuery();
            }

            // The failed wipe changed nothing: the floor-anchored read view and the recorded
            // checkpoint are exactly what they were before the faulted delete.
            Assert.Equal(viewBefore, wal.ReadLogs(3).Select(l => l.Id).ToList());
            Assert.Equal(3, wal.GetLastCheckpoint(3));

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(3));
            Assert.Empty(wal.ReadLogs(3));
            Assert.Equal(-1, wal.GetLastCheckpoint(3));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A truncation whose cut sits between two recorded checkpoints recomputes the persisted
    /// floor from the surviving rows: the checkpoint above the cut is gone with its row, so the
    /// floor must fall back to the highest surviving checkpoint below the cut.
    /// </summary>
    [Fact]
    public void TruncateLogsAfter_CutBetweenTwoCheckpoints_RecomputesTheFloorToTheSurvivingOne()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            List<RaftLog> seed =
            [
                Row(1), Row(2), Row(3),
                Checkpoint(4),
                Row(5),
                Checkpoint(6),
                Row(7), Row(8),
            ];
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, seed)]));
            Assert.Equal(6, wal.GetLastCheckpoint(1));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 5));

            Assert.Equal(4, wal.GetLastCheckpoint(1));
            Assert.Equal([4L, 5L], wal.ReadLogs(1).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(1));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A truncation that removes the only checkpoint — leaving no surviving checkpoint below the
    /// cut — deletes the recorded floor row entirely, so a later read reports "no checkpoint"
    /// rather than a floor that certifies a prefix the log no longer holds.
    /// </summary>
    [Fact]
    public void TruncateLogsAfter_CutRemovingEveryCheckpoint_ClearsTheRecordedFloor()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            List<RaftLog> seed = [Row(1), Row(2), Row(3), Row(4), Row(5), Checkpoint(6), Row(7), Row(8)];
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, seed)]));
            Assert.Equal(6, wal.GetLastCheckpoint(1));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, afterLogId: 3));

            Assert.Equal(-1, wal.GetLastCheckpoint(1));
            // With no floor, replay reads the whole surviving prefix.
            Assert.Equal([1L, 2L, 3L], wal.ReadLogs(1).Select(l => l.Id));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The combined truncate-and-read returns the max observed AFTER the deletion, and applies
    /// the same checkpoint recomputation as the plain truncation: the floor is cleared when the
    /// cut removed the only checkpoint.
    /// </summary>
    [Fact]
    public void TruncateLogsAfterAndGetMax_WithSurvivingRows_ReturnsThePostTruncationMaxAndAdjustsTheFloor()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            List<RaftLog> seed = [Row(1), Row(2), Row(3), Row(4), Row(5), Row(6), Checkpoint(7), Row(8), Row(9)];
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, seed)]));

            Assert.Equal((RaftOperationStatus.Success, 6L), wal.TruncateLogsAfterAndGetMax(1, afterLogId: 6));

            Assert.Equal(6, wal.GetMaxLog(1));
            Assert.Equal(-1, wal.GetLastCheckpoint(1));
            Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A boundary install on a durable instance that opts out of the fsync toggles the shard's
    /// synchronous pragma OFF for the install and back to FULL afterwards — the install itself
    /// must still land: the boundary row, its term and the recorded floor survive a reopen.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_WithSyncOptOutOnADurableInstance_InstallsTheBoundaryDurably()
    {
        string path = CreateTempWalPath();

        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true, shardCount: 1))
            {
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1), Row(2), Row(3)])]));

                (RaftOperationStatus status, bool suffixTruncated) =
                    wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 9, sync: false);

                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.False(suffixTruncated);
                Assert.Equal(9, wal.GetTermAt(1, 3));
            }

            // The sync-off install is durable: a fresh instance reads the boundary and its floor.
            using SqliteWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 0);
            Assert.Equal(9, reopened.GetTermAt(1, 3));
            Assert.Equal(3, reopened.GetLastCheckpoint(1));
            Assert.Equal(3, reopened.GetMaxLog(1));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A retained-suffix install (matching term at the boundary) must not lower the recorded
    /// checkpoint when a HIGHER checkpoint still sits above the boundary: the floor is the
    /// greater of the existing record and the boundary index.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_RetainedSuffixWithAHigherRecordedCheckpoint_KeepsTheHigherFloor()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            List<RaftLog> seed = [Row(1), Row(2), Row(3), Row(4), Row(5), Checkpoint(6)];
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, seed)]));
            Assert.Equal(6, wal.GetLastCheckpoint(1));

            (RaftOperationStatus status, bool suffixTruncated) =
                wal.InstallSnapshotBoundary(1, snapshotIndex: 2, lastIncludedTerm: 1, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(suffixTruncated);

            Assert.Equal(6, wal.GetLastCheckpoint(1));
            Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
            Assert.Equal(6, wal.GetMaxLog(1));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A boundary install on a partition with no rows at all creates the boundary row (there is
    /// no local term to match, so nothing is truncated) and records the floor at the boundary —
    /// this is the shape a snapshot transfer to a fresh follower produces.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_OnAPartitionWithoutRows_CreatesTheBoundaryAndTheFloor()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            (RaftOperationStatus status, bool suffixTruncated) =
                wal.InstallSnapshotBoundary(9, snapshotIndex: 4, lastIncludedTerm: 2, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(suffixTruncated);

            RaftLog boundary = Assert.Single(wal.ReadLogsRange(9, 1));
            Assert.Equal(4, boundary.Id);
            Assert.Equal(2, boundary.Term);
            Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
            Assert.Equal(4, wal.GetLastCheckpoint(9));
            Assert.Equal(4, wal.GetMaxLog(9));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A boundary install whose upsert statement fails rolls back the whole transaction: no
    /// boundary row, no recorded floor — and once the fault is cleared the same install succeeds,
    /// proving the shard connection and its cached statements survived the failure.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_StatementFailure_RollsBackAndLeavesTheShardUsable()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);
            // The shard's tables are created lazily by the first write; give the trigger a table
            // to attach to.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1)])]));

            using (SqliteConnection injector = new($"Data Source={path}/raft_shard0_wal.db;Pooling=False"))
            {
                injector.Open();
                using SqliteCommand createTrigger = new(
                    "CREATE TRIGGER block_boundary_insert BEFORE INSERT ON logs " +
                    "BEGIN SELECT RAISE(ABORT, 'wal-test injected boundary failure'); END",
                    injector);
                createTrigger.ExecuteNonQuery();

                Assert.Equal((RaftOperationStatus.Errored, false),
                    wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 2, sync: true));

                using SqliteCommand dropTrigger = new("DROP TRIGGER block_boundary_insert", injector);
                dropTrigger.ExecuteNonQuery();
            }

            // The faulted install changed nothing: the seed row survives, no floor, no growth.
            Assert.Equal([1L], wal.ReadLogs(1).Select(l => l.Id));
            Assert.Equal(-1, wal.GetLastCheckpoint(1));
            Assert.Equal(1, wal.GetMaxLog(1));

            (RaftOperationStatus status, bool suffixTruncated) =
                wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 2, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(suffixTruncated);
            Assert.Equal(3, wal.GetLastCheckpoint(1));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A compaction pass whose DELETE statement fails reports Errored, removes nothing, and
    /// leaves the shard fully usable once the fault clears.
    /// </summary>
    [Fact]
    public void CompactLogsOlderThan_StatementFailure_RemovesNothingAndLeavesTheShardUsable()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(2, [Row(1), Row(2), Row(3), Row(4), Row(5)])]));

            using (SqliteConnection injector = new($"Data Source={path}/raft_shard0_wal.db;Pooling=False"))
            {
                injector.Open();
                using SqliteCommand createTrigger = new(
                    "CREATE TRIGGER block_compact_deletes BEFORE DELETE ON logs " +
                    "BEGIN SELECT RAISE(ABORT, 'wal-test injected compaction failure'); END",
                    injector);
                createTrigger.ExecuteNonQuery();

                Assert.Equal((RaftOperationStatus.Errored, 0), wal.CompactLogsOlderThan(2, lastCheckpoint: 10, compactNumberEntries: 10));

                using SqliteCommand dropTrigger = new("DROP TRIGGER block_compact_deletes", injector);
                dropTrigger.ExecuteNonQuery();
            }

            Assert.Equal([1L, 2L, 3L, 4L, 5L], wal.ReadLogsRange(2, 1).Select(l => l.Id));

            Assert.Equal((RaftOperationStatus.Success, 5), wal.CompactLogsOlderThan(2, lastCheckpoint: 10, compactNumberEntries: 10));
            Assert.Empty(wal.ReadLogs(2));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Reopening a data directory with an explicit shard count that differs from the pinned one
    /// fails fast naming both values: honouring the new count would remap partition ids onto
    /// different shard files and orphan every log already written.
    /// </summary>
    [Fact]
    public void Reopen_WithADifferentExplicitShardCount_FailsFastNamingBothValues()
    {
        string path = CreateTempWalPath();

        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 2))
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1)])]));

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 3));

            Assert.Contains("3", ex.Message);
            Assert.Contains("2", ex.Message);
            Assert.Contains("shard_count", ex.Message);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A persisted shard_count that parses but is not a positive integer is corruption, not a
    /// configuration choice: construction fails fast naming the stored value rather than mapping
    /// partitions through a zero or negative modulus.
    /// </summary>
    [Theory]
    [InlineData("0")]
    [InlineData("-4")]
    public void PersistedShardCountThatIsNotPositive_FailsFastNamingTheStoredValue(string corruptedValue)
    {
        string path = CreateTempWalPath();

        try
        {
            using (SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 2))
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1)])]));

            using (SqliteConnection connection = new($"Data Source={path}/raft_metadata_wal.db;Pooling=False"))
            {
                connection.Open();
                using SqliteCommand corrupt = new(
                    "UPDATE metadata SET value = @value WHERE key = 'shard_count'", connection);
                corrupt.Parameters.AddWithValue("@value", corruptedValue);
                Assert.Equal(1, corrupt.ExecuteNonQuery());
            }

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance));

            Assert.Contains(corruptedValue, ex.Message);
            Assert.Contains("shard_count", ex.Message);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static RaftLog Row(long id, long term = 1) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "sqlite-boundary", LogData = [1, 2, 3] };

    private static RaftLog Checkpoint(long id, long term = 1) =>
        new() { Id = id, Term = term, Type = RaftLogType.CommittedCheckpoint, LogType = "sqlite-boundary-ck" };

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-sqlite-edge-{Guid.NewGuid():N}");
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
        catch
        {
            // Best-effort cleanup: a connection the constructor leaked while failing fast can hold
            // the metadata file open on platforms that lock deleted files.
        }
    }
}
