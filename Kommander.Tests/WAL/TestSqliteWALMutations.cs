using Kommander.Data;
using Kommander.WAL;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Boundary conditions of <see cref="SqliteWAL"/>'s mutation and maintenance statements: the
/// proposed-tail sweep's type selectivity, snapshot-boundary installs at the exact end of the log
/// and re-installed verbatim, compaction around a non-positive or checkpoint-carrying floor, the
/// contiguity certificate behind a checkpoint-floor advance, and a write whose statement fails
/// mid-transaction. Every mutation runs inside a transaction on the shard, so the recurring
/// assertion is all-or-nothing: a failure or an edge shape must leave the shard exactly as it
/// was, neighbours included.
/// </summary>
public sealed class TestSqliteWALMutations
{
    /// <summary>
    /// The proposed-tail sweep removes only unresolved entries (<c>Proposed</c> /
    /// <c>ProposedCheckpoint</c>) strictly above the anchor; resolved entries of any type above
    /// the anchor survive, and re-running the sweep changes nothing. Deleting a resolved row
    /// here would silently discard a quorum-agreed entry.
    /// </summary>
    [Fact]
    public void TruncateProposedLogsAfter_RemovesOnlyUnresolvedTypesAboveTheCut_AndIsIdempotent()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(5,
            [
                Row(1, type: RaftLogType.Committed),
                Row(2, type: RaftLogType.Proposed),
                Row(3, type: RaftLogType.Committed),
                Row(4, type: RaftLogType.Proposed),
                Row(5, type: RaftLogType.ProposedCheckpoint),
                Row(6, type: RaftLogType.RolledBack),
                Row(7, type: RaftLogType.CommittedCheckpoint),
                Row(8, type: RaftLogType.Proposed),
            ])]));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(5, afterLogId: 1));

            // Removed: the Proposed (2, 8) and ProposedCheckpoint (5) rows above the anchor.
            // Survivors: the anchor itself, and every resolved type (Committed 3, RolledBack 6,
            // CommittedCheckpoint 7) even though they sit above the anchor.
            List<RaftLog> afterSweep = wal.ReadLogsRange(5, 1);
            Assert.Equal([1L, 3L, 6L, 7L], afterSweep.Select(l => l.Id));
            Assert.Equal(RaftLogType.Committed, afterSweep[0].Type);
            Assert.Equal(RaftLogType.RolledBack, afterSweep[2].Type);
            Assert.Equal(RaftLogType.CommittedCheckpoint, afterSweep[3].Type);
            Assert.Equal(7, wal.GetMaxLog(5));

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(5, afterLogId: 1));
            Assert.Equal([1L, 3L, 6L, 7L], wal.ReadLogsRange(5, 1).Select(l => l.Id));
            Assert.Equal(7, wal.GetMaxLog(5));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A boundary install at the exact last entry with a DIFFERENT term has an empty suffix to
    /// delete, so it must report <c>SuffixTruncated = false</c> — yet the boundary row itself is
    /// rewritten with the snapshot's last-included term and checkpoint type.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_AtTheExactLastEntry_WithADifferentTerm_RewritesTheBoundaryRowWithoutReportingTruncation()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1), Row(2), Row(3), Row(4), Row(5)])]));

            (RaftOperationStatus status, bool suffixTruncated) =
                wal.InstallSnapshotBoundary(1, snapshotIndex: 5, lastIncludedTerm: 9, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(suffixTruncated, "nothing exists above the last entry, so no suffix was removed");

            Assert.Equal(9, wal.GetTermAt(1, 5));
            RaftLog boundary = wal.ReadLogsRange(1, 5).Single();
            Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
            Assert.Equal(9, boundary.Term);

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
    /// Installing the same boundary twice must be idempotent: re-installing a boundary whose term
    /// now matches reports no truncation and changes nothing, both after a suffix-retaining
    /// install and after a truncating one. Snapshots are re-delivered on retry, so the second
    /// install may neither re-delete rows nor resurrect deleted ones.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_InstallingTheSameBoundaryTwice_ChangesNothing()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

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

            // Truncating shape: the first install removes the suffix, the re-install of the
            // identical boundary is a full no-op.
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
    /// A non-positive retention floor removes nothing: no log id is below zero, so the pass
    /// succeeds, reports zero removals, still commits its single transaction, and leaves every
    /// row readable.
    /// </summary>
    [Fact]
    public void CompactLogsOlderThan_NonPositiveCheckpoint_RemovesNothing()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(3, [Row(1), Row(2), Row(3)])]));

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(3, lastCheckpoint: 0, compactNumberEntries: 10);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(0, removed);
            Assert.Equal(1, wal.LastCompactionCommitCount);

            (status, removed) = wal.CompactLogsOlderThan(3, lastCheckpoint: -5, compactNumberEntries: 10);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(0, removed);

            Assert.Equal([1L, 2L, 3L], wal.ReadLogsRange(3, 1).Select(l => l.Id));
            Assert.Equal(3, wal.GetMaxLog(3));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Compaction may legitimately delete the checkpoint ROW itself (any id below the retention
    /// floor goes), but the recorded checkpoint in the <c>checkpoints</c> table is the durable
    /// certificate for the applied prefix and must survive its own row's removal: the recorded
    /// value stays, and reads anchor on it exactly as before.
    /// </summary>
    [Fact]
    public void CompactLogsOlderThan_RemovingTheCheckpointRowItself_DoesNotRegressTheRecordedCheckpoint()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            List<RaftLog> seed =
            [
                Row(1), Row(2), Row(3),
                Row(4, type: RaftLogType.CommittedCheckpoint),
                Row(5), Row(6), Row(7), Row(8), Row(9), Row(10)
            ];

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(3, seed)]));
            Assert.Equal(4, wal.GetLastCheckpoint(3));

            // Retention floor above the checkpoint row: every id below 8 goes, including the row
            // that carried the checkpoint at id 4.
            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(3, lastCheckpoint: 8, compactNumberEntries: 100);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(7, removed);

            // The recorded certificate survived the removal of its own row.
            Assert.Equal(4, wal.GetLastCheckpoint(3));

            // Reads anchor at the recorded floor: the compacted prefix (checkpoint row included)
            // is gone, the surviving tail reads back.
            Assert.Equal([8L, 9L, 10L], wal.ReadLogs(3).Select(l => l.Id));
            Assert.Equal(10, wal.GetMaxLog(3));

            // A second pass over the same floor has nothing left to remove.
            (status, removed) = wal.CompactLogsOlderThan(3, lastCheckpoint: 8, compactNumberEntries: 100);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(0, removed);
            Assert.Equal(4, wal.GetLastCheckpoint(3));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The contiguity certificate behind a checkpoint-floor advance: a checkpoint that lands over
    /// a replication gap is stored as a plain row and the recorded floor stays unset, and the
    /// floor advances only once a later batch backfills the gap and re-carries the checkpoint.
    /// The recorded floor certifies that every id up to it was applied here, so advancing it
    /// across ids this node never held would make restore skip undelivered committed entries.
    /// </summary>
    [Fact]
    public void Checkpoint_LandingOverAGap_WithholdsTheRecordedFloor_UntilBackfillClosesIt()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(9, [Row(1), Row(2)])]));

            // The checkpoint at id 5 arrives while ids 3 and 4 are missing.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(9, [Row(5, type: RaftLogType.CommittedCheckpoint)])]));

            Assert.Equal(-1, wal.GetLastCheckpoint(9));
            Assert.Equal([1L, 2L, 5L], wal.ReadLogsRange(9, 1).Select(l => l.Id));

            // The gap closes and the checkpoint is re-carried in the same batch as the backfill.
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(9, [Row(3), Row(4), Row(5, type: RaftLogType.CommittedCheckpoint)])]));

            Assert.Equal(5, wal.GetLastCheckpoint(9));
            Assert.Equal([5L], wal.ReadLogs(9).Select(l => l.Id));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A write whose statement fails mid-transaction reports <see cref="RaftOperationStatus.Errored"/>
    /// and rolls back the WHOLE shard transaction: rows upserted by earlier statements of the same
    /// batch are gone too. The failure is injected with a trigger on the shard database (the
    /// storage engine's own fault injection — no product seam), and after the trigger is dropped
    /// the shard and its cached statements stay fully usable.
    /// </summary>
    [Fact]
    public void Write_StatementFailure_RollsBackTheWholeShardTransaction_AndLeavesTheShardUsable()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, [Row(1, term: 11)])]));

            string shardFile = Path.Combine(path, "raft_shard0_wal.db");

            using (SqliteConnection injector = new($"Data Source={shardFile};Pooling=False"))
            {
                injector.Open();

                using SqliteCommand createTrigger = new(
                    "CREATE TRIGGER block_log_inserts BEFORE INSERT ON logs " +
                    "BEGIN SELECT RAISE(ABORT, 'wal-test injected insert failure'); END",
                    injector);
                createTrigger.ExecuteNonQuery();

                // One batch, two partitions on the shared shard: the upsert of partition 1's
                // EXISTING row applies inside the transaction, then partition 2's new row aborts.
                // The rollback must take partition 1's update with it.
                Assert.Equal(RaftOperationStatus.Errored,
                    wal.Write([(1, [Row(1, term: 12)]), (2, [Row(1, term: 21)])]));

                using SqliteCommand dropTrigger = new("DROP TRIGGER block_log_inserts", injector);
                dropTrigger.ExecuteNonQuery();
            }

            // Nothing from the failed batch survived — the earlier statement's update included:
            // the pre-failure row still carries its original term, not the batch's 12.
            RaftLog survivor = Assert.Single(wal.ReadLogs(1));
            Assert.Equal(1, survivor.Id);
            Assert.Equal(11, survivor.Term);
            Assert.Empty(wal.ReadLogs(2));
            Assert.Equal(1, wal.GetMaxLog(1));
            Assert.Equal(0, wal.GetMaxLog(2));

            // The shard, its connection and its cached prepared statements survived the failure.
            Assert.Equal(RaftOperationStatus.Success,
                wal.Write([(1, [Row(1, term: 12)]), (2, [Row(1, term: 21)])]));

            Assert.Equal(12, wal.GetTermAt(1, 1));
            Assert.Equal([1L], wal.ReadLogs(2).Select(l => l.Id));
            Assert.Equal(21, wal.GetTermAt(2, 1));
            Assert.Equal(1, wal.GetMaxLog(1));
            Assert.Equal(1, wal.GetMaxLog(2));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The combined truncate-and-read on a partition without rows (never written, and emptied by
    /// a truncation) succeeds, deletes nothing, and reports 0 as the max.
    /// </summary>
    [Fact]
    public void TruncateLogsAfterAndGetMax_OnAPartitionWithoutRows_ReturnsSuccessAndZero()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);

            Assert.Equal((RaftOperationStatus.Success, 0L), wal.TruncateLogsAfterAndGetMax(77, afterLogId: 10));
            Assert.Equal(0, wal.GetMaxLog(77));

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(78, [Row(1), Row(2)])]));
            Assert.Equal((RaftOperationStatus.Success, 0L), wal.TruncateLogsAfterAndGetMax(78, afterLogId: 0));
            Assert.Empty(wal.ReadLogs(78));
            Assert.Equal(0, wal.GetMaxLog(78));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static RaftLog Row(long id, long term = 1, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = "sqlite-mutation", LogData = [1, 2, 3] };

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-sqlite-mut-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
