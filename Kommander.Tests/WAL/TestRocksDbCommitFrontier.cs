using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Pins the persisted commit-frontier behavior of <see cref="RocksDbWAL"/>: a contiguous
/// <c>Committed</c> marker is absorbed into one small metadata value instead of rewriting the
/// full entry, reads derive the effective type from the frontier, restarts replay absorbed
/// commits as <c>Committed</c>, and every path that can invalidate the frontier (truncation,
/// snapshot boundary, partition wipe) re-baselines it. Also covers the range-delete compaction
/// that replaced per-key tombstones.
/// </summary>
public sealed class TestRocksDbCommitFrontier
{
    private const int Partition = 1;

    [Fact]
    public void ContiguousCommit_AbsorbedIntoFrontier_ReadsCommittedAndSurvivesReopen()
    {
        string path = CreateTempWalPath();

        try
        {
            byte[] payload = [1, 2, 3, 4];

            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                Assert.Equal(RaftOperationStatus.Success, wal.Write(
                    [(Partition, [Proposed(1, 5, payload), Proposed(2, 5, payload), Proposed(3, 5, payload)])]));

                Assert.Equal(RaftOperationStatus.Success, wal.Write(
                    [(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));

                Assert.Equal(3, wal.CommitMarkersAbsorbed);

                List<RaftLog> logs = wal.ReadLogs(Partition);
                Assert.Equal(3, logs.Count);
                Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
                Assert.All(logs, log => Assert.Equal(payload, log.LogData));
                Assert.All(logs, log => Assert.Equal(5, log.Term));
            }

            // The commit survives the restart through the frontier, not through row rewrites.
            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                List<RaftLog> logs = reopened.ReadLogs(Partition);
                Assert.Equal(3, logs.Count);
                Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
                Assert.All(logs, log => Assert.Equal(payload, log.LogData));
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void DuplicateCommitBelowFrontier_IsNoOpSuccess()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5)])]));
            Assert.Equal(1, wal.CommitMarkersAbsorbed);

            // The re-shipped duplicate marker changes nothing and reports success.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5)])]));

            RaftLog log = Assert.Single(wal.ReadLogs(Partition));
            Assert.Equal(RaftLogType.Committed, log.Type);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void CommitOverGap_WritesFullRow_ThenFrontierCatchesUpOverResolvedRows()
    {
        string path = CreateTempWalPath();

        try
        {
            byte[] payload = [9, 9];

            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                Assert.Equal(RaftOperationStatus.Success, wal.Write(
                    [(Partition, [Proposed(1, 5, payload), Proposed(2, 5, payload), Proposed(3, 5, payload)])]));

                // Commit of id 2 lands over the unresolved id 1: must persist as a full row.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(2, 5, payload)])]));
                Assert.Equal(0, wal.CommitMarkersAbsorbed);

                // Commit of id 1 chains from the frontier and is absorbed.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 5, payload)])]));
                Assert.Equal(1, wal.CommitMarkersAbsorbed);

                // Commit of id 3 sits above the gap id 2, but that row is already resolved on
                // disk (the full-row commit above), so the frontier catches up and absorbs.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(3, 5, payload)])]));
                Assert.Equal(2, wal.CommitMarkersAbsorbed);
            }

            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                List<RaftLog> logs = reopened.ReadLogs(Partition);
                Assert.Equal([1L, 2L, 3L], logs.Select(l => l.Id));
                Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void CommitWithDivergentTerm_RewritesFullRow()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 5)])]));

            // A commit carrying a different term must never be absorbed by the frontier over the
            // stale row: the full row (with the marker's term) wins, exactly as before.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(1, 7)])]));
            Assert.Equal(0, wal.CommitMarkersAbsorbed);

            RaftLog log = Assert.Single(wal.ReadLogs(Partition));
            Assert.Equal(RaftLogType.Committed, log.Type);
            Assert.Equal(7, log.Term);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void TruncateLogsAfter_ClampsFrontier_FreshRowsReadProposed()
    {
        string path = CreateTempWalPath();

        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                Assert.Equal(RaftOperationStatus.Success, wal.Write(
                    [(Partition, [Proposed(1, 5), Proposed(2, 5), Proposed(3, 5)])]));
                Assert.Equal(RaftOperationStatus.Success, wal.Write(
                    [(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));

                Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 1));

                // A divergent-tail replacement at a truncated id must NOT read as Committed
                // through a stale frontier.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(2, 9)])]));

                List<RaftLog> logs = wal.ReadLogs(Partition);
                Assert.Equal([1L, 2L], logs.Select(l => l.Id));
                Assert.Equal(RaftLogType.Committed, logs[0].Type);
                Assert.Equal(RaftLogType.Proposed, logs[1].Type);
                Assert.Equal(9, logs[1].Term);
            }

            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                List<RaftLog> logs = reopened.ReadLogs(Partition);
                Assert.Equal(RaftLogType.Committed, logs[0].Type);
                Assert.Equal(RaftLogType.Proposed, logs[1].Type);
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void TruncateProposedLogsAfter_DoesNotDeleteFrontierCommittedRows()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [Proposed(1, 5), Proposed(2, 5), Proposed(3, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));
            Assert.Equal(3, wal.CommitMarkersAbsorbed);

            // The rows are still Proposed-typed on disk, but the frontier certifies them
            // committed — the proposed-tail sweep must leave them alone.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, 0));

            List<RaftLog> logs = wal.ReadLogs(Partition);
            Assert.Equal(3, logs.Count);
            Assert.All(logs, log => Assert.Equal(RaftLogType.Committed, log.Type));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void BackfilledResolvedRows_FrontierCatchesUp_AndAbsorbsNextCommit()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            // Backfill shape: already-committed rows arrive as first writes. No frontier chain
            // exists from id 1 here — absorption must not fire, full rows must land.
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5), Committed(4, 5), Committed(5, 5)])]));
            Assert.Equal(0, wal.CommitMarkersAbsorbed);

            // Live traffic after the backfill: the catch-up walk certifies the row-resolved prefix
            // 1..5 and absorbs the contiguous commit of id 6.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(6, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(6, 5)])]));
            Assert.Equal(1, wal.CommitMarkersAbsorbed);

            List<RaftLog> logs = wal.ReadLogs(Partition);
            Assert.Equal(RaftLogType.Committed, logs.Single(l => l.Id == 6).Type);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void CompactedPrefix_FrontierCatchesUpViaCheckpointCertifiedAbsence()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            // Committed rows 1..9 plus a checkpoint at 10, then compaction removes 1..9. The
            // absent prefix stays certified by the persisted checkpoint.
            List<RaftLog> logs = [];
            for (long id = 1; id <= 9; id++)
                logs.Add(Committed(id, 5));
            logs.Add(new RaftLog { Id = 10, Term = 5, Type = RaftLogType.CommittedCheckpoint, LogType = "chk" });
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, logs)]));
            Assert.Equal(10, wal.GetLastCheckpoint(Partition));

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, 10, 100);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(9, removed);

            // The catch-up walk hops the compacted-certified absent span in one step, passes the
            // checkpoint row, and absorbs the contiguous commit of id 11.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(11, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Committed(11, 5)])]));
            Assert.Equal(1, wal.CommitMarkersAbsorbed);

            Assert.Equal(RaftLogType.Committed, wal.ReadLogs(Partition).Single(l => l.Id == 11).Type);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void ProposedRowBelowCheckpoint_KeepsReadingProposed()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            // A Proposed row below a CommittedCheckpoint keeps its on-disk type on read: the
            // frontier never certifies a row it did not verify against its own commit marker.
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [
                    Proposed(1, 5),
                    Committed(2, 5),
                    new RaftLog { Id = 3, Term = 5, Type = RaftLogType.CommittedCheckpoint, LogType = "chk" }
                ])]));

            List<RaftLog> logs = wal.ReadLogsRange(Partition, 1);
            Assert.Equal(RaftLogType.Proposed, logs.Single(l => l.Id == 1).Type);
            Assert.Equal(RaftLogType.Committed, logs.Single(l => l.Id == 2).Type);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void SnapshotBoundaryWithConflict_ClampsFrontierToBoundary()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [Proposed(1, 5), Proposed(2, 5), Proposed(3, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [Committed(1, 5), Committed(2, 5), Committed(3, 5)])]));

            // The boundary's term conflicts with the local row at id 2, so the suffix above it is
            // truncated and the frontier must clamp to the boundary.
            (RaftOperationStatus status, bool suffixTruncated) = wal.InstallSnapshotBoundary(
                Partition, snapshotIndex: 2, lastIncludedTerm: 99, sync: false);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(suffixTruncated);

            // A fresh proposal re-filling the truncated id must read Proposed, not Committed.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(3, 100)])]));

            List<RaftLog> logs = wal.ReadLogs(Partition);
            RaftLog boundary = logs.Single(l => l.Id == 2);
            Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
            Assert.Equal(99, boundary.Term);

            RaftLog refilled = logs.Single(l => l.Id == 3);
            Assert.Equal(RaftLogType.Proposed, refilled.Type);
            Assert.Equal(100, refilled.Term);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void DeletePartitionWal_ClearsFrontier_ForReusedPartitionIds()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [Proposed(1, 5), Proposed(2, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [Committed(1, 5), Committed(2, 5)])]));

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(Partition));
            Assert.Empty(wal.ReadLogs(Partition));

            // A reused partition id starting over from id 1 must not inherit the old frontier.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(1, 8)])]));

            RaftLog log = Assert.Single(wal.ReadLogs(Partition));
            Assert.Equal(RaftLogType.Proposed, log.Type);
            Assert.Equal(8, log.Term);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void Compaction_RangeDelete_LeavesFloorAndTailIntact_AcrossReopen()
    {
        string path = CreateTempWalPath();

        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                List<RaftLog> logs = [];
                for (long id = 1; id <= 9; id++)
                    logs.Add(Committed(id, 5, [1]));
                logs.Add(new RaftLog { Id = 10, Term = 5, Type = RaftLogType.CommittedCheckpoint, LogType = "chk" });

                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, logs)]));
                Assert.Equal(10, wal.GetLastCheckpoint(Partition));

                (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(
                    Partition, lastCheckpoint: 10, compactNumberEntries: 100);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.Equal(9, removed);
                Assert.Equal(1, wal.LastCompactionWriteCount);

                RaftLog survivor = Assert.Single(wal.ReadLogs(Partition));
                Assert.Equal(10, survivor.Id);
                Assert.Equal(1, wal.CountPersistedLogs(Partition));
                Assert.Equal(10, wal.GetMaxLog(Partition));
            }

            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                RaftLog survivor = Assert.Single(reopened.ReadLogs(Partition));
                Assert.Equal(10, survivor.Id);
                Assert.Equal(RaftLogType.CommittedCheckpoint, survivor.Type);
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The whole-file drop must physically reclaim the SST bytes of a dead prefix that has reached a
    /// bottom level. Reclaim is anchored at the persisted compaction floor, which a pass advances
    /// straight to the caller's effective floor regardless of the entry cap (the cap bounds the
    /// counting scan only), so one pass reclaims the whole dead prefix. This is the L6-non-reclaim
    /// the w3 probe surfaced, isolated. The dead prefix is compacted to the bottom first because RocksDB
    /// <c>DeleteFilesInRange</c> only considers levels below L0.
    /// </summary>
    [Fact]
    public void WholeFileDrop_ReclaimsBottomLevelBytesOfDeadPrefix_AnchoredAtFloor()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            // Poorly-compressible payload so the on-disk size is real (a zero buffer would Snappy
            // down to almost nothing and make the byte assertion meaningless).
            byte[] payload = new byte[4096];
            for (int i = 0; i < payload.Length; i++)
                payload[i] = (byte)((i * 2654435761L) >> 13);

            List<RaftLog> deadPrefix = [];
            for (long id = 1; id <= 2000; id++)
                deadPrefix.Add(Committed(id, 5, payload));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, deadPrefix)]));
            wal.FlushMemTablesForTesting(wait: true);

            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(Partition, [new RaftLog { Id = 2001, Term = 5, Type = RaftLogType.CommittedCheckpoint, LogType = "chk" }])]));
            wal.FlushMemTablesForTesting(wait: true);

            // DeleteFilesInRange only acts on L1+, so push the data down first — the natural fate of
            // an entry that outlived L0 in production.
            wal.CompactShardToBottomForTesting(Partition);

            long before = wal.GetShardLiveSstBytes();
            Assert.True(before > 1_000_000, $"expected the payload prefix on disk, saw {before} bytes");

            // A pass capped at 500 entries against a 2000-entry prefix: the floor still advances to
            // the checkpoint and the whole dead prefix is dropped; the count is completed
            // arithmetically past the cap.
            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(
                Partition, lastCheckpoint: 2001, compactNumberEntries: 100, maxTotalEntries: 500);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(2000, removed);

            long after = wal.GetShardLiveSstBytes();

            Assert.True(after < before / 2,
                $"whole-file drop did not reclaim the dead prefix: {before} -> {after} bytes (removed {removed})");

            // The surviving checkpoint row is intact.
            Assert.Equal(2001, Assert.Single(wal.ReadLogs(Partition)).Id);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    private static RaftLog Proposed(long id, long term, byte[]? payload = null) =>
        new() { Id = id, Term = term, Type = RaftLogType.Proposed, LogType = "op", LogData = payload };

    private static RaftLog Committed(long id, long term, byte[]? payload = null) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "op", LogData = payload };

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-rocksdb-frontier-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
