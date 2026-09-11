using System.Diagnostics;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The persisted compaction floor of <see cref="RocksDbWAL"/>: compaction deletes the log's dead
/// prefix logically (a per-partition floor) and reclaims it physically as whole files, so no log
/// row is ever rewritten by RocksDB compaction. These tests pin both halves — the read semantics
/// (identical to the old physical delete) and the physical layout that makes the reclaim free
/// (non-overlapping files, trivial moves out of L0, floor-anchored whole-file drops, memtable-
/// confined tombstones).
/// </summary>
public sealed class TestRocksDbCompactionFloor
{
    private const int Partition = 1;

    // ───────────────────────────── logical semantics ─────────────────────────────

    [Fact]
    public void Compaction_HidesPrefixBelowFloor_AndSurvivesReopen()
    {
        string path = CreateTempWalPath();

        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 10))]));
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11)])]));

                (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 6, compactNumberEntries: 100);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.Equal(5, removed);
                Assert.Equal(1, wal.LastCompactionWriteCount);
                Assert.Equal(6, wal.GetCompactionFloorForTesting(Partition));

                // Rows 1-5 are still physically present (nothing was flushed, no file was dropped),
                // yet every read path must report them gone.
                Assert.Equal([6, 7, 8, 9, 10, 11], wal.ReadLogsRange(Partition, 0).Select(l => l.Id));
                Assert.Equal([6, 7, 8, 9, 10, 11], wal.ReadLogsRange(Partition, 3).Select(l => l.Id));
                Assert.Equal(6, wal.CountPersistedLogs(Partition));
                Assert.Equal(5, wal.CountRemovableLogs(Partition));
                Assert.Equal(11, wal.GetMaxLog(Partition));
                Assert.Equal(-1, wal.GetTermAt(Partition, 5));
                Assert.Equal(5, wal.GetTermAt(Partition, 6));

                // A second pass with the same floor has nothing to do and writes nothing.
                (status, removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 6, compactNumberEntries: 100);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.Equal(0, removed);
                Assert.Equal(0, wal.LastCompactionWriteCount);
            }

            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                Assert.Equal(6, reopened.GetCompactionFloorForTesting(Partition));
                Assert.Equal([6, 7, 8, 9, 10, 11], reopened.ReadLogsRange(Partition, 0).Select(l => l.Id));
                Assert.Equal(6, reopened.CountPersistedLogs(Partition));

                // Restore reads from the checkpoint, which always sits at or above the floor.
                Assert.Equal([11], reopened.ReadLogs(Partition).Select(l => l.Id));
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void CappedPass_AdvancesFloorToEffectiveFloor_CountStaysExact()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 20))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(21)])]));

            // The cap bounds the counting scan, not the deletion: the floor is the caller's effective
            // retention floor and advances to it in one pass (one metadata put, one confined
            // tombstone). Beyond the cap the count is completed arithmetically and stays exact.
            (_, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 21, compactNumberEntries: 4, maxTotalEntries: 8);
            Assert.Equal(20, removed);
            Assert.Equal(21, wal.GetCompactionFloorForTesting(Partition));
            Assert.Equal([21], wal.ReadLogsRange(Partition, 0).Select(l => l.Id));
            Assert.Equal(0, wal.CountRemovableLogs(Partition));

            (_, removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 21, compactNumberEntries: 100, maxTotalEntries: 100);
            Assert.Equal(0, removed);
            Assert.Equal(0, wal.LastCompactionWriteCount);
            Assert.Equal(21, wal.GetCompactionFloorForTesting(Partition));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void TruncateLogsAfter_BelowFloor_ClampsFloor_SoRewrittenTailIsVisible()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 10))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11)])]));
            wal.CompactLogsOlderThan(Partition, lastCheckpoint: 8, compactNumberEntries: 100);
            Assert.Equal(8, wal.GetCompactionFloorForTesting(Partition));

            // A truncation below the floor (never issued by Raft in practice, but the invariant must
            // hold): rows written afterwards at 4.. must read back.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 3));
            Assert.Equal(4, wal.GetCompactionFloorForTesting(Partition));

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(4, 7), Proposed(5, 7)])]));
            Assert.Equal([4, 5], wal.ReadLogsRange(Partition, 0).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(Partition));
            Assert.Equal(7, wal.GetCurrentTerm(Partition));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void SnapshotBoundary_BelowFloor_ClampsFloor_SoBoundaryRowIsVisible()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 10))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11)])]));
            wal.CompactLogsOlderThan(Partition, lastCheckpoint: 9, compactNumberEntries: 100);
            Assert.Equal(9, wal.GetCompactionFloorForTesting(Partition));

            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, snapshotIndex: 5, lastIncludedTerm: 9, sync: false);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(truncated);
            Assert.Equal(5, wal.GetCompactionFloorForTesting(Partition));

            RaftLog boundary = Assert.Single(wal.ReadLogsRange(Partition, 0));
            Assert.Equal(5, boundary.Id);
            Assert.Equal(RaftLogType.CommittedCheckpoint, boundary.Type);
            Assert.Equal(5, wal.GetLastCheckpoint(Partition));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void DeletePartitionWAL_ResetsFloor_SoReusedIdsAreVisible()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 10))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(11)])]));
            wal.CompactLogsOlderThan(Partition, lastCheckpoint: 11, compactNumberEntries: 100);
            Assert.Equal(11, wal.GetCompactionFloorForTesting(Partition));

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(Partition));
            Assert.Equal(0, wal.GetCompactionFloorForTesting(Partition));
            Assert.Empty(wal.ReadLogsRange(Partition, 0));

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 3))]));
            Assert.Equal([1, 2, 3], wal.ReadLogsRange(Partition, 0).Select(l => l.Id));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public void FrontierCatchUp_CrossesDeadButPresentPrefix_AsCompacted()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            // Full Committed rows 1-5 (a backfilled node: no frontier), checkpoint at 6, compacted to 6.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 5))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(6)])]));
            wal.CompactLogsOlderThan(Partition, lastCheckpoint: 6, compactNumberEntries: 100);
            Assert.Equal(6, wal.GetCompactionFloorForTesting(Partition));

            // The marker for 7 walks the frontier from 1: ids 1-5 are dead-but-present and must be
            // crossed as a compacted span below the certified checkpoint, 6 is the checkpoint row,
            // and 7 absorbs — no full-row rewrite.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Proposed(7, 5)])]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [new RaftLog { Id = 7, Term = 5, Type = RaftLogType.Committed, LogType = "op" }])]));
            Assert.Equal(1, wal.CommitMarkersAbsorbed);

            RaftLog seven = Assert.Single(wal.ReadLogsRange(Partition, 7));
            Assert.Equal(RaftLogType.Committed, seven.Type);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    // ───────────────────────────── physical layout ─────────────────────────────

    /// <summary>
    /// The write-amplification mechanism, end to end on the real code path: flushed files of an
    /// append-only partition do not overlap, so RocksDB moves them out of L0 into the base level
    /// under the SAME file numbers (a metadata-only trivial move, no rewrite), and the floor-
    /// anchored whole-file drop then reclaims every file below the floor.
    /// </summary>
    [Fact]
    public void FlushedFiles_AreMovedNotRewritten_AndDroppedWholeBelowFloor()
    {
        string path = CreateTempWalPath();

        try
        {
            RocksDbWalTuning tuning = RocksDbWalTuning.Default with { ShardLevel0FileNumCompactionTrigger = 2 };
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, tuning: tuning);

            byte[] payload = IncompressiblePayload(4096);

            for (int chunk = 0; chunk < 4; chunk++)
            {
                long first = chunk * 300 + 1;
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(first, first + 299, payload))]));
                wal.FlushMemTablesForTesting(wait: true);
            }

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(1201)])]));

            // Files never overlap: sorted by first id, each file ends before the next begins.
            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = wal.GetShardLiveFilesForTesting(Partition);
            AssertNonOverlapping(files);

            // With the L0 trigger at 2, RocksDB moves files out of L0 in the background. The engine
            // log must show trivial moves for the shard and no compaction at all.
            WaitUntil(
                () => wal.GetShardLiveFilesForTesting(Partition),
                f => f.Any(x => x.Level > 0),
                TimeSpan.FromSeconds(20));

            (int moves, int compactions) = WaitUntil(
                () => CountShardMovesAndCompactions(path),
                counts => counts.Moves > 0,
                TimeSpan.FromSeconds(20));
            Assert.True(moves > 0, "expected a trivial move out of L0");
            Assert.Equal(0, compactions);

            // Compact to a floor inside the third chunk: every file entirely below it must be gone,
            // the straddling file and everything above must stay, and the rows must still read.
            // The reclaim is accounted file by file, not as a fraction of the total: the four
            // files are NOT equal in size (fixed-width decimal keys with more leading zeros
            // compress better, so the earliest chunk is the smallest file), and the exact sizes
            // shift with the RocksDB SST format between package versions. A ratio threshold
            // (the two dead files being at least half the bytes) sat ~500 bytes from the truth
            // and flipped on the 11.1 → 11.8 bump; the identity below holds on any build.
            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> beforeFiles = wal.GetShardLiveFilesForTesting(Partition);
            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> deadFiles = beforeFiles.Where(f => f.LargestId < 699).ToList();
            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> liveFiles = beforeFiles.Where(f => f.LargestId >= 699).ToList();
            Assert.Equal(2, deadFiles.Count);
            Assert.Equal(2, liveFiles.Count);
            long deadBytes = deadFiles.Sum(f => f.SizeBytes);
            Assert.True(deadBytes > 0, "the dead files report no size");

            long before = wal.GetShardLiveSstBytes();
            Assert.Equal(beforeFiles.Sum(f => f.SizeBytes), before);

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 700, compactNumberEntries: 100, maxTotalEntries: 1000);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(699, removed);

            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> after = WaitUntil(
                () => wal.GetShardLiveFilesForTesting(Partition),
                f => f.All(x => x.LargestId >= 699),
                TimeSpan.FromSeconds(20));

            // Every dead file is gone by name; every surviving file is still there under its own
            // name and size (dropped whole or untouched — nothing rewritten); and the live bytes
            // fell by exactly the dead files' bytes.
            Assert.DoesNotContain(after, f => f.LargestId < 699);
            foreach ((string name, _, _, _, _) in deadFiles)
                Assert.DoesNotContain(after, f => f.Name == name);
            foreach ((string name, _, _, _, long sizeBytes) in liveFiles)
                Assert.Contains(after, f => f.Name == name && f.SizeBytes == sizeBytes);

            long reclaimed = before - wal.GetShardLiveSstBytes();
            Assert.True(reclaimed == deadBytes,
                $"whole-file drop did not reclaim exactly the dead prefix: {before} -> {wal.GetShardLiveSstBytes()} bytes ({reclaimed} reclaimed, {deadBytes} dead)");

            List<RaftLog> visible = wal.ReadLogsRange(Partition, 0);
            Assert.Equal(700, visible[0].Id);
            Assert.Equal(1201, visible[^1].Id);
            Assert.Equal(502, visible.Count);
            Assert.All(visible.Where(l => l.Id <= 1200), l => Assert.Equal(payload, l.LogData));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The confined tombstone: rows that die while still in the active memtable are dropped at flush
    /// (they never reach disk), and because the tombstone starts at or above the memtable's own
    /// first row the flushed file's key range is not widened below it — successive files stay
    /// non-overlapping, which is what keeps the trivial move available.
    /// </summary>
    [Fact]
    public void ConfinedTombstone_DropsMemtableRowsAtFlush_AndKeepsFilesNonOverlapping()
    {
        string path = CreateTempWalPath();

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            byte[] payload = IncompressiblePayload(4096);

            // 2000 rows (~8 MB) in the memtable, floor moved to 1500 before anything is flushed:
            // the pass tombstones [~65, 1500) inside the memtable and the flush drops those rows.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 2000, payload))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(2001)])]));
            (_, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 1500, compactNumberEntries: 100, maxTotalEntries: 2000);
            Assert.Equal(1499, removed);
            Assert.Equal(1, wal.LastCompactionWriteCount);

            wal.FlushMemTablesForTesting(wait: true);

            long flushed = wal.GetShardLiveSstBytes();
            Assert.True(flushed < 3_500_000,
                $"the confined tombstone should have dropped ~1400 of 2000 rows at flush; {flushed} bytes reached disk");

            // Second generation: 2001-4000 in a fresh memtable, floor to 3500, flush. The tombstone
            // must not reach below 2001, so the two files do not overlap.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(2002, 4000, payload))]));
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(4001)])]));
            (_, removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 3500, compactNumberEntries: 100, maxTotalEntries: 5000);
            Assert.Equal(2000, removed);

            wal.FlushMemTablesForTesting(wait: true);

            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = wal.GetShardLiveFilesForTesting(Partition);
            Assert.True(files.Count >= 2, $"expected two flushed files, saw {files.Count}");
            AssertNonOverlapping(files);
            Assert.Contains(files, f => f.SmallestId >= 2001);

            Assert.Equal(3500, wal.ReadLogsRange(Partition, 0)[0].Id);
            Assert.Equal(4001, wal.GetMaxLog(Partition));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Two partitions congruent modulo the shard count share one column family. Whole-file drops
    /// cannot serve one of them (the files hold the other's live rows), so the pass must fall back
    /// to the full-range tombstone and the dead rows must physically disappear under compaction.
    /// </summary>
    [Fact]
    public void SharedShard_FallsBackToFullTombstone_AndCompactionRemovesDeadRows()
    {
        string path = CreateTempWalPath();
        const int other = Partition + 8;

        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            byte[] payload = IncompressiblePayload(2048);
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(1, 500, payload)), (other, Committed(1, 500, payload))]));
            wal.FlushMemTablesForTesting(wait: true);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Checkpoint(501)]), (other, [Checkpoint(501)])]));

            (_, int removed) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: 401, compactNumberEntries: 100, maxTotalEntries: 1000);
            Assert.Equal(400, removed);
            Assert.Equal(1, wal.LastCompactionWriteCount);

            wal.FlushMemTablesForTesting(wait: true);
            wal.CompactShardToBottomForTesting(Partition);

            // The full-range tombstone met the rows in compaction: the surviving file of the shard
            // begins at partition 1's first live row, i.e. the dead prefix is physically gone.
            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = wal.GetShardLiveFilesForTesting(Partition);
            Assert.NotEmpty(files);
            Assert.All(files.Where(f => f.SmallestId >= 0), f => Assert.True(f.SmallestId >= 401, $"dead row {f.SmallestId} survived compaction"));

            Assert.Equal(401, wal.ReadLogsRange(Partition, 0)[0].Id);
            Assert.Equal(501, wal.ReadLogsRange(other, 0).Count);
            Assert.Equal(0, wal.GetCompactionFloorForTesting(other));
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// A sustained append-then-compact run through the real backend with small memtables (the
    /// shape a starved shared WriteBufferManager imposes): every file that leaves L0 must do so
    /// under its flushed name (moved, never rewritten), and the live footprint must track the
    /// retained window rather than the bytes ingested.
    /// </summary>
    [Fact]
    public void SustainedIngest_NeverRewritesLogFiles_AndFootprintTracksRetainedWindow()
    {
        string path = CreateTempWalPath();

        try
        {
            RocksDbWalTuning tuning = RocksDbWalTuning.Default with
            {
                ShardWriteBufferSizeBytes = 2L * 1024 * 1024,
                ShardMinWriteBufferNumberToMerge = 1,
                ShardMaxWriteBufferNumber = 3,
                ShardLevel0FileNumCompactionTrigger = 4,
            };
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, tuning: tuning);

            byte[] payload = IncompressiblePayload(512);
            const long total = 60_000;
            const long lag = 3_000;
            long ingested = 0;

            for (long id = 1; id <= total; id += 100)
            {
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(id, id + 99, payload))]));
                ingested += 100 * payload.Length;

                if (id % 5_000 == 1 && id > lag)
                {
                    long floor = id + 99 - lag;
                    (RaftOperationStatus status, _) = wal.CompactLogsOlderThan(Partition, lastCheckpoint: floor, compactNumberEntries: 100, maxTotalEntries: 10_000);
                    Assert.Equal(RaftOperationStatus.Success, status);
                }
            }

            wal.FlushMemTablesForTesting(wait: true);
            List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files = WaitUntil(
                () => wal.GetShardLiveFilesForTesting(Partition),
                f => f.Count(x => x.Level == 0) < tuning.ShardLevel0FileNumCompactionTrigger,
                TimeSpan.FromSeconds(20));

            AssertNonOverlapping(files);

            // The engine's own log is the authority: files left L0 by trivial move only, and no
            // compaction ever ran over the shard — every flushed byte was either dropped whole or is
            // still live.
            (int moves, int compactions) = WaitUntil(
                () => CountShardMovesAndCompactions(path),
                counts => counts.Moves > 0,
                TimeSpan.FromSeconds(20));
            Assert.True(moves > 0, "expected at least one trivial move out of L0");
            Assert.Equal(0, compactions);

            long live = wal.GetShardLiveSstBytes();
            Assert.True(live < ingested / 2,
                $"live SST bytes {live} should track the retained window, not the {ingested} bytes ingested");
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The metadata column family (commit frontier and compaction floor puts) never fills its
    /// memtable, and a RocksDB write-ahead log stays alive until every CF with data in it has
    /// flushed — so without a cadence of its own it pins every log since its last flush. The WAL
    /// flushes it once a shard flush unit of rows has been written; the alive logs must therefore
    /// stay within a few flush units however much is ingested.
    /// </summary>
    [Fact]
    public void MetadataFlushCadence_ReleasesWriteAheadLogs()
    {
        string path = CreateTempWalPath();

        try
        {
            RocksDbWalTuning tuning = RocksDbWalTuning.Default with
            {
                ShardWriteBufferSizeBytes = 1L * 1024 * 1024,
                ShardMinWriteBufferNumberToMerge = 1,
                ShardMaxWriteBufferNumber = 3,
            };
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, tuning: tuning);

            byte[] payload = IncompressiblePayload(1024);
            const long total = 40_000; // ~40 MB of rows = 40 flush units of 1 MB

            for (long id = 1; id <= total; id += 100)
            {
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Committed(id, id + 99, payload))]));
                // A frontier-style metadata put per batch, as the commit path issues.
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [new RaftLog { Id = id + 99, Term = 5, Type = RaftLogType.Committed, LogType = "op" }])]));
            }

            string engineDir = Path.Combine(path, "wal");
            long logBytes = WaitUntil(
                () => Directory.GetFiles(engineDir, "*.log").Sum(f => new FileInfo(f).Length),
                bytes => bytes < 6L * 1024 * 1024,
                TimeSpan.FromSeconds(20));

            Assert.True(logBytes < 6L * 1024 * 1024,
                $"write-ahead logs are pinned: {logBytes >> 20} MB alive for ~40 MB ingested at a 1 MB flush unit");

            // Everything written is still readable — the flushes released logs, not data.
            Assert.Equal(total, wal.GetMaxLog(Partition));
            Assert.Equal(RaftLogType.Committed, wal.ReadLogsRange(Partition, total, 1)[0].Type);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    // ───────────────────────────── helpers ─────────────────────────────

    private static void AssertNonOverlapping(List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> files)
    {
        List<(string Name, int Level, long SmallestId, long LargestId, long SizeBytes)> ordered = files
            .Where(f => f.SmallestId >= 0 && f.LargestId >= 0)
            .OrderBy(f => f.SmallestId)
            .ToList();

        for (int i = 1; i < ordered.Count; i++)
            Assert.True(ordered[i - 1].LargestId < ordered[i].SmallestId,
                $"files overlap: {ordered[i - 1].Name} [{ordered[i - 1].SmallestId}, {ordered[i - 1].LargestId}] and {ordered[i].Name} [{ordered[i].SmallestId}, {ordered[i].LargestId}]");
    }

    /// <summary>
    /// Reads the engine's RocksDB LOG: trivial moves are logged as "[shardN] Moved #file to level-L",
    /// real compactions as a compaction_finished event tagged with the CF name.
    /// </summary>
    private static (int Moves, int Compactions) CountShardMovesAndCompactions(string walPath)
    {
        string shard = "shard" + (Partition % 8);
        int moves = 0, compactions = 0;

        string logPath = Path.Combine(walPath, "wal", "LOG");
        if (!File.Exists(logPath))
            return (0, 0);

        using FileStream stream = new(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using StreamReader reader = new(stream);
        while (reader.ReadLine() is { } line)
        {
            if (line.Contains($"[{shard}] Moved #", StringComparison.Ordinal))
                moves++;
            else if (line.Contains("\"event\": \"compaction_finished\"", StringComparison.Ordinal) && line.Contains($"\"cf_name\": \"{shard}\"", StringComparison.Ordinal))
                compactions++;
        }

        return (moves, compactions);
    }

    private static T WaitUntil<T>(Func<T> read, Func<T, bool> ready, TimeSpan timeout)
    {
        Stopwatch clock = Stopwatch.StartNew();
        T value = read();
        while (!ready(value))
        {
            Assert.True(clock.Elapsed < timeout, $"condition not met within {timeout}");
            Thread.Sleep(50);
            value = read();
        }

        return value;
    }

    private static byte[] IncompressiblePayload(int size)
    {
        byte[] payload = new byte[size];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)((i * 2654435761L) >> 13);
        return payload;
    }

    private static List<RaftLog> Committed(long firstId, long lastId, byte[]? payload = null)
    {
        List<RaftLog> logs = new((int)(lastId - firstId + 1));
        for (long id = firstId; id <= lastId; id++)
            logs.Add(new RaftLog { Id = id, Term = 5, Type = RaftLogType.Committed, LogType = "op", LogData = payload ?? [1, 2, 3] });
        return logs;
    }

    private static RaftLog Proposed(long id, long term) =>
        new() { Id = id, Term = term, Type = RaftLogType.Proposed, LogType = "op", LogData = [1, 2, 3] };

    private static RaftLog Checkpoint(long id) =>
        new() { Id = id, Term = 5, Type = RaftLogType.CommittedCheckpoint, LogType = "chk" };

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-rocksdb-floor-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
