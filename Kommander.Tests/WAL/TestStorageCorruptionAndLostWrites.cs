using System.Text;
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using RocksDbSharp;

namespace Kommander.Tests.WAL;

/// <summary>
/// The storage-level half of the storage-corruption and lost-writes work, pinned against the real
/// RocksDB engine:
/// <list type="bullet">
///   <item>the Raft hard state (term, vote) is written with the engine's sync options, so a vote is
///   fsynced before its reply leaves;</item>
///   <item>the acknowledged-presence watermark lands with the batch that carries it, is clamped by
///   every path that removes rows, follows a snapshot boundary, and leaves with the partition;</item>
///   <item>a <c>Corruption</c> status fail-stops the engine: writes are refused without a reopen,
///   reads of intact data still serve, and a damaged SST refuses to open at all.</item>
/// </list>
/// The corruption cases materialise the damage on disk (a flipped byte inside an SST's first data
/// block, a truncated SST) exactly as a fault-injection campaign would, then observe the engine.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestStorageCorruptionAndLostWrites
{
    private const int Partition = 1;

    // ── Hard state ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// With <c>syncWrites</c> on, every hard-state write is one synchronous metadata put — the fsync
    /// that keeps a granted vote from being lost between the reply and a power failure. With it off
    /// the node already accepts loss of unflushed writes, and the hard state follows the log.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void HardState_IsWrittenWithTheEngineSyncOptions(bool syncWrites)
    {
        string path = CreateTempPath();
        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: syncWrites);
            IWAL hardState = wal;

            long before = wal.SyncMetadataWrites;

            Assert.True(hardState.PersistHardState(Partition, currentTerm: 3, votedFor: "node-b"));
            Assert.True(hardState.PersistHardState(Partition, currentTerm: 4, votedFor: null));

            Assert.Equal(syncWrites ? before + 2 : before, wal.SyncMetadataWrites);

            Assert.True(hardState.TryGetHardState(Partition, out long term, out string? votedFor));
            Assert.Equal(4, term);
            Assert.Null(votedFor);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    // ── Acknowledged presence ────────────────────────────────────────────────────────────

    [Fact]
    public void AcknowledgedPresence_LandsWithTheBatch_SurvivesReopen_AndIsClampedByEveryRowRemoval()
    {
        string path = CreateTempPath();
        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance))
            {
                IWAL asInterface = wal;
                Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition));

                // Lands atomically with the rows it describes.
                Assert.Equal(RaftOperationStatus.Success,
                    asInterface.Write([(Partition, Rows(1, 10, RaftLogType.Committed))], sync: true, new Dictionary<int, long> { [Partition] = 10 }));
                Assert.Equal(10, wal.GetAcknowledgedPresence(Partition));

                // A batch without a claim leaves it alone; a batch with no rows for the partition
                // (the regression correction) assigns it, lower included.
                Assert.Equal(RaftOperationStatus.Success, asInterface.Write([(Partition, Rows(11, 11, RaftLogType.Committed))], sync: true));
                Assert.Equal(10, wal.GetAcknowledgedPresence(Partition));
                Assert.Equal(RaftOperationStatus.Success, asInterface.Write([], sync: true, new Dictionary<int, long> { [Partition] = 9 }));
                Assert.Equal(9, wal.GetAcknowledgedPresence(Partition));
                Assert.Equal(RaftOperationStatus.Success, asInterface.Write([], sync: true, new Dictionary<int, long> { [Partition] = 11 }));
                Assert.Equal(11, wal.GetAcknowledgedPresence(Partition));
            }

            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance))
            {
                IWAL asInterface = wal;

                // Survives a reopen.
                Assert.Equal(11, wal.GetAcknowledgedPresence(Partition));

                // A truncation clamps to the cut, in the same batch as the deletes.
                Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 5));
                Assert.Equal(5, wal.GetAcknowledgedPresence(Partition));
                Assert.Equal(5, wal.GetMaxLog(Partition));

                // A proposed-only sweep that removes a row inside the claimed prefix clamps too.
                Assert.Equal(RaftOperationStatus.Success,
                    asInterface.Write([(Partition, Rows(6, 9, RaftLogType.Proposed))], sync: true, new Dictionary<int, long> { [Partition] = 9 }));
                Assert.Equal(9, wal.GetAcknowledgedPresence(Partition));
                Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(Partition, 7));
                Assert.Equal(7, wal.GetAcknowledgedPresence(Partition));

                // A snapshot boundary installed above the log raises the watermark to the boundary
                // (the certified checkpoint attests the prefix) ...
                (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(Partition, snapshotIndex: 12, lastIncludedTerm: 3, sync: true);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.False(truncated);
                Assert.Equal(12, wal.GetAcknowledgedPresence(Partition));

                // ... and a truncating install (term mismatch at the boundary) clamps to it.
                (status, truncated) = wal.InstallSnapshotBoundary(Partition, snapshotIndex: 4, lastIncludedTerm: 9, sync: true);
                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.True(truncated);
                Assert.Equal(4, wal.GetAcknowledgedPresence(Partition));

                // The out-of-batch assignment (the accepted-loss rewrite) and removal.
                Assert.True(wal.SetAcknowledgedPresence(Partition, 2));
                Assert.Equal(2, wal.GetAcknowledgedPresence(Partition));
                Assert.True(wal.SetAcknowledgedPresence(Partition, -1));
                Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition));

                Assert.True(wal.SetAcknowledgedPresence(Partition, 3));
                Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(Partition));
                Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition));

                // Another partition is untouched by all of the above.
                Assert.Equal(-1, wal.GetAcknowledgedPresence(Partition + 1));
            }
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    // ── Corruption fail-stop ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A read that reaches a damaged block throws the engine's own <c>Corruption</c> and fail-stops
    /// the engine: from then on every write is refused as <see cref="RaftOperationStatus.Errored"/>
    /// without a reopen attempt, hard state cannot be persisted, and reads of intact data (another
    /// column family) still serve. Before this the ENOSPC recovery reopened on any engine error and
    /// looped on the damage.
    /// </summary>
    [Fact]
    public void Corruption_OnRead_FailStopsWrites_NeverReopens_IntactReadsStillServe()
    {
        string path = CreateTempPath();
        try
        {
            WriteFlushedPartition(path);
            CorruptFirstShardSst(path, flipByteAt: 8);

            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            IWAL asInterface = wal;

            RocksDbException read = Assert.Throws<RocksDbException>(() => wal.ReadLogs(Partition));
            Assert.StartsWith("Corruption:", read.Message, StringComparison.Ordinal);

            Assert.NotNull(wal.FailStopReason);
            Assert.StartsWith("Corruption:", asInterface.GetFailStopReason(), StringComparison.Ordinal);

            for (int attempt = 0; attempt < 3; attempt++)
                Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, Rows(100, 100, RaftLogType.Proposed))]));

            Assert.False(asInterface.PersistHardState(Partition, currentTerm: 9, votedFor: "node-c"));
            Assert.Equal(RaftOperationStatus.Errored, wal.TruncateLogsAfter(Partition, 1));
            Assert.Equal(0, wal.EngineReopenCount);

            // The metadata column family is intact and still readable.
            Assert.Equal("1", wal.GetMetaData("intact-marker"));
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// The write path hits the damage itself: a commit marker over a damaged row probes the row's
    /// header, the probe throws <c>Corruption</c>, and the write is reported as Errored with the engine
    /// fail-stopped — no filesystem probe, no reopen.
    /// </summary>
    [Fact]
    public void Corruption_OnWritePathProbe_FailStopsWithoutReopen()
    {
        string path = CreateTempPath();
        try
        {
            WriteFlushedPartition(path);
            CorruptFirstShardSst(path, flipByteAt: 8);

            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            // A marker for a present Proposed row: the frontier advance reads the row header.
            RaftOperationStatus status = wal.Write([(Partition, Rows(5, 5, RaftLogType.Committed))]);

            Assert.Equal(RaftOperationStatus.Errored, status);
            Assert.NotNull(wal.FailStopReason);
            Assert.StartsWith("Corruption:", wal.FailStopReason, StringComparison.Ordinal);
            Assert.Equal(0, wal.EngineReopenCount);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// An SST shorter than the MANIFEST recorded it (a truncation) fails the open with a Corruption
    /// status: the node refuses to start on partial data instead of opening short.
    /// </summary>
    [Fact]
    public void Corruption_TruncatedSst_RefusesToOpen()
    {
        string path = CreateTempPath();
        try
        {
            WriteFlushedPartition(path);
            string sst = FirstShardSst(path);
            using (FileStream stream = new(sst, FileMode.Open, FileAccess.ReadWrite))
                stream.SetLength(stream.Length - 16);

            RocksDbException open = Assert.Throws<RocksDbException>(() => new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance));
            Assert.StartsWith("Corruption:", open.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    /// <summary>Writes 1..40 for the partition, flushes them to SSTs, and closes the engine cleanly.</summary>
    private static void WriteFlushedPartition(string path)
    {
        using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1, 40, RaftLogType.Proposed))]));
        Assert.True(wal.SetMetaData("intact-marker", "1"));
        wal.FlushMemTablesForTesting(wait: true);
        Assert.NotEmpty(wal.GetShardLiveFilesForTesting(Partition));
    }

    private static string FirstShardSst(string path)
    {
        using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
        (string name, _, _, _, _) = wal.GetShardLiveFilesForTesting(Partition)[0];
        string file = Path.Combine(path, "wal", name.TrimStart('/'));
        Assert.True(File.Exists(file), $"SST not found: {file}");
        return file;
    }

    /// <summary>
    /// Flips one byte inside the first data block of the partition's SST. Data blocks come first in
    /// the block-based table format, and every block carries a checksum, so the next read of that
    /// block reports a checksum mismatch — the open itself only verifies the footer and meta blocks.
    /// </summary>
    private static void CorruptFirstShardSst(string path, int flipByteAt)
    {
        string sst = FirstShardSst(path);
        using FileStream stream = new(sst, FileMode.Open, FileAccess.ReadWrite);
        stream.Position = flipByteAt;
        int original = stream.ReadByte();
        stream.Position = flipByteAt;
        stream.WriteByte((byte)(original ^ 0xFF));
    }

    private static List<RaftLog> Rows(long from, long to, RaftLogType type)
    {
        List<RaftLog> rows = [];
        for (long id = from; id <= to; id++)
            rows.Add(new RaftLog
            {
                Id = id,
                Term = 1,
                Type = type,
                LogType = "storage-test",
                LogData = Encoding.UTF8.GetBytes($"payload-{id}"),
            });
        return rows;
    }

    private static string CreateTempPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-storage-corruption-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempPath(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort */ }
    }
}
