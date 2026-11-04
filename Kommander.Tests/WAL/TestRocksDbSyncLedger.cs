using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The sync ledger (<see cref="RocksDbSyncLedger"/>) and the hard-state ledger
/// (<see cref="RocksDbHardStateLedger"/>) against the real engine and real file damage. Every scenario
/// the storage-corruption review ran at the WAL level is here: the live log truncated to half, cut by
/// one record, truncated after a metadata flush moved the watermark into a closed log, deleted
/// outright; the hard state cut off the tail. Each one reopened clean before the ledgers, because the
/// watermark and the hard state were cut together with the rows they described. Each one now opens
/// with a persisted lost-synced-writes verdict that every partition carries until it is reconciled
/// (<see cref="IWAL.GetLostSyncedWritesReason"/>), the engine keeps accepting writes so a leader can
/// repair it, and a forgotten vote is written back from the hard-state ledger.
///
/// <para>The counterweights matter as much: a torn <em>unsynced</em> record (a real crash), a clean
/// reopen, a ledger copied from another engine, a torn or deleted ledger, a directory from before the
/// ledger existed — none of them may report a loss, because a false quarantine takes a healthy voter
/// out of elections.</para>
///
/// <para>The damage is staged after a clean dispose: with the write-ahead log enabled, RocksDB does
/// not flush memtables on close, so the rows stay in the live log and a truncation removes them
/// exactly as it would after a kill. Each damage test asserts that rows really went missing, so it
/// cannot pass vacuously.</para>
/// </summary>
public sealed class TestRocksDbSyncLedger
{
    private const int Partition = 1;

    private const int Total = 40;

    // ── Damage the ledger must detect ───────────────────────────────────────────────────────

    [Fact]
    public void LiveLogTruncatedToHalf_RecordsTheVerdict_ForEveryPartition_UntilEachReconciles()
    {
        string path = CreateTempPath();
        try
        {
            // Attested at dispose, from the same life: a clean reopen would flush the rows into SSTs
            // and start an empty live log, and the truncation below would then remove nothing.
            long attestedBefore = WriteSyncedRows(path);

            TruncateNewestLog(path, length => length / 2);

            long surviving;
            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                IWAL asInterface = reopened;

                surviving = reopened.GetMaxLog(Partition);
                Assert.True(surviving < Total, "the truncation removed no row; the fault was not reproduced");

                // The watermark alone would have said nothing: it went with the rows.
                Assert.Equal(surviving, reopened.GetAcknowledgedPresence(Partition));

                // The verdict names the sequences and applies to every partition on the engine.
                string reason = Assert.IsType<string>(asInterface.GetLostSyncedWritesReason(Partition));
                Assert.Contains("lost synced writes", reason);
                Assert.Contains($"attests sequence {attestedBefore}", reason);
                Assert.Equal(reason, asInterface.GetLostSyncedWritesReason(Partition + 1));
                Assert.Equal(1, reopened.LostSyncedWritesVerdict);

                // Not a fail-stop: the engine is consistent, only stale, so it keeps serving reads and
                // accepting the writes a leader's repair needs, and the ledger follows the engine again.
                Assert.Null(reopened.FailStopReason);
                Assert.Equal(Enumerable.Range(1, (int)surviving).Select(i => (long)i), reopened.ReadLogs(Partition).Select(l => l.Id));
                Assert.Equal(RaftOperationStatus.Success, reopened.Write([(Partition, Rows(surviving + 1, surviving + 1))]));
                Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);

                // One partition reconciles; the other still carries the verdict.
                Assert.True(asInterface.MarkLostSyncedWritesReconciled(Partition));
                Assert.Null(asInterface.GetLostSyncedWritesReason(Partition));
                Assert.NotNull(asInterface.GetLostSyncedWritesReason(Partition + 1));
            }

            // The verdict and the reconciliation are persisted: a restart before the other partition
            // reconciled quarantines it again, and only it.
            using (RocksDbWAL again = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                IWAL asInterface = again;
                Assert.Equal(1, again.LostSyncedWritesVerdict);
                Assert.Null(asInterface.GetLostSyncedWritesReason(Partition));
                Assert.NotNull(asInterface.GetLostSyncedWritesReason(Partition + 1));
                Assert.True(asInterface.MarkLostSyncedWritesReconciled(Partition + 1));
                WriteSyncedRows(again, surviving + 2, surviving + 20);
            }

            // Damage again after a full reconciliation: a new verdict, and the old reconciliations do
            // not cover it.
            TruncateNewestLog(path, length => length / 2);
            using (RocksDbWAL damagedAgain = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                IWAL asInterface = damagedAgain;
                Assert.Equal(2, damagedAgain.LostSyncedWritesVerdict);
                Assert.NotNull(asInterface.GetLostSyncedWritesReason(Partition));
                Assert.NotNull(asInterface.GetLostSyncedWritesReason(Partition + 1));
            }
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>One record cut off the tail: the smallest truncation, one acknowledged entry lost.</summary>
    [Fact]
    public void LiveLogCutByOneRecord_RecordsTheVerdict()
    {
        string path = CreateTempPath();
        try
        {
            WriteSyncedRows(path);
            TruncateNewestLog(path, length => length - 16);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            Assert.Equal(Total - 1, reopened.GetMaxLog(Partition));
            Assert.Contains("lost synced writes", ((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.Null(reopened.FailStopReason);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// The production flush cadence closes the log for the metadata CF midway (the watermark through 20
    /// moves into a closed, MANIFEST-tracked log) and the rows after it land in a new live log. The
    /// closed log is protected by the engine; the new live log is not, and only the ledger covers it.
    /// </summary>
    [Fact]
    public void MetadataFlushedMidway_ThenNewLiveLogTruncated_RecordsTheVerdict()
    {
        string path = CreateTempPath();
        try
        {
            WriteSyncedRows(path, flushMetadataAfter: 20);
            TruncateNewestLog(path, length => length / 2);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            long surviving = reopened.GetMaxLog(Partition);
            Assert.InRange(surviving, 20, Total - 1);
            Assert.Contains("lost synced writes", ((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// The live log deleted after a full flush: the engine does not track the live log, so it opens on
    /// the SSTs alone and the rows after the flush are simply absent. The ledger attests them.
    /// </summary>
    [Fact]
    public void LiveLogDeletedAfterFlush_RecordsTheVerdict()
    {
        string path = CreateTempPath();
        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                WriteSyncedRows(wal, 1, 20);
                wal.FlushMemTablesForTesting(wait: true);
                WriteSyncedRows(wal, 21, Total);
            }

            File.Delete(NewestLog(path).FullName);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            Assert.Equal(20, reopened.GetMaxLog(Partition));
            Assert.Contains("lost synced writes", ((IWAL)reopened).GetLostSyncedWritesReason(Partition));
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// The live log deleted before any flush: the engine replays nothing, not even the format version.
    /// Before the ledger this threw "format version '&lt;missing&gt;' is not compatible", which does not
    /// name the damage; the ledger proves the directory was initialised, the verdict is recorded, and
    /// the directory is re-initialised so the node can be repaired from its peers.
    /// </summary>
    [Fact]
    public void LiveLogDeletedBeforeAnyFlush_RecordsTheVerdict_InsteadOfReportingAnIncompatibleFormat()
    {
        string path = CreateTempPath();
        try
        {
            WriteSyncedRows(path);
            File.Delete(NewestLog(path).FullName);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            Assert.Equal(0, reopened.GetMaxLog(Partition));
            string reason = Assert.IsType<string>(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.Contains("lost synced writes", reason);
            Assert.Contains("format version", reason);
            Assert.Equal("2.0.0", reopened.GetMetaData("version"));
            Assert.True(Guid.TryParseExact(reopened.GetMetaData("raft_engine_identity"), "N", out _));
            Assert.Null(reopened.FailStopReason);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// The hard state is a synced metadata put in the same log. A tail cut that removes it leaves the
    /// engine with an older term and no vote — the second vote in one term from the cluster run. The
    /// hard-state ledger holds the put, and the open writes it back before the Raft layer reads it.
    /// </summary>
    [Fact]
    public void HardStateCutOffTheTail_IsRestoredFromTheHardStateLedger()
    {
        string path = CreateTempPath();
        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                WriteSyncedRows(wal, 1, 10);
                Assert.True(((IWAL)wal).PersistHardState(Partition, currentTerm: 7, votedFor: "node-b"));
            }

            TruncateNewestLog(path, length => length - 16);

            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                IWAL asInterface = reopened;

                Assert.True(asInterface.TryGetHardState(Partition, out long term, out string? votedFor));
                Assert.Equal(7, term);
                Assert.Equal("node-b", votedFor);
                Assert.Equal(10, reopened.GetMaxLog(Partition));
                Assert.Contains("lost synced writes", asInterface.GetLostSyncedWritesReason(Partition));

                // Later hard state keeps flowing through both the engine and the ledger.
                Assert.True(asInterface.PersistHardState(Partition, currentTerm: 8, votedFor: null));
            }

            using RocksDbWAL again = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            Assert.True(((IWAL)again).TryGetHardState(Partition, out long laterTerm, out string? laterVote));
            Assert.Equal(8, laterTerm);
            Assert.Null(laterVote);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    // ── Shapes that must not be reported ────────────────────────────────────────────────────

    /// <summary>
    /// A record appended without a sync and then cut: the residue of a real kill. The ledger never
    /// attested it, so the reopen is healthy and the next synced write attests exactly what landed.
    /// </summary>
    [Fact]
    public void TornUnsyncedTailRecord_ReopensHealthy()
    {
        string path = CreateTempPath();
        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                WriteSyncedRows(wal, 1, 10);
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(11, 11))], sync: false));
            }

            TruncateNewestLog(path, length => length - 4);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            Assert.Null(reopened.FailStopReason);
            Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.Equal(10, reopened.GetMaxLog(Partition));
            Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);

            Assert.Equal(RaftOperationStatus.Success, reopened.Write([(Partition, Rows(11, 11))]));
            Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    [Fact]
    public void CleanReopen_IsHealthy_AndTheLedgerFollowsTheEngine()
    {
        string path = CreateTempPath();
        try
        {
            WriteSyncedRows(path);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            Assert.Null(reopened.FailStopReason);
            Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.Equal(-1, reopened.LostSyncedWritesVerdict);
            Assert.Equal(Total, reopened.GetMaxLog(Partition));
            Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
            Assert.True(File.Exists(reopened.SyncLedgerPath));
            Assert.True(File.Exists(reopened.HardStateLedgerPath));
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// Every synchronous write is attested before it returns, including the single-row fast path, the
    /// absorbed commit marker, the hard state, the watermark rewrite and a truncation; an unsynced
    /// write is not, and the next synced one covers it.
    /// </summary>
    [Fact]
    public void EverySynchronousWrite_IsAttestedBeforeItReturns()
    {
        string path = CreateTempPath();
        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            IWAL asInterface = wal;

            void AssertAttested() => Assert.Equal(wal.EngineLatestSequence, wal.SyncLedgerAttestedSequence);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1, 1, RaftLogType.Proposed))])); // single-row fast path
            AssertAttested();
            Assert.Equal(RaftOperationStatus.Success, asInterface.Write([(Partition, Rows(2, 5, RaftLogType.Proposed))], sync: true, new Dictionary<int, long> { [Partition] = 5 }));
            AssertAttested();
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1, 1, RaftLogType.Committed))])); // absorbed marker
            AssertAttested();
            Assert.True(asInterface.PersistHardState(Partition, 3, "node-b"));
            AssertAttested();
            Assert.True(wal.SetAcknowledgedPresence(Partition, 4));
            AssertAttested();
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(Partition, 3));
            AssertAttested();

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(4, 4, RaftLogType.Proposed))], sync: false));
            Assert.True(wal.SyncLedgerAttestedSequence < wal.EngineLatestSequence);

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(5, 5, RaftLogType.Proposed))], sync: true));
            AssertAttested();
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// Concurrent synced and unsynced writers: the ledger never attests past what the engine fsynced
    /// (an unsynced batch landing right after a synced one must not be counted), and a final synced
    /// write brings it level.
    /// </summary>
    [Fact]
    public async Task ConcurrentWriters_NeverAttestPastTheEngine_AndGroupCommit()
    {
        string path = CreateTempPath();
        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            await Parallel.ForAsync(0, 200, async (i, _) =>
            {
                await Task.Yield();
                int partition = 1 + i % 4;
                long id = 1 + i / 4;
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(partition, Rows(id, id, RaftLogType.Proposed, partition))], sync: i % 3 != 0));
                Assert.True(wal.SyncLedgerAttestedSequence <= wal.EngineLatestSequence);
            });

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, Rows(1000, 1000, RaftLogType.Proposed))], sync: true));
            Assert.Equal(wal.EngineLatestSequence, wal.SyncLedgerAttestedSequence);
            Assert.Null(wal.FailStopReason);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    [Fact]
    public void LedgerFromAnotherEngine_IsResetNotBelieved()
    {
        string a = CreateTempPath();
        string b = CreateTempPath();
        try
        {
            WriteSyncedRows(a);
            using (RocksDbWAL small = new(b, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
                WriteSyncedRows(small, 1, 5);

            File.Copy(Path.Combine(a, "wal", RocksDbSyncLedger.FileName), Path.Combine(b, "wal", RocksDbSyncLedger.FileName), overwrite: true);

            using (RocksDbWAL reopened = new(b, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                Assert.Null(reopened.FailStopReason);
                Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
                Assert.Equal(5, reopened.GetMaxLog(Partition));
                Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
            }

            // The reset outranks the foreign record: the next open adopts it instead of the copy.
            using (RocksDbWAL again = new(b, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
                Assert.Null(((IWAL)again).GetLostSyncedWritesReason(Partition));
        }
        finally
        {
            DeleteTempPath(a);
            DeleteTempPath(b);
        }
    }

    /// <summary>A torn slot is skipped; the other slot's older attestation is never above the engine.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void TornLedgerSlot_FallsBackToTheOtherSlot(int slot)
    {
        string path = CreateTempPath();
        try
        {
            WriteSyncedRows(path);
            FlipLedgerByte(path, offset: slot * 512 + 20);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.Equal(Total, reopened.GetMaxLog(Partition));
            Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// Both slots unreadable is no shape a crash leaves; it is treated as no ledger (with a warning)
    /// rather than as a loss, and a fresh ledger is written.
    /// </summary>
    [Fact]
    public void UnreadableLedger_IsReinitialised_NotReportedAsALoss()
    {
        string path = CreateTempPath();
        try
        {
            WriteSyncedRows(path);
            FlipLedgerByte(path, offset: 20);
            FlipLedgerByte(path, offset: 512 + 20);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);

            Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.Equal(reopened.EngineLatestSequence, reopened.SyncLedgerAttestedSequence);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// A directory from before the ledger existed (no ledger file, no engine identity): the open
    /// writes the identity, creates the ledger, and is healthy. A deleted ledger is the same case.
    /// </summary>
    [Fact]
    public void DirectoryWithoutLedgerOrIdentity_IsUpgraded_AndHealthy()
    {
        string path = CreateTempPath();
        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                WriteSyncedRows(wal, 1, 10);
                Assert.True(wal.SetMetaData("raft_engine_identity", "not-a-guid")); // what a pre-ledger directory has: nothing usable
            }

            File.Delete(Path.Combine(path, "wal", RocksDbSyncLedger.FileName));

            using (RocksDbWAL upgraded = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                Assert.Null(((IWAL)upgraded).GetLostSyncedWritesReason(Partition));
                Assert.True(Guid.TryParseExact(upgraded.GetMetaData("raft_engine_identity"), "N", out _));
                Assert.True(File.Exists(upgraded.SyncLedgerPath));
                Assert.Equal(upgraded.EngineLatestSequence, upgraded.SyncLedgerAttestedSequence);
                WriteSyncedRows(upgraded, 11, 20);
            }

            // From this open on the directory is protected.
            TruncateNewestLog(path, length => length / 2);
            using RocksDbWAL damaged = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            Assert.Contains("lost synced writes", ((IWAL)damaged).GetLostSyncedWritesReason(Partition));
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// With <c>syncWrites</c> off nothing is promised durable, so nothing is attested after the open:
    /// a crash that loses the unsynced tail must not read as damage on the next open.
    /// </summary>
    [Fact]
    public void SyncWritesOff_AttestsNothing_SoALostUnsyncedTailIsNotDamage()
    {
        string path = CreateTempPath();
        try
        {
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false))
            {
                WriteSyncedRows(wal, 1, Total);
                Assert.True(wal.SyncLedgerAttestedSequence < wal.EngineLatestSequence);
            }

            TruncateNewestLog(path, length => length / 2);

            using RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);
            Assert.Null(((IWAL)reopened).GetLostSyncedWritesReason(Partition));
            Assert.True(reopened.GetMaxLog(Partition) < Total);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    /// <summary>
    /// A torn record at the hard-state ledger's tail (a crash during an append) is cut off at open and
    /// never restored over the engine's state; later appends land after the cut.
    /// </summary>
    [Fact]
    public void HardStateLedger_TornTailRecord_IsCutAndIgnored()
    {
        string path = CreateTempPath();
        try
        {
            string ledgerPath;
            using (RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                Assert.True(((IWAL)wal).PersistHardState(Partition, 3, "node-a"));
                Assert.True(((IWAL)wal).PersistHardState(Partition, 4, "node-b"));
                ledgerPath = wal.HardStateLedgerPath;
            }

            long whole = new FileInfo(ledgerPath).Length;
            using (FileStream stream = File.Open(ledgerPath, FileMode.Open, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End);
                stream.Write(new byte[] { 0x48, 0x44, 0x53, 0x54, 1, 0, 0, 0, 99 }); // a record header with no body
            }

            using (RocksDbWAL reopened = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true))
            {
                Assert.Equal(whole, new FileInfo(ledgerPath).Length);
                Assert.True(((IWAL)reopened).TryGetHardState(Partition, out long term, out string? votedFor));
                Assert.Equal(4, term);
                Assert.Equal("node-b", votedFor);
                Assert.True(((IWAL)reopened).PersistHardState(Partition, 5, null));
            }

            using RocksDbWAL again = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
            Assert.True(((IWAL)again).TryGetHardState(Partition, out long laterTerm, out _));
            Assert.Equal(5, laterTerm);
        }
        finally
        {
            DeleteTempPath(path);
        }
    }

    // ── Scaffolding ────────────────────────────────────────────────────────────────────────

    /// <summary>Writes the 40 synced rows in one engine life and returns the sequence the ledger attested at its end.</summary>
    private static long WriteSyncedRows(string path, int flushMetadataAfter = 0)
    {
        using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: true);
        WriteSyncedRows(wal, 1, Total, flushMetadataAfter);
        return wal.SyncLedgerAttestedSequence;
    }

    /// <summary>One synced batch per id, each carrying its watermark — the production shape.</summary>
    private static void WriteSyncedRows(RocksDbWAL wal, long from, long to, int flushMetadataAfter = 0)
    {
        IWAL asInterface = wal;
        for (long id = from; id <= to; id++)
        {
            Assert.Equal(RaftOperationStatus.Success,
                asInterface.Write([(Partition, Rows(id, id, RaftLogType.Proposed))], sync: true, new Dictionary<int, long> { [Partition] = id }));

            if (id == flushMetadataAfter)
                wal.FlushMetadataForTesting();
        }
    }

    private static List<RaftLog> Rows(long from, long to, RaftLogType type = RaftLogType.Proposed, int partition = Partition)
    {
        List<RaftLog> rows = [];
        for (long id = from; id <= to; id++)
            rows.Add(new RaftLog { Id = id, Term = 1, Type = type, LogType = "ledger-test", LogData = new byte[256] });
        return rows;
    }

    private static FileInfo NewestLog(string path) =>
        new DirectoryInfo(Path.Combine(path, "wal")).GetFiles("*.log").OrderByDescending(f => f.Name, StringComparer.Ordinal).First();

    private static void TruncateNewestLog(string path, Func<long, long> newLength)
    {
        FileInfo log = NewestLog(path);
        using FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.Write);
        long target = newLength(stream.Length);
        Assert.InRange(target, 0, stream.Length - 1);
        stream.SetLength(target);
    }

    private static void FlipLedgerByte(string path, int offset)
    {
        using FileStream stream = File.Open(Path.Combine(path, "wal", RocksDbSyncLedger.FileName), FileMode.Open, FileAccess.ReadWrite);
        stream.Position = offset;
        int b = stream.ReadByte();
        stream.Position = offset;
        stream.WriteByte((byte)(b ^ 0xFF));
    }

    private static string CreateTempPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-sync-ledger-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempPath(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort */ }
    }
}
