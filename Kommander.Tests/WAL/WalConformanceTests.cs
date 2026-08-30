
using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Shared conformance suite executed against every <see cref="IWAL"/> adapter.
/// Concrete subclasses supply the adapter via <see cref="CreateWal"/>.
///
/// <para>Adapters that do not persist checkpoint metadata (e.g. InMemoryWAL) set
/// <see cref="SupportsCheckpoints"/> to <c>false</c> to skip checkpoint-specific assertions.</para>
/// </summary>
public abstract class WalConformanceTests
{
    /// <summary>
    /// Creates a fresh adapter instance. The out <paramref name="cleanup"/> action should release
    /// any temporary resources (directories, files) created alongside the adapter.
    /// The adapter itself is disposed by each test via <c>using</c>.
    /// </summary>
    protected abstract IWAL CreateWal(out Action cleanup);

    /// <summary>
    /// Override to <c>false</c> for adapters that do not persist checkpoint log types
    /// (e.g. InMemoryWAL always returns -1 for GetLastCheckpoint).
    /// </summary>
    protected virtual bool SupportsCheckpoints => true;

    /// <summary>
    /// Override to <c>false</c> for adapters that always return 0 from CountRemovableLogs.
    /// </summary>
    protected virtual bool SupportsRemovableLogCount => true;

    /// <summary>
    /// Whether <see cref="IWAL.GetLastCheckpoint"/> reports the real last-checkpoint id. True for all
    /// current adapters — including <see cref="InMemoryWAL"/>, which now maintains the value like the
    /// durable backends. Distinct from <see cref="SupportsCheckpoints"/>, which also gates
    /// <c>ReadLogs</c> checkpoint-floor filtering (InMemoryWAL returns the full log there).
    /// </summary>
    protected virtual bool SupportsCheckpointLookup => true;

    // ──────────────────────────── basic write / read ────────────────────────────

    [Fact]
    public void Write_SingleLog_ReadBackCorrectly()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(1, [Log(id: 1, term: 5)])]
            ));

            RaftLog result = Assert.Single(wal.ReadLogs(1));
            Assert.Equal(1, result.Id);
            Assert.Equal(5, result.Term);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Write_OutOfOrderIds_ReadLogsReturnsSortedAscending()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write(
                [(2, [Log(id: 3, term: 1), Log(id: 1, term: 1), Log(id: 2, term: 1)])]
            ));

            List<RaftLog> result = wal.ReadLogs(2);
            Assert.Equal([1L, 2L, 3L], result.Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── B2b: Raft hard state ────────────────────────────

    /// <summary>
    /// B2b: <c>PersistHardState</c>/<c>TryGetHardState</c> must round-trip the (term, votedFor) pair on
    /// every backend, namespace by partition, and store the two fields atomically (one metadata value).
    /// </summary>
    [Fact]
    public void HardState_PersistThenRead_RoundTrips()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Assert.False(wal.TryGetHardState(7, out _, out _)); // none written yet

            Assert.True(wal.PersistHardState(7, currentTerm: 4, votedFor: "node-b:8000"));

            Assert.True(wal.TryGetHardState(7, out long term, out string? votedFor));
            Assert.Equal(4, term);
            Assert.Equal("node-b:8000", votedFor);

            // Overwrite with a higher term and no vote — both fields update together.
            Assert.True(wal.PersistHardState(7, currentTerm: 5, votedFor: null));
            Assert.True(wal.TryGetHardState(7, out term, out votedFor));
            Assert.Equal(5, term);
            Assert.Null(votedFor);

            // Distinct partitions do not collide.
            Assert.False(wal.TryGetHardState(8, out _, out _));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Write_OverwriteExistingLog_ReadBackUpdatedTerm()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(3, [Log(id: 1, term: 1)])]);
            wal.Write([(3, [Log(id: 1, term: 7)])]);

            RaftLog result = Assert.Single(wal.ReadLogs(3));
            Assert.Equal(7, result.Term);
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── batch writes ──────────────────────────────────

    [Fact]
    public void BatchWrite_MultiplePartitionsInOneBatch_AreIsolatedFromEachOther()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([
                (10, [Log(id: 1, term: 10)]),
                (20, [Log(id: 1, term: 20)])
            ]));

            RaftLog p10 = Assert.Single(wal.ReadLogs(10));
            Assert.Equal(10, p10.Term);

            RaftLog p20 = Assert.Single(wal.ReadLogs(20));
            Assert.Equal(20, p20.Term);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void BatchWrite_MultipleLogsPerPartition_AllPersistedInOrder()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Assert.Equal(RaftOperationStatus.Success, wal.Write([
                (13, [Log(id: 1, term: 1), Log(id: 2, term: 1), Log(id: 3, term: 2)]),
                (14, [Log(id: 1, term: 3), Log(id: 2, term: 3)])
            ]));

            Assert.Equal([1L, 2L, 3L], wal.ReadLogs(13).Select(l => l.Id));
            Assert.Equal([1L, 2L], wal.ReadLogs(14).Select(l => l.Id));
            Assert.Equal(3, wal.GetMaxLog(13));
            Assert.Equal(2, wal.GetMaxLog(14));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void BatchWrite_OutOfOrderIdsAcrossPartitions_EachPartitionReadsSortedAscending()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            // Write both partitions with IDs in reverse order to ensure the adapter sorts on read.
            Assert.Equal(RaftOperationStatus.Success, wal.Write([
                (30, [Log(id: 5, term: 1), Log(id: 3, term: 1), Log(id: 1, term: 1)]),
                (31, [Log(id: 4, term: 2), Log(id: 2, term: 2)])
            ]));

            Assert.Equal([1L, 3L, 5L], wal.ReadLogs(30).Select(l => l.Id));
            Assert.Equal([2L, 4L], wal.ReadLogs(31).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── GetMaxLog ─────────────────────────────────────

    [Fact]
    public void GetMaxLog_EmptyPartition_ReturnsZero()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try { Assert.Equal(0, wal.GetMaxLog(99)); }
        finally { cleanup(); }
    }

    [Fact]
    public void GetMaxLog_ReturnsLargestId()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(4, [Log(id: 1), Log(id: 5), Log(id: 3)])]);
            Assert.Equal(5, wal.GetMaxLog(4));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── GetCurrentTerm ────────────────────────────────

    [Fact]
    public void GetCurrentTerm_EmptyPartition_ReturnsZero()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try { Assert.Equal(0, wal.GetCurrentTerm(98)); }
        finally { cleanup(); }
    }

    [Fact]
    public void GetCurrentTerm_ReturnsTermOfEntryWithHighestId()
    {
        // Write logs with IDs out of order so that the entry with the highest id (3)
        // has a lower term (5) than the entry with id 2 (term 8).
        // MAX(term) would return 8; correct answer is 5 (term of log id=3).
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(5, [Log(id: 1, term: 1), Log(id: 3, term: 5), Log(id: 2, term: 8)])]);
            Assert.Equal(5, wal.GetCurrentTerm(5));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── GetTermAt ─────────────────────────────────────

    [Fact]
    public void GetTermAt_EmptyPartition_ReturnsNegativeOne()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try { Assert.Equal(-1, wal.GetTermAt(96, 1)); }
        finally { cleanup(); }
    }

    [Fact]
    public void GetTermAt_ExactId_ReturnsThatEntrysTerm()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(40, [Log(id: 1, term: 3), Log(id: 2, term: 5), Log(id: 3, term: 8)])]);
            Assert.Equal(3, wal.GetTermAt(40, 1));
            Assert.Equal(5, wal.GetTermAt(40, 2));
            Assert.Equal(8, wal.GetTermAt(40, 3));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetTermAt_MissingIdWithinRange_ReturnsNegativeOne_NotNextEntry()
    {
        // A gap at id=2: GetTermAt(2) must return -1, NOT the term of the next present entry (id=3).
        // This is the exact contract the Log Matching Property check relies on — a hole is -1.
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(41, [Log(id: 1, term: 1), Log(id: 3, term: 9)])]);
            Assert.Equal(-1, wal.GetTermAt(41, 2));
            Assert.Equal(9, wal.GetTermAt(41, 3));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetTermAt_BeyondEnd_ReturnsNegativeOne()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(42, [Log(id: 1, term: 1), Log(id: 2, term: 1)])]);
            Assert.Equal(-1, wal.GetTermAt(42, 99));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetTermAt_AllLogTypes_ReturnTermIncludingUncommittedAnchors()
    {
        // The anchor entry for a Log Matching check may be uncommitted, so every type must
        // report its term (matching ReadLogsRange's any-type behavior), not just Committed ones.
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(43, [
                new RaftLog { Id = 1, Term = 2, Type = RaftLogType.Proposed,             LogType = "t" },
                new RaftLog { Id = 2, Term = 2, Type = RaftLogType.Committed,            LogType = "t" },
                new RaftLog { Id = 3, Term = 3, Type = RaftLogType.ProposedCheckpoint,   LogType = "t" },
                new RaftLog { Id = 4, Term = 3, Type = RaftLogType.CommittedCheckpoint,  LogType = "t" },
                new RaftLog { Id = 5, Term = 4, Type = RaftLogType.RolledBack,           LogType = "t" },
                new RaftLog { Id = 6, Term = 4, Type = RaftLogType.RolledBackCheckpoint, LogType = "t" }
            ])]);

            Assert.Equal(2, wal.GetTermAt(43, 1));
            Assert.Equal(2, wal.GetTermAt(43, 2));
            Assert.Equal(3, wal.GetTermAt(43, 3));
            Assert.Equal(3, wal.GetTermAt(43, 4));
            Assert.Equal(4, wal.GetTermAt(43, 5));
            Assert.Equal(4, wal.GetTermAt(43, 6));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetTermAt_TermZeroEntry_ReturnsZeroNotMinusOne()
    {
        // An entry that exists but has term 0 must return 0 (it is present), not -1 (absent).
        // This guards the RocksDB wire-scan path, where proto3 omits the default-valued term field:
        // "field absent on the wire" must be read as term 0 for a present entry, distinct from a
        // missing key which returns -1.
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(46, [new RaftLog { Id = 1, Term = 0, Type = RaftLogType.Committed, LogType = "t" }])]);
            Assert.Equal(0, wal.GetTermAt(46, 1));   // present with term 0
            Assert.Equal(-1, wal.GetTermAt(46, 2));  // absent
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetTermAt_AfterProposedUpgradedToCommitted_ReturnsTerm()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(44, [new RaftLog { Id = 1, Term = 6, Type = RaftLogType.Proposed, LogType = "t" }])]);
            Assert.Equal(6, wal.GetTermAt(44, 1));

            wal.Write([(44, [new RaftLog { Id = 1, Term = 6, Type = RaftLogType.Committed, LogType = "t" }])]);
            Assert.Equal(6, wal.GetTermAt(44, 1));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetTermAt_MatchesReadLogsRangeContract()
    {
        // GetTermAt must be observationally identical to the old ReadLogsRange(id, 1) path it replaced:
        // entries[0].Id == id ? entries[0].Term : -1.
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(45, [Log(id: 2, term: 4), Log(id: 4, term: 7), Log(id: 5, term: 9)])]);

            for (long id = 1; id <= 6; id++)
            {
                List<RaftLog> range = wal.ReadLogsRange(45, id, 1);
                long expected = range.Count > 0 && range[0].Id == id ? range[0].Term : -1;
                Assert.Equal(expected, wal.GetTermAt(45, id));
            }
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── GetLastCheckpoint ─────────────────────────────

    [Fact]
    public void GetLastCheckpoint_NoCheckpointEntry_ReturnsNegativeOne()
    {
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(6, [Log(id: 1, term: 1)])]);
            Assert.Equal(-1, wal.GetLastCheckpoint(6));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetLastCheckpoint_WithCommittedCheckpoint_ReturnsCheckpointLogId()
    {
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(7, [
                Log(id: 1, term: 1),
                Log(id: 2, term: 1, type: RaftLogType.CommittedCheckpoint),
                Log(id: 3, term: 1)
            ])]);
            Assert.Equal(2, wal.GetLastCheckpoint(7));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetLastCheckpoint_MultipleCheckpoints_ReturnsLatestById()
    {
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(8, [
                Log(id: 1, term: 1, type: RaftLogType.CommittedCheckpoint),
                Log(id: 2, term: 1),
                Log(id: 3, term: 2, type: RaftLogType.CommittedCheckpoint),
                Log(id: 4, term: 2)
            ])]);
            Assert.Equal(3, wal.GetLastCheckpoint(8));
        }
        finally { cleanup(); }
    }

    // ─────────── last-checkpoint persistence: cross-check vs an independent scan ───────────
    //
    // These guard the "persist the last-checkpoint id" feature: GetLastCheckpoint must always equal what
    // a full scan of the log would return, after EVERY mutating operation. ScanMaxCheckpoint is an
    // independent oracle built on ReadLogsRange (a code path untouched by the feature), so a bug in the
    // persisted-value maintenance (a missed write path, a bad truncation adjustment) surfaces here rather
    // than as a silent wrong replay floor at restore.

    /// <summary>
    /// Independent oracle: the highest <see cref="RaftLogType.CommittedCheckpoint"/> id physically present
    /// in the partition, or -1. Uses <see cref="IWAL.ReadLogsRange"/> (not the checkpoint metadata) so it
    /// cannot share a bug with <see cref="IWAL.GetLastCheckpoint"/>.
    /// </summary>
    private static long ScanMaxCheckpoint(IWAL wal, int partitionId)
    {
        long max = -1;
        foreach (RaftLog log in wal.ReadLogsRange(partitionId, 0, int.MaxValue))
            if (log.Type == RaftLogType.CommittedCheckpoint && log.Id > max)
                max = log.Id;
        return max;
    }

    private void AssertCheckpointMatchesScan(IWAL wal, int partitionId, long expected)
    {
        Assert.Equal(expected, ScanMaxCheckpoint(wal, partitionId));
        Assert.Equal(expected, wal.GetLastCheckpoint(partitionId));
    }

    [Fact]
    public void LastCheckpoint_TracksScanAcrossEveryMutation()
    {
        if (!SupportsCheckpointLookup) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            const int p = 40;

            // No checkpoint yet.
            AssertCheckpointMatchesScan(wal, p, -1);

            // Write with a checkpoint in the middle of the batch.
            wal.Write([(p, [Log(1), Log(2, type: RaftLogType.CommittedCheckpoint), Log(3)])]);
            AssertCheckpointMatchesScan(wal, p, 2);

            // A later, higher checkpoint advances the recorded id.
            wal.Write([(p, [Log(4), Log(5, type: RaftLogType.CommittedCheckpoint)])]);
            AssertCheckpointMatchesScan(wal, p, 5);

            // Non-checkpoint writes do not change it.
            wal.Write([(p, [Log(6), Log(7)])]);
            AssertCheckpointMatchesScan(wal, p, 5);

            // Snapshot boundary above the tail installs a new highest checkpoint.
            wal.InstallSnapshotBoundary(p, snapshotIndex: 9, lastIncludedTerm: 3, sync: true);
            AssertCheckpointMatchesScan(wal, p, 9);

            // Compaction removes only entries below the checkpoint — recorded id unchanged.
            wal.CompactLogsOlderThan(p, lastCheckpoint: 9, compactNumberEntries: 100);
            AssertCheckpointMatchesScan(wal, p, 9);

            // Truncation that removes the recorded checkpoint must adjust down to the surviving one.
            wal.Write([(p, [Log(10), Log(11, type: RaftLogType.CommittedCheckpoint), Log(12)])]);
            AssertCheckpointMatchesScan(wal, p, 11);
            wal.TruncateLogsAfter(p, afterLogId: 10); // drops 11 (checkpoint) and 12
            AssertCheckpointMatchesScan(wal, p, 9);    // falls back to the 9 boundary checkpoint

            // Truncation that does NOT reach the checkpoint leaves it unchanged.
            wal.Write([(p, [Log(13), Log(14)])]);
            wal.TruncateLogsAfter(p, afterLogId: 13); // drops only 14
            AssertCheckpointMatchesScan(wal, p, 9);

            // Truncating everything drops the recorded checkpoint entirely (→ -1).
            wal.TruncateLogsAfter(p, afterLogId: 0);
            AssertCheckpointMatchesScan(wal, p, -1);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void LastCheckpoint_FarBehindLargeTail_IsStillResolvedCorrectly()
    {
        if (!SupportsCheckpointLookup) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            const int p = 41;

            // Checkpoint at id 2, then a long non-checkpoint tail. The old implementation walked this tail
            // backwards on every read; the persisted value makes it an O(1) lookup, but the answer must be
            // identical.
            wal.Write([(p, [Log(1), Log(2, type: RaftLogType.CommittedCheckpoint)])]);

            List<RaftLog> tail = [];
            for (long id = 3; id <= 400; id++)
                tail.Add(Log(id));
            wal.Write([(p, tail)]);

            AssertCheckpointMatchesScan(wal, p, 2);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void LastCheckpoint_SnapshotBoundaryRetainingSuffix_KeepsHigherCheckpoint()
    {
        if (!SupportsCheckpointLookup) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            const int p = 42;

            // A higher checkpoint at id 5 exists; installing a boundary at a LOWER matching index must not
            // regress the recorded checkpoint when the suffix (including id 5) is retained. The prefix is
            // contiguous: a checkpoint written over a gap no longer advances the recorded id.
            wal.Write([(p, [
                Log(1),
                Log(2, term: 4, type: RaftLogType.CommittedCheckpoint),
                Log(3, term: 4),
                Log(4, term: 4),
                Log(5, term: 4, type: RaftLogType.CommittedCheckpoint)
            ])]);
            AssertCheckpointMatchesScan(wal, p, 5);

            // Boundary at id 2 with the matching term → suffix retained (id 3, 5 survive).
            (RaftOperationStatus status, bool suffixTruncated) = wal.InstallSnapshotBoundary(
                p, snapshotIndex: 2, lastIncludedTerm: 4, sync: true);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(suffixTruncated);

            // The recorded checkpoint must remain the surviving higher one (id 5), matching the scan.
            AssertCheckpointMatchesScan(wal, p, 5);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void LastCheckpoint_ProposedTailTruncation_LeavesCheckpointIntact()
    {
        if (!SupportsCheckpointLookup) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            const int p = 43;

            wal.Write([(p, [
                Log(1),
                Log(2, type: RaftLogType.CommittedCheckpoint),
                Log(3, type: RaftLogType.Proposed)
            ])]);
            AssertCheckpointMatchesScan(wal, p, 2);

            // Removing the unresolved tail must not touch the committed checkpoint.
            wal.TruncateProposedLogsAfter(p, afterLogId: 1);
            AssertCheckpointMatchesScan(wal, p, 2);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void LastCheckpoint_DeletePartitionWal_ClearsRecordedFloor()
    {
        if (!SupportsCheckpointLookup) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            const int p = 44;

            wal.Write([(p, [Log(1), Log(2, type: RaftLogType.CommittedCheckpoint)])]);
            AssertCheckpointMatchesScan(wal, p, 2);

            // Wiping the partition must clear the recorded checkpoint so a reused id does not inherit a
            // stale replay floor (there is no scan fallback to correct it).
            wal.DeletePartitionWAL(p);
            Assert.Equal(-1, wal.GetLastCheckpoint(p));

            // Reusing the partition id with fresh, checkpoint-free logs must still report -1.
            wal.Write([(p, [Log(1), Log(2)])]);
            AssertCheckpointMatchesScan(wal, p, -1);
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── ReadLogs + checkpoint filtering ───────────────

    [Fact]
    public void ReadLogs_NoCheckpoint_ReturnsAllLogs()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(25, [Log(id: 1), Log(id: 2), Log(id: 3)])]);
            Assert.Equal([1L, 2L, 3L], wal.ReadLogs(25).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void ReadLogs_WithCheckpoint_IncludesCheckpointEntryAndNewer()
    {
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(26, [
                Log(id: 1),
                Log(id: 2, type: RaftLogType.CommittedCheckpoint),
                Log(id: 3),
                Log(id: 4)
            ])]);
            // Must include the checkpoint entry itself (id=2) and all entries after it.
            Assert.Equal([2L, 3L, 4L], wal.ReadLogs(26).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── ReadLogsRange ─────────────────────────────────

    [Fact]
    public void ReadLogsRange_StartsAtIndexInclusive()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(9, [Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5)])]);

            List<RaftLog> result = wal.ReadLogsRange(9, 3);
            Assert.Equal([3L, 4L, 5L], result.Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void ReadLogsRange_BeyondEnd_ReturnsEmpty()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(15, [Log(id: 1), Log(id: 2)])]);
            Assert.Empty(wal.ReadLogsRange(15, 100));
        }
        finally { cleanup(); }
    }

    // ─────────────────────── ReadLogsRange byte budget ──────────────────────────
    // The byte-budgeted overload bounds the leader-backfill allocation: the batch stops once
    // adding the next entry would exceed the payload budget, whichever of the entry and byte
    // bounds is hit first, but never returns fewer than one available entry.

    [Fact]
    public void ReadLogsRange_ByteBudget_StopsBeforeExceedingBudget()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            // Five entries of 100 payload bytes each; a 250-byte budget fits exactly two.
            wal.Write([(41, [
                PayloadLog(id: 1, payloadSize: 100),
                PayloadLog(id: 2, payloadSize: 100),
                PayloadLog(id: 3, payloadSize: 100),
                PayloadLog(id: 4, payloadSize: 100),
                PayloadLog(id: 5, payloadSize: 100)])]);

            List<RaftLog> result = wal.ReadLogsRange(41, 1, maxEntries: 128, maxBytes: 250);
            Assert.Equal([1L, 2L], result.Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void ReadLogsRange_ByteBudget_AlwaysReturnsAtLeastOneEntry()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            // The first entry alone is larger than the whole budget: it must still ship,
            // or a follower behind an oversized entry could never converge.
            wal.Write([(42, [
                PayloadLog(id: 1, payloadSize: 1000),
                PayloadLog(id: 2, payloadSize: 10)])]);

            List<RaftLog> result = wal.ReadLogsRange(42, 1, maxEntries: 128, maxBytes: 100);
            Assert.Equal([1L], result.Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void ReadLogsRange_ByteBudget_EntryBoundStillApplies()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(43, [
                PayloadLog(id: 1, payloadSize: 10),
                PayloadLog(id: 2, payloadSize: 10),
                PayloadLog(id: 3, payloadSize: 10)])]);

            // A generous byte budget must not defeat the entry cap.
            List<RaftLog> result = wal.ReadLogsRange(43, 1, maxEntries: 2, maxBytes: long.MaxValue);
            Assert.Equal([1L, 2L], result.Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void ReadLogsRange_ByteBudget_UnlimitedBudgetMatchesPlainOverload()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(44, [
                PayloadLog(id: 1, payloadSize: 50),
                PayloadLog(id: 2, payloadSize: 50),
                PayloadLog(id: 3, payloadSize: 50)])]);

            Assert.Equal(
                wal.ReadLogsRange(44, 1, 128).Select(l => l.Id),
                wal.ReadLogsRange(44, 1, 128, long.MaxValue).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Every field of an entry must survive a write/read round-trip — not just the ids and terms most
    /// tests assert on. Backends that decode the stored record field by field (RocksDB parses the
    /// Protobuf wire format by hand on this path) can silently drop or mis-tag a field without any
    /// id-level test noticing, so the HLC components, log type and payload are checked explicitly.
    /// </summary>
    [Fact]
    public void ReadLogsRange_RoundTripsEveryField()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x7F, 0x80, 0xFF];

            RaftLog written = new()
            {
                Id = 7,
                Term = 9,
                Type = RaftLogType.CommittedCheckpoint,
                Time = new(n: 3, l: 1_700_000_000_123, c: 42),
                LogType = "round-trip",
                LogData = payload
            };

            wal.Write([(31, [written])]);

            RaftLog read = Assert.Single(wal.ReadLogsRange(31, 1));

            Assert.Equal(7, read.Id);
            Assert.Equal(9, read.Term);
            Assert.Equal(RaftLogType.CommittedCheckpoint, read.Type);
            Assert.Equal(3, read.Time.N);
            Assert.Equal(1_700_000_000_123, read.Time.L);
            Assert.Equal(42u, read.Time.C);
            Assert.Equal("round-trip", read.LogType);
            Assert.Equal(payload, read.LogData);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// A single range read mixes log types and payload shapes, which is where a per-entry decoder is
    /// most likely to leak state between entries: a cached/reused log-type string, a payload length
    /// carried over from the previous record, or a missing field left holding its predecessor's value.
    /// The payloads deliberately straddle the 127-byte single-byte-varint length boundary.
    /// </summary>
    [Fact]
    public void ReadLogsRange_MixedLogTypesAndPayloads_RoundTripIndependently()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            byte[] large = new byte[300];
            for (int i = 0; i < large.Length; i++)
                large[i] = (byte)(i % 251);

            wal.Write([(32, [
                new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "alpha", LogData = [1, 2, 3] },
                new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "beta",  LogData = null },
                new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "alpha", LogData = large },
                new RaftLog { Id = 4, Term = 1, Type = RaftLogType.Committed, LogType = "gamma", LogData = [] },
                new RaftLog { Id = 5, Term = 1, Type = RaftLogType.Committed, LogType = "alpha", LogData = [9] }
            ])]);

            List<RaftLog> read = wal.ReadLogsRange(32, 1);

            Assert.Equal(5, read.Count);
            Assert.Equal(["alpha", "beta", "alpha", "gamma", "alpha"], read.Select(l => l.LogType));
            Assert.Equal([1, 2, 3], read[0].LogData);
            Assert.Null(read[1].LogData);
            Assert.Equal(large, read[2].LogData);
            Assert.Empty(read[3].LogData!);
            Assert.Equal([9], read[4].LogData);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Verifies that <paramref name="maxEntries"/> is honoured at the storage level: writing 10 entries
    /// and requesting 3 must return exactly 3, not all 10. This guards against the O(n²) full-tail
    /// scan that happens when the limit is only applied in memory after reading everything.
    /// </summary>
    [Fact]
    public void ReadLogsRange_MaxEntriesIsEnforcedAtStorageLevel()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(200, [
                Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5),
                Log(id: 6), Log(id: 7), Log(id: 8), Log(id: 9), Log(id: 10)
            ])]);

            List<RaftLog> result = wal.ReadLogsRange(200, 1, maxEntries: 3);

            Assert.Equal(3, result.Count);
            Assert.Equal([1L, 2L, 3L], result.Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── CountPersistedLogs ────────────────────────────

    [Fact]
    public void CountPersistedLogs_ReturnsTotal()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(16, [Log(id: 1), Log(id: 2), Log(id: 3)])]);
            Assert.Equal(3, wal.CountPersistedLogs(16));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void CountPersistedLogs_EmptyPartition_ReturnsZero()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try { Assert.Equal(0, wal.CountPersistedLogs(97)); }
        finally { cleanup(); }
    }

    // ──────────────────────────── CountRemovableLogs ────────────────────────────

    [Fact]
    public void CountRemovableLogs_CountsLogsBelowCheckpoint()
    {
        if (!SupportsRemovableLogCount) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(17, [
                Log(id: 1),
                Log(id: 2, type: RaftLogType.CommittedCheckpoint),
                Log(id: 3),
                Log(id: 4)
            ])]);
            // Only log id=1 is strictly below the checkpoint at id=2.
            Assert.Equal(1, wal.CountRemovableLogs(17));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void CountRemovableLogs_NoCheckpoint_ReturnsZero()
    {
        if (!SupportsRemovableLogCount) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(18, [Log(id: 1), Log(id: 2), Log(id: 3)])]);
            Assert.Equal(0, wal.CountRemovableLogs(18));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── CompactLogsOlderThan ──────────────────────────

    [Fact]
    public void CompactLogsOlderThan_RemovesLogsStrictlyBelowCheckpoint()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(19, [Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5)])]);

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(19, lastCheckpoint: 4, compactNumberEntries: 10);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, removed);
            Assert.Equal([4L, 5L], wal.ReadLogs(19).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void CompactLogsOlderThan_RespectsMaxEntryLimit()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(20, [Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5)])]);

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(20, lastCheckpoint: 5, compactNumberEntries: 2);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(2, removed);
            // Exactly 2 oldest removed; ids 3, 4, 5 remain.
            List<long> remaining = wal.ReadLogs(20).Select(l => l.Id).ToList();
            Assert.Equal(3, remaining.Count);
            Assert.DoesNotContain(1L, remaining);
            Assert.DoesNotContain(2L, remaining);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void CompactLogsOlderThan_DoesNotRemoveCheckpointOrNewer()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(21, [Log(id: 1), Log(id: 2), Log(id: 3)])]);

            (RaftOperationStatus status, int removed) = wal.CompactLogsOlderThan(21, lastCheckpoint: 2, compactNumberEntries: 10);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(1, removed); // only id=1 < 2
            Assert.Equal([2L, 3L], wal.ReadLogs(21).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void CompactLogsOlderThan_MultipleCappedPasses_RemoveAllBelowCheckpoint()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            const int p = 22;
            // 1..5 removable, checkpoint at 6. Capped passes must resume where the previous stopped and
            // collectively remove exactly 1..5 (guards the compaction resume-hint advancing correctly).
            wal.Write([(p, [Log(1), Log(2), Log(3), Log(4), Log(5), Log(6, type: RaftLogType.CommittedCheckpoint)])]);

            (_, int r1) = wal.CompactLogsOlderThan(p, lastCheckpoint: 6, compactNumberEntries: 2);
            (_, int r2) = wal.CompactLogsOlderThan(p, lastCheckpoint: 6, compactNumberEntries: 2);
            (_, int r3) = wal.CompactLogsOlderThan(p, lastCheckpoint: 6, compactNumberEntries: 2);
            (_, int r4) = wal.CompactLogsOlderThan(p, lastCheckpoint: 6, compactNumberEntries: 2);

            Assert.Equal(2, r1); // 1,2
            Assert.Equal(2, r2); // 3,4
            Assert.Equal(1, r3); // 5 (only one left below the checkpoint)
            Assert.Equal(0, r4); // nothing left to remove
            Assert.Equal([6L], wal.ReadLogs(p).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void CompactLogsOlderThan_AfterDeleteAndReuse_StillCompactsFreshLowIds()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            const int p = 23;

            wal.Write([(p, [Log(1), Log(2), Log(3, type: RaftLogType.CommittedCheckpoint), Log(4)])]);
            (_, int firstRemoved) = wal.CompactLogsOlderThan(p, lastCheckpoint: 3, compactNumberEntries: 100);
            Assert.Equal(2, firstRemoved); // 1, 2
            Assert.Equal([3L, 4L], wal.ReadLogs(p).Select(l => l.Id));

            // Wipe and reuse the same partition id from low ids again. A backend that caches a compaction
            // resume position must reset it here; otherwise the second compaction would seek past the fresh
            // low entries and silently leak them (remove 0).
            wal.DeletePartitionWAL(p);
            Assert.Empty(wal.ReadLogs(p));

            wal.Write([(p, [Log(1), Log(2), Log(3, type: RaftLogType.CommittedCheckpoint), Log(4)])]);
            (RaftOperationStatus status, int reusedRemoved) =
                wal.CompactLogsOlderThan(p, lastCheckpoint: 3, compactNumberEntries: 100);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(2, reusedRemoved); // 1, 2 removed again — not skipped by a stale resume hint
            Assert.Equal([3L, 4L], wal.ReadLogs(p).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── metadata ──────────────────────────────────────

    [Fact]
    public void MetaData_SetAndGet_RoundTrips()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Assert.True(wal.SetMetaData("k1", "v1"));
            Assert.Equal("v1", wal.GetMetaData("k1"));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void MetaData_Upsert_OverwritesPreviousValue()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.SetMetaData("k", "old");
            wal.SetMetaData("k", "new");
            Assert.Equal("new", wal.GetMetaData("k"));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void MetaData_MissingKey_ReturnsNull()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try { Assert.Null(wal.GetMetaData("no-such-key")); }
        finally { cleanup(); }
    }

    // ──────────────────────────── log lifecycle (propose / commit / rollback) ───

    [Fact]
    public void Lifecycle_Proposed_ThenCommit_TypeUpdated()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(70, [new RaftLog { Id = 1, Term = 2, Type = RaftLogType.Proposed, LogType = "t" }])]);
            wal.Write([(70, [new RaftLog { Id = 1, Term = 2, Type = RaftLogType.Committed, LogType = "t" }])]);

            RaftLog result = Assert.Single(wal.ReadLogs(70));
            Assert.Equal(RaftLogType.Committed, result.Type);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Lifecycle_Proposed_ThenRolledBack_TypeUpdated()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(71, [new RaftLog { Id = 1, Term = 2, Type = RaftLogType.Proposed, LogType = "t" }])]);
            wal.Write([(71, [new RaftLog { Id = 1, Term = 2, Type = RaftLogType.RolledBack, LogType = "t" }])]);

            RaftLog result = Assert.Single(wal.ReadLogs(71));
            Assert.Equal(RaftLogType.RolledBack, result.Type);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Lifecycle_ProposedCheckpoint_ThenCommit_RegistersAsLastCheckpoint()
    {
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(72, [new RaftLog { Id = 1, Term = 3, Type = RaftLogType.ProposedCheckpoint, LogType = "t" }])]);
            Assert.Equal(-1, wal.GetLastCheckpoint(72)); // not committed yet

            wal.Write([(72, [new RaftLog { Id = 1, Term = 3, Type = RaftLogType.CommittedCheckpoint, LogType = "t" }])]);
            Assert.Equal(1, wal.GetLastCheckpoint(72));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Lifecycle_ProposedCheckpoint_ThenRolledBack_DoesNotRegisterAsCheckpoint()
    {
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(73, [
                new RaftLog { Id = 1, Term = 3, Type = RaftLogType.ProposedCheckpoint, LogType = "t" },
                Log(id: 2, term: 3)
            ])]);
            wal.Write([(73, [new RaftLog { Id = 1, Term = 3, Type = RaftLogType.RolledBackCheckpoint, LogType = "t" }])]);

            Assert.Equal(-1, wal.GetLastCheckpoint(73));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void GetLastCheckpoint_OnlyCommittedCheckpointTypeCounts()
    {
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(74, [
                new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Proposed,             LogType = "t" },
                new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed,            LogType = "t" },
                new RaftLog { Id = 3, Term = 1, Type = RaftLogType.ProposedCheckpoint,   LogType = "t" },
                new RaftLog { Id = 4, Term = 1, Type = RaftLogType.RolledBack,           LogType = "t" },
                new RaftLog { Id = 5, Term = 1, Type = RaftLogType.RolledBackCheckpoint, LogType = "t" },
                new RaftLog { Id = 6, Term = 1, Type = RaftLogType.CommittedCheckpoint,  LogType = "t" },
                new RaftLog { Id = 7, Term = 1, Type = RaftLogType.Committed,            LogType = "t" }
            ])]);
            Assert.Equal(6, wal.GetLastCheckpoint(74));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── data fidelity ─────────────────────────────────

    [Fact]
    public void Write_NullLogData_RoundTripsAsNull()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(75, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogData = null, LogType = "t" }])]);
            RaftLog result = Assert.Single(wal.ReadLogs(75));
            Assert.Null(result.LogData);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Write_BinaryLogData_RoundTripsExactly()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            byte[] payload = [0x00, 0xFF, 0x42, 0x01, 0x80, 0x7F];
            wal.Write([(76, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogData = payload, LogType = "t" }])]);
            RaftLog result = Assert.Single(wal.ReadLogs(76));
            Assert.Equal(payload, result.LogData);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Write_LogTypeString_RoundTripsCorrectly()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(77, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "myapp.SomeCommand", LogData = null }])]);
            RaftLog result = Assert.Single(wal.ReadLogs(77));
            Assert.Equal("myapp.SomeCommand", result.LogType);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void Write_AllLogEnumValues_CanBeStoredAndReadBack()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            RaftLogType[] all = [
                RaftLogType.Proposed,
                RaftLogType.Committed,
                RaftLogType.ProposedCheckpoint,
                RaftLogType.CommittedCheckpoint,
                RaftLogType.RolledBack,
                RaftLogType.RolledBackCheckpoint
            ];

            List<RaftLog> logs = all.Select((t, i) =>
                new RaftLog { Id = i + 1, Term = 1, Type = t, LogType = "t" }).ToList();
            wal.Write([(78, logs)]);

            // Use ReadLogsRange from id=1 to bypass checkpoint filtering
            // (CommittedCheckpoint at id=4 would otherwise cause ReadLogs to skip ids 1-3).
            List<RaftLog> result = wal.ReadLogsRange(78, 1);
            Assert.Equal(all.Length, result.Count);
            for (int i = 0; i < all.Length; i++)
                Assert.Equal(all[i], result[i].Type);
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── restore-from-checkpoint semantics ─────────────

    [Fact]
    public void RestoreSemantics_UncommittedCheckpoint_DoesNotFilterReadLogs()
    {
        // A ProposedCheckpoint that was never upgraded should not act as a restore point.
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(79, [
                Log(id: 1),
                new RaftLog { Id = 2, Term = 1, Type = RaftLogType.ProposedCheckpoint, LogType = "t" },
                Log(id: 3)
            ])]);

            Assert.Equal(-1, wal.GetLastCheckpoint(79));
            Assert.Equal([1L, 2L, 3L], wal.ReadLogs(79).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void RestoreSemantics_AfterCompact_ReadLogsIncludesCheckpointAndNewer()
    {
        // Simulate: write logs → checkpoint → compact → restore reads from checkpoint.
        if (!SupportsCheckpoints) return;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(80, [
                Log(id: 1), Log(id: 2), Log(id: 3),
                Log(id: 4, type: RaftLogType.CommittedCheckpoint),
                Log(id: 5), Log(id: 6)
            ])]);

            wal.CompactLogsOlderThan(80, lastCheckpoint: 4, compactNumberEntries: 10);

            List<long> ids = wal.ReadLogs(80).Select(l => l.Id).ToList();
            Assert.DoesNotContain(1L, ids);
            Assert.DoesNotContain(2L, ids);
            Assert.DoesNotContain(3L, ids);
            Assert.Contains(4L, ids);
            Assert.Contains(5L, ids);
            Assert.Contains(6L, ids);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void RestoreSemantics_TermAdvances_GetCurrentTermTracksLatestId()
    {
        // Simulate multiple term changes; GetCurrentTerm must track the entry with the highest id.
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(81, [Log(id: 1, term: 1), Log(id: 2, term: 2), Log(id: 3, term: 3)])]);
            Assert.Equal(3, wal.GetCurrentTerm(81));

            wal.Write([(81, [Log(id: 4, term: 4)])]);
            Assert.Equal(4, wal.GetCurrentTerm(81));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void MetaData_NodeState_TermAndVotePersistence()
    {
        // Simulates the scheduler persisting node term and voted-for values.
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Assert.True(wal.SetMetaData("current-term", "3"));
            Assert.True(wal.SetMetaData("voted-for", "node-2"));

            Assert.Equal("3", wal.GetMetaData("current-term"));
            Assert.Equal("node-2", wal.GetMetaData("voted-for"));

            wal.SetMetaData("current-term", "4");
            Assert.Equal("4", wal.GetMetaData("current-term"));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── concurrent access ─────────────────────────────

    /// <summary>
    /// Multiple threads reading the same partition concurrently must not corrupt the connection
    /// or return garbled data. This is the direct regression test for the SqliteWAL
    /// ReaderWriterLock bug where shared SqliteConnection was raced by concurrent readers.
    /// </summary>
    [Fact]
    public async Task ConcurrentReads_SamePartition_NeverCorruptOrThrow()
    {
        const int partitionId = 50;
        const int readerCount = 20;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(partitionId, [Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5)])]);

            Task[] readers = Enumerable.Range(0, readerCount).Select(_ => Task.Run(() =>
            {
                List<RaftLog> logs = wal.ReadLogs(partitionId);
                Assert.Equal(5, logs.Count);
                Assert.Equal([1L, 2L, 3L, 4L, 5L], logs.Select(l => l.Id));

                List<RaftLog> range = wal.ReadLogsRange(partitionId, 3);
                Assert.Equal([3L, 4L, 5L], range.Select(l => l.Id));

                Assert.Equal(5, wal.GetMaxLog(partitionId));
                Assert.Equal(1, wal.GetCurrentTerm(partitionId));
            }, TestContext.Current.CancellationToken)).ToArray();

            await Task.WhenAll(readers);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Concurrent writes and reads on the same partition must not produce torn reads, lost writes,
    /// or exceptions. Final state must be consistent.
    /// </summary>
    [Fact]
    public async Task ConcurrentWritesAndReads_SamePartition_NeverCorruptOrThrow()
    {
        const int partitionId = 51;
        const int workerCount = 10;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            // Seed a base log so reads on an empty partition don't complicate assertions.
            wal.Write([(partitionId, [Log(id: 1, term: 1)])]);

            Task[] workers = Enumerable.Range(0, workerCount).Select(i => Task.Run(() =>
            {
                long id = (long)i + 2; // ids 2..11, no collisions
                Assert.Equal(RaftOperationStatus.Success,
                    wal.Write([(partitionId, [Log(id: id, term: (long)i + 1)])]));

                // Read must not throw regardless of concurrent writers.
                List<RaftLog> _ = wal.ReadLogs(partitionId);
                long maxLog = wal.GetMaxLog(partitionId);
                Assert.True(maxLog >= 1);
            }, TestContext.Current.CancellationToken)).ToArray();

            await Task.WhenAll(workers);

            // After all writers finished, all 11 logs must be present.
            Assert.Equal(11, wal.CountPersistedLogs(partitionId));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Operations on different partitions must be able to run in parallel without blocking
    /// or corrupting each other.
    /// </summary>
    [Fact]
    public async Task ConcurrentAccess_DifferentPartitions_RunInParallel()
    {
        const int partitionCount = 8;
        const int opsPerPartition = 10;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Task[] workers = Enumerable.Range(0, partitionCount).Select(p => Task.Run(() =>
            {
                int partitionId = 60 + p;

                for (int i = 1; i <= opsPerPartition; i++)
                    Assert.Equal(RaftOperationStatus.Success,
                        wal.Write([(partitionId, [Log(id: i, term: 1)])]));

                Assert.Equal(opsPerPartition, wal.CountPersistedLogs(partitionId));
                Assert.Equal(opsPerPartition, wal.GetMaxLog(partitionId));
            }, TestContext.Current.CancellationToken)).ToArray();

            await Task.WhenAll(workers);

            // Cross-check: no partition leaked logs into another.
            for (int p = 0; p < partitionCount; p++)
                Assert.Equal(opsPerPartition, wal.CountPersistedLogs(60 + p));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Concurrent write + compact on the same partition must not corrupt the log or deadlock.
    /// </summary>
    [Fact]
    public async Task ConcurrentWriteAndCompact_SamePartition_NeverDeadlocksOrCorrupts()
    {
        const int partitionId = 52;
        const int rounds = 30;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Task[] tasks = Enumerable.Range(1, rounds).SelectMany(i =>
            {
                long id = i;
                return new Task[]
                {
                    Task.Run(() => wal.Write([(partitionId, [Log(id: id, term: 1)])]),
                        TestContext.Current.CancellationToken),
                    Task.Run(() => wal.CompactLogsOlderThan(partitionId, lastCheckpoint: id, compactNumberEntries: 3),
                        TestContext.Current.CancellationToken)
                };
            }).ToArray();

            await Task.WhenAll(tasks);

            Assert.True(wal.GetMaxLog(partitionId) >= 1);
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── scheduler-driven concurrent patterns ──────────

    /// <summary>
    /// Simulates the leader's Propose → Commit lifecycle across multiple partitions running
    /// concurrently, mirroring how the Raft scheduler drives separate partitions on separate threads.
    /// </summary>
    [Fact]
    public async Task SchedulerDriven_ProposeThenCommit_ConcurrentPartitions()
    {
        const int partitions = 8;
        const int logsPerPartition = 5;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Task[] workers = Enumerable.Range(0, partitions).Select(p => Task.Run(() =>
            {
                int partitionId = 100 + p;

                // Phase 1 — propose
                List<RaftLog> proposed = Enumerable.Range(1, logsPerPartition)
                    .Select(i => new RaftLog { Id = i, Term = 1, Type = RaftLogType.Proposed, LogType = "t" })
                    .ToList();
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, proposed)]));

                // Phase 2 — commit (same ids, upgraded type)
                List<RaftLog> committed = Enumerable.Range(1, logsPerPartition)
                    .Select(i => new RaftLog { Id = i, Term = 1, Type = RaftLogType.Committed, LogType = "t" })
                    .ToList();
                Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, committed)]));

                List<RaftLog> result = wal.ReadLogs(partitionId);
                Assert.Equal(logsPerPartition, result.Count);
                Assert.All(result, l => Assert.Equal(RaftLogType.Committed, l.Type));
            }, TestContext.Current.CancellationToken)).ToArray();

            await Task.WhenAll(workers);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Simulates follower state-check reads (GetCurrentTerm + GetMaxLog) running concurrently
    /// with leader writes — the pattern during replication where the scheduler queries progress
    /// while new entries arrive.
    /// </summary>
    [Fact]
    public async Task SchedulerDriven_FollowerReads_ConcurrentWithLeaderWrites()
    {
        const int partitionId = 110;
        const int writerCount = 5;
        const int readerCount = 10;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(partitionId, [Log(id: 1, term: 1)])]);

            Task[] writers = Enumerable.Range(0, writerCount).Select(i => Task.Run(() =>
            {
                wal.Write([(partitionId, [Log(id: (long)i + 2, term: (long)i + 2)])]);
            }, TestContext.Current.CancellationToken)).ToArray();

            Task[] readers = Enumerable.Range(0, readerCount).Select(_ => Task.Run(() =>
            {
                long term = wal.GetCurrentTerm(partitionId);
                Assert.True(term >= 1);
                long maxLog = wal.GetMaxLog(partitionId);
                Assert.True(maxLog >= 1);
            }, TestContext.Current.CancellationToken)).ToArray();

            await Task.WhenAll([.. writers, .. readers]);

            Assert.Equal(writerCount + 1, wal.CountPersistedLogs(partitionId));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Simulates the full checkpoint lifecycle under concurrent load: multiple partitions
    /// writing, checkpointing, and compacting simultaneously.
    /// </summary>
    [Fact]
    public async Task SchedulerDriven_CheckpointAndCompact_ConcurrentPartitions()
    {
        if (!SupportsCheckpoints) return;
        const int partitions = 4;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Task[] workers = Enumerable.Range(0, partitions).Select(p => Task.Run(() =>
            {
                int partitionId = 120 + p;

                wal.Write([(partitionId, [
                    Log(id: 1), Log(id: 2), Log(id: 3),
                    Log(id: 4, type: RaftLogType.CommittedCheckpoint),
                    Log(id: 5), Log(id: 6), Log(id: 7)
                ])]);

                Assert.Equal(4, wal.GetLastCheckpoint(partitionId));
                wal.CompactLogsOlderThan(partitionId, lastCheckpoint: 4, compactNumberEntries: 10);

                List<long> ids = wal.ReadLogs(partitionId).Select(l => l.Id).ToList();
                Assert.DoesNotContain(1L, ids);
                Assert.DoesNotContain(2L, ids);
                Assert.DoesNotContain(3L, ids);
                Assert.Contains(4L, ids);
                Assert.Contains(5L, ids);
            }, TestContext.Current.CancellationToken)).ToArray();

            await Task.WhenAll(workers);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Simulates the metadata access pattern used by the scheduler to persist node state
    /// (current term, voted-for): concurrent reads and writes must not corrupt each other.
    /// </summary>
    [Fact]
    public async Task SchedulerDriven_MetadataConcurrentReadWrite_NoCorruption()
    {
        const int workerCount = 20;

        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            Task[] workers = Enumerable.Range(0, workerCount).Select(i => Task.Run(() =>
            {
                string key = $"node-{i % 4}-term"; // 4 distinct keys, 5 writers each
                Assert.True(wal.SetMetaData(key, $"term-{i}"));
                Assert.NotNull(wal.GetMetaData(key));
            }, TestContext.Current.CancellationToken)).ToArray();

            await Task.WhenAll(workers);

            // Each key was written at least once; all four must be readable.
            for (int k = 0; k < 4; k++)
                Assert.NotNull(wal.GetMetaData($"node-{k}-term"));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── DeletePartitionWAL ────────────────────────────

    [Fact]
    public void DeletePartitionWAL_RemovesAllEntriesForPartition()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(5, [Log(id: 1), Log(id: 2), Log(id: 3)])]);
            wal.Write([(6, [Log(id: 1), Log(id: 2)])]);

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(5));

            // Partition 5 must be empty after deletion.
            Assert.Empty(wal.ReadLogs(5));
            Assert.Equal(0, wal.GetMaxLog(5));

            // Neighbouring partition 6 must be unaffected.
            Assert.Equal(2, wal.ReadLogs(6).Count);
        }
        finally { cleanup(); }
    }

    [Fact]
    public void DeletePartitionWAL_Idempotent_ReturnsSuccess()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(7, [Log(id: 1)])]);

            // First call removes the data.
            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(7));
            // Second call on already-empty partition.
            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(7));
            // Call on a partition that was never written.
            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(99));
        }
        finally { cleanup(); }
    }

    [Fact]
    public void DeletePartitionWAL_BoundaryPartitions_NotAffected()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            // Write logs to three adjacent partition IDs.
            wal.Write([(10, [Log(id: 1), Log(id: 2)])]);
            wal.Write([(11, [Log(id: 1), Log(id: 2)])]);
            wal.Write([(12, [Log(id: 1), Log(id: 2)])]);

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(11));

            // Partitions 10 and 12 must be completely untouched.
            Assert.Equal(2, wal.ReadLogs(10).Count);
            Assert.Empty(wal.ReadLogs(11));
            Assert.Equal(2, wal.ReadLogs(12).Count);
        }
        finally { cleanup(); }
    }

    // ──────────────────────── TruncateLogsAfter ──────────────────────────────────

    /// <summary>
    /// Entries strictly beyond <paramref name="afterLogId"/> are removed; entries at or
    /// below the boundary survive intact.
    /// </summary>
    [Fact]
    public void TruncateLogsAfter_RemovesTailBeyondBoundary()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(1, [Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5)])]);

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, 3));

            List<RaftLog> remaining = wal.ReadLogsRange(1, 1);
            Assert.Equal(3, remaining.Count);
            Assert.Equal(1, remaining[0].Id);
            Assert.Equal(2, remaining[1].Id);
            Assert.Equal(3, remaining[2].Id);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// When nothing exists beyond the boundary the call is a no-op and returns
    /// <see cref="RaftOperationStatus.Success"/>.
    /// </summary>
    [Fact]
    public void TruncateLogsAfter_IsNoOp_WhenNothingExistsBeyondBoundary()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(1, [Log(id: 1), Log(id: 2), Log(id: 3)])]);

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, 3));

            List<RaftLog> remaining = wal.ReadLogsRange(1, 1);
            Assert.Equal(3, remaining.Count);
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Simulates the in-order happy path: a FollowerAppend batch ending at id=4 truncates
    /// only entries beyond 4 (here id=5), while entries 1-4 remain intact.
    /// This is the canonical scenario for per-endpoint ordered delivery: the follower held
    /// a divergent proposal at id=5 from a previous term; truncation at batch-max=4 removes it.
    /// </summary>
    [Fact]
    public void TruncateLogsAfter_PrefixAppend_PreservesEntriesUpToBatchMax()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            // Follower holds 1-5; leader's batch ends at 4.
            wal.Write([(1, [Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5)])]);

            // op.LogIndex = 4 (batch max) — mirrors what FairWalScheduler passes.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(1, 4));

            List<RaftLog> remaining = wal.ReadLogsRange(1, 1);
            Assert.Equal(4, remaining.Count);
            Assert.Equal(new long[] { 1, 2, 3, 4 }, remaining.Select(e => e.Id).ToArray());
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Truncation on one partition must not touch any other partition's data.
    /// </summary>
    [Fact]
    public void TruncateLogsAfter_BoundaryPartitions_NotAffected()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(10, [Log(id: 1), Log(id: 2), Log(id: 3)])]);
            wal.Write([(11, [Log(id: 1), Log(id: 2), Log(id: 3)])]);
            wal.Write([(12, [Log(id: 1), Log(id: 2), Log(id: 3)])]);

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(11, 1));

            // Partitions 10 and 12 must be completely untouched.
            Assert.Equal(3, wal.ReadLogsRange(10, 1).Count);
            Assert.Single(wal.ReadLogsRange(11, 1));     // only id=1 survives
            Assert.Equal(3, wal.ReadLogsRange(12, 1).Count);
        }
        finally { cleanup(); }
    }

    // ──────────────────────── TruncateLogsAfterAndGetMax ─────────────────────────

    /// <summary>
    /// Removes entries strictly beyond the cut and returns the post-truncation max in a
    /// single operation. Verifies the SQLite DELETE+MAX and RocksDB seek-then-delete paths.
    /// </summary>
    [Fact]
    public void TruncateLogsAfterAndGetMax_RemovesTailAndReturnsNewMax()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(1, [Log(id: 1), Log(id: 2), Log(id: 3), Log(id: 4), Log(id: 5)])]);

            (RaftOperationStatus status, long newMax) = wal.TruncateLogsAfterAndGetMax(1, afterLogId: 3);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, newMax);
            Assert.Equal([1L, 2L, 3L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Truncating a holey log (missing index N) at N-1 returns N-1 as the new max, simulating
    /// the exact hole-repair frontier the leader uses when a follower reports localTerm=-1.
    /// </summary>
    [Fact]
    public void TruncateLogsAfterAndGetMax_OnHoleyLog_ReturnsContiguousFrontier()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            // Write 1..5 skipping 3 to plant a hole.
            wal.Write([(1, [Log(id: 1), Log(id: 2), Log(id: 4), Log(id: 5)])]);

            (RaftOperationStatus status, long newMax) = wal.TruncateLogsAfterAndGetMax(1, afterLogId: 2);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(2, newMax);
            Assert.Equal([1L, 2L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// No-op safety: cutting at or above the current max leaves the log unchanged and
    /// returns the existing max.
    /// </summary>
    [Fact]
    public void TruncateLogsAfterAndGetMax_CutAboveMax_IsNoOp()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(1, [Log(id: 1), Log(id: 2), Log(id: 3)])]);

            (RaftOperationStatus status, long newMax) = wal.TruncateLogsAfterAndGetMax(1, afterLogId: 10);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, newMax);
            Assert.Equal(3, wal.GetMaxLog(1));
        }
        finally { cleanup(); }
    }

    // ──────────────────────── InstallSnapshotBoundary ───────────────────────────

    /// <summary>
    /// When the stored term at the boundary index matches the snapshot's last-included term, the suffix
    /// above the index is retained and a CommittedCheckpoint is stamped at the boundary carrying that term.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_MatchingTerm_RetainsSuffix()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(1, [Log(id: 1, term: 2), Log(id: 2, term: 2), Log(id: 3, term: 2), Log(id: 4, term: 2), Log(id: 5, term: 2)])]);

            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 2, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);
            // Suffix (4,5) retained; boundary index still present.
            Assert.Equal([1L, 2L, 3L, 4L, 5L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
            Assert.Equal(5, wal.GetMaxLog(1));
            // Checkpoint stamped at the boundary with the last-included term.
            Assert.Equal(3, wal.GetLastCheckpoint(1));
            Assert.Equal(2, wal.GetTermAt(1, 3));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// When the stored term at the boundary index conflicts with the snapshot's last-included term, the
    /// entire suffix above the index is discarded and the checkpoint carries the last-included term.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_ConflictingTerm_TruncatesSuffix()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(1, [Log(id: 1, term: 2), Log(id: 2, term: 2), Log(id: 3, term: 2), Log(id: 4, term: 2), Log(id: 5, term: 2)])]);

            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 3, lastIncludedTerm: 9, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.True(truncated);
            // Suffix (4,5) discarded; boundary is the new max, stamped with the last-included term.
            Assert.Equal([1L, 2L, 3L], wal.ReadLogsRange(1, 1).Select(l => l.Id));
            Assert.Equal(3, wal.GetMaxLog(1));
            Assert.Equal(3, wal.GetLastCheckpoint(1));
            Assert.Equal(9, wal.GetTermAt(1, 3));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// Installing a boundary above the current max (no local entry there) stamps the checkpoint and does
    /// not remove any lower entries, since there is no suffix above the boundary to truncate.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_AboveMax_StampsCheckpointAndKeepsLowerEntries()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(1, [Log(id: 1, term: 2), Log(id: 2, term: 2), Log(id: 3, term: 2)])]);

            (RaftOperationStatus status, bool truncated) = wal.InstallSnapshotBoundary(1, snapshotIndex: 10, lastIncludedTerm: 4, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.False(truncated);          // nothing existed above 10
            Assert.Equal(10, wal.GetMaxLog(1));
            Assert.Equal(10, wal.GetLastCheckpoint(1));
            Assert.Equal(4, wal.GetTermAt(1, 10));
            // Lower entries survive (they are below the boundary).
            Assert.Contains(1L, wal.ReadLogsRange(1, 1).Select(l => l.Id));
        }
        finally { cleanup(); }
    }

    /// <summary>
    /// A boundary install on one partition must not touch any other partition's data.
    /// </summary>
    [Fact]
    public void InstallSnapshotBoundary_OtherPartitions_NotAffected()
    {
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            wal.Write([(10, [Log(id: 1, term: 2), Log(id: 2, term: 2), Log(id: 3, term: 2)])]);
            wal.Write([(11, [Log(id: 1, term: 2), Log(id: 2, term: 2), Log(id: 3, term: 2)])]);
            wal.Write([(12, [Log(id: 1, term: 2), Log(id: 2, term: 2), Log(id: 3, term: 2)])]);

            // Conflicting-term install on 11 truncates 11's suffix only.
            (RaftOperationStatus status, _) = wal.InstallSnapshotBoundary(11, snapshotIndex: 1, lastIncludedTerm: 9, sync: true);

            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(3, wal.ReadLogsRange(10, 1).Count);
            Assert.Single(wal.ReadLogsRange(11, 1));   // only the boundary at id=1 survives
            Assert.Equal(3, wal.ReadLogsRange(12, 1).Count);
        }
        finally { cleanup(); }
    }

    // ─────────────── max-log-id cache vs. recomputed max (all mutation paths) ───────────────

    /// <summary>
    /// Drives every mutation kind the adapter exposes and, after each one, cross-checks the
    /// incrementally-maintained answers (<see cref="IWAL.GetMaxLog"/>/<see cref="IWAL.GetCurrentTerm"/>)
    /// against an independent full read. This guards the two failure directions a cached max can drift
    /// into: stale-HIGH makes <c>GetCurrentTerm</c> silently return 0 (the lookup at the cached id
    /// misses and falls through), corrupting lastLogTerm comparisons in elections; stale-LOW lets the
    /// promotion catch-up barrier conclude early, so a freshly promoted leader serves before applying
    /// inherited committed entries.
    /// </summary>
    [Fact]
    public void MaxLogAndCurrentTerm_StayConsistentWithFullScan_AcrossAllMutationPaths()
    {
        const int p = 77;
        using IWAL wal = CreateWal(out Action cleanup);
        try
        {
            void AssertConsistent()
            {
                List<RaftLog> logs = wal.ReadLogs(p);
                long expectedMax = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
                long reportedMax = wal.GetMaxLog(p);
                Assert.Equal(expectedMax, reportedMax);

                long expectedTerm = logs.Count > 0 ? logs.First(l => l.Id == expectedMax).Term : 0;
                Assert.Equal(expectedTerm, wal.GetCurrentTerm(p));

                // Stale-high detector independent of ReadLogs: the id GetMaxLog reports must exist.
                if (reportedMax > 0)
                    Assert.NotEqual(-1, wal.GetTermAt(p, reportedMax));
            }

            AssertConsistent(); // empty partition: max 0, term 0

            wal.Write([(p, [
                Log(id: 1, term: 1), Log(id: 2, term: 1), Log(id: 3, term: 1),
                Log(id: 4, term: 1), Log(id: 5, term: 1), Log(id: 6, term: 2), Log(id: 7, term: 2),
                Log(id: 8, term: 2, type: RaftLogType.Proposed),
                Log(id: 9, term: 2, type: RaftLogType.Proposed),
                Log(id: 10, term: 2, type: RaftLogType.Proposed)])]);
            AssertConsistent();

            // Proposed-tail cleanup removes the current max (8..10) — max must fall back to 7.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateProposedLogsAfter(p, 7));
            AssertConsistent();
            Assert.Equal(7, wal.GetMaxLog(p));

            wal.Write([(p, [Log(id: 8, term: 3), Log(id: 9, term: 3), Log(id: 10, term: 3),
                Log(id: 11, term: 3), Log(id: 12, term: 3)])]);
            AssertConsistent();

            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(p, 9));
            AssertConsistent();
            Assert.Equal(9, wal.GetMaxLog(p));

            // The combined truncate+read must report the same max the queries then see.
            (RaftOperationStatus status, long combinedMax) = wal.TruncateLogsAfterAndGetMax(p, 5);
            Assert.Equal(RaftOperationStatus.Success, status);
            Assert.Equal(5, combinedMax);
            AssertConsistent();

            // Boundary term (2) mismatches the stored entry at id 4 (term 1) → suffix truncation:
            // everything above 4 goes, and the boundary entry itself becomes the max.
            (status, _) = wal.InstallSnapshotBoundary(p, snapshotIndex: 4, lastIncludedTerm: 2, sync: false);
            Assert.Equal(RaftOperationStatus.Success, status);
            AssertConsistent();
            Assert.Equal(4, wal.GetMaxLog(p));

            wal.Write([(p, [Log(id: 5, term: 4), Log(id: 6, term: 4), Log(id: 7, term: 4),
                Log(id: 8, term: 4), Log(id: 9, term: 4)])]);
            AssertConsistent();

            // Head compaction never touches the tail — max must survive whatever was removed.
            (status, _) = wal.CompactLogsOlderThan(p, lastCheckpoint: 7, compactNumberEntries: 100);
            Assert.Equal(RaftOperationStatus.Success, status);
            AssertConsistent();
            Assert.Equal(9, wal.GetMaxLog(p));

            // Truncate-to-empty: the cached max must not survive the last entry's removal.
            Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(p, 0));
            AssertConsistent();
            Assert.Equal(0, wal.GetMaxLog(p));
            Assert.Equal(0, wal.GetCurrentTerm(p));

            wal.Write([(p, [Log(id: 1, term: 5)])]);
            AssertConsistent();

            Assert.Equal(RaftOperationStatus.Success, wal.DeletePartitionWAL(p));
            AssertConsistent();
            Assert.Equal(0, wal.GetMaxLog(p));
        }
        finally { cleanup(); }
    }

    // ──────────────────────────── helpers ───────────────────────────────────────

    protected static RaftLog Log(long id, long term = 1, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = "conformance" };

    /// <summary>An entry with a payload of exactly <paramref name="payloadSize"/> bytes, for the byte-budget tests.</summary>
    protected static RaftLog PayloadLog(long id, int payloadSize) =>
        new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogType = "conformance", LogData = new byte[payloadSize] };
}

// ─────────────────────── SqliteWAL-specific tests ───────────────────────────

/// <summary>
/// Tests that exercise SQLite-specific behavior of DeletePartitionWAL:
/// no file creation for never-written partitions, and connection eviction after delete.
/// These cannot be expressed in the abstract conformance suite because they depend on
/// file-system state and the internal connection dictionary.
/// </summary>
public sealed class SqliteDeletePartitionWalTests
{
    private static (SqliteWAL Wal, string Dir, Action Cleanup) BuildWal()
    {
        string dir = Path.Combine(Path.GetTempPath(), $"sqlite-del-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        SqliteWAL wal = new(dir, "rev1", NullLogger<IRaft>.Instance, syncWrites: false);
        Action cleanup = () => { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); };
        return (wal, dir, cleanup);
    }

    /// <summary>
    /// DeletePartitionWAL on a partition that was never written must NOT create a .db file.
    /// The old implementation called TryOpenDatabase unconditionally, which ran
    /// CREATE TABLE IF NOT EXISTS and left a stale open connection in the dictionary.
    /// </summary>
    [Fact]
    public void DeletePartitionWAL_NeverWrittenPartition_DoesNotCreateDbFile()
    {
        (SqliteWAL wal, string dir, Action cleanup) = BuildWal();
        using (wal)
        {
            try
            {
                RaftOperationStatus status = wal.DeletePartitionWAL(42);

                Assert.Equal(RaftOperationStatus.Success, status);
                Assert.Empty(Directory.GetFiles(dir, "raft42_*.db"));
            }
            finally { cleanup(); }
        }
    }

    /// <summary>
    /// After DeletePartitionWAL on a partition that was written to, the connection must
    /// be closed and removed from the internal dictionary. A subsequent write must open a
    /// fresh connection (not reuse the evicted handle) and the data must be readable.
    /// </summary>
    [Fact]
    public void DeletePartitionWAL_EvictsConnection_SubsequentWriteOpensNewConnection()
    {
        (SqliteWAL wal, string dir, Action cleanup) = BuildWal();
        using (wal)
        {
            try
            {
                // Write data, then delete.
                wal.Write([(55, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" }])]);
                Assert.Single(wal.ReadLogs(55));

                RaftOperationStatus deleteStatus = wal.DeletePartitionWAL(55);
                Assert.Equal(RaftOperationStatus.Success, deleteStatus);

                // After delete, reads must return empty.
                Assert.Empty(wal.ReadLogs(55));

                // Write again — must open a fresh connection without error.
                RaftOperationStatus writeStatus = wal.Write(
                    [(55, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t2" }])]);
                Assert.Equal(RaftOperationStatus.Success, writeStatus);
                Assert.Single(wal.ReadLogs(55));
            }
            finally { cleanup(); }
        }
    }
}

// ─────────────────────────── concrete adapters ──────────────────────────────

public sealed class InMemoryWalConformanceTests : WalConformanceTests
{
    protected override bool SupportsCheckpoints => false;
    protected override bool SupportsRemovableLogCount => false;

    protected override IWAL CreateWal(out Action cleanup)
    {
        cleanup = () => { };
        return new InMemoryWAL(NullLogger<IRaft>.Instance);
    }
}

public sealed class SqliteWalConformanceTests : WalConformanceTests
{
    protected override IWAL CreateWal(out Action cleanup)
    {
        string path = Path.Combine(Path.GetTempPath(), $"wal-conform-sqlite-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        cleanup = () =>
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        };
        return new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);
    }
}

/// <summary>
/// Runs the full WAL conformance suite against a <see cref="SqliteWAL"/> configured with
/// shardCount=1 (all partitions on a single shard) to verify the sharded code path preserves
/// all IWAL semantics when co-resident partitions share one database and one transaction.
/// </summary>
public sealed class SqliteWalSingleShardConformanceTests : WalConformanceTests
{
    protected override IWAL CreateWal(out Action cleanup)
    {
        string path = Path.Combine(Path.GetTempPath(), $"wal-conform-sqlite-s1-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        cleanup = () =>
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        };
        return new SqliteWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false, shardCount: 1);
    }
}

public sealed class RocksDbWalConformanceTests : WalConformanceTests
{
    protected override IWAL CreateWal(out Action cleanup)
    {
        string path = Path.Combine(Path.GetTempPath(), $"wal-conform-rocksdb-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        cleanup = () =>
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        };
        return new RocksDbWAL(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);
    }
}
