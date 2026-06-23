
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

    // ──────────────────────────── helpers ───────────────────────────────────────

    protected static RaftLog Log(long id, long term = 1, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = "conformance" };
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
