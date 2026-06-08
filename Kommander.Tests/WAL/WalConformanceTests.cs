
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

    // ──────────────────────────── helpers ───────────────────────────────────────

    protected static RaftLog Log(long id, long term = 1, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = "conformance" };
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
