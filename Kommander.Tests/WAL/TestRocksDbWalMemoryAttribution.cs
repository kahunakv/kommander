using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Covers the native-memory attribution readers on <see cref="RocksDbWAL"/> that feed the
/// <c>raft.wal.table_readers_memory</c>, <c>raft.wal.memtable_memory</c>,
/// <c>raft.wal.block_cache_usage</c> and <c>raft.wal.block_cache_pinned_usage</c> gauges.
///
/// <para>These exist because a node's resident set is mostly native memory that neither the .NET GC
/// nor a shared block-cache budget accounts for. CamusDB's Phase 5 fault soaks lost two nodes to
/// container OOM with ~3 GB of native memory per node that a 768 MiB shared budget did not move, and
/// nothing in the process could say where it was. Table-reader memory — per-SST index and filter
/// blocks, held outside the block cache because <c>cache_index_and_filter_blocks</c> is left at
/// RocksDB's default — is the candidate these readers make visible.</para>
/// </summary>
public sealed class TestRocksDbWalMemoryAttribution
{
    private static RaftLog MakeLog(int partitionId, long id, long term = 1) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = $"p{partitionId}" };

    private static string TempPath()
    {
        string p = Path.Combine(Path.GetTempPath(), $"kommander-memattr-{Guid.NewGuid():N}");
        Directory.CreateDirectory(p);
        return p;
    }

    private static void Cleanup(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    /// <summary>
    /// Writes enough across several partitions to materialize memtables, then checks the readers
    /// report real numbers rather than the 0 a closed or failing engine reports.
    /// </summary>
    [Fact]
    public void Readers_ReportNativeMemory_OnALiveEngine()
    {
        string path = TempPath();
        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            for (int partition = 0; partition < 4; partition++)
            {
                List<RaftLog> logs = [];
                for (long id = 1; id <= 200; id++)
                    logs.Add(MakeLog(partition, id));

                Assert.Equal(RaftOperationStatus.Success, wal.Write([(partition, logs)]));
            }

            // Memtables hold what was just written, so this must be positive while the engine is live.
            Assert.True(wal.GetMemtableMemoryBytes() > 0, "memtable memory should be positive after writes");

            // The cache readers are database-level and must not throw or go negative. They may be 0
            // when nothing has been read back yet, so the assertion is on sanity, not on magnitude.
            Assert.True(wal.GetBlockCacheUsageBytes() >= 0);
            Assert.True(wal.GetBlockCachePinnedUsageBytes() >= 0);
            Assert.True(wal.GetTableReadersMemoryBytes() >= 0);
        }
        finally { Cleanup(path); }
    }

    /// <summary>
    /// Table-reader memory appears once data is on disk: reading logs back opens the SSTs and their
    /// index blocks. This is the reader that matters, so it gets its own case with a flush.
    /// </summary>
    [Fact]
    public void TableReadersMemory_IsPositive_OnceSstsAreOpen()
    {
        string path = TempPath();
        try
        {
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

            List<RaftLog> logs = [];
            for (long id = 1; id <= 2_000; id++)
                logs.Add(MakeLog(1, id));

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, logs)]));

            // Force the memtable to disk so a table reader exists at all, then touch the data so
            // RocksDB opens the file.
            wal.FlushMemTablesForTesting(wait: true);
            Assert.NotEmpty(wal.ReadLogs(1));

            Assert.True(
                wal.GetTableReadersMemoryBytes() > 0,
                "table-reader memory should be positive once SSTs are open and read");
        }
        finally { Cleanup(path); }
    }

    /// <summary>
    /// Under a shared bundle, index and filter blocks are charged to the shared block cache instead
    /// of being pinned in the table readers. This is the fix for the growth that killed two CamusDB
    /// soak nodes: table-reader memory was measured at ~8.9% of live SST bytes, bounded by nothing,
    /// so a host that sized its container from the cache budget was short by gigabytes. Asserted as a
    /// ratio against live SST bytes rather than an absolute, so the test states the property
    /// ("bounded by the cache, not by the data") rather than a machine-specific number.
    /// </summary>
    [Fact]
    public void SharedResources_ChargeIndexAndFilterBlocksToTheCache_SoTableReadersStaySmall()
    {
        string path = TempPath();
        try
        {
            using RocksDbSharedResources shared = RocksDbSharedResources.CreateWithUnifiedBudget(
                totalBytes: 64 * 1024 * 1024, memtableBudgetBytes: 16 * 1024 * 1024);

            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: shared);

            byte[] payload = new byte[256];
            Random.Shared.NextBytes(payload);

            long id = 0;
            for (int batch = 0; batch < 40; batch++)
            {
                List<RaftLog> logs = [];
                for (int i = 0; i < 1_000; i++)
                    logs.Add(new RaftLog
                    {
                        Id = ++id, Term = 1, Type = RaftLogType.Committed, LogType = "p1", LogData = payload
                    });

                Assert.Equal(RaftOperationStatus.Success, wal.Write([(1, logs)]));
            }

            wal.FlushMemTablesForTesting(wait: true);
            Assert.NotEmpty(wal.ReadLogs(1));

            long liveSst = wal.GetShardLiveSstBytes();
            long tableReaders = wal.GetTableReadersMemoryBytes();

            Assert.True(liveSst > 0, "the probe must actually produce SSTs");

            // Unbounded, the readers run at ~8.9% of live SST bytes. Charged to the cache they are a
            // small fraction of that; 3% leaves generous room for RocksDB version differences while
            // still failing if the option is ever dropped.
            Assert.True(
                tableReaders < liveSst * 0.03,
                $"table readers {tableReaders} should be a small fraction of live SST {liveSst} when charged to the shared cache");
        }
        finally { Cleanup(path); }
    }

    /// <summary>
    /// A disposed engine reports 0 from every reader rather than throwing: the gauges are observed on
    /// a metrics thread that must not fault while a node is shutting down.
    /// </summary>
    [Fact]
    public void Readers_ReportZero_OnAClosedEngine()
    {
        string path = TempPath();
        try
        {
            RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance, syncWrites: false);
            wal.Write([(1, [MakeLog(1, id: 1)])]);
            wal.Dispose();

            Assert.Equal(0, wal.GetTableReadersMemoryBytes());
            Assert.Equal(0, wal.GetMemtableMemoryBytes());
            Assert.Equal(0, wal.GetBlockCacheUsageBytes());
            Assert.Equal(0, wal.GetBlockCachePinnedUsageBytes());
        }
        finally { Cleanup(path); }
    }
}
