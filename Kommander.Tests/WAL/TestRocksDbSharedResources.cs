using Kommander.Data;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// Validates <see cref="RocksDbSharedResources"/> and the optional shared-resources path in
/// <see cref="RocksDbWAL"/>. Each test allocates a modest in-memory budget (no disk-level RocksDB
/// tuning required for correctness) and cleans up temp directories unconditionally.
/// </summary>
public sealed class TestRocksDbSharedResources
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static RaftLog MakeLog(int partitionId, long id, long term = 1, RaftLogType type = RaftLogType.Committed) =>
        new() { Id = id, Term = term, Type = type, LogType = $"p{partitionId}" };

    private static string TempPath()
    {
        string p = Path.Combine(Path.GetTempPath(), $"kommander-shared-{Guid.NewGuid():N}");
        Directory.CreateDirectory(p);
        return p;
    }

    private static void Cleanup(params string[] paths)
    {
        foreach (string p in paths)
            if (Directory.Exists(p))
                Directory.Delete(p, recursive: true);
    }

    // ── T1: RocksDbSharedResources unit tests ─────────────────────────────────

    [Fact]
    public void CreateWithUnifiedBudget_RejectsMemtableBudgetExceedingTotal()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RocksDbSharedResources.CreateWithUnifiedBudget(
                totalBytes: 64 * 1024 * 1024,
                memtableBudgetBytes: 128 * 1024 * 1024));
    }

    [Fact]
    public void CreateWithUnifiedBudget_EqualBudgetIsAllowed()
    {
        const long budget = 64 * 1024 * 1024;
        using RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(budget, budget);

        Assert.NotEqual(IntPtr.Zero, res.WriteBufferManagerHandle);
        Assert.NotNull(res.BlockCache);
        Assert.True(res.MemtableMemoryUsage >= 0);
    }

    [Fact]
    public void Dispose_CanBeCalledTwiceWithoutCrash()
    {
        RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
            64 * 1024 * 1024, 16 * 1024 * 1024);

        res.Dispose(); // first: drops the bundle's native references
        res.Dispose(); // second: must be a no-op (idempotent guard)
    }

    // ── T2: RocksDbWAL backward compatibility ─────────────────────────────────

    [Fact]
    public void RocksDbWAL_NullSharedResources_BehaviorIdenticalToDefault()
    {
        string path = TempPath();
        try
        {
            // sharedResources = null  →  same as the existing no-arg constructor
            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: null);

            RaftOperationStatus status = wal.Write(
                [( 1, [MakeLog(1, id: 1, term: 42)] )]);
            Assert.Equal(RaftOperationStatus.Success, status);

            RaftLog log = Assert.Single(wal.ReadLogs(1));
            Assert.Equal(42, log.Term);
        }
        finally { Cleanup(path); }
    }

    // ── T2: shared resources applied — WAL still works correctly ──────────────

    [Fact]
    public void RocksDbWAL_WithSharedResources_RoundTripsLogsAcrossPartitions()
    {
        string path = TempPath();
        try
        {
            using RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                128 * 1024 * 1024, 32 * 1024 * 1024);

            using RocksDbWAL wal = new(path, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);

            // Write across several partitions / shards
            for (int pid = 0; pid < 5; pid++)
            {
                List<RaftLog> logs = [];
                for (long id = 1; id <= 3; id++)
                    logs.Add(MakeLog(pid, id, term: 100 + pid));

                Assert.Equal(RaftOperationStatus.Success,
                    wal.Write([(pid, logs)]));
            }

            // Verify round-trips
            for (int pid = 0; pid < 5; pid++)
            {
                List<RaftLog> readBack = wal.ReadLogs(pid);
                Assert.Equal(3, readBack.Count);
                Assert.All(readBack, l => Assert.Equal(100 + pid, l.Term));
            }
        }
        finally { Cleanup(path); }
    }

    // ── Validation: sharing is real across two WAL instances ──────────────────

    /// <summary>
    /// Opens two independent <see cref="RocksDbWAL"/> instances sharing one
    /// <see cref="RocksDbSharedResources"/> bundle, writes to both, and asserts that
    /// <see cref="RocksDbSharedResources.MemtableMemoryUsage"/> reflects combined activity —
    /// proving a single WBM is tracking both databases.
    /// </summary>
    [Fact]
    public void TwoWALs_SharedBundle_MemtableUsageRisesFromBothWrites()
    {
        string path1 = TempPath();
        string path2 = TempPath();
        try
        {
            using RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                256 * 1024 * 1024, 64 * 1024 * 1024);

            long usageBefore = res.MemtableMemoryUsage;

            using (RocksDbWAL wal1 = new(path1, "wal", NullLogger<IRaft>.Instance,
                       syncWrites: false, sharedResources: res))
            using (RocksDbWAL wal2 = new(path2, "wal", NullLogger<IRaft>.Instance,
                       syncWrites: false, sharedResources: res))
            {
                // Write a meaningful volume so memtable bytes accumulate noticeably
                const int entriesToWrite = 500;
                byte[] payload = new byte[1024]; // 1 KiB per entry

                List<(int, List<RaftLog>)> batch1 = [];
                List<(int, List<RaftLog>)> batch2 = [];

                for (long id = 1; id <= entriesToWrite; id++)
                {
                    batch1.Add((1, [new RaftLog { Id = id, Term = 1, Type = RaftLogType.Committed, LogData = payload }]));
                    batch2.Add((2, [new RaftLog { Id = id, Term = 1, Type = RaftLogType.Committed, LogData = payload }]));
                }

                foreach ((int pid, List<RaftLog> logs) in batch1)
                    wal1.Write([(pid, logs)]);
                foreach ((int pid, List<RaftLog> logs) in batch2)
                    wal2.Write([(pid, logs)]);

                long usageAfter = res.MemtableMemoryUsage;

                // The WBM tracks memtable bytes from both databases; usage should have grown.
                Assert.True(usageAfter > usageBefore,
                    $"Expected MemtableMemoryUsage to grow after writes from both WALs. " +
                    $"Before: {usageBefore}, After: {usageAfter}");
            }
        }
        finally { Cleanup(path1, path2); }
    }

    /// <summary>
    /// Disposes both WALs, then disposes the bundle — the correct teardown order. No crash.
    /// </summary>
    [Fact]
    public void DisposeOrder_WALsFirst_ThenBundle_NoCrash()
    {
        string path1 = TempPath();
        string path2 = TempPath();
        try
        {
            RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                64 * 1024 * 1024, 16 * 1024 * 1024);

            RocksDbWAL wal1 = new(path1, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);
            RocksDbWAL wal2 = new(path2, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);

            wal1.Dispose();
            wal2.Dispose();
            res.Dispose(); // correct order: both DBs closed before the bundle is released
        }
        finally { Cleanup(path1, path2); }
    }

    /// <summary>
    /// Disposes the bundle <em>before</em> closing the WALs. The native objects are refcounted
    /// shared_ptrs — each open DB holds its own reference — so this ordering does not crash.
    /// It is nonetheless a usage error for budget-accounting purposes: the WBM and cache will no
    /// longer correctly track memory after their bundle reference is released.
    /// </summary>
    [Fact]
    public void DisposeOrder_BundleFirst_ThenWALs_DoesNotCrash()
    {
        string path1 = TempPath();
        string path2 = TempPath();
        try
        {
            RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                64 * 1024 * 1024, 16 * 1024 * 1024);

            RocksDbWAL wal1 = new(path1, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);
            RocksDbWAL wal2 = new(path2, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);

            // Bundle disposed before WALs — should not crash because DBs hold own shared_ptr refs.
            res.Dispose();

            wal1.Dispose();
            wal2.Dispose();
        }
        finally { Cleanup(path1, path2); }
    }
}
