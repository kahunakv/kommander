using System.Diagnostics;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

public sealed class TestRaftWriteAheadCompaction
{
    [Fact]
    public async Task Compact_UsesSingleStorageCommitPerPass()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactNumberEntries = 2;
            const int maxEntriesPerCompaction = 100;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries,
                maxEntriesPerCompaction,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                Assert.Equal(1, wal.LastCompactionCommitCount);
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public async Task Compact_DrainsMultipleBatchesWhenRemovableExceedsBatchSize()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactNumberEntries = 2;
            const int maxEntriesPerCompaction = 100;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries,
                maxEntriesPerCompaction,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
                Assert.Equal(removableCount + 1, wal.GetMaxLog(partitionId));
                Assert.Equal(removableCount + 1, wal.GetLastCheckpoint(partitionId));
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public async Task Compact_StopsAtMaxEntriesPerCompactionAndLeavesRemainder()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactNumberEntries = 5;
            const int maxEntriesPerCompaction = 25;
            const int removableCount = 50;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries,
                maxEntriesPerCompaction,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(25, remainingRemovable);

                (_, int noneLeft) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, noneLeft);
                Assert.Equal(removableCount + 1, wal.GetLastCheckpoint(partitionId));
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public async Task NotifyCommitted_TriggersCompactionAfterConfiguredCommitCount()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int compactEveryOperations = 5;
            const int removableCount = 20;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 5,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                for (int i = 0; i < compactEveryOperations; i++)
                    writeAhead.NotifyCommitted();

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public async Task NotifyCommitted_DisabledWhenCompactEveryOperationsIsZero()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 20;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 5,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                for (int i = 0; i < 100; i++)
                    writeAhead.NotifyCommitted();

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(removableCount, remainingRemovable);
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public async Task NotifyCommitted_DoesNotAwaitCompactionPass()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 200;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 2,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations: 1,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                writeAhead.NotifyCommitted();
                stopwatch.Stop();

                Assert.True(stopwatch.ElapsedMilliseconds < 100);

                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    [Fact]
    public async Task Compact_ConcurrentTriggersCoalesceToSinglePass()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 100;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal,
                compactNumberEntries: 2,
                maxEntriesPerCompaction: 5000,
                compactEveryOperations: 0,
                partitionId,
                out RaftManager manager,
                out RaftPartition partition);

            try
            {
                List<Task> tasks = [];

                for (int i = 0; i < 100; i++)
                {
                    tasks.Add(Task.Run(
                        () => writeAhead.Compact(),
                        TestContext.Current.CancellationToken));
                }

                await Task.WhenAll(tasks).ConfigureAwait(true);
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                Assert.Equal(1, writeAhead.CompactionPassCount);

                (_, int remainingRemovable) = wal.CompactLogsOlderThan(
                    partitionId,
                    lastCheckpoint: removableCount + 1,
                    compactNumberEntries: 100);
                Assert.Equal(0, remainingRemovable);
                Assert.Equal(removableCount + 1, wal.GetMaxLog(partitionId));
                Assert.Equal(removableCount + 1, wal.GetLastCheckpoint(partitionId));
            }
            finally
            {
                partition.Dispose();
                manager.Dispose();
            }
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    // ── minRetainIndex (WAL retention floor) tests ───────────────────────────

    /// <summary>
    /// When SetMinRetainIndex is not called the default is long.MaxValue, so effectiveFloor
    /// equals lastCheckpoint and compaction removes everything below the checkpoint — identical
    /// to the pre-floor behaviour.
    /// </summary>
    [Fact]
    public async Task RetainFloor_Default_TruncatesToCheckpoint()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                Assert.Equal(long.MaxValue, writeAhead.MinRetainIndex);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                // All committed entries below the checkpoint should be gone.
                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: removableCount + 1, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// When the retention floor (k) is below the checkpoint, effectiveFloor = k.
    /// Entries with id &lt; k are removed; entries in [k, checkpoint) survive.
    /// </summary>
    [Fact]
    public async Task RetainFloor_BelowCheckpoint_ProtectsEntriesAtOrAboveFloor()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // k=5 < checkpoint=10 → effectiveFloor=5 → removes ids 1-4, retains 5-9 + checkpoint
                writeAhead.SetMinRetainIndex(5);
                Assert.Equal(5, writeAhead.MinRetainIndex);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                // ReadLogsRange(0) scans from id=0 so we see everything still in the DB,
                // including entries below the checkpoint that the floor should have preserved.
                List<RaftLog> surviving = wal.ReadLogsRange(partitionId, 0);
                long[] survivingIds = surviving.Select(l => l.Id).ToArray();

                // ids 1-4 must be gone; ids 5-9 and the checkpoint (10) must survive
                Assert.DoesNotContain(1L, survivingIds);
                Assert.DoesNotContain(4L, survivingIds);
                Assert.Contains(5L, survivingIds);
                Assert.Contains(9L, survivingIds);
                Assert.Contains(10L, survivingIds);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// When the retention floor (k) is above the checkpoint, effectiveFloor = lastCheckpoint
    /// (the checkpoint is binding) and compaction removes everything below the checkpoint.
    /// </summary>
    [Fact]
    public async Task RetainFloor_AboveCheckpoint_CheckpointIsBinding()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // k=20 > checkpoint=10 → effectiveFloor=10 → all 9 committed entries removed
                writeAhead.SetMinRetainIndex(20);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: removableCount + 1, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// Advancing the floor across successive compaction passes progressively unprotects the
    /// prefix: each pass deletes only the newly-exposed prefix and leaves entries at or above
    /// the new floor intact.
    /// </summary>
    [Fact]
    public async Task RetainFloor_AdvancingFloorAcrossPasses_DeletesNewlyExposedPrefix()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // Pass 1: floor=5 → removes 1-4
                writeAhead.SetMinRetainIndex(5);
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                // Pass 2: advance floor to 8 → removes 5-7
                writeAhead.SetMinRetainIndex(8);
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                List<RaftLog> surviving = wal.ReadLogsRange(partitionId, 0);
                long[] survivingIds = surviving.Select(l => l.Id).ToArray();

                // 1-7 must be gone
                for (long id = 1; id <= 7; id++)
                    Assert.DoesNotContain(id, survivingIds);

                // 8, 9, and checkpoint 10 must survive
                Assert.Contains(8L, survivingIds);
                Assert.Contains(9L, survivingIds);
                Assert.Contains(10L, survivingIds);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// Setting the floor to 0 or negative normalises to long.MaxValue (no protection) and
    /// compaction falls through to the checkpoint — entries below the checkpoint are still removed.
    /// </summary>
    [Fact]
    public async Task RetainFloor_ZeroOrNegative_ClampsToNoProtection()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                writeAhead.SetMinRetainIndex(0);
                Assert.Equal(long.MaxValue, writeAhead.MinRetainIndex);

                writeAhead.SetMinRetainIndex(-1);
                Assert.Equal(long.MaxValue, writeAhead.MinRetainIndex);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                // Compaction must not have been suppressed — all removable entries gone.
                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: removableCount + 1, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// After SetMinRetainIndex returns, the value is immediately visible — the next compaction
    /// pass observes it without any scheduling round-trip.
    /// </summary>
    [Fact]
    public void RetainFloor_SetterIsSynchronous_ValueVisibleImmediately()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 5, maxEntriesPerCompaction: 100, compactEveryOperations: 0,
                partitionId: 1, out RaftManager manager, out RaftPartition partition);

            try
            {
                writeAhead.SetMinRetainIndex(42);
                Assert.Equal(42, writeAhead.MinRetainIndex);

                writeAhead.SetMinRetainIndex(99);
                Assert.Equal(99, writeAhead.MinRetainIndex);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// IRaft.SetMinRetainIndex for a partition that is not hosted on this node must be a no-op
    /// and must not throw.
    /// </summary>
    [Fact]
    public void RetainFloor_UnknownPartition_NoOp()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            CreateWriteAhead(
                wal, compactNumberEntries: 5, maxEntriesPerCompaction: 100, compactEveryOperations: 0,
                partitionId: 1, out RaftManager manager, out RaftPartition partition);

            try
            {
                // partitionId 999 is not hosted — must not throw.
                IRaft raft = manager;
                Exception? ex = Record.Exception(() => raft.SetMinRetainIndex(999, 42));
                Assert.Null(ex);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    // ── Composable retention holds (min-of-holds floor) tests ────────────────

    /// <summary>
    /// Two concurrent holds at different indices ⇒ the effective floor is the smaller of the two,
    /// and compaction never truncates below it. Entries below the min-held index are removed;
    /// entries at or above it survive.
    /// </summary>
    [Fact]
    public async Task Holds_TwoConcurrent_FloorIsMinimum()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // Two independent consumers hold at 5 and 8 → effective floor = min(5, 8) = 5.
                using IDisposable holdHigh = writeAhead.AcquireRetentionHold(8);
                using IDisposable holdLow = writeAhead.AcquireRetentionHold(5);
                Assert.Equal(5, writeAhead.EffectiveRetentionFloor);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                long[] survivingIds = wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray();

                // ids 1-4 gone; 5-9 and checkpoint 10 retained (the lower hold is binding).
                Assert.DoesNotContain(1L, survivingIds);
                Assert.DoesNotContain(4L, survivingIds);
                Assert.Contains(5L, survivingIds);
                Assert.Contains(9L, survivingIds);
                Assert.Contains(10L, survivingIds);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// Releasing the lowest hold raises the floor to the min of the remaining holds; releasing all
    /// holds drops back to unprotected (compaction truncates to the checkpoint).
    /// </summary>
    [Fact]
    public async Task Holds_ReleasingLowest_RaisesFloorToRemainingMin()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                IDisposable holdLow = writeAhead.AcquireRetentionHold(5);
                using IDisposable holdHigh = writeAhead.AcquireRetentionHold(8);
                Assert.Equal(5, writeAhead.EffectiveRetentionFloor);

                // Release the lower hold → floor rises to the remaining hold at 8.
                holdLow.Dispose();
                Assert.Equal(8, writeAhead.EffectiveRetentionFloor);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                long[] survivingIds = wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray();

                // 1-7 gone; 8, 9, checkpoint 10 retained.
                for (long id = 1; id <= 7; id++)
                    Assert.DoesNotContain(id, survivingIds);
                Assert.Contains(8L, survivingIds);
                Assert.Contains(9L, survivingIds);
                Assert.Contains(10L, survivingIds);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// Zero active holds ⇒ no hold protection: <see cref="RaftWriteAhead.EffectiveRetentionFloor"/>
    /// is <see cref="long.MaxValue"/> and compaction truncates all committed entries below the
    /// checkpoint, identical to the pre-holds behaviour.
    /// </summary>
    [Fact]
    public async Task Holds_ReleasingAll_Unprotected()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                IDisposable hold1 = writeAhead.AcquireRetentionHold(3);
                IDisposable hold2 = writeAhead.AcquireRetentionHold(6);
                hold1.Dispose();
                hold2.Dispose();
                Assert.Equal(long.MaxValue, writeAhead.EffectiveRetentionFloor);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: removableCount + 1, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// A hold acquired before a compaction pass is honored by that pass; once released, the next
    /// pass observes the raised floor and deletes the newly-exposed prefix.
    /// </summary>
    [Fact]
    public async Task Holds_HonoredByCurrentPass_ReleaseObservedByNextPass()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // Pass 1: hold at 5 is honored → removes 1-4, retains 5-9.
                IDisposable hold = writeAhead.AcquireRetentionHold(5);
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                long[] afterPass1 = wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray();
                Assert.DoesNotContain(4L, afterPass1);
                Assert.Contains(5L, afterPass1);

                // Release the hold → Pass 2 observes no protection and truncates to the checkpoint.
                hold.Dispose();
                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: 10, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// A single hold behaves identically to <see cref="RaftWriteAhead.SetMinRetainIndex"/> at the
    /// same index — the composable primitive is a strict superset of the legacy floor.
    /// </summary>
    [Fact]
    public async Task Holds_SingleHold_MatchesSetMinRetainIndex()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // Single hold at 5, exactly as SetMinRetainIndex(5) in RetainFloor_BelowCheckpoint.
                using IDisposable hold = writeAhead.AcquireRetentionHold(5);
                Assert.Equal(5, writeAhead.EffectiveRetentionFloor);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                long[] survivingIds = wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray();
                Assert.DoesNotContain(1L, survivingIds);
                Assert.DoesNotContain(4L, survivingIds);
                Assert.Contains(5L, survivingIds);
                Assert.Contains(9L, survivingIds);
                Assert.Contains(10L, survivingIds);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// The legacy <see cref="RaftWriteAhead.SetMinRetainIndex"/> floor and an active hold compose:
    /// the effective floor is the minimum of the two, so neither mechanism can raise the floor above
    /// the other's protected index.
    /// </summary>
    [Fact]
    public void Holds_ComposeWithLegacyFloor_EffectiveIsMinimum()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId: 1, out RaftManager manager, out RaftPartition partition);

            try
            {
                // Legacy floor higher than the hold → hold binds.
                writeAhead.SetMinRetainIndex(20);
                using IDisposable hold = writeAhead.AcquireRetentionHold(7);
                Assert.Equal(7, writeAhead.EffectiveRetentionFloor);

                // Legacy floor lower than the hold → legacy floor binds.
                writeAhead.SetMinRetainIndex(3);
                Assert.Equal(3, writeAhead.EffectiveRetentionFloor);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// Disposing the same handle twice releases exactly one hold — the second dispose is a no-op and
    /// must not remove a different consumer's hold that happens to remain active.
    /// </summary>
    [Fact]
    public void Holds_DisposeIsIdempotent()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId: 1, out RaftManager manager, out RaftPartition partition);

            try
            {
                IDisposable holdA = writeAhead.AcquireRetentionHold(5);
                using IDisposable holdB = writeAhead.AcquireRetentionHold(9);
                Assert.Equal(5, writeAhead.EffectiveRetentionFloor);

                // Double-dispose of A must not disturb B's hold at 9.
                holdA.Dispose();
                holdA.Dispose();
                Assert.Equal(9, writeAhead.EffectiveRetentionFloor);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// An <c>index &lt;= 0</c> hold is normalized to no protection (<see cref="long.MaxValue"/>),
    /// mirroring <see cref="RaftWriteAhead.SetMinRetainIndex"/>, so a consumer that has not yet
    /// computed its protected index cannot accidentally suppress compaction.
    /// </summary>
    [Fact]
    public void Holds_NonPositiveIndex_NoProtection()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId: 1, out RaftManager manager, out RaftPartition partition);

            try
            {
                using IDisposable holdZero = writeAhead.AcquireRetentionHold(0);
                using IDisposable holdNeg = writeAhead.AcquireRetentionHold(-1);
                Assert.Equal(long.MaxValue, writeAhead.EffectiveRetentionFloor);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// IRaft.AcquireRetentionHold for a partition not hosted on this node must be a no-op that
    /// returns a disposable and does not throw, mirroring <see cref="IRaft.SetMinRetainIndex"/>.
    /// </summary>
    [Fact]
    public void Holds_UnknownPartition_ReturnsNoOpHandle()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);

            CreateWriteAhead(
                wal, compactNumberEntries: 5, maxEntriesPerCompaction: 100, compactEveryOperations: 0,
                partitionId: 1, out RaftManager manager, out RaftPartition partition);

            try
            {
                IRaft raft = manager;
                Exception? ex = Record.Exception(() =>
                {
                    using IDisposable hold = raft.AcquireRetentionHold(999, 42);
                });
                Assert.Null(ex);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    // ── Live-replica retention floor (compaction lag budget) tests ───────────

    /// <summary>
    /// A live follower's published retention floor below the checkpoint holds compaction at that
    /// floor, so the entries the follower still needs stay servable by ordinary backfill instead
    /// of forcing the follower into snapshot dependence (the non-converging rescue loop's deeper
    /// cause).
    /// </summary>
    [Fact]
    public async Task LiveReplicaFloor_WithinBudget_HoldsCompaction()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // A live follower still needs entry 5 — the default budget easily covers a 5-entry lag.
                writeAhead.SetLiveReplicaRetentionFloor(5);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                long[] survivingIds = wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray();
                Assert.DoesNotContain(4L, survivingIds);
                Assert.Contains(5L, survivingIds);
                Assert.Contains(9L, survivingIds);
                Assert.Contains(10L, survivingIds);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// The hold is bounded: a follower further behind than
    /// <see cref="RaftConfiguration.CompactionLiveReplicaLagBudget"/> cannot grow the WAL without
    /// limit — the floor clamps to (checkpoint − budget) and the follower beyond it is left to the
    /// snapshot path.
    /// </summary>
    [Fact]
    public async Task LiveReplicaFloor_BeyondBudget_ClampsToTheBudget()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            // logs 1-9 committed, checkpoint at 10
            SeedRemovableLogs(wal, partitionId, removableCount: 9, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                // Budget 3 with checkpoint 10 → the floor can be held no lower than 7, however far
                // behind the published follower position sits.
                manager.Configuration.CompactionLiveReplicaLagBudget = 3;
                writeAhead.SetLiveReplicaRetentionFloor(2);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                long[] survivingIds = wal.ReadLogsRange(partitionId, 0).Select(l => l.Id).ToArray();
                Assert.DoesNotContain(2L, survivingIds);
                Assert.DoesNotContain(6L, survivingIds);
                Assert.Contains(7L, survivingIds);
                Assert.Contains(9L, survivingIds);
                Assert.Contains(10L, survivingIds);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// A budget of 0 disables the hold entirely: the published floor is ignored and compaction
    /// truncates to the checkpoint — the pre-fix behaviour.
    /// </summary>
    [Fact]
    public async Task LiveReplicaFloor_BudgetDisabled_TruncatesToCheckpoint()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                manager.Configuration.CompactionLiveReplicaLagBudget = 0;
                writeAhead.SetLiveReplicaRetentionFloor(5);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: removableCount + 1, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// A stale published floor is ignored: the publisher runs only on an active leader's heartbeat
    /// round, so expiry — not a step-down hook — is what keeps a stepped-down or stalled leader
    /// from pinning retention forever.
    /// </summary>
    [Fact]
    public async Task LiveReplicaFloor_Stale_IsIgnored()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                writeAhead.SetLiveReplicaRetentionFloor(5);
                writeAhead.SetLiveReplicaFloorStalenessForTesting(TimeSpan.FromMilliseconds(1));
                await Task.Delay(50, TestContext.Current.CancellationToken);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: removableCount + 1, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    /// <summary>
    /// A published floor at or above the checkpoint constrains nothing — a caught-up follower
    /// leaves compaction identical to the pre-fix behaviour, and non-positive floors normalize to
    /// no protection.
    /// </summary>
    [Fact]
    public async Task LiveReplicaFloor_AtOrAboveCheckpoint_NoEffect()
    {
        string path = CreateTempWalPath();
        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            const int removableCount = 9;

            SeedRemovableLogs(wal, partitionId, removableCount, checkpointId: removableCount + 1);

            RaftWriteAhead writeAhead = CreateWriteAhead(
                wal, compactNumberEntries: 100, maxEntriesPerCompaction: 1000, compactEveryOperations: 0,
                partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                writeAhead.SetLiveReplicaRetentionFloor(0);
                Assert.Equal(long.MaxValue, writeAhead.LiveReplicaRetentionFloor);

                writeAhead.SetLiveReplicaRetentionFloor(removableCount + 5);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);

                (_, int remaining) = wal.CompactLogsOlderThan(
                    partitionId, lastCheckpoint: removableCount + 1, compactNumberEntries: 100);
                Assert.Equal(0, remaining);
            }
            finally { partition.Dispose(); manager.Dispose(); }
        }
        finally { DeleteTempWalPath(path); }
    }

    private static RaftWriteAhead CreateWriteAhead(
        SqliteWAL wal,
        int compactNumberEntries,
        int maxEntriesPerCompaction,
        int compactEveryOperations,
        int partitionId,
        out RaftManager manager,
        out RaftPartition partition)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            CompactEveryOperations = compactEveryOperations,
            CompactNumberEntries = compactNumberEntries,
            MaxEntriesPerCompaction = maxEntriesPerCompaction,
        };

        manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        partition = new(
            manager,
            wal,
            partitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        return new RaftWriteAhead(manager, _ => { }, partition, wal);
    }

    private static void SeedRemovableLogs(SqliteWAL wal, int partitionId, int removableCount, long checkpointId)
    {
        List<RaftLog> logs = [];

        for (long id = 1; id <= removableCount; id++)
            logs.Add(CreateCommittedLog(id));

        logs.Add(CreateCheckpointLog(checkpointId));

        Assert.Equal(
            RaftOperationStatus.Success,
            wal.Write(new List<(int, List<RaftLog>)> { (partitionId, logs) }));
    }

    private static RaftLog CreateCommittedLog(long id)
    {
        return new()
        {
            Id = id,
            Term = id,
            Type = RaftLogType.Committed,
            LogType = "compaction-test",
            LogData = [1, 2, 3],
        };
    }

    private static RaftLog CreateCheckpointLog(long id)
    {
        return new()
        {
            Id = id,
            Term = id,
            Type = RaftLogType.CommittedCheckpoint,
            LogType = "compaction-test-checkpoint",
            LogData = [1, 2, 3],
        };
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-sqlite-wal-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
