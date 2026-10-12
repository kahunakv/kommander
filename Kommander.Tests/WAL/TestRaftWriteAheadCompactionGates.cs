using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The trigger-side gates of a <see cref="RaftWriteAhead"/> compaction pass:
///
/// <list type="bullet">
///   <item>A pass that is still running swallows the next trigger instead of starting a second
///         concurrent pass (two passes deleting below overlapping floors would race).</item>
///   <item>The leader's published live-replica retention floor holds compaction above the lowest
///         position a live, acking follower still needs — but only while the publication is fresh:
///         a stale value from a stopped publisher expires and the pass compacts to the checkpoint.</item>
/// </list>
/// </summary>
public sealed class TestRaftWriteAheadCompactionGates
{
    /// <summary>
    /// While a pass is parked on its first storage read, a second <c>Compact()</c> trigger must be
    /// a no-op: the in-flight flag serializes passes, and the skipped trigger leaves exactly one
    /// counted pass. Once the parked pass finishes, a later trigger starts a new one.
    /// </summary>
    [Fact]
    public async Task Compact_TriggeredWhileAPassIsInFlight_IsSkippedUntilThePassFinishes()
    {
        GatedCheckpointWal wal = new();
        RaftWriteAhead writeAhead = CreateWriteAhead(wal, partitionId: 1, out RaftManager manager, out RaftPartition partition);

        try
        {
            writeAhead.Compact();
            await wal.WaitUntilCheckpointReadAsync();

            // The overlap: the first pass is parked inside its GetLastCheckpoint read.
            writeAhead.Compact();

            wal.Release();
            await writeAhead.WaitForCompactionIdleAsync();

            Assert.Equal(1, writeAhead.CompactionPassCount);

            // The in-flight flag was released: a later trigger runs a real second pass.
            writeAhead.Compact();
            await writeAhead.WaitForCompactionIdleAsync();
            Assert.Equal(2, writeAhead.CompactionPassCount);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
            wal.Dispose();
        }
    }

    /// <summary>
    /// A live-replica floor published by the leader's heartbeat round (below the checkpoint, with
    /// a budget that covers the lag) holds the pass at that position so ordinary backfill can
    /// still serve the follower. Once the publication is older than the staleness window, the
    /// pass ignores it and compacts down to the checkpoint — a stepped-down or crashed leader
    /// must not pin retention forever.
    /// </summary>
    [Fact]
    public async Task Compaction_HonorsThePublishedLiveReplicaFloor_UntilThePublicationExpires()
    {
        string path = CreateTempWalPath();

        try
        {
            using SqliteWAL wal = new(path, "wal", NullLogger<IRaft>.Instance);
            const int partitionId = 1;
            SeedLogs(wal, partitionId, count: 12, checkpointId: 10);

            RaftWriteAhead writeAhead = CreateWriteAhead(wal, partitionId, out RaftManager manager, out RaftPartition partition);

            try
            {
                writeAhead.SetLiveReplicaRetentionFloor(6);
                Assert.Equal(6, writeAhead.LiveReplicaRetentionFloor);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync();

                Assert.Equal([6L, 7L, 8L, 9L, 10L, 11L, 12L], wal.ReadLogsRange(partitionId, 1).Select(l => l.Id));

                // Expire the publication: only an active leader keeps the value fresh.
                writeAhead.SetLiveReplicaFloorStalenessForTesting(TimeSpan.FromSeconds(-1));
                await Task.Delay(20, TestContext.Current.CancellationToken);

                writeAhead.Compact();
                await writeAhead.WaitForCompactionIdleAsync();

                Assert.Equal([10L, 11L, 12L], wal.ReadLogsRange(partitionId, 1).Select(l => l.Id));
                Assert.Equal(10, wal.GetLastCheckpoint(partitionId));
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

    // ── harness ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A WAL whose FIRST <see cref="IWAL.GetLastCheckpoint"/> read parks until the test releases
    /// it, so a compaction pass can be held deterministically inside its first storage read.
    /// Every other member is a safe no-op: the parked pass never gets past the checkpoint gate.
    /// </summary>
    private sealed class GatedCheckpointWal : IWAL
    {
        private readonly TaskCompletionSource checkpointReadStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int checkpointReads;

        public Task WaitUntilCheckpointReadAsync() => checkpointReadStarted.Task;

        public void Release() => releaseGate.TrySetResult();

        public long GetLastCheckpoint(int partitionId)
        {
            if (Interlocked.Increment(ref checkpointReads) == 1)
            {
                checkpointReadStarted.TrySetResult();
                releaseGate.Task.Wait();
            }

            return -1;
        }

        public List<RaftLog> ReadLogs(int partitionId) => [];
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => [];
        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => RaftOperationStatus.Success;
        public long GetMaxLog(int partitionId) => 0;
        public long GetCurrentTerm(int partitionId) => 0;
        public int CountPersistedLogs(int partitionId) => 0;
        public int CountRemovableLogs(int partitionId) => 0;
        public string? GetMetaData(string key) => null;
        public bool SetMetaData(string key, string value) => true;
        public (RaftOperationStatus, int) CompactLogsOlderThan(int p, long lc, int n, int? maxTotalEntries = null) => (RaftOperationStatus.Success, 0);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => RaftOperationStatus.Success;
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => RaftOperationStatus.Success;
        public (RaftOperationStatus, long) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => (RaftOperationStatus.Success, afterLogId);
        public void Dispose() { }
    }

    private static RaftWriteAhead CreateWriteAhead(
        IWAL wal,
        int partitionId,
        out RaftManager manager,
        out RaftPartition partition)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            CompactEveryOperations = 0,
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

    private static void SeedLogs(SqliteWAL wal, int partitionId, int count, long checkpointId)
    {
        List<RaftLog> logs = [];

        for (long id = 1; id <= count; id++)
        {
            logs.Add(new RaftLog
            {
                Id = id,
                Term = id,
                Type = RaftLogType.Committed,
                LogType = "compaction-gate-test",
                LogData = [1, 2, 3],
            });
        }

        logs.Add(new RaftLog
        {
            Id = checkpointId,
            Term = checkpointId,
            Type = RaftLogType.CommittedCheckpoint,
            LogType = "compaction-gate-test-checkpoint",
            LogData = [1, 2, 3],
        });

        Assert.Equal(
            RaftOperationStatus.Success,
            wal.Write(new List<(int, List<RaftLog>)> { (partitionId, logs) }));
    }

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-sqlite-gate-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
