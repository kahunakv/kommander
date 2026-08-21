using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.WAL;

/// <summary>
/// The restored commit frontier must never sit below the WAL's durable checkpoint.
///
/// <para>The frontier reconstruction in <see cref="RaftWriteAhead.CompleteRestoreAsync"/> scans the
/// restore read, and that read is anchored at the checkpoint LOG ENTRY. When the entry is absent —
/// the boundary survives only in backend metadata (corruption, a partial write, or an unknown
/// producer; the durable backends keep the two in sync, so the shape is staged here through a
/// wrapper) — the scan sees an empty list and used to reconstruct a frontier of 0. The node then
/// reported commit frontier 0 in every ack, the leader anchored backfill at 1 below its own
/// compaction floor, and the partition could only be rescued by a snapshot install: the
/// "anchored at 1" shape from the Caraxes soak wedge. The restore now floors the frontier at the
/// recorded checkpoint, exactly as a snapshot install would seed it.</para>
/// </summary>
public sealed class TestRestoreFrontierCheckpointFloor
{
    private const int PartitionId = 1;

    /// <summary>
    /// The corrupt shape: an empty restore read with a durable checkpoint at 5. Both frontier
    /// reconstruction paths (single-fsync and legacy) must seed the frontier from the checkpoint
    /// instead of publishing 0.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EmptyRestoreRead_WithDurableCheckpoint_SeedsFrontierFromCheckpoint(bool singleFsync)
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(
            checkpointOverride: 5, singleFsync, out RaftManager manager, out RaftPartition partition);

        try
        {
            IReadOnlyList<RaftLog> logs = await writeAhead.LoadRestoreLogsAsync();
            Assert.Empty(logs);

            await writeAhead.CompleteRestoreAsync(logs);

            // Pre-fix this reconstructed to 0: the node advertised an empty log and re-served
            // low ids despite a durable boundary certifying the prefix through 5.
            Assert.Equal(5, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>Control: with no checkpoint at all, an empty restore still yields frontier 0.</summary>
    [Fact]
    public async Task EmptyRestoreRead_WithoutCheckpoint_KeepsFrontierAtZero()
    {
        RaftWriteAhead writeAhead = CreateWriteAhead(
            checkpointOverride: null, singleFsync: true, out RaftManager manager, out RaftPartition partition);

        try
        {
            IReadOnlyList<RaftLog> logs = await writeAhead.LoadRestoreLogsAsync();
            await writeAhead.CompleteRestoreAsync(logs);

            Assert.Equal(0, writeAhead.GetCommitIndex());
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    // ── harness ───────────────────────────────────────────────────────────────

    private static RaftWriteAhead CreateWriteAhead(
        long? checkpointOverride, bool singleFsync, out RaftManager manager, out RaftPartition partition)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            WalSingleFsyncCommit = singleFsync,
        };

        IWAL wal = new CheckpointMetaOnlyWal(new InMemoryWAL(NullLogger<IRaft>.Instance), checkpointOverride);

        manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        partition = new(
            manager,
            wal,
            PartitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        return new RaftWriteAhead(manager, _ => { }, partition, wal);
    }

    /// <summary>
    /// Delegating WAL whose <see cref="GetLastCheckpoint"/> reports an injected boundary while the
    /// log itself stays empty — the metadata/log divergence the durable backends normally prevent.
    /// </summary>
    private sealed class CheckpointMetaOnlyWal(InMemoryWAL inner, long? checkpointOverride) : IWAL
    {
        public long GetLastCheckpoint(int partitionId) =>
            checkpointOverride ?? inner.GetLastCheckpoint(partitionId);

        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) =>
            inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => inner.Write(logs);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
            int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
            inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) =>
            inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) =>
            inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() => inner.Dispose();
    }
}
