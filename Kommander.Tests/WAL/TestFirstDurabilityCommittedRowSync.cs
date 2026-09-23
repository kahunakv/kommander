using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.WAL;

/// <summary>
/// The single-fsync fast path classifies a batch as lazy commit markers by log type alone, and a
/// marker may skip its fsync only because the row it marks is already durable from its own
/// propose write. A <c>Committed</c> row the partition has never held has no such earlier write:
/// it is the row's first and only durable write, and the type cannot tell the two apart. The
/// enqueuing partition flags it with <see cref="WALWriteOperation.RequiresSync"/>; these tests pin
/// that the scheduler honours the flag and that nothing else about the fast path moved.
///
/// <para>Driven in manual mode over a sync-recording store, so the assertion is on the exact
/// <c>sync</c> argument the scheduler passed to the backend — the thing a real crash cares about.
/// Manual mode writes each operation inline at enqueue, so every enqueue here is one batch.</para>
/// </summary>
public sealed class TestFirstDurabilityCommittedRowSync
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestFirstDurabilityCommittedRowSync(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// An all-<c>Committed</c> batch flagged as a first-durability write is fsynced even with the
    /// fast path on; the same batch without the flag rides, as before.
    /// </summary>
    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public void AllCommittedBatch_SyncFollowsTheFlag(bool requiresSync, bool expectedSync)
    {
        using SyncRecordingWal store = new(new InMemoryWAL(logger));
        using FairWalScheduler scheduler = new(store, logger, lazyCommitMarkers: true, manualExecution: true);
        scheduler.Start();

        RaftWalCompletion? completion = null;
        scheduler.Enqueue(new WALWriteOperation(
            c => completion = c,
            operationId: 1,
            WALWriteOperationType.FollowerAppend,
            (PartitionId, [Committed(1), Committed(2)]),
            logIndex: 2,
            requiresSync: requiresSync));

        Assert.NotNull(completion);
        Assert.Equal(RaftOperationStatus.Success, completion.Status);

        bool recorded = Assert.Single(store.SyncFlags);
        Assert.Equal(expectedSync, recorded);
    }

    /// <summary>
    /// The flag never removes an fsync: a batch that already forces one (a proposed row in it) is
    /// still written sync when flagged, and the fast path being off keeps every batch sync.
    /// </summary>
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public void FlagNeverRemovesAnFsync(bool lazyMarkers, bool requiresSync)
    {
        using SyncRecordingWal store = new(new InMemoryWAL(logger));
        using FairWalScheduler scheduler = new(store, logger, lazyCommitMarkers: lazyMarkers, manualExecution: true);
        scheduler.Start();

        scheduler.Enqueue(new WALWriteOperation(
            _ => { },
            operationId: 1,
            WALWriteOperationType.FollowerAppend,
            (PartitionId, [Proposed(1), Committed(2)]),
            logIndex: 2,
            requiresSync: requiresSync));

        Assert.True(Assert.Single(store.SyncFlags));
    }

    private static RaftLog Proposed(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Proposed,
        Time = new HLCTimestamp(0, 1_000 + id, 0),
        LogType = "test",
        LogData = [],
    };

    private static RaftLog Committed(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Committed,
        Time = new HLCTimestamp(0, 1_000 + id, 0),
        LogType = "test",
        LogData = [],
    };

    /// <summary>Records the <c>sync</c> argument of every batch write and delegates everything else.</summary>
    private sealed class SyncRecordingWal(IWAL inner) : IWAL, IDisposable
    {
        public List<bool> SyncFlags { get; } = [];

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => Write(logs, sync: true);

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
        {
            SyncFlags.Add(sync);
            return inner.Write(logs, sync);
        }

        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public long GetTermAt(int partitionId, long logIndex) => inner.GetTermAt(partitionId, logIndex);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) => inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => inner.TruncateProposedLogsAfter(partitionId, afterLogId);

        public void Dispose() => inner.Dispose();
    }
}
