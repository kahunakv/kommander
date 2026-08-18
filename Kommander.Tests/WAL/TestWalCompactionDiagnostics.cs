using System.Diagnostics.Metrics;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.WAL;

/// <summary>
/// Guards the diagnosability of <c>RaftWriteAhead</c> compaction: every reason a pass can decline to
/// remove anything must be attributable from the node's own output.
///
/// <para>Both retention gates used to return above the first log statement, so a WAL that never
/// shrank produced no evidence at any level — "the trigger never fired", "nothing has checkpointed",
/// and "a retention floor is holding" were indistinguishable in a post-mortem. These tests fail if
/// any of those paths goes quiet again.</para>
///
/// <para>Each test uses its own partition id so the process-wide
/// <c>raft.wal.compaction_passes_total</c> meter can be filtered to this test's measurements while
/// other suites run in parallel.</para>
/// </summary>
public sealed class TestWalCompactionDiagnostics : IDisposable
{
    private const string NoCheckpointDebugMarker = "Compaction pass skipped: no checkpoint yet";
    private const string AwaitingCheckpointInfoMarker = "has never had a checkpoint to compact to";
    private const string CompactionStartedMarker = "Compaction process started";

    private readonly MeterListener listener = new();
    private readonly Dictionary<(int Partition, string Outcome), long> passCounts = [];
    private readonly object metricsLock = new();

    public TestWalCompactionDiagnostics()
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName &&
                instrument.Name == "raft.wal.compaction_passes_total")
                l.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            int partitionId = -1;
            string outcome = "";

            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "partition_id" && tag.Value is int id)
                    partitionId = id;
                else if (tag.Key == "outcome" && tag.Value is string o)
                    outcome = o;
            }

            lock (metricsLock)
            {
                passCounts.TryGetValue((partitionId, outcome), out long current);
                passCounts[(partitionId, outcome)] = current + value;
            }
        });

        listener.Start();
    }

    public void Dispose() => listener.Dispose();

    private long PassCount(int partitionId, string outcome)
    {
        lock (metricsLock)
            return passCounts.TryGetValue((partitionId, outcome), out long count) ? count : 0;
    }

    /// <summary>
    /// A partition that commits steadily but never checkpoints must say so: a Debug line per pass
    /// naming the value that caused the skip, plus exactly one Information line for the stall.
    /// </summary>
    [Fact]
    public async Task NoCheckpoint_LogsEveryPassAtDebugAndOnceAtInformation()
    {
        const int partitionId = 7101;
        const int compactEveryOperations = 5;
        const int operations = 30;

        CapturingLogger logger = new();
        using GatedCheckpointWal wal = new(new InMemoryWAL(logger));

        RaftWriteAhead writeAhead = CreateWriteAhead(
            wal, logger, partitionId, compactEveryOperations, out RaftManager manager, out RaftPartition partition);

        try
        {
            // Committed entries only: nothing writes a CommittedCheckpoint, so the floor never moves.
            await DriveCommitsAsync(wal, writeAhead, partitionId, operations, withCheckpoints: false).ConfigureAwait(true);

            int passes = writeAhead.CompactionPassCount;
            Assert.Equal(operations / compactEveryOperations, passes);
            Assert.Equal(0, writeAhead.EffectiveCompactionPassCount);

            Assert.Equal(passes, logger.CountAtLevel(LogLevel.Debug, NoCheckpointDebugMarker));
            Assert.Contains(logger.Messages, m => m.Message.Contains(NoCheckpointDebugMarker) && m.Message.Contains($"/{partitionId}]"));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Information, AwaitingCheckpointInfoMarker));
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Information, CompactionStartedMarker));

            Assert.Equal(passes, PassCount(partitionId, KommanderMetrics.CompactionOutcome.NoCheckpoint));
            Assert.Equal(0, PassCount(partitionId, KommanderMetrics.CompactionOutcome.Effective));
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The Information line is one-shot per stall, not per process: a partition that checkpoints and
    /// then stops must report again rather than staying silent because an earlier stall set the flag.
    /// </summary>
    [Fact]
    public async Task AwaitingCheckpointNotice_RearmsAfterACheckpointIsObserved()
    {
        const int partitionId = 7102;
        const int compactEveryOperations = 5;

        CapturingLogger logger = new();
        using GatedCheckpointWal wal = new(new InMemoryWAL(logger));

        RaftWriteAhead writeAhead = CreateWriteAhead(
            wal, logger, partitionId, compactEveryOperations, out RaftManager manager, out RaftPartition partition);

        try
        {
            // Stall one: no checkpoint at all.
            await DriveCommitsAsync(wal, writeAhead, partitionId, compactEveryOperations, withCheckpoints: false).ConfigureAwait(true);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Information, AwaitingCheckpointInfoMarker));

            // A checkpoint lands: the next pass is effective and rearms the notice.
            await DriveCommitsAsync(wal, writeAhead, partitionId, compactEveryOperations, withCheckpoints: true).ConfigureAwait(true);
            Assert.True(writeAhead.EffectiveCompactionPassCount > 0);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Information, AwaitingCheckpointInfoMarker));

            // Stall two: the checkpoint is no longer visible (simulating a reset WAL). Because the
            // notice rearmed, the operator hears about the second stall too.
            wal.HideCheckpoint = true;
            await DriveCommitsAsync(wal, writeAhead, partitionId, compactEveryOperations, withCheckpoints: false).ConfigureAwait(true);
            Assert.Equal(2, logger.CountAtLevel(LogLevel.Information, AwaitingCheckpointInfoMarker));
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The regression guard the spec asks for: sustained commits with checkpoints and a low
    /// <c>CompactEveryOperations</c> must reach <c>LogInfoCompactionStarted</c> and actually truncate,
    /// so a return to "silently does nothing" is caught here rather than by a downstream harness.
    /// </summary>
    [Fact]
    public async Task SustainedCommitsWithCheckpoints_ReachCompactionStarted()
    {
        const int partitionId = 7103;
        const int compactEveryOperations = 5;
        const int operations = 40;

        CapturingLogger logger = new();
        using GatedCheckpointWal wal = new(new InMemoryWAL(logger));

        RaftWriteAhead writeAhead = CreateWriteAhead(
            wal, logger, partitionId, compactEveryOperations, out RaftManager manager, out RaftPartition partition);

        try
        {
            await DriveCommitsAsync(wal, writeAhead, partitionId, operations, withCheckpoints: true).ConfigureAwait(true);

            Assert.Equal(operations / compactEveryOperations, writeAhead.CompactionPassCount);
            Assert.Equal(writeAhead.CompactionPassCount, writeAhead.EffectiveCompactionPassCount);

            Assert.True(
                logger.CountAtLevel(LogLevel.Information, CompactionStartedMarker) > 0,
                "Compaction never reported a started pass despite checkpointed commits.");
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Debug, NoCheckpointDebugMarker));
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Information, AwaitingCheckpointInfoMarker));

            Assert.Equal(
                writeAhead.CompactionPassCount,
                PassCount(partitionId, KommanderMetrics.CompactionOutcome.Effective));
            Assert.Equal(0, PassCount(partitionId, KommanderMetrics.CompactionOutcome.NoCheckpoint));

            // And the passes were not merely announced — the WAL is smaller than what was written.
            Assert.True(wal.CountPersistedLogs(partitionId) < operations * 2);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// Writes <paramref name="operations"/> committed operations (optionally each followed by a
    /// checkpoint entry) and notifies the write-ahead after each, waiting for the triggered pass so
    /// pass counts and log counts are deterministic rather than racing the background task.
    /// </summary>
    private static async Task DriveCommitsAsync(
        IWAL wal,
        RaftWriteAhead writeAhead,
        int partitionId,
        int operations,
        bool withCheckpoints)
    {
        long nextId = wal.GetMaxLog(partitionId) + 1;

        for (int operation = 0; operation < operations; operation++)
        {
            List<RaftLog> batch = [CreateLog(nextId++, RaftLogType.Committed)];

            if (withCheckpoints)
                batch.Add(CreateLog(nextId++, RaftLogType.CommittedCheckpoint));

            Assert.Equal(
                RaftOperationStatus.Success,
                wal.Write([(partitionId, batch)]));

            writeAhead.NotifyCommitted();
            await writeAhead.WaitForCompactionIdleAsync().ConfigureAwait(true);
        }
    }

    private static RaftLog CreateLog(long id, RaftLogType type) => new()
    {
        Id = id,
        Term = 1,
        Type = type,
        LogType = "compaction-diagnostics",
        LogData = [1, 2, 3],
    };

    private static RaftWriteAhead CreateWriteAhead(
        IWAL wal,
        ILogger<IRaft> logger,
        int partitionId,
        int compactEveryOperations,
        out RaftManager manager,
        out RaftPartition partition)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            CompactEveryOperations = compactEveryOperations,
            CompactNumberEntries = 10,
            MaxEntriesPerCompaction = 100,
        };

        manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            logger);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        partition = new(manager, wal, partitionId, startRange: 0, endRange: 0, logger);

        return new RaftWriteAhead(manager, _ => { }, partition, wal);
    }

    /// <summary>
    /// Records every log line with its level so tests can assert not just that something was said but
    /// at which severity. <see cref="IsEnabled"/> is true at every level because the paths under test
    /// log at Debug and Trace.
    /// </summary>
    private sealed class CapturingLogger : ILogger<IRaft>
    {
        private readonly List<(LogLevel Level, string Message)> messages = [];
        private readonly object sync = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Messages
        {
            get
            {
                lock (sync)
                    return messages.ToList();
            }
        }

        public int CountAtLevel(LogLevel level, string substring)
        {
            lock (sync)
                return messages.Count(m => m.Level == level && m.Message.Contains(substring));
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (sync)
                messages.Add((logLevel, formatter(state, exception)));
        }
    }

    /// <summary>
    /// <see cref="InMemoryWAL"/> with a switch that hides the recorded checkpoint, so a test can put a
    /// partition back into the "nothing has checkpointed" state without rebuilding the WAL — the only
    /// way to exercise the rearm on the one-shot notice.
    /// </summary>
    private sealed class GatedCheckpointWal(InMemoryWAL inner) : IWAL
    {
        public bool HideCheckpoint { get; set; }

        public long GetLastCheckpoint(int partitionId) =>
            HideCheckpoint ? -1 : inner.GetLastCheckpoint(partitionId);

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
