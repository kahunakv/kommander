
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Regression coverage for "WaitForLeader should await restore completion, not a fixed 10 s wall
/// clock". <see cref="RaftManager.WaitForLeader"/> used to poll partition state under a hard-coded
/// 10 000 ms cap; during a slow WAL restore the partition cannot settle on a leader, so the cap
/// raced the restore and threw <c>RaftException("Leader couldn't be found or is not decided")</c>
/// prematurely. The fix awaits <c>RaftPartition.RestoreTask</c> first (bounded only by the caller's
/// token) and applies the election budget only afterwards.
///
/// <para>The tests drive restore through a <see cref="GatedWAL"/> that intercepts the Phase 1
/// <see cref="IWAL.ReadLogs"/> read for a single target partition: it can block that read past the
/// 10 s cap (slow restore) or fault it (failed restore), while every other partition — notably the
/// system partition 0 that <c>JoinCluster</c> needs — reads through untouched.</para>
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestWaitForLeaderRestore
{
    private const int TargetPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestWaitForLeaderRestore()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// The core regression: a restore whose Phase 1 read is held past the old 10 000 ms cap must not
    /// cause <see cref="RaftManager.WaitForLeader"/> to throw. We hold the gate ~10.5 s (just beyond
    /// the cap), assert the call has not completed (before the fix it would have faulted with the
    /// generic "Leader couldn't be found" timeout at ~10 s), then release restore and assert the
    /// leader is returned.
    /// </summary>
    [Fact]
    public async Task WaitForLeader_SlowRestore_DoesNotThrowAtTenSecondCap()
    {
        using GatedWAL wal = new(new InMemoryWAL(logger), TargetPartition);
        wal.BlockReadLogs(); // hold restore Phase 1 for the target partition

        IRaft node = BuildSingleNode(wal);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            await node.JoinCluster(cts.Token);
            Assert.True(node.IsInitialized);

            Task<string> waitTask = node.WaitForLeader(TargetPartition, cts.Token).AsTask();

            // Past the old 10 s cap. Before the fix, WaitForLeader would have thrown by now.
            await Task.Delay(TimeSpan.FromMilliseconds(10_500), cts.Token);
            Assert.False(
                waitTask.IsCompleted,
                "WaitForLeader completed while restore was still blocked — the fixed 10 s cap appears to be racing the restore again.");

            // Let restore finish; the node then self-elects and WaitForLeader returns the leader.
            wal.ReleaseReadLogs();

            string leader = await waitTask.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
            Assert.Equal(node.GetLocalEndpoint(), leader);
        }
        finally
        {
            wal.ReleaseReadLogs(); // ensure no thread is parked on the gate during teardown
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// A faulted restore must surface to the <see cref="RaftManager.WaitForLeader"/> caller as its
    /// real cause wrapped in a <see cref="RaftException"/>, not the generic "Leader couldn't be
    /// found or is not decided" timeout.
    /// </summary>
    [Fact]
    public async Task WaitForLeader_FaultedRestore_ThrowsWithUnderlyingCause()
    {
        InvalidOperationException cause = new("restore-read blew up");
        using GatedWAL wal = new(new InMemoryWAL(logger), TargetPartition);
        wal.FaultReadLogs(cause);

        IRaft node = BuildSingleNode(wal);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        try
        {
            await node.JoinCluster(cts.Token);

            RaftException ex = await Assert.ThrowsAsync<RaftException>(
                async () => await node.WaitForLeader(TargetPartition, cts.Token));

            Assert.Contains("restore failed", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Leader couldn't be found", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.NotNull(ex.InnerException);
            Assert.Contains("restore-read blew up", ex.InnerException!.Message);
        }
        finally
        {
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// Fast-path guard: on an already-restored partition <c>RestoreTask</c> is already complete, so
    /// awaiting it adds no latency and <see cref="RaftManager.WaitForLeader"/> returns the leader
    /// promptly (behaviour unchanged by the fix).
    /// </summary>
    [Fact]
    public async Task WaitForLeader_AlreadyRestored_ReturnsWithoutAddedLatency()
    {
        using GatedWAL wal = new(new InMemoryWAL(logger), TargetPartition); // gate never engaged

        IRaft node = BuildSingleNode(wal);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        try
        {
            await node.JoinCluster(cts.Token);
            Assert.True(node.IsInitialized);

            // Ensure the election has settled so we measure only the restore-await fast path, not
            // election latency.
            await node.WaitForLeader(TargetPartition, cts.Token);

            long startTimestamp = Stopwatch.GetTimestamp();
            string leader = await node.WaitForLeader(TargetPartition, cts.Token);
            TimeSpan elapsed = Stopwatch.GetElapsedTime(startTimestamp);

            Assert.Equal(node.GetLocalEndpoint(), leader);
            Assert.True(
                elapsed < TimeSpan.FromMilliseconds(500),
                $"WaitForLeader took {elapsed.TotalMilliseconds:F0} ms on an already-restored partition — the restore await appears to add latency on the fast path.");
        }
        finally
        {
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    private IRaft BuildSingleNode(IWAL wal)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8011,
            InitialPartitions = 1,
            CompactEveryOperations = 100,
            CompactNumberEntries = 50,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            logger);
    }

    /// <summary>
    /// An <see cref="IWAL"/> decorator that intercepts the Phase 1 restore read
    /// (<see cref="ReadLogs"/>) for one target partition so a test can block it past the election
    /// cap or fault it, while delegating everything else — and every other partition — to the inner
    /// WAL untouched.
    /// </summary>
    private sealed class GatedWAL : IWAL
    {
        private readonly IWAL inner;
        private readonly int targetPartition;
        private readonly ManualResetEventSlim gate = new(initialState: true);
        private Exception? readFault;

        public GatedWAL(IWAL inner, int targetPartition)
        {
            this.inner = inner;
            this.targetPartition = targetPartition;
        }

        /// <summary>Closes the gate so the next <see cref="ReadLogs"/> for the target partition blocks.</summary>
        public void BlockReadLogs() => gate.Reset();

        /// <summary>Opens the gate, releasing any blocked <see cref="ReadLogs"/>. Idempotent.</summary>
        public void ReleaseReadLogs() => gate.Set();

        /// <summary>Makes <see cref="ReadLogs"/> for the target partition throw <paramref name="ex"/>.</summary>
        public void FaultReadLogs(Exception ex) => readFault = ex;

        public List<RaftLog> ReadLogs(int partitionId)
        {
            if (partitionId == targetPartition)
            {
                if (readFault is not null)
                    throw readFault;

                gate.Wait();
            }

            return inner.ReadLogs(partitionId);
        }

        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) =>
            inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);

        public long GetTermAt(int partitionId, long logIndex) => inner.GetTermAt(partitionId, logIndex);

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => inner.Write(logs);

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync) => inner.Write(logs, sync);

        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);

        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);

        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);

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

        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) =>
            inner.TruncateProposedLogsAfter(partitionId, afterLogId);

        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) =>
            inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);

        public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(
            int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync) =>
            inner.InstallSnapshotBoundary(partitionId, snapshotIndex, lastIncludedTerm, sync);

        public void Dispose()
        {
            gate.Set();
            gate.Dispose();
            inner.Dispose();
        }
    }
}
