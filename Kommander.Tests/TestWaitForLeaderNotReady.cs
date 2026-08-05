
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Regression coverage for the join/restart window where a node already reports
/// <see cref="IRaft.Joined"/> (set at the very start of both join paths) but has not yet applied
/// the partition map, so user partitions don't exist and <see cref="IRaft.IsInitialized"/> is
/// false. <see cref="RaftManager.WaitForLeader"/> used to call <c>GetPartition</c> unguarded in
/// that window and leak <c>RaftException("Invalid partition: N")</c> — a misleading error for a
/// partition id that is perfectly valid once assembly completes. Routing callers (e.g. a KV store
/// deciding whether to redirect a request) saw an unhandled exception instead of a retryable
/// condition, while the sibling <c>AmILeader</c> guard silently returned false.
///
/// <para>The window is reproduced exactly as it occurs in production: a <see cref="GatedWAL"/>
/// blocks the system partition's Phase 1 restore read, which holds back the partition-map
/// application (and therefore <c>IsInitialized</c>) while <c>JoinCluster</c> has already flipped
/// <c>Joined</c>.</para>
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestWaitForLeaderNotReady
{
    private const int UserPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestWaitForLeaderNotReady()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    [Fact]
    public async Task WaitForLeader_JoinedButNotInitialized_ThrowsTypedNotReady()
    {
        using GatedWAL wal = new(new InMemoryWAL(logger), RaftSystemConfig.SystemPartition);
        wal.BlockReadLogs(); // hold P0 restore so the partition map is never applied

        IRaft node = BuildSingleNode(wal);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Task joinTask = node.JoinCluster(cts.Token);

        try
        {
            // Joined flips at the start of the join path, long before initialization.
            while (!node.Joined)
                await Task.Delay(10, cts.Token);

            Assert.False(node.IsInitialized);

            // The window under test: joined, uninitialized, no user partitions constructed.
            RaftNodeNotReadyException ex = await Assert.ThrowsAsync<RaftNodeNotReadyException>(
                async () => await node.WaitForLeader(UserPartition, cts.Token));

            // The misleading pre-fix error must be gone: the id is valid, the node just isn't ready.
            Assert.DoesNotContain("Invalid partition", ex.Message, StringComparison.OrdinalIgnoreCase);

            // Once assembly completes the same call resolves normally.
            wal.ReleaseReadLogs();
            await joinTask;
            Assert.True(node.IsInitialized);

            string leader = await node.WaitForLeader(UserPartition, cts.Token);
            Assert.Equal(node.GetLocalEndpoint(), leader);
        }
        finally
        {
            wal.ReleaseReadLogs(); // ensure no thread is parked on the gate during teardown
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    /// <summary>
    /// A genuinely unknown partition id on a fully initialized node must keep throwing the plain
    /// "Invalid partition" <see cref="RaftException"/> — that case is a caller error, not a
    /// retryable not-ready condition, and must not be reclassified by the guard.
    /// </summary>
    [Fact]
    public async Task WaitForLeader_UnknownPartitionAfterInit_StillThrowsInvalidPartition()
    {
        IRaft node = BuildSingleNode(new InMemoryWAL(logger));

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        try
        {
            await node.JoinCluster(cts.Token);
            Assert.True(node.IsInitialized);

            RaftException ex = await Assert.ThrowsAsync<RaftException>(
                async () => await node.WaitForLeader(999, cts.Token));

            Assert.IsNotType<RaftNodeNotReadyException>(ex);
            Assert.Contains("Invalid partition", ex.Message, StringComparison.OrdinalIgnoreCase);
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
            Port = 8021,
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
    /// An <see cref="IWAL"/> decorator that blocks the Phase 1 restore read
    /// (<see cref="ReadLogs"/>) for one target partition, delegating everything else to the inner
    /// WAL untouched. Blocking the system partition keeps <c>IsInitialized</c> false while
    /// <c>JoinCluster</c> has already marked the node joined — the exact restart window under test.
    /// </summary>
    private sealed class GatedWAL : IWAL
    {
        private readonly IWAL inner;
        private readonly int targetPartition;
        private readonly ManualResetEventSlim gate = new(initialState: true);

        public GatedWAL(IWAL inner, int targetPartition)
        {
            this.inner = inner;
            this.targetPartition = targetPartition;
        }

        public void BlockReadLogs() => gate.Reset();

        public void ReleaseReadLogs() => gate.Set();

        public List<RaftLog> ReadLogs(int partitionId)
        {
            if (partitionId == targetPartition)
                gate.Wait();

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
