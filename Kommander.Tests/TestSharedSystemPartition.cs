
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Integration tests for the shared P0 feature: consumer-typed log entries co-located on
/// the system partition alongside the coordinator's _RaftSystem entries.
///
/// All tests use an in-process three-node cluster so follower dispatch, WAL restore, and the
/// system map can all be exercised in a single test run.  Tests are in the
/// ClusterIntegrationCollection so they are serialized with the other cluster tests.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSharedSystemPartition
{
    private readonly ILogger<IRaft> logger;

    public TestSharedSystemPartition(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    // ── node factories ────────────────────────────────────────────────────────

    private RaftManager MakeNode(
        int id,
        InMemoryCommunication communication,
        IWAL wal,
        string[] peers)
    {
        RaftConfiguration config = new()
        {
            NodeName = $"node{id}",
            NodeId = id,
            Host = "localhost",
            Port = 8000 + id,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            wal,
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private async Task<(RaftManager n1, RaftManager n2, RaftManager n3, InMemoryCommunication comm)>
        AssembleCluster(IWAL wal1, IWAL wal2, IWAL wal3)
    {
        InMemoryCommunication comm = new();

        RaftManager n1 = MakeNode(1, comm, wal1, ["localhost:8002", "localhost:8003"]);
        RaftManager n2 = MakeNode(2, comm, wal2, ["localhost:8001", "localhost:8003"]);
        RaftManager n3 = MakeNode(3, comm, wal3, ["localhost:8001", "localhost:8002"]);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            { "localhost:8001", n1 },
            { "localhost:8002", n2 },
            { "localhost:8003", n3 },
        });

        await n1.UpdateNodes();
        await n2.UpdateNodes();
        await n3.UpdateNodes();

        await Task.WhenAll(
            n1.JoinCluster(TestContext.Current.CancellationToken),
            n2.JoinCluster(TestContext.Current.CancellationToken),
            n3.JoinCluster(TestContext.Current.CancellationToken));

        // Wait for partition 1 (user partition) to elect a leader.
        await WaitForAnyLeader([n1, n2, n3], 1, TestContext.Current.CancellationToken);

        return (n1, n2, n3, comm);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static async Task WaitForAnyLeader(
        IRaft[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 15_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (IRaft n in nodes)
                if (await n.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return;
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"No leader for partition {partitionId} within 15 s.");
    }

    private static async Task<IRaft?> GetLeader(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft n in nodes)
            if (await n.AmILeaderQuick(partitionId).ConfigureAwait(false))
                return n;
        return null;
    }

    private static async Task WaitForConditionAsync(
        Func<bool> condition, CancellationToken ct, int timeoutMs = 10_000)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (condition()) return;
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// A consumer-typed entry replicated to P0 fires OnReplicationReceived on the P0
    /// follower nodes, while the system coordinator continues to receive and process its
    /// own _RaftSystem entries normally (the partition map is intact after both writes).
    /// </summary>
    [Fact]
    public async Task ConsumerEntryOnP0_FiresOnReplicationReceived_AndSystemMapIntact()
    {
        using IWAL wal1 = new InMemoryWAL(logger);
        using IWAL wal2 = new InMemoryWAL(logger);
        using IWAL wal3 = new InMemoryWAL(logger);

        (RaftManager n1, RaftManager n2, RaftManager n3, _) =
            await AssembleCluster(wal1, wal2, wal3);

        try
        {
            IRaft[] nodes = [n1, n2, n3];

            // Find the P0 leader (elected during JoinCluster / IsInitialized).
            IRaft? p0Leader = await GetLeader(RaftSystemConfig.SystemPartition, nodes);
            Assert.NotNull(p0Leader);

            // Register OnReplicationReceived on all nodes to capture P0 consumer entries.
            ConcurrentBag<(int partitionId, string? logType)> received = [];
            foreach (IRaft node in nodes)
            {
                node.OnReplicationReceived += (pid, log) =>
                {
                    received.Add((pid, log.LogType));
                    return Task.FromResult(true);
                };
            }

            // Replicate a consumer-typed entry to P0.
            byte[] payload = "consumer-data"u8.ToArray();
            RaftReplicationResult result = await p0Leader.ReplicateLogs(
                RaftSystemConfig.SystemPartition,
                "consumer-type",
                payload,
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.True(result.Success);
            Assert.Equal(RaftOperationStatus.Success, result.Status);

            // Wait for followers to receive the P0 consumer entry.
            await WaitForConditionAsync(
                () => received.Count(r => r.partitionId == RaftSystemConfig.SystemPartition
                                          && r.logType == "consumer-type") >= 2,
                TestContext.Current.CancellationToken);

            // The system partition map must still be intact: partition 1 exists and is Active.
            IReadOnlyList<RaftPartitionRange> map = p0Leader.GetPartitionMap();
            Assert.Contains(map, r => r.PartitionId == 1 && r.State == RaftPartitionState.Active);

            // _RaftSystem entries must NOT have been delivered to the consumer callback.
            Assert.DoesNotContain(received, r => r.logType == RaftSystemConfig.RaftLogType);
        }
        finally
        {
            await n1.LeaveCluster(true);
            await n2.LeaveCluster(true);
            await n3.LeaveCluster(true);
        }
    }

    /// <summary>
    /// After a node restarts, consumer P0 entries are re-delivered via OnLogRestored during
    /// WAL replay, and the system partition map is intact on the restarted node.
    /// Uses a SQLite WAL so data survives the RaftManager.Dispose() / re-open cycle.
    /// </summary>
    [Fact]
    public async Task ConsumerEntryOnP0_RedeliveredViaOnLogRestored_AfterNodeRestart()
    {
        // Use a fixed temp path per node so we can reopen the WAL after restart.
        string tmpDir = Path.Combine(Path.GetTempPath(), $"shared-p0-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmpDir);

        try
        {
            InMemoryCommunication comm = new();

            // First boot: start all three nodes with SQLite WAL.
            IWAL wal1 = new SqliteWAL(tmpDir, "node1", logger, syncWrites: false);
            IWAL wal2 = new SqliteWAL(tmpDir, "node2", logger, syncWrites: false);
            IWAL wal3 = new SqliteWAL(tmpDir, "node3", logger, syncWrites: false);

            RaftManager n1 = MakeNode(1, comm, wal1, ["localhost:8002", "localhost:8003"]);
            RaftManager n2 = MakeNode(2, comm, wal2, ["localhost:8001", "localhost:8003"]);
            RaftManager n3 = MakeNode(3, comm, wal3, ["localhost:8001", "localhost:8002"]);

            comm.SetNodes(new Dictionary<string, IRaft>
            {
                { "localhost:8001", n1 },
                { "localhost:8002", n2 },
                { "localhost:8003", n3 },
            });

            await n1.UpdateNodes();
            await n2.UpdateNodes();
            await n3.UpdateNodes();

            await Task.WhenAll(
                n1.JoinCluster(TestContext.Current.CancellationToken),
                n2.JoinCluster(TestContext.Current.CancellationToken),
                n3.JoinCluster(TestContext.Current.CancellationToken));

            await WaitForAnyLeader([n1, n2, n3], 1, TestContext.Current.CancellationToken);

            // Find and use the P0 leader to replicate a consumer entry.
            IRaft? p0Leader = await GetLeader(RaftSystemConfig.SystemPartition, [n1, n2, n3]);
            Assert.NotNull(p0Leader);

            byte[] payload = "restore-test-data"u8.ToArray();
            RaftReplicationResult result = await p0Leader.ReplicateLogs(
                RaftSystemConfig.SystemPartition,
                "consumer-type",
                payload,
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.True(result.Success);

            // Wait for all three nodes to persist the entry before we stop node3.
            await WaitForConditionAsync(
                () => n1.WalAdapter.GetMaxLog(RaftSystemConfig.SystemPartition) >= result.LogIndex
                   && n2.WalAdapter.GetMaxLog(RaftSystemConfig.SystemPartition) >= result.LogIndex
                   && n3.WalAdapter.GetMaxLog(RaftSystemConfig.SystemPartition) >= result.LogIndex,
                TestContext.Current.CancellationToken);

            // Stop node3 — this disposes node3's WAL, but the SQLite file stays on disk.
            await n3.LeaveCluster(true);

            // Reopen node3's WAL from the same path and restart the node.
            IWAL wal3Reopen = new SqliteWAL(tmpDir, "node3", logger, syncWrites: false);
            RaftManager n3Restart = MakeNode(3, comm, wal3Reopen, ["localhost:8001", "localhost:8002"]);

            // Update the communication layer so the other nodes reach the restarted instance.
            comm.SetNodes(new Dictionary<string, IRaft>
            {
                { "localhost:8001", n1 },
                { "localhost:8002", n2 },
                { "localhost:8003", n3Restart },
            });

            // Capture what OnLogRestored delivers on the restarted node.
            ConcurrentBag<(int partitionId, string? logType, byte[] data)> restored = [];
            n3Restart.OnLogRestored += (pid, log) =>
            {
                restored.Add((pid, log.LogType, log.LogData ?? []));
                return Task.FromResult(true);
            };

            // Track OnRestoreFinished so we know when WAL replay is complete for P0.
            TaskCompletionSource p0RestoreDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
            n3Restart.OnRestoreFinished += pid =>
            {
                if (pid == RaftSystemConfig.SystemPartition)
                    p0RestoreDone.TrySetResult();
            };

            await n3Restart.UpdateNodes();
            await n3Restart.JoinCluster(TestContext.Current.CancellationToken);

            // Wait for restore to finish for P0 on the restarted node.
            await p0RestoreDone.Task.WaitAsync(
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // The consumer entry must have been re-delivered via OnLogRestored on P0.
            Assert.Contains(restored, r =>
                r.partitionId == RaftSystemConfig.SystemPartition
                && r.logType == "consumer-type"
                && r.data.SequenceEqual(payload));

            // _RaftSystem entries must never reach the consumer OnLogRestored callback.
            Assert.DoesNotContain(restored, r => r.logType == RaftSystemConfig.RaftLogType);

            // The partition map must be intact on the restarted node.
            await WaitForConditionAsync(
                () =>
                {
                    IReadOnlyList<RaftPartitionRange> map = n3Restart.GetPartitionMap();
                    return map.Any(r => r.PartitionId == 1 && r.State == RaftPartitionState.Active);
                },
                TestContext.Current.CancellationToken);

            await n1.LeaveCluster(true);
            await n2.LeaveCluster(true);
            await n3Restart.LeaveCluster(true);
        }
        finally
        {
            if (Directory.Exists(tmpDir))
                Directory.Delete(tmpDir, recursive: true);
        }
    }

    /// <summary>
    /// P0 is permanently protected: CreatePartitionAsync, RemovePartitionAsync,
    /// SplitPartitionAsync, and MergePartitionsAsync all throw RaftException when the
    /// system partition (id 0) is the target, regardless of whether the node is the
    /// P0 leader.
    /// </summary>
    [Fact]
    public async Task P0LifecycleOperations_AllRejectedWithRaftException()
    {
        using IWAL wal1 = new InMemoryWAL(logger);
        using IWAL wal2 = new InMemoryWAL(logger);
        using IWAL wal3 = new InMemoryWAL(logger);

        (RaftManager n1, RaftManager n2, RaftManager n3, _) =
            await AssembleCluster(wal1, wal2, wal3);

        try
        {
            IRaft[] nodes = [n1, n2, n3];
            IRaft? p0Leader = await GetLeader(RaftSystemConfig.SystemPartition, nodes);
            Assert.NotNull(p0Leader);

            // Create P0 must throw.
            await Assert.ThrowsAsync<RaftException>(() =>
                p0Leader.CreatePartitionAsync(
                    RaftSystemConfig.SystemPartition,
                    ct: TestContext.Current.CancellationToken));

            // Remove P0 must throw.
            await Assert.ThrowsAsync<RaftException>(() =>
                p0Leader.RemovePartitionAsync(
                    RaftSystemConfig.SystemPartition,
                    ct: TestContext.Current.CancellationToken));

            // Split P0 as source must throw.
            await Assert.ThrowsAsync<RaftException>(() =>
                p0Leader.SplitPartitionAsync(
                    RaftSystemConfig.SystemPartition,
                    ct: TestContext.Current.CancellationToken));

            // Merge with P0 as survivor must throw.
            await Assert.ThrowsAsync<RaftException>(() =>
                p0Leader.MergePartitionsAsync(
                    survivorPartitionId: RaftSystemConfig.SystemPartition,
                    sourcePartitionId: 1,
                    ct: TestContext.Current.CancellationToken));

            // Merge with P0 as source must throw.
            await Assert.ThrowsAsync<RaftException>(() =>
                p0Leader.MergePartitionsAsync(
                    survivorPartitionId: 1,
                    sourcePartitionId: RaftSystemConfig.SystemPartition,
                    ct: TestContext.Current.CancellationToken));
        }
        finally
        {
            await n1.LeaveCluster(true);
            await n2.LeaveCluster(true);
            await n3.LeaveCluster(true);
        }
    }
}
