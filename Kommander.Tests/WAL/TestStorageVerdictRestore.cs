using System.Collections.Concurrent;
using System.Text;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.WAL;

/// <summary>
/// A whole node restarting on a RocksDB directory whose on-disk shape carries a storage verdict —
/// the way a fault campaign leaves it — and what the node does about it:
/// <list type="bullet">
///   <item>rows missing below the acknowledged-presence watermark (a synced tail truncated after its
///   fsync): the partition reports <see cref="RaftPartitionStorageState.LostAcknowledgedEntries"/>
///   and, as the only voter, never elects itself; with
///   <see cref="LostAcknowledgedEntriesPolicy.AcceptAndLog"/> it runs, and the rewritten watermark
///   keeps a second restart healthy;</item>
///   <item>an application floor below the compaction floor: the partition refuses consumer replay,
///   reports <see cref="RaftPartitionStorageState.ApplicationFloorBelowCompaction"/>, and never
///   elects itself; a floor at the boundary replays and leads normally.</item>
/// </list>
/// Each shape is written straight to a standalone WAL, exactly as the crash-matrix tests do.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestStorageVerdictRestore
{
    private const int UserPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestStorageVerdictRestore()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    [Fact]
    public async Task LostAcknowledgedEntries_SingleVoter_IsQuarantinedUnderTheDefaultPolicy()
    {
        string dir = CreateTempDir();
        try
        {
            SeedRows(dir, committedThrough: 7);
            using (RocksDbWAL seed = new(dir, "node1", logger))
                Assert.True(seed.SetAcknowledgedPresence(UserPartition, 10)); // acknowledged through 10, holds through 7

            RaftManager node = MakeNode(dir, LostAcknowledgedEntriesPolicy.Quarantine);
            try
            {
                await JoinAsync(node);

                RaftPartitionStorageHealth health = await WaitForVerdictAsync(node);
                Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, health.State);
                Assert.Equal(10, health.AcknowledgedThrough);
                Assert.Equal(7, health.PresentThrough);

                // The only voter, yet it never elects itself: there is nobody to replicate past the watermark.
                await Task.Delay(TestTimeouts.Scale(1500), TestContext.Current.CancellationToken);
                Assert.False(await node.AmILeaderQuick(UserPartition));
                Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, node.GetPartitionStorageHealth(UserPartition)?.State);
            }
            finally
            {
                node.Dispose();
            }
        }
        finally
        {
            DeleteTempDir(dir);
        }
    }

    [Fact]
    public async Task LostAcknowledgedEntries_AcceptAndLog_RunsAndRewritesTheWatermark()
    {
        string dir = CreateTempDir();
        try
        {
            SeedRows(dir, committedThrough: 7);
            using (RocksDbWAL seed = new(dir, "node1", logger))
                Assert.True(seed.SetAcknowledgedPresence(UserPartition, 10));

            RaftManager accepting = MakeNode(dir, LostAcknowledgedEntriesPolicy.AcceptAndLog);
            try
            {
                await JoinAsync(accepting);
                await WaitForConditionAsync(() => accepting.AmILeaderQuick(UserPartition).GetAwaiter().GetResult(), TestContext.Current.CancellationToken);
                Assert.Equal(RaftPartitionStorageState.Healthy, accepting.GetPartitionStorageHealth(UserPartition)?.State);
            }
            finally
            {
                accepting.Dispose();
            }

            // The watermark was rewritten to the surviving log, so the default policy finds nothing on the next start.
            RaftManager strict = MakeNode(dir, LostAcknowledgedEntriesPolicy.Quarantine);
            try
            {
                await JoinAsync(strict);
                await WaitForConditionAsync(() => strict.AmILeaderQuick(UserPartition).GetAwaiter().GetResult(), TestContext.Current.CancellationToken);
                Assert.Equal(RaftPartitionStorageState.Healthy, strict.GetPartitionStorageHealth(UserPartition)?.State);
            }
            finally
            {
                strict.Dispose();
            }
        }
        finally
        {
            DeleteTempDir(dir);
        }
    }

    [Fact]
    public async Task ApplicationFloorBelowCompaction_RefusesReplay_AndNeverSelfElects()
    {
        string dir = CreateTempDir();
        try
        {
            SeedCompactedLog(dir); // rows 12..15 survive above a checkpoint at 11; 1..10 compacted

            ConcurrentBag<long> restored = [];
            RaftManager node = MakeNode(dir, LostAcknowledgedEntriesPolicy.Quarantine, new FixedFloorProvider(3));
            node.OnLogRestored += (pid, log) =>
            {
                if (pid == UserPartition) restored.Add(log.Id);
                return Task.FromResult(true);
            };

            try
            {
                await JoinAsync(node);

                RaftPartitionStorageHealth health = await WaitForVerdictAsync(node);
                Assert.Equal(RaftPartitionStorageState.ApplicationFloorBelowCompaction, health.State);

                await Task.Delay(TestTimeouts.Scale(1500), TestContext.Current.CancellationToken);
                Assert.Empty(restored);
                Assert.False(await node.AmILeaderQuick(UserPartition));
            }
            finally
            {
                node.Dispose();
            }
        }
        finally
        {
            DeleteTempDir(dir);
        }
    }

    /// <summary>
    /// The boundary: a floor one below the compaction floor means the first entry the application lacks
    /// is the first one the WAL still holds. Nothing is missing, replay runs, the node leads.
    /// </summary>
    [Theory]
    [InlineData(10)]
    [InlineData(-1)]
    public async Task ApplicationFloorAtOrAboveTheBoundary_ReplaysAndLeads(long floor)
    {
        string dir = CreateTempDir();
        try
        {
            SeedCompactedLog(dir);

            ConcurrentBag<long> restored = [];
            RaftManager node = MakeNode(dir, LostAcknowledgedEntriesPolicy.Quarantine, new FixedFloorProvider(floor));
            node.OnLogRestored += (pid, log) =>
            {
                if (pid == UserPartition) restored.Add(log.Id);
                return Task.FromResult(true);
            };

            try
            {
                await JoinAsync(node);
                await WaitForConditionAsync(() => node.AmILeaderQuick(UserPartition).GetAwaiter().GetResult(), TestContext.Current.CancellationToken);

                Assert.Equal(RaftPartitionStorageState.Healthy, node.GetPartitionStorageHealth(UserPartition)?.State);
                Assert.Equal(new long[] { 12, 13, 14, 15 }, restored.OrderBy(id => id).ToArray());
            }
            finally
            {
                node.Dispose();
            }
        }
        finally
        {
            DeleteTempDir(dir);
        }
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    private sealed class FixedFloorProvider(long floor) : IApplicationDurabilityProvider
    {
        public long GetDurablyAppliedIndex(int partitionId) => partitionId == UserPartition ? floor : -1;
    }

    private void SeedRows(string dir, long committedThrough)
    {
        using RocksDbWAL wal = new(dir, "node1", logger);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(UserPartition, Rows(1, committedThrough, RaftLogType.Committed))]));
    }

    /// <summary>Committed 1..10, checkpoint 11, committed 12..15, then compaction below the checkpoint (floor 11).</summary>
    private void SeedCompactedLog(string dir)
    {
        using RocksDbWAL wal = new(dir, "node1", logger);

        List<RaftLog> batch = Rows(1, 10, RaftLogType.Committed);
        batch.Add(new RaftLog { Id = 11, Term = 1, Type = RaftLogType.CommittedCheckpoint, LogType = "checkpoint", LogData = [] });
        batch.AddRange(Rows(12, 15, RaftLogType.Committed));
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(UserPartition, batch)]));

        (RaftOperationStatus status, _) = wal.CompactLogsOlderThan(UserPartition, 11, 1000, 1000);
        Assert.Equal(RaftOperationStatus.Success, status);
        Assert.Equal(11, ((IWAL)wal).GetCompactionFloor(UserPartition));
    }

    private static List<RaftLog> Rows(long from, long to, RaftLogType type)
    {
        List<RaftLog> rows = [];
        for (long id = from; id <= to; id++)
            rows.Add(new RaftLog { Id = id, Term = 1, Type = type, LogType = "verdict-test", LogData = Encoding.UTF8.GetBytes($"row-{id}") });
        return rows;
    }

    private RaftManager MakeNode(string dir, LostAcknowledgedEntriesPolicy policy, IApplicationDurabilityProvider? provider = null)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
            LostAcknowledgedEntriesPolicy = policy,
            ApplicationDurabilityProvider = provider,
        };

        return new RaftManager(
            config,
            new StaticDiscovery([]),
            new RocksDbWAL(dir, "node1", logger, syncWrites: true),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            logger);
    }

    private static async Task JoinAsync(RaftManager node)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TestTimeouts.Scale(TimeSpan.FromSeconds(20)));
        await node.UpdateNodes();
        await node.JoinCluster(cts.Token);
    }

    private static async Task<RaftPartitionStorageHealth> WaitForVerdictAsync(RaftManager node)
    {
        RaftPartitionStorageHealth? health = null;
        await WaitForConditionAsync(() =>
        {
            health = node.GetPartitionStorageHealth(UserPartition);
            return health is not null && health.State != RaftPartitionStorageState.Restoring;
        }, TestContext.Current.CancellationToken);
        return health!;
    }

    private static async Task WaitForConditionAsync(Func<bool> condition, CancellationToken cancellationToken, int timeoutSeconds = 15)
    {
        timeoutSeconds = TestTimeouts.Scale(timeoutSeconds);
        for (int i = 0; i < timeoutSeconds * 40; i++)
        {
            if (condition())
                return;
            await Task.Delay(25, cancellationToken);
        }
        throw new TimeoutException("Condition not met in time.");
    }

    private static string CreateTempDir()
    {
        string dir = Path.Combine(Path.GetTempPath(), $"kommander-storage-verdict-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void DeleteTempDir(string dir)
    {
        try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); }
        catch { /* best-effort */ }
    }
}
