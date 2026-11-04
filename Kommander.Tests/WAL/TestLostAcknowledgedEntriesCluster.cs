using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.WAL;

/// <summary>
/// The cluster half of the lost-acknowledged-entries quarantine. A live three-node cluster commits
/// entries; the acknowledged-presence watermark each follower persisted through the real scheduler
/// path equals the log it holds. One follower is crashed and its log shortened below its watermark —
/// the shape a synced tail truncated after its fsync leaves — and restarted while the leader's
/// heartbeats are paused, so the verdict can be observed before the repair. The restarted follower
/// reports <see cref="RaftPartitionStorageState.LostAcknowledgedEntries"/>; once heartbeats resume,
/// the leader's backfill replicates past the watermark, the quarantine lifts on its own, and every
/// acknowledged entry is applied on the recovered node.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLostAcknowledgedEntriesCluster
{
    private const int UserPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestLostAcknowledgedEntriesCluster()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    [Fact]
    public async Task Follower_LostAcknowledgedTail_IsQuarantined_ThenRepairedByTheLeader()
    {
        const int total = 20;
        const int lostTail = 8;

        string tmpDir = Path.Combine(Path.GetTempPath(), $"kommander-lost-acked-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmpDir);

        InMemoryCommunication comm = new();
        RaftManager?[] live = new RaftManager?[3];
        string[][] peersById =
        [
            ["localhost:8002", "localhost:8003"],
            ["localhost:8001", "localhost:8003"],
            ["localhost:8001", "localhost:8002"],
        ];

        try
        {
            for (int i = 0; i < 3; i++)
                live[i] = MakeNode(i + 1, comm, new RocksDbWAL(tmpDir, $"node{i + 1}", logger, syncWrites: true), peersById[i]);

            SetNetwork(comm, live);

            await Task.WhenAll(live.Select(n => n!.UpdateNodes()));
            await Task.WhenAll(live.Select(n => n!.JoinCluster(TestContext.Current.CancellationToken)));

            await WaitForAnyLeader(live!, UserPartition, TestContext.Current.CancellationToken);
            IRaft leader = await GetLeader(UserPartition, live!) ?? throw new InvalidOperationException("no leader");

            byte[] data = "Hello World"u8.ToArray();
            for (int i = 1; i <= total; i++)
            {
                RaftReplicationResult r = await leader.ReplicateLogs(UserPartition, "Greeting", data, cancellationToken: TestContext.Current.CancellationToken);
                Assert.True(r.Success, $"replicate {i} failed: {r.Status}");
            }

            long leaderMax = leader.WalAdapter.GetMaxLog(UserPartition);
            await WaitForConditionAsync(
                () => live.All(n => n!.WalAdapter.GetMaxLog(UserPartition) >= leaderMax),
                TestContext.Current.CancellationToken);

            int victimIdx = -1;
            for (int i = 0; i < 3; i++)
            {
                if (!ReferenceEquals(live[i], leader) && !await live[i]!.AmILeaderQuick(UserPartition))
                {
                    victimIdx = i;
                    break;
                }
            }
            Assert.InRange(victimIdx, 0, 2);
            int victimNodeId = victimIdx + 1;

            // The production path persisted the watermark alongside the rows: it equals the contiguous log.
            long victimMax = live[victimIdx]!.WalAdapter.GetMaxLog(UserPartition);
            long acknowledged = -1;
            await WaitForConditionAsync(() =>
            {
                acknowledged = live[victimIdx]!.WalAdapter.GetAcknowledgedPresence(UserPartition);
                return acknowledged >= victimMax;
            }, TestContext.Current.CancellationToken);
            Assert.Equal(victimMax, acknowledged);

            // Crash the follower and materialise the lost tail: rows above the cut gone, watermark intact.
            live[victimIdx]!.Dispose();
            live[victimIdx] = null;

            long cut = victimMax - lostTail;
            using (RocksDbWAL wal = new(tmpDir, $"node{victimNodeId}", logger, syncWrites: true))
            {
                Assert.Equal(RaftOperationStatus.Success, wal.TruncateLogsAfter(UserPartition, cut));
                Assert.True(wal.SetAcknowledgedPresence(UserPartition, acknowledged));
            }

            // Pause the leader's heartbeats so the verdict is observable before the repair.
            Assert.Equal(RaftOperationStatus.Success, await leader.SuspendHeartbeatsAsync(UserPartition, TestContext.Current.CancellationToken));

            RaftManager restarted = MakeNode(victimNodeId, comm, new RocksDbWAL(tmpDir, $"node{victimNodeId}", logger, syncWrites: true), peersById[victimIdx]);

            ConcurrentBag<long> applied = [];
            restarted.OnLogRestored += (pid, log) =>
            {
                if (pid == UserPartition) applied.Add(log.Id);
                return Task.FromResult(true);
            };
            restarted.OnReplicationReceived += (pid, log) =>
            {
                if (pid == UserPartition) applied.Add(log.Id);
                return Task.FromResult(true);
            };

            live[victimIdx] = restarted;
            SetNetwork(comm, live);

            await restarted.UpdateNodes();
            await restarted.JoinCluster(TestContext.Current.CancellationToken);

            RaftPartitionStorageHealth? health = null;
            await WaitForConditionAsync(() =>
            {
                health = restarted.GetPartitionStorageHealth(UserPartition);
                return health is not null && health.State != RaftPartitionStorageState.Restoring;
            }, TestContext.Current.CancellationToken);

            Assert.Equal(RaftPartitionStorageState.LostAcknowledgedEntries, health!.State);
            Assert.Equal(acknowledged, health.AcknowledgedThrough);
            Assert.Equal(cut, health.PresentThrough);

            // Resume: the leader sees the regressed frontier in the acks, backfills past the watermark,
            // and the quarantine lifts by itself.
            Assert.Equal(RaftOperationStatus.Success, await leader.ResumeHeartbeatsAsync(UserPartition, TestContext.Current.CancellationToken));

            await WaitForConditionAsync(
                () => restarted.GetPartitionStorageHealth(UserPartition)?.State == RaftPartitionStorageState.Healthy,
                TestContext.Current.CancellationToken,
                timeoutSeconds: 25);

            await WaitForConditionAsync(
                () => restarted.WalAdapter.GetMaxLog(UserPartition) >= victimMax,
                TestContext.Current.CancellationToken);

            // Every user entry is applied on the recovered node, from the restore or the re-supply.
            HashSet<long> userIds = [.. leader.WalAdapter.ReadLogs(UserPartition).Where(l => l.LogType == "Greeting").Select(l => l.Id)];
            Assert.Equal(total, userIds.Count);
            await WaitForConditionAsync(
                () => userIds.All(applied.Contains),
                TestContext.Current.CancellationToken,
                timeoutSeconds: 25);
        }
        finally
        {
            foreach (RaftManager? n in live)
            {
                try { n?.Dispose(); }
                catch { /* already disposed */ }
            }
            try { if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, recursive: true); }
            catch { /* best-effort */ }
        }
    }

    private static void SetNetwork(InMemoryCommunication comm, RaftManager?[] live)
    {
        Dictionary<string, IRaft> network = [];
        for (int i = 0; i < live.Length; i++)
            if (live[i] is not null)
                network[$"localhost:800{i + 1}"] = live[i]!;
        comm.SetNodes(network);
    }

    private RaftManager MakeNode(int id, InMemoryCommunication communication, IWAL wal, string[] peers)
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
            EnableQuiescence = false,
            EnableCheckQuorum = false,
            BackfillThreshold = 1,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            wal,
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<IRaft?> GetLeader(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (node is not null && await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                return node;
        }
        return null;
    }

    private static async Task WaitForAnyLeader(IRaft[] nodes, int partitionId, CancellationToken cancellationToken)
    {
        for (int i = 0; i < 600; i++)
        {
            foreach (IRaft node in nodes)
                if (node is not null && await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return;
            await Task.Delay(25, cancellationToken);
        }
        throw new TimeoutException($"No leader for partition {partitionId}.");
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
}
