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
/// path equals the log it holds. One follower is crashed and its log shortened, and restarted while
/// the leader's heartbeats are paused, so the verdict can be observed before the repair. The restarted
/// follower reports <see cref="RaftPartitionStorageState.LostAcknowledgedEntries"/>; once heartbeats
/// resume, the leader's backfill replicates past the loss, the quarantine lifts on its own, and every
/// acknowledged entry is applied on the recovered node.
/// <list type="bullet">
///   <item>The first test shortens the log through the API and re-raises the watermark: the shape
///   where the rows are gone but the metadata survives, which the watermark comparison detects.</item>
///   <item>The second truncates the live RocksDB log file on disk, the shape the kommander-jepsen
///   <c>lost-tail</c> fault leaves. The watermark goes with the rows there, so the verdict comes from the
///   sync ledger instead; the same leader (same term) repairs it through the frontier, the partition
///   records the reconciliation, and a restart after the repair is healthy.</item>
/// </list>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestLostAcknowledgedEntriesCluster
{
    private const int UserPartition = 1;

    private readonly ILogger<IRaft> logger;

    public TestLostAcknowledgedEntriesCluster(ITestOutputHelper output)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddXUnit(output).SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    [Fact]
    public Task Follower_LostAcknowledgedTail_IsQuarantined_ThenRepairedByTheLeader() => RunAsync(realFileDamage: false);

    [Fact]
    public Task Follower_LiveLogTruncatedOnDisk_IsQuarantinedByTheSyncLedger_ThenRepairedByTheLeader() => RunAsync(realFileDamage: true);

    private async Task RunAsync(bool realFileDamage)
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

            // The live log sizes before the user writes: the on-disk cut below lands inside them.
            long[] logSizeBeforeWrites = new long[3];
            for (int i = 0; i < 3; i++)
                logSizeBeforeWrites[i] = NewestLog(tmpDir, i + 1).Length;

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

            // Crash the follower and materialise the lost tail.
            live[victimIdx]!.Dispose();
            live[victimIdx] = null;

            long cut = victimMax - lostTail;
            if (realFileDamage)
            {
                // The live RocksDB log cut back into the middle of the user writes: rows, watermark,
                // commit markers and hard state above the cut are all gone together.
                FileInfo log = NewestLog(tmpDir, victimNodeId);
                long before = logSizeBeforeWrites[victimIdx];
                using FileStream stream = File.Open(log.FullName, FileMode.Open, FileAccess.Write);
                Assert.True(stream.Length > before, "the user writes did not reach the victim's live log");
                stream.SetLength(before + (stream.Length - before) / 2);
            }
            else
            {
                // Rows above the cut gone, watermark intact: the API-level shape.
                using RocksDbWAL wal = new(tmpDir, $"node{victimNodeId}", logger, syncWrites: true);
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
            if (realFileDamage)
            {
                Assert.Contains("lost synced writes", health.Reason);
                Assert.Equal(-1, health.AcknowledgedThrough);
                Assert.True(health.PresentThrough < victimMax, "the on-disk cut removed no user row");
                // The watermark went with the rows: on its own it would have said nothing.
                Assert.True(restarted.WalAdapter.GetAcknowledgedPresence(UserPartition) <= health.PresentThrough);
            }
            else
            {
                Assert.Equal(acknowledged, health.AcknowledgedThrough);
                Assert.Equal(cut, health.PresentThrough);
            }

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

            if (realFileDamage)
            {
                // The system partition shared the engine and was quarantined too; it wrote nothing
                // after the cut, so its only evidence is the leader's heartbeat confirming its frontier.
                try
                {
                    await WaitForConditionAsync(
                        () => restarted.GetPartitionStorageHealth(0)?.State == RaftPartitionStorageState.Healthy,
                        TestContext.Current.CancellationToken,
                        timeoutSeconds: 25);
                }
                catch (TimeoutException)
                {
                    RaftPartitionStorageHealth? system = restarted.GetPartitionStorageHealth(0);
                    string nodes = string.Join(" | ", live.Select((n, i) =>
                    {
                        if (n is null) return $"node{i + 1}: down";
                        n.WalAdapter.TryGetHardState(0, out long hsTerm, out string? hsVote);
                        return $"node{i + 1}: leader0={n.AmILeaderQuick(0).AsTask().GetAwaiter().GetResult()} max0={n.WalAdapter.GetMaxLog(0)} logTerm0={n.WalAdapter.GetCurrentTerm(0)} hard=({hsTerm},{hsVote}) health0={n.GetPartitionStorageHealth(0)?.State}";
                    }));
                    Assert.Fail($"the system partition never lifted its quarantine: {system?.State} — {system?.Reason}; victim=node{victimNodeId}; {nodes}");
                }

                // The reconciliation was recorded: a restart after the repair is healthy at once,
                // for the user partition and for the system partition that shared the engine.
                live[victimIdx]!.Dispose();
                live[victimIdx] = null;

                RaftManager recovered = MakeNode(victimNodeId, comm, new RocksDbWAL(tmpDir, $"node{victimNodeId}", logger, syncWrites: true), peersById[victimIdx]);
                live[victimIdx] = recovered;
                SetNetwork(comm, live);
                await recovered.UpdateNodes();
                await recovered.JoinCluster(TestContext.Current.CancellationToken);

                await WaitForConditionAsync(() =>
                {
                    RaftPartitionStorageHealth? user = recovered.GetPartitionStorageHealth(UserPartition);
                    RaftPartitionStorageHealth? system = recovered.GetPartitionStorageHealth(0);
                    return user is not null && user.State != RaftPartitionStorageState.Restoring
                        && system is not null && system.State != RaftPartitionStorageState.Restoring;
                }, TestContext.Current.CancellationToken);

                Assert.Equal(RaftPartitionStorageState.Healthy, recovered.GetPartitionStorageHealth(UserPartition)?.State);
                Assert.Equal(RaftPartitionStorageState.Healthy, recovered.GetPartitionStorageHealth(0)?.State);
            }
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

    private static FileInfo NewestLog(string tmpDir, int nodeId) =>
        new DirectoryInfo(Path.Combine(tmpDir, $"node{nodeId}")).GetFiles("*.log").OrderByDescending(f => f.Name, StringComparer.Ordinal).First();

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
