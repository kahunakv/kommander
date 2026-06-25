using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.WAL;

/// <summary>
/// Task 9 of the WAL double-fsync spec — the cluster half of the crash matrix: a follower that lost
/// its lazy commit markers on a crash must re-converge to the full committed log via the leader's
/// re-supply on reconnect, losing no acknowledged write.
///
/// <para>The crash is staged faithfully. A live 3-node cluster (fast path on) replicates and durably
/// commits N entries, then one follower is <b>crashed</b> via <see cref="RaftManager.Dispose"/> — an
/// abrupt local stop that, unlike a graceful <c>LeaveCluster</c>, does <b>not</b> announce a membership
/// removal, so the leader keeps treating the endpoint as a peer (exactly as in a real crash). The
/// crashed node's persistent WAL is then rewritten to demote the tail's commit markers back to
/// <see cref="RaftLogType.Proposed"/> — the rows a crash that dropped the un-fsynced markers leaves —
/// and the node is restarted on that WAL.</para>
///
/// <para>Recovery sets the restarted node's commit frontier to the contiguous committed prefix below the
/// demoted tail (the safe lower bound) and applies only that prefix. On its heartbeat acks it advertises
/// that lower frontier; the leader detects the regression and backfills the still-committed tail, which
/// the node applies. The assertion: every acked entry is eventually applied on the recovered node
/// (restored ∪ re-supplied == 1..N), the restore never promoted the Proposed tail, and the leader's log
/// is intact.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class RecoveryReSupplyClusterTests
{
    private readonly ILogger<IRaft> logger;

    public RecoveryReSupplyClusterTests()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    private const int UserPartition = 1;

    [Fact]
    public async Task Follower_LostLazyMarkers_ReConvergesViaLeaderReSupply_NoAckedWriteLost()
    {
        const int total = 20;
        const int keepCommitted = 5; // demote ids 6..20 back to Proposed on the crashed follower

        string tmpDir = Path.Combine(Path.GetTempPath(), $"kommander-resupply-{Guid.NewGuid():N}");
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
                Assert.Equal(i, r.LogIndex);
            }

            await WaitForConditionAsync(
                () => live.All(n => n!.WalAdapter.GetMaxLog(UserPartition) >= total),
                TestContext.Current.CancellationToken);

            // Pick a follower (never the partition leader) as the crash victim.
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

            // Crash it: abrupt local stop, NO graceful membership removal. The leader keeps the endpoint.
            live[victimIdx]!.Dispose();
            live[victimIdx] = null;

            // Simulate the crash window: demote the tail's commit markers (ids keepCommitted+1..total)
            // back to Proposed, preserving every other field — exactly what a lost lazy fsync leaves.
            DemoteCommitMarkers(tmpDir, $"node{victimNodeId}", from: keepCommitted + 1, to: total);

            // Restart on the same WAL.
            RaftManager restarted = MakeNode(victimNodeId, comm, new RocksDbWAL(tmpDir, $"node{victimNodeId}", logger, syncWrites: true), peersById[victimIdx]);

            ConcurrentBag<long> restoredIds = [];
            ConcurrentBag<long> receivedIds = [];
            restarted.OnLogRestored += (pid, log) =>
            {
                if (pid == UserPartition) restoredIds.Add(log.Id);
                return Task.FromResult(true);
            };
            restarted.OnReplicationReceived += (pid, log) =>
            {
                if (pid == UserPartition) receivedIds.Add(log.Id);
                return Task.FromResult(true);
            };

            live[victimIdx] = restarted;
            SetNetwork(comm, live);

            await restarted.UpdateNodes();
            await restarted.JoinCluster(TestContext.Current.CancellationToken);

            // The recovered follower must eventually apply EVERY acked entry: the contiguous prefix via
            // restore, and the demoted tail via the leader's re-supply. No acked write is lost.
            await WaitForConditionAsync(
                () =>
                {
                    HashSet<long> applied = [.. restoredIds, .. receivedIds];
                    return Enumerable.Range(1, total).All(id => applied.Contains(id));
                },
                TestContext.Current.CancellationToken,
                timeoutSeconds: 25);

            // The restored prefix never exceeded the contiguous committed floor (no Proposed tail was
            // ever treated as committed on restore); the tail arrived strictly via re-supply.
            Assert.DoesNotContain(restoredIds, id => id > keepCommitted);

            // The leader's committed log is intact.
            Assert.True(leader.WalAdapter.GetMaxLog(UserPartition) >= total);
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

    // ── Harness ────────────────────────────────────────────────────────────

    private static void SetNetwork(InMemoryCommunication comm, RaftManager?[] live)
    {
        Dictionary<string, IRaft> network = [];
        for (int i = 0; i < live.Length; i++)
            if (live[i] is not null)
                network[$"localhost:800{i + 1}"] = live[i]!;
        comm.SetNodes(network);
    }

    /// <summary>
    /// Rewrites ids <paramref name="from"/>..<paramref name="to"/> for the partition from
    /// <see cref="RaftLogType.Committed"/> back to <see cref="RaftLogType.Proposed"/>, preserving term,
    /// payload and time. Models the on-disk effect of a crash that dropped the lazy commit markers.
    /// </summary>
    private void DemoteCommitMarkers(string tmpDir, string revision, long from, long to)
    {
        using IWAL wal = new RocksDbWAL(tmpDir, revision, logger, syncWrites: true);

        List<RaftLog> demoted = [];
        foreach (RaftLog log in wal.ReadLogsRange(UserPartition, from))
        {
            if (log.Id < from || log.Id > to || log.Type != RaftLogType.Committed)
                continue;
            log.Type = RaftLogType.Proposed;
            demoted.Add(log);
        }

        Assert.NotEmpty(demoted);
        Assert.Equal(RaftOperationStatus.Success, wal.Write([(UserPartition, demoted)]));
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
            EnableQuiescence = false,
            EndElectionTimeout = 250,
            // Re-supply the recovered follower's regressed tail even for a small gap.
            BackfillThreshold = 1,
            WalSingleFsyncCommit = true,
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
        for (int i = 0; i < timeoutSeconds * 40; i++)
        {
            if (condition())
                return;
            await Task.Delay(25, cancellationToken);
        }
        throw new TimeoutException("Condition not met in time.");
    }
}
