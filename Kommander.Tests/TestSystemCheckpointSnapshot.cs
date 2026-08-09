
using System.Collections.Concurrent;
using System.Text.Json;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.System.Protos;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Tests for the system-configuration snapshot embedded in P0 checkpoint entries
/// (<see cref="RaftSystemConfig.CheckpointLogType"/>). The membership roster and partition map
/// are committed to P0 once (or rarely), near the start of the log; once WAL compaction advances
/// past those records, a restarting node's replay would otherwise reconstruct an empty roster —
/// masked as Voter by the pre-seed fallback, losing learner/eviction history and wedging user
/// partitions. Embedding the config map in every P0 checkpoint makes replay-from-checkpoint
/// always able to rebuild it.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSystemCheckpointSnapshot
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    // ── Test 1: serializer round-trip ─────────────────────────────────────────

    [Fact]
    public void CheckpointSnapshot_SerializeRoundTrip_PreservesEntries()
    {
        ConcurrentDictionary<string, string> config = new();
        config["members"] = "{\"MembershipVersion\":3}";
        config["partitions"] = "[{\"PartitionId\":1}]";

        byte[]? payload = RaftSystemCoordinatorHelpers.SerializeCheckpointSnapshot(config);
        Assert.NotNull(payload);

        RaftSystemCheckpointSnapshot parsed = RaftSystemCoordinatorHelpers.UnserializeCheckpointSnapshot(payload);
        Assert.Equal(2, parsed.Entries.Count);
        Assert.Contains(parsed.Entries, e => e.Key == "members" && e.Value == config["members"]);
        Assert.Contains(parsed.Entries, e => e.Key == "partitions" && e.Value == config["partitions"]);

        // Empty map produces no payload so the checkpoint stays payload-free on greenfield.
        Assert.Null(RaftSystemCoordinatorHelpers.SerializeCheckpointSnapshot(new ConcurrentDictionary<string, string>()));
    }

    // ── Test 2: restore rebuilds the roster from a checkpoint payload ─────────

    /// <summary>
    /// A P0 WAL whose only surviving entry is a CommittedCheckpoint carrying a config snapshot
    /// (all delta entries below it compacted away) must restore the membership roster at its
    /// checkpoint-time version.
    /// </summary>
    [Fact]
    public async Task Restore_P0CheckpointWithSnapshot_RebuildsRoster()
    {
        const int partitionId = RaftSystemConfig.SystemPartition;

        InMemoryWAL wal = new(logger);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9600,
            InitialPartitions = 0,
        };

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        try
        {
            ((FairReadScheduler)manager.ReadScheduler).Start();

            RaftPartition partition = new(
                manager, wal,
                partitionId: partitionId,
                startRange: 0, endRange: 0,
                NullLogger<IRaft>.Instance);

            RaftWriteAhead writeAhead = new(manager, _ => { }, partition, wal);

            // Roster v3: local node is a Voter alongside two peers.
            ClusterMembership roster = new()
            {
                MembershipVersion = 3,
                Members =
                [
                    new() { Endpoint = "localhost:9600", NodeId = 1, Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
                    new() { Endpoint = "localhost:9601", NodeId = 2, Role = ClusterMemberRole.Voter, JoinedVersion = 1 },
                    new() { Endpoint = "localhost:9602", NodeId = 3, Role = ClusterMemberRole.Learner, JoinedVersion = 3 },
                ]
            };

            ConcurrentDictionary<string, string> snapshotConfig = new();
            snapshotConfig[RaftSystemConfigKeys.Members] = JsonSerializer.Serialize(roster);

            byte[]? payload = RaftSystemCoordinatorHelpers.SerializeCheckpointSnapshot(snapshotConfig);
            Assert.NotNull(payload);

            // The WAL contains ONLY the checkpoint (everything below was compacted away).
            List<RaftLog> logs =
            [
                new()
                {
                    Id = 42, Term = 5, Type = RaftLogType.CommittedCheckpoint,
                    LogType = RaftSystemConfig.CheckpointLogType, LogData = payload
                }
            ];
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, logs)]));

            IReadOnlyList<RaftLog> restoreLogs = await writeAhead.LoadRestoreLogsAsync();
            await writeAhead.CompleteRestoreAsync(restoreLogs);

            // Flush the coordinator channel so ConfigCheckpointRestored has been applied.
            await manager.SystemCoordinator.DrainAsync().WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            ClusterMembership restored = manager.GetMembership();
            Assert.Equal(3, restored.MembershipVersion);
            Assert.Equal(3, restored.Members.Count);
            Assert.Equal(ClusterMemberRole.Voter, manager.LocalRole);
            Assert.Contains(restored.Members, m => m.Endpoint == "localhost:9602" && m.Role == ClusterMemberRole.Learner);
        }
        finally
        {
            manager.Dispose();
        }
    }

    // ── Test 3: the P0 leader embeds the config snapshot in checkpoints ───────

    /// <summary>
    /// After the initial roster is seeded, a P0 checkpoint proposed by the leader must carry the
    /// system-configuration snapshot (members and partitions) in its LogData.
    /// </summary>
    [Fact]
    public async Task Checkpoint_P0Leader_EmbedsSystemConfigSnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryWAL wal = new(logger);
        RaftManager n1 = BuildNode(new InMemoryCommunication(), "localhost", 9610, 1, [], wal, logger, initialPartitions: 1);

        try
        {
            await n1.JoinCluster(ct);
            await WaitForAsync(() => n1.IsInitialized, ct);
            await WaitForAsync(() => n1.GetMembership().MembershipVersion >= 1, ct);

            RaftReplicationResult result = await n1.ReplicateCheckpoint(RaftSystemConfig.SystemPartition, ct);
            Assert.Equal(RaftOperationStatus.Success, result.Status);

            // The single-node commit path flips ProposedCheckpoint → CommittedCheckpoint in place.
            await WaitForAsync(() => wal.GetLastCheckpoint(RaftSystemConfig.SystemPartition) > 0, ct);

            List<RaftLog> logs = wal.ReadLogs(RaftSystemConfig.SystemPartition);
            RaftLog? checkpoint = logs.FirstOrDefault(l => l.Type == RaftLogType.CommittedCheckpoint);
            Assert.NotNull(checkpoint);
            Assert.Equal(RaftSystemConfig.CheckpointLogType, checkpoint.LogType);
            Assert.NotNull(checkpoint.LogData);
            Assert.NotEmpty(checkpoint.LogData);

            RaftSystemCheckpointSnapshot snapshot = RaftSystemCoordinatorHelpers.UnserializeCheckpointSnapshot(checkpoint.LogData);
            Assert.Contains(snapshot.Entries, e => e.Key == RaftSystemConfigKeys.Members);
            Assert.Contains(snapshot.Entries, e => e.Key == RaftSystemConfigKeys.Partitions);

            RaftSystemMessage members = snapshot.Entries.First(e => e.Key == RaftSystemConfigKeys.Members);
            ClusterMembership? roster = JsonSerializer.Deserialize<ClusterMembership>(members.Value);
            Assert.NotNull(roster);
            Assert.True(roster.MembershipVersion >= 1);
            Assert.Contains(roster.Members, m => m.Endpoint == "localhost:9610");
        }
        finally
        {
            n1.Dispose();
        }
    }

    // ── Test 4: end-to-end restart after compaction ───────────────────────────

    /// <summary>
    /// The full incident scenario: a 3-node cluster seeds its roster, checkpoints P0, and a
    /// follower's P0 WAL is compacted past the original members record. The follower then
    /// restarts in complete isolation (no gossip, no reachable peers, no possibility of reseed
    /// since it cannot become leader alone) and must still reconstruct the roster — version and
    /// all three members — purely from the checkpoint payload in its own WAL.
    /// </summary>
    [Fact]
    public async Task Restart_AfterP0Compaction_RestoresRosterFromCheckpoint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        InMemoryWAL innerWal3 = new(logger);
        NonDisposingWAL wal3 = new(innerWal3);

        RaftManager n1 = BuildNode(comm, "localhost", 9621, 1, ["localhost:9622", "localhost:9623"], new InMemoryWAL(logger), logger);
        RaftManager n2 = BuildNode(comm, "localhost", 9622, 2, ["localhost:9621", "localhost:9623"], new InMemoryWAL(logger), logger);
        RaftManager n3 = BuildNode(comm, "localhost", 9623, 3, ["localhost:9621", "localhost:9622"], wal3, logger);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:9621"] = n1,
            ["localhost:9622"] = n2,
            ["localhost:9623"] = n3,
        });

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);
            await WaitForAsync(() =>
                n1.GetMembership().MembershipVersion >= 1 &&
                n2.GetMembership().MembershipVersion >= 1 &&
                n3.GetMembership().MembershipVersion >= 1, ct);

            RaftManager p0Leader = await FindLeaderForPartitionAsync([n1, n2, n3], RaftSystemConfig.SystemPartition, ct);

            RaftReplicationResult cpResult = await p0Leader.ReplicateCheckpoint(RaftSystemConfig.SystemPartition, ct);
            Assert.Equal(RaftOperationStatus.Success, cpResult.Status);

            // Wait for the checkpoint to reach n3's WAL as CommittedCheckpoint.
            await WaitForAsync(() => innerWal3.GetLastCheckpoint(RaftSystemConfig.SystemPartition) > 0, ct);
            long checkpointId = innerWal3.GetLastCheckpoint(RaftSystemConfig.SystemPartition);

            // Compact n3's P0 WAL past the seed members record (everything below the checkpoint).
            (RaftOperationStatus compactStatus, _) = innerWal3.CompactLogsOlderThan(
                RaftSystemConfig.SystemPartition, checkpointId, 100);
            Assert.Equal(RaftOperationStatus.Success, compactStatus);

            // Discriminator guard: the original committed members delta must be gone from the WAL.
            List<RaftLog> survivors = innerWal3.ReadLogs(RaftSystemConfig.SystemPartition);
            Assert.DoesNotContain(survivors, l =>
                l.Type == RaftLogType.Committed && l.LogType == RaftSystemConfig.RaftLogType && l.Id < checkpointId);

            n1.Dispose();
            n2.Dispose();
            n3.Dispose();

            // Restart n3 in isolation: a fresh communication fabric that knows no peers, so the
            // roster can only come from its own WAL. It also cannot reseed (never leader alone).
            InMemoryCommunication isolatedComm = new();
            RaftManager restarted = BuildNode(isolatedComm, "localhost", 9623, 3,
                ["localhost:9621", "localhost:9622"], wal3, logger);
            isolatedComm.SetNodes(new Dictionary<string, IRaft> { ["localhost:9623"] = restarted });

            try
            {
                // JoinCluster cannot complete without quorum — run it in the background just to
                // drive partition startup and WAL restore, then observe the restored roster.
                using CancellationTokenSource joinCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                Task joinTask = Task.Run(() => restarted.JoinCluster(joinCts.Token), ct);

                await WaitForAsync(() => restarted.GetMembership().MembershipVersion >= 1, ct);

                ClusterMembership restored = restarted.GetMembership();
                Assert.Equal(3, restored.Members.Count);
                Assert.Contains(restored.Members, m => m.Endpoint == "localhost:9621");
                Assert.Contains(restored.Members, m => m.Endpoint == "localhost:9622");
                Assert.Contains(restored.Members, m => m.Endpoint == "localhost:9623");
                Assert.Equal(ClusterMemberRole.Voter, restarted.LocalRole);

                await joinCts.CancelAsync();
                try { await joinTask.WaitAsync(TimeSpan.FromSeconds(5), ct); } catch { /* cancellation expected */ }
            }
            finally
            {
                restarted.Dispose();
            }
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static RaftManager BuildNode(
        InMemoryCommunication comm,
        string host, int port, int nodeId,
        string[] peers,
        IWAL wal,
        ILogger<IRaft> logger,
        int initialPartitions = 1)
    {
        RaftConfiguration cfg = new()
        {
            NodeId = nodeId, Host = host, Port = port,
            InitialPartitions = initialPartitions,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(200),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 300,
            BackfillThreshold = 0,
            MaxBackfillEntriesPerRound = 128,
        };
        return new RaftManager(cfg,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            wal, comm, new HybridLogicalClock(), logger);
    }

    private static async Task WaitForAsync(Func<bool> cond, CancellationToken ct, int timeoutMs = 15_000)
    {
        timeoutMs = TestTimeouts.Scale(timeoutMs);
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    private static async Task<RaftManager> FindLeaderForPartitionAsync(
        RaftManager[] nodes, int partitionId, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 15_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
                if (await n.AmILeaderQuick(partitionId))
                    return n;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"No leader for partition {partitionId} within 15 s.");
    }

    // ── stubs ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Delegates everything to the inner WAL but ignores Dispose, so the same in-memory WAL
    /// instance can survive a <see cref="RaftManager"/> restart (the manager disposes its WAL).
    /// </summary>
    private sealed class NonDisposingWAL : IWAL
    {
        private readonly InMemoryWAL inner;

        public NonDisposingWAL(InMemoryWAL inner) => this.inner = inner;

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => inner.Write(logs);
        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
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
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() { /* survives manager restarts */ }
    }
}
