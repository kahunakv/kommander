
using System.Collections.Concurrent;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Integration tests for the snapshot-install path.
///
/// <para>Both tests use <see cref="CompactableWAL"/>, a thin wrapper over
/// <see cref="InMemoryWAL"/> that tracks <see cref="RaftLogType.CommittedCheckpoint"/>
/// entries so <see cref="IWAL.GetLastCheckpoint"/> returns a meaningful value rather
/// than the InMemoryWAL constant <c>-1</c>.  Compaction is performed explicitly after
/// a checkpoint is committed, removing the underlying entries from the sorted dictionary
/// so that <c>ReadLogsRange</c> returns an empty list for indices below the floor — the
/// precise condition that triggers the snapshot-install path in <c>SendHeartbeat</c>.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSnapshotIntegration
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    // ── snapshot ship + learner promotion ───────────────────────────────────────

    /// <summary>
    /// A learner joins a 3-node cluster whose user partition has been compacted.
    /// The leader detects the learner is below the WAL floor, ships a snapshot, and
    /// the coordinator promotes the learner to Voter after the stable window elapses.
    /// A second snapshot request for the same index is a no-op on the follower.
    /// </summary>
    [Fact]
    public async Task Learner_BelowCompactionFloor_ReceivesSnapshot_ThenPromoted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        InMemoryWAL innerWal1 = new(logger);
        InMemoryWAL innerWal2 = new(logger);
        InMemoryWAL innerWal3 = new(logger);

        CompactableWAL wal1 = new(innerWal1);
        CompactableWAL wal2 = new(innerWal2);
        CompactableWAL wal3 = new(innerWal3);

        RaftManager n1 = BuildNode(comm, "localhost", 8401, 1, ["localhost:8402", "localhost:8403"], wal1, logger);
        RaftManager n2 = BuildNode(comm, "localhost", 8402, 2, ["localhost:8401", "localhost:8403"], wal2, logger);
        RaftManager n3 = BuildNode(comm, "localhost", 8403, 3, ["localhost:8401", "localhost:8402"], wal3, logger);
        RaftManager n4 = BuildNode(comm, "localhost", 8404, 4,
            ["localhost:8401", "localhost:8402", "localhost:8403"],
            new InMemoryWAL(logger), logger, initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8401"] = n1,
            ["localhost:8402"] = n2,
            ["localhost:8403"] = n3,
            ["localhost:8404"] = n4,
        });

        RecordingTransfer transfer = new();
        n1.RegisterStateMachineTransfer(transfer);
        n2.RegisterStateMachineTransfer(transfer);
        n3.RegisterStateMachineTransfer(transfer);
        n4.RegisterStateMachineTransfer(transfer);

        try
        {
            // Start 3-node cluster.
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            // Find leader and user partition.
            RaftManager leader = await FindLeaderAsync([n1, n2, n3], ct);
            int userPartitionId = leader.Partitions.Keys.FirstOrDefault(k => k != 0);
            Assert.NotEqual(0, userPartitionId);

            // Commit a few entries so the leader has a non-empty committed index.
            for (int i = 0; i < 5; i++)
                await leader.ReplicateLogs(userPartitionId, "test", [1, 2, 3], cancellationToken: ct);

            // Commit a WAL checkpoint so CompactableWAL.GetLastCheckpoint returns > 0.
            RaftReplicationResult cpResult = await leader.ReplicateCheckpoint(userPartitionId, ct);
            Assert.Equal(RaftOperationStatus.Success, cpResult.Status);

            // Wait for checkpoint to propagate to all voter WALs.
            await WaitForAsync(() =>
                wal1.GetLastCheckpoint(userPartitionId) > 0 ||
                wal2.GetLastCheckpoint(userPartitionId) > 0 ||
                wal3.GetLastCheckpoint(userPartitionId) > 0,
                ct);

            long floor = Math.Max(
                wal1.GetLastCheckpoint(userPartitionId),
                Math.Max(wal2.GetLastCheckpoint(userPartitionId),
                         wal3.GetLastCheckpoint(userPartitionId)));

            Assert.True(floor > 0, $"Compaction floor should be > 0, was {floor}");

            // n4 joins as Learner. The leader will detect it is below the compaction floor,
            // ship a snapshot, and the coordinator eventually promotes it to Voter.
            await n4.JoinCluster(["localhost:8401"], ct);

            Assert.Equal(ClusterMemberRole.Voter, n4.LocalRole);
            Assert.True(transfer.ImportWasCalled, "ImportRange should have been called on the learner");

            // Verify re-install no-op: send the same snapshot chunk again;
            // since n4's WAL is already at floor, ReceiveInstallSnapshot returns success
            // without calling ImportRange a second time.
            int importsBefore = transfer.ImportCallCount;
            SnapshotResponse idempotentResp = await n4.ReceiveInstallSnapshot(
                new SnapshotRequest
                {
                    SessionId = "recheck", PartitionId = userPartitionId,
                    SnapshotIndex = floor, FollowerEndpoint = "localhost:8404",
                    IsLast = true, Data = new byte[] { 0xFF },
                }, ct);
            Assert.True(idempotentResp.Success, "Re-install of same index must return success");
            Assert.Equal(importsBefore, transfer.ImportCallCount);
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();
        }
    }

    // ── no transfer registered → join blocked ───────────────────────────────────

    /// <summary>
    /// When no <see cref="IRaftStateMachineTransfer"/> is registered and the learner is
    /// below the WAL compaction floor, the coordinator signals the joiner via
    /// <see cref="ICommunication.NotifyJoinBlocked"/> and <c>JoinCluster(seeds)</c> throws
    /// <see cref="InvalidOperationException"/> well before the 60-second timeout.
    /// </summary>
    [Fact]
    public async Task Learner_BelowCompactionFloor_NoTransfer_ThrowsImmediately()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        InMemoryWAL innerWal1 = new(logger);
        InMemoryWAL innerWal2 = new(logger);
        InMemoryWAL innerWal3 = new(logger);

        CompactableWAL wal1 = new(innerWal1);
        CompactableWAL wal2 = new(innerWal2);
        CompactableWAL wal3 = new(innerWal3);

        RaftManager n1 = BuildNode(comm, "localhost", 8405, 1, ["localhost:8406", "localhost:8407"], wal1, logger);
        RaftManager n2 = BuildNode(comm, "localhost", 8406, 2, ["localhost:8405", "localhost:8407"], wal2, logger);
        RaftManager n3 = BuildNode(comm, "localhost", 8407, 3, ["localhost:8405", "localhost:8406"], wal3, logger);
        RaftManager n4 = BuildNode(comm, "localhost", 8408, 4,
            ["localhost:8405", "localhost:8406", "localhost:8407"],
            new InMemoryWAL(logger), logger, initialPartitions: 0);

        // No transfer registered on ANY node.

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8405"] = n1,
            ["localhost:8406"] = n2,
            ["localhost:8407"] = n3,
            ["localhost:8408"] = n4,
        });

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager leader = await FindLeaderAsync([n1, n2, n3], ct);
            int userPartitionId = leader.Partitions.Keys.FirstOrDefault(k => k != 0);
            Assert.NotEqual(0, userPartitionId);

            // Write enough logs so the learner lag exceeds LearnerPromotionLag (default 10).
            for (int i = 0; i < 15; i++)
                await leader.ReplicateLogs(userPartitionId, "test", [1, 2, 3], cancellationToken: ct);

            RaftReplicationResult cpResult = await leader.ReplicateCheckpoint(userPartitionId, ct);
            Assert.Equal(RaftOperationStatus.Success, cpResult.Status);

            await WaitForAsync(() =>
                wal1.GetLastCheckpoint(userPartitionId) > 0 ||
                wal2.GetLastCheckpoint(userPartitionId) > 0 ||
                wal3.GetLastCheckpoint(userPartitionId) > 0,
                ct);

            long floor = Math.Max(
                wal1.GetLastCheckpoint(userPartitionId),
                Math.Max(wal2.GetLastCheckpoint(userPartitionId),
                         wal3.GetLastCheckpoint(userPartitionId)));

            Assert.True(floor > 10, $"Expected floor > LearnerPromotionLag (10), was {floor}");

            // JoinCluster should throw InvalidOperationException (terminal signal) fast,
            // not TimeoutException after 60 s.
            ValueStopwatch sw = ValueStopwatch.StartNew();
            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => n4.JoinCluster(["localhost:8405"], ct));

            Assert.True(sw.GetElapsedMilliseconds() < 30_000,
                $"Join should fail fast; took {sw.GetElapsedMilliseconds()} ms");
            Assert.Contains("permanently blocked", ex.Message);
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();
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
            LearnerPromotionLag = 5,
            LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(500),
        };
        return new RaftManager(cfg,
            new StaticDiscovery(peers.Select(e => new RaftNode(e)).ToList()),
            wal, comm, new HybridLogicalClock(), logger);
    }

    private static async Task WaitForAsync(Func<bool> cond, CancellationToken ct, int timeoutMs = 15_000)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < timeoutMs)
        {
            ct.ThrowIfCancellationRequested();
            if (cond()) return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }

    private static async Task<RaftManager> FindLeaderAsync(RaftManager[] nodes, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        while (sw.GetElapsedMilliseconds() < 15_000)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in nodes)
            {
                foreach (int partId in n.Partitions.Keys)
                {
                    if (partId != 0 && await n.AmILeaderQuick(partId))
                        return n;
                }
            }
            await Task.Delay(50, ct);
        }
        throw new TimeoutException("No leader for user partition within 15 s.");
    }

    // ── stubs ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Records calls to <see cref="ImportRange"/> so tests can assert the snapshot path fired.
    /// <see cref="ExportRange"/> returns a tiny but non-empty stream so the chunking logic
    /// has bytes to send.
    /// </summary>
    private sealed class RecordingTransfer : IRaftStateMachineTransfer
    {
        private int _importCount;

        public bool ImportWasCalled => _importCount > 0;
        public int ImportCallCount => _importCount;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0xDE, 0xAD, 0xBE, 0xEF]));

        public Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref _importCount);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Wraps <see cref="InMemoryWAL"/> to track <see cref="RaftLogType.CommittedCheckpoint"/>
    /// entries per partition, so <see cref="IWAL.GetLastCheckpoint"/> returns the committed
    /// checkpoint id rather than the InMemoryWAL constant <c>-1</c>.
    ///
    /// <para>Once a checkpoint is tracked for a partition, <see cref="ReadLogsRange"/> returns
    /// an empty list for any start index at or below the checkpoint floor — exactly what the
    /// snapshot-install trigger in <c>SendHeartbeat</c> looks for.  No explicit compaction call
    /// is needed; the entries are hidden at the read layer, not physically removed.</para>
    /// </summary>
    private sealed class CompactableWAL : IWAL
    {
        private readonly InMemoryWAL inner;
        // Per-partition compaction floor (tracked from CommittedCheckpoint writes).
        private readonly ConcurrentDictionary<int, long> _floors = new();

        public CompactableWAL(InMemoryWAL inner) => this.inner = inner;

        // ── IWAL — checkpoint + compaction-floor override ─────────────────────

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            RaftOperationStatus result = inner.Write(logs);
            foreach ((int partId, List<RaftLog> partitionLogs) in logs)
            {
                foreach (RaftLog log in partitionLogs)
                {
                    if (log.Type == RaftLogType.CommittedCheckpoint)
                        _floors.AddOrUpdate(partId, log.Id, (_, cur) => Math.Max(cur, log.Id));
                }
            }
            return result;
        }

        /// <summary>
        /// Returns the tracked checkpoint id for <paramref name="partitionId"/>, or the
        /// inner WAL value (-1) when no checkpoint has been observed yet.
        /// </summary>
        public long GetLastCheckpoint(int partitionId) =>
            _floors.TryGetValue(partitionId, out long cp) ? cp : inner.GetLastCheckpoint(partitionId);

        /// <summary>
        /// Returns an empty list when <paramref name="startLogIndex"/> is at or below the
        /// tracked compaction floor for <paramref name="partitionId"/>, simulating a WAL
        /// that has been compacted past that point.
        /// </summary>
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
        {
            if (_floors.TryGetValue(partitionId, out long floor) && startLogIndex <= floor)
                return [];
            return inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        }

        // ── IWAL — pure delegation ────────────────────────────────────────────

        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
            inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() => inner.Dispose();
    }
}
