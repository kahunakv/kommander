
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
/// End-to-end acceptance tests for the P0 delta-consumer / SystemState snapshot path.
/// Three scenarios are covered:
/// <list type="bullet">
///   <item><b>Catch-up:</b> a follower that enters below the compaction floor is repaired via
///     <see cref="IRaftSystemStateTransfer.ExportPartitionState"/> →
///     <see cref="IRaftSystemStateTransfer.ImportPartitionState"/> → <c>CommittedCheckpoint</c>
///     and then converges by receiving subsequent P0 deltas through normal backfill.</item>
///   <item><b>Normal replay:</b> a follower that never fell behind receives all P0 delta entries
///     via <see cref="IRaft.OnReplicationReceived"/> backfill — the snapshot path is not taken
///     and existing replication is not regressed.</item>
///   <item><b>Negative:</b> with no <see cref="IRaftSystemStateTransfer"/> registered, P0
///     compaction still runs to the checkpoint (no regression in the compaction gate) and a
///     below-floor joiner is fast-failed via <see cref="System.InvalidOperationException"/>
///     instead of spinning to the 60-second timeout.</item>
/// </list>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestDeltaConsumerEndToEnd
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    // ── Test 1: Catch-up ──────────────────────────────────────────────────────

    /// <summary>
    /// A fourth node (n4) that joins after the P0 WAL has been compacted is repaired via the
    /// SystemState snapshot path, then converges by receiving subsequent P0 deltas through the
    /// normal backfill pipeline.
    /// </summary>
    [Fact]
    public async Task DeltaConsumer_CatchUp_ExportImportConverges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        InMemoryWAL innerWal1 = new(logger);
        InMemoryWAL innerWal2 = new(logger);
        InMemoryWAL innerWal3 = new(logger);
        InMemoryWAL innerWal4 = new(logger);

        CompactableWAL wal1 = new(innerWal1);
        CompactableWAL wal2 = new(innerWal2);
        CompactableWAL wal3 = new(innerWal3);

        // n1–n3: existing 3-node cluster with compacted P0 WALs.
        RaftManager n1 = BuildNode(comm, "localhost", 8521, 1, ["localhost:8522", "localhost:8523"], wal1, logger, initialPartitions: 1);
        RaftManager n2 = BuildNode(comm, "localhost", 8522, 2, ["localhost:8521", "localhost:8523"], wal2, logger, initialPartitions: 1);
        RaftManager n3 = BuildNode(comm, "localhost", 8523, 3, ["localhost:8521", "localhost:8522"], wal3, logger, initialPartitions: 1);
        // n4: joins later with an empty WAL — below the P0 compaction floor.
        RaftManager n4 = BuildNode(comm, "localhost", 8524, 4,
            ["localhost:8521", "localhost:8522", "localhost:8523"],
            innerWal4, logger, initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8521"] = n1,
            ["localhost:8522"] = n2,
            ["localhost:8523"] = n3,
            ["localhost:8524"] = n4,
        });

        RecordingSystemTransfer transfer = new();
        n1.RegisterSystemStateTransfer(transfer);
        n2.RegisterSystemStateTransfer(transfer);
        n3.RegisterSystemStateTransfer(transfer);
        n4.RegisterSystemStateTransfer(transfer);

        // Collect post-snapshot P0 deltas received by n4 via OnReplicationReceived.
        ConcurrentBag<long> n4PostSnapDeltas = [];

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager p0Leader = await FindLeaderForPartitionAsync([n1, n2, n3], partitionId: 0, ct);

            // Replicate 5 deltas and commit a checkpoint so CompactableWAL returns empty below the floor.
            for (int i = 0; i < 5; i++)
                await p0Leader.ReplicateLogs(0, "delta", [(byte)i], cancellationToken: ct);

            RaftReplicationResult cpResult = await p0Leader.ReplicateCheckpoint(0, cancellationToken: ct);
            Assert.Equal(RaftOperationStatus.Success, cpResult.Status);

            // Wait for the checkpoint to appear in at least one voter WAL.
            await WaitForAsync(() =>
                wal1.GetLastCheckpoint(0) > 0 || wal2.GetLastCheckpoint(0) > 0 || wal3.GetLastCheckpoint(0) > 0, ct);

            long floor = Math.Max(wal1.GetLastCheckpoint(0),
                Math.Max(wal2.GetLastCheckpoint(0), wal3.GetLastCheckpoint(0)));
            Assert.True(floor > 0, $"P0 floor must be > 0 before n4 joins; got {floor}");

            // Register n4's callback BEFORE starting the join so post-snapshot backfill is captured.
            n4.OnReplicationReceived += (partId, log) =>
            {
                if (partId == 0 && log.LogType == "delta")
                    n4PostSnapDeltas.Add(log.Id);
                return Task.FromResult(true);
            };

            // Start n4's join in the background.  Full Voter promotion is not required here — we
            // only need the Learner membership entry committed so the P0 leader heartbeats n4 and
            // triggers the SystemState snapshot.
            using CancellationTokenSource joinCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task joinTask = Task.Run(() => n4.JoinCluster(["localhost:8521"], joinCts.Token), ct);

            // Phase 1: snapshot repair — wait for ImportPartitionState to fire on n4.
            await WaitForAsync(() => transfer.ImportCount > 0, ct, timeoutMs: 15_000);

            Assert.True(transfer.ImportCount > 0,
                "ImportPartitionState must have been called at least once — snapshot path was not taken");
            Assert.True(innerWal4.GetLastCheckpoint(0) > 0,
                "n4's P0 WAL must have a CommittedCheckpoint at the snapshot index");

            // Phase 2: convergence — replicate 2 more deltas and verify n4 receives them via backfill.
            // This proves the replication pipeline resumed after the snapshot.
            p0Leader = await FindLeaderForPartitionAsync([n1, n2, n3], partitionId: 0, ct);
            for (int i = 0; i < 2; i++)
                await p0Leader.ReplicateLogs(0, "delta", [(byte)(10 + i)], cancellationToken: ct);

            await WaitForAsync(() => n4PostSnapDeltas.Count >= 2, ct, timeoutMs: 10_000);

            Assert.True(n4PostSnapDeltas.Count >= 2,
                $"n4 must receive post-snapshot P0 deltas via backfill; only got {n4PostSnapDeltas.Count}");

            await joinCts.CancelAsync();
            try { await joinTask.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* cancellation / timeout expected */ }
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();
        }
    }

    // ── Test 2: Normal replay ─────────────────────────────────────────────────

    /// <summary>
    /// A follower that joins after deltas were replicated — but with no compaction — receives all
    /// P0 delta entries via <see cref="IRaft.OnReplicationReceived"/> through the normal backfill
    /// pipeline, without touching the snapshot path at all.
    /// </summary>
    [Fact]
    public async Task DeltaConsumer_NormalReplay_FollowerReceivesAllDeltas()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int DeltaCount = 5;
        InMemoryCommunication comm = new();

        RaftManager n1 = BuildNode(comm, "localhost", 8531, 1, ["localhost:8532", "localhost:8533"], new InMemoryWAL(logger), logger, initialPartitions: 1);
        RaftManager n2 = BuildNode(comm, "localhost", 8532, 2, ["localhost:8531", "localhost:8533"], new InMemoryWAL(logger), logger, initialPartitions: 1);
        RaftManager n3 = BuildNode(comm, "localhost", 8533, 3, ["localhost:8531", "localhost:8532"], new InMemoryWAL(logger), logger, initialPartitions: 1);
        RaftManager n4 = BuildNode(comm, "localhost", 8534, 4,
            ["localhost:8531", "localhost:8532", "localhost:8533"],
            new InMemoryWAL(logger), logger, initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8531"] = n1,
            ["localhost:8532"] = n2,
            ["localhost:8533"] = n3,
            ["localhost:8534"] = n4,
        });

        // Collect all P0 delta ids delivered to n4 via OnReplicationReceived (backfill).
        ConcurrentBag<long> n4Received = [];
        n4.OnReplicationReceived += (partId, log) =>
        {
            if (partId == 0 && log.LogType == "delta")
                n4Received.Add(log.Id);
            return Task.FromResult(true);
        };

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager p0Leader = await FindLeaderForPartitionAsync([n1, n2, n3], partitionId: 0, ct);

            // Replicate all deltas BEFORE n4 joins; n4 must receive them via backfill.
            List<long> replicatedIndexes = [];
            for (int i = 0; i < DeltaCount; i++)
            {
                RaftReplicationResult r = await p0Leader.ReplicateLogs(0, "delta", [(byte)i], cancellationToken: ct);
                Assert.Equal(RaftOperationStatus.Success, r.Status);
                replicatedIndexes.Add(r.LogIndex);
            }

            // n4 joins after all deltas exist.  No compaction → backfill is the only path.
            // JoinCluster waits for Voter promotion, which requires P0 backfill to complete.
            await n4.JoinCluster(["localhost:8531"], ct);
            await WaitForAsync(() => n4.IsInitialized, ct);

            // OnReplicationReceived fires during backfill (before promotion), so all entries must
            // already be present — the WaitForAsync is a safety guard.
            await WaitForAsync(() => n4Received.Count >= DeltaCount, ct);

            HashSet<long> expected = [..replicatedIndexes];
            HashSet<long> delivered = [..n4Received];
            foreach (long idx in expected)
                Assert.Contains(idx, delivered);
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();
        }
    }

    // ── Test 3: Negative / no regression ──────────────────────────────────────

    /// <summary>
    /// When no <see cref="IRaftSystemStateTransfer"/> is registered, P0 compaction still runs to
    /// the checkpoint (the compaction gate is not widened) and a below-floor joiner receives a
    /// fast <see cref="System.InvalidOperationException"/> via <c>NotifyJoinBlocked</c> rather
    /// than spinning to the 60-second timeout — identical to the pre-change build's behaviour.
    /// </summary>
    [Fact]
    public async Task DeltaConsumer_NoTransfer_BelowFloor_JoinFails()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        InMemoryWAL innerWal1 = new(logger);
        InMemoryWAL innerWal2 = new(logger);
        InMemoryWAL innerWal3 = new(logger);

        CompactableWAL wal1 = new(innerWal1);
        CompactableWAL wal2 = new(innerWal2);
        CompactableWAL wal3 = new(innerWal3);

        RaftManager n1 = BuildNode(comm, "localhost", 8541, 1, ["localhost:8542", "localhost:8543"], wal1, logger, initialPartitions: 1);
        RaftManager n2 = BuildNode(comm, "localhost", 8542, 2, ["localhost:8541", "localhost:8543"], wal2, logger, initialPartitions: 1);
        RaftManager n3 = BuildNode(comm, "localhost", 8543, 3, ["localhost:8541", "localhost:8542"], wal3, logger, initialPartitions: 1);
        RaftManager n4 = BuildNode(comm, "localhost", 8544, 4,
            ["localhost:8541", "localhost:8542", "localhost:8543"],
            new InMemoryWAL(logger), logger, initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8541"] = n1,
            ["localhost:8542"] = n2,
            ["localhost:8543"] = n3,
            ["localhost:8544"] = n4,
        });

        // Intentionally NO RegisterSystemStateTransfer — this is the no-transfer negative case.

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager p0Leader = await FindLeaderForPartitionAsync([n1, n2, n3], partitionId: 0, ct);

            // Replicate deltas and commit a checkpoint to create a non-zero compaction floor.
            for (int i = 0; i < 5; i++)
                await p0Leader.ReplicateLogs(0, "delta", [(byte)i], cancellationToken: ct);

            RaftReplicationResult cpResult = await p0Leader.ReplicateCheckpoint(0, cancellationToken: ct);
            Assert.Equal(RaftOperationStatus.Success, cpResult.Status);

            // Wait for the checkpoint to appear in at least one voter — proving compaction ran to
            // the checkpoint even without a SystemStateTransfer registered.
            await WaitForAsync(() =>
                wal1.GetLastCheckpoint(0) > 0 || wal2.GetLastCheckpoint(0) > 0 || wal3.GetLastCheckpoint(0) > 0, ct);

            long floor = Math.Max(wal1.GetLastCheckpoint(0),
                Math.Max(wal2.GetLastCheckpoint(0), wal3.GetLastCheckpoint(0)));
            Assert.True(floor > 0,
                "P0 checkpoint (compaction floor) must be recorded even without a SystemStateTransfer");

            // n4 joins below the floor with no transfer registered.  The coordinator must detect
            // this is un-repairable and deliver a fast-fail rather than spinning to the timeout.
            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => n4.JoinCluster(["localhost:8541"], ct));

            Assert.Contains("permanently blocked", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    private sealed class RecordingSystemTransfer : IRaftSystemStateTransfer
    {
        private int _importCount;
        public int ImportCount => Volatile.Read(ref _importCount);

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([0xDE, 0xAD, 0xBE, 0xEF]));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref _importCount);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Wraps <see cref="InMemoryWAL"/> to simulate a compacted floor: once a partition receives a
    /// <see cref="RaftLogType.CommittedCheckpoint"/>, <see cref="ReadLogsRange"/> returns an empty
    /// list for any start index at or below the checkpoint, causing the snapshot-install trigger to fire.
    /// </summary>
    private sealed class CompactableWAL : IWAL
    {
        private readonly InMemoryWAL _inner;
        private readonly ConcurrentDictionary<int, long> _floors = new();

        public CompactableWAL(InMemoryWAL inner) => _inner = inner;

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            RaftOperationStatus result = _inner.Write(logs);
            foreach ((int partId, List<RaftLog> partitionLogs) in logs)
                foreach (RaftLog log in partitionLogs)
                    if (log.Type == RaftLogType.CommittedCheckpoint)
                        _floors.AddOrUpdate(partId, log.Id, (_, cur) => Math.Max(cur, log.Id));
            return result;
        }

        public long GetLastCheckpoint(int partitionId) =>
            _floors.TryGetValue(partitionId, out long cp) ? cp : _inner.GetLastCheckpoint(partitionId);

        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
        {
            if (_floors.TryGetValue(partitionId, out long floor) && startLogIndex <= floor)
                return [];
            return _inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        }

        public List<RaftLog> ReadLogs(int partitionId)              => _inner.ReadLogs(partitionId);
        public long GetMaxLog(int partitionId)                       => _inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId)                  => _inner.GetCurrentTerm(partitionId);
        public int CountPersistedLogs(int partitionId)               => _inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId)               => _inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key)                       => _inner.GetMetaData(key);
        public bool SetMetaData(string key, string value)            => _inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
            int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
            _inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => _inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => _inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => _inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() => _inner.Dispose();
    }
}
