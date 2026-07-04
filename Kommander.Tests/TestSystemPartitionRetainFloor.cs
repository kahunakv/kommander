
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
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Tests for three safety properties of system-partition delta logs:
/// <list type="bullet">
///   <item>Retain floor: <see cref="RaftWriteAhead.SetMinRetainIndex"/> clips compaction below the
///     floor even when the checkpoint is higher, so un-snapshotted deltas survive a compaction pass.</item>
///   <item>Callback index: <see cref="RaftLog.Id"/> delivered via
///     <see cref="IRaft.OnReplicationReceived"/> equals the WAL-committed index for P0 user entries.</item>
///   <item>Live catch-up: a P0 follower that enters below the compaction floor with a registered
///     <see cref="IRaftSystemStateTransfer"/> is repaired via <c>ExportPartitionState</c> +
///     <c>ImportPartitionState</c> and its WAL is seeded with a <c>CommittedCheckpoint</c>.</item>
/// </list>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestSystemPartitionRetainFloor
{
    private readonly ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

    // ── Test 1: Retain floor ──────────────────────────────────────────────────

    /// <summary>
    /// Compaction with a retain floor stops at the floor rather than the checkpoint when the floor
    /// is lower: entries below the floor are removed; entries at or above the floor (up to the
    /// checkpoint) survive even though the checkpoint is higher than the floor.
    /// </summary>
    [Fact]
    public async Task RetainFloor_BelowCheckpoint_CompactionHonorsFloor()
    {
        const int partitionId = 0;
        const long retainFloor = 6;   // entries < 6 should be removed
        const long checkpoint  = 10;  // CommittedCheckpoint sits above the retain floor

        InMemoryWAL wal = new(logger);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
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

            // Seed the WAL with Committed entries 1–(checkpoint-1) plus a CommittedCheckpoint.
            List<RaftLog> logs = [];
            for (long i = 1; i < checkpoint; i++)
                logs.Add(new RaftLog { Id = i, Type = RaftLogType.Committed, Term = 1, LogType = "delta" });
            logs.Add(new RaftLog { Id = checkpoint, Type = RaftLogType.CommittedCheckpoint, Term = 1 });

            Assert.Equal(RaftOperationStatus.Success, wal.Write([(partitionId, logs)]));

            // Verify setup: the checkpoint is recognized before compaction.
            Assert.Equal(checkpoint, wal.GetLastCheckpoint(partitionId));

            // Set retain floor below the checkpoint.  effectiveFloor = min(checkpoint=10, floor=6) = 6.
            writeAhead.SetMinRetainIndex(retainFloor);
            Assert.Equal(retainFloor, writeAhead.MinRetainIndex);

            writeAhead.Compact();
            await writeAhead.WaitForCompactionIdleAsync();

            // CompactLogsOlderThan removes entries with id < effectiveFloor (6).
            // Entries 6, 7, 8, 9 (Committed) and 10 (CommittedCheckpoint) must survive.
            List<RaftLog> remaining = wal.ReadLogsRange(partitionId, 1, 100);
            Assert.NotEmpty(remaining);
            Assert.True(remaining[0].Id >= retainFloor,
                $"First remaining entry Id={remaining[0].Id} must be >= retain floor {retainFloor}");
            Assert.All(remaining, l =>
                Assert.True(l.Id >= retainFloor,
                    $"Entry Id={l.Id} survived compaction below the retain floor {retainFloor}"));

            // The CommittedCheckpoint must still be present so GetLastCheckpoint is valid.
            Assert.Equal(checkpoint, wal.GetLastCheckpoint(partitionId));

            // Entry exactly at the floor must survive.
            List<RaftLog> atFloor = wal.ReadLogsRange(partitionId, retainFloor, 1);
            Assert.Single(atFloor);
            Assert.Equal(retainFloor, atFloor[0].Id);
        }
        finally
        {
            manager.Dispose();
        }
    }

    // ── Test 2: Callback index ────────────────────────────────────────────────

    /// <summary>
    /// The <see cref="RaftLog.Id"/> delivered to <see cref="IRaft.OnReplicationReceived"/> on
    /// followers equals the WAL-committed index returned by the leader's <c>ReplicateLogs</c> for
    /// user-type entries on the system partition (P0).
    /// </summary>
    [Fact]
    public async Task CallbackIndex_P0Delta_IdMatchesWalIndex()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        RaftManager n1 = BuildNode(comm, "localhost", 8501, 1, ["localhost:8502", "localhost:8503"], new InMemoryWAL(logger), logger);
        RaftManager n2 = BuildNode(comm, "localhost", 8502, 2, ["localhost:8501", "localhost:8503"], new InMemoryWAL(logger), logger);
        RaftManager n3 = BuildNode(comm, "localhost", 8503, 3, ["localhost:8501", "localhost:8502"], new InMemoryWAL(logger), logger);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8501"] = n1,
            ["localhost:8502"] = n2,
            ["localhost:8503"] = n3,
        });

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            // Find P0 leader; it may differ from the user-partition leader.
            RaftManager p0Leader = await FindLeaderForPartitionAsync([n1, n2, n3], partitionId: 0, ct);
            List<RaftManager> p0Followers = [n1, n2, n3];
            p0Followers.Remove(p0Leader);

            // Collect (log.Id, index-from-replicate) pairs from followers.
            ConcurrentBag<(long CallbackId, long ReplicateIndex)> observations = [];
            List<long> replicateIndexes = [];

            foreach (RaftManager follower in p0Followers)
                follower.OnReplicationReceived += (partId, log) =>
                {
                    if (partId == 0 && log.LogType == "delta")
                        observations.Add((log.Id, -1)); // ReplicateIndex filled after the fact
                    return Task.FromResult(true);
                };

            // Replicate 3 user-type entries on P0.
            for (int i = 0; i < 3; i++)
            {
                RaftReplicationResult result = await p0Leader.ReplicateLogs(
                    partitionId: 0, type: "delta", data: [(byte)i],
                    cancellationToken: ct);

                Assert.Equal(RaftOperationStatus.Success, result.Status);
                replicateIndexes.Add(result.LogIndex);
            }

            // Wait for both followers to fire the callback for all 3 entries.
            await WaitForAsync(() => observations.Count >= p0Followers.Count * 3, ct);

            // Every callback Id must be one of the WAL indexes returned by ReplicateLogs.
            HashSet<long> expectedIds = [.. replicateIndexes];
            foreach ((long callbackId, _) in observations)
                Assert.Contains(callbackId, expectedIds);

            // Each expected index must appear in at least one callback (i.e., no Id was dropped).
            HashSet<long> deliveredIds = [.. observations.Select(o => o.CallbackId)];
            foreach (long expected in expectedIds)
                Assert.Contains(expected, deliveredIds);
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose();
        }
    }

    // ── Test 3: Live catch-up ─────────────────────────────────────────────────

    /// <summary>
    /// A fourth node (n4) that joins a cluster whose P0 WAL has been compacted is repaired via
    /// the <see cref="IRaftSystemStateTransfer"/> path: the leader calls
    /// <see cref="IRaftSystemStateTransfer.ExportPartitionState"/> and sends the chunks as
    /// <see cref="SnapshotKind.SystemState"/>; n4 calls
    /// <see cref="IRaftSystemStateTransfer.ImportPartitionState"/> and its WAL receives a
    /// <see cref="RaftLogType.CommittedCheckpoint"/> entry at the snapshot index.
    /// </summary>
    [Fact]
    public async Task P0_BelowFloor_SystemStateTransfer_RepairsFollower()
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

        // n1–n3: 3-node cluster with 1 user partition (P0 + P1).
        RaftManager n1 = BuildNode(comm, "localhost", 8511, 1, ["localhost:8512", "localhost:8513"], wal1, logger, initialPartitions: 1);
        RaftManager n2 = BuildNode(comm, "localhost", 8512, 2, ["localhost:8511", "localhost:8513"], wal2, logger, initialPartitions: 1);
        RaftManager n3 = BuildNode(comm, "localhost", 8513, 3, ["localhost:8511", "localhost:8512"], wal3, logger, initialPartitions: 1);
        // n4: joins later with an empty WAL — will be below the P0 floor.
        RaftManager n4 = BuildNode(comm, "localhost", 8514, 4,
            ["localhost:8511", "localhost:8512", "localhost:8513"],
            innerWal4, logger, initialPartitions: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8511"] = n1,
            ["localhost:8512"] = n2,
            ["localhost:8513"] = n3,
            ["localhost:8514"] = n4,
        });

        RecordingSystemTransfer transfer = new();
        n1.RegisterSystemStateTransfer(transfer);
        n2.RegisterSystemStateTransfer(transfer);
        n3.RegisterSystemStateTransfer(transfer);
        n4.RegisterSystemStateTransfer(transfer);

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager p0Leader = await FindLeaderForPartitionAsync([n1, n2, n3], partitionId: 0, ct);

            // Replicate a few entries on P0 so the leader has a non-zero committed index.
            for (int i = 0; i < 5; i++)
                await p0Leader.ReplicateLogs(partitionId: 0, type: "delta", data: [(byte)i], cancellationToken: ct);

            // Commit a checkpoint so CompactableWAL.GetLastCheckpoint returns > 0.
            RaftReplicationResult cpResult = await p0Leader.ReplicateCheckpoint(partitionId: 0, cancellationToken: ct);
            Assert.Equal(RaftOperationStatus.Success, cpResult.Status);

            // Wait for the checkpoint to propagate to at least one voter WAL.
            await WaitForAsync(() =>
                wal1.GetLastCheckpoint(0) > 0 ||
                wal2.GetLastCheckpoint(0) > 0 ||
                wal3.GetLastCheckpoint(0) > 0,
                ct);

            long floor = Math.Max(
                wal1.GetLastCheckpoint(0),
                Math.Max(wal2.GetLastCheckpoint(0), wal3.GetLastCheckpoint(0)));
            Assert.True(floor > 0, $"P0 compaction floor should be > 0, was {floor}");

            // n4 joins with an empty P0 WAL.  Start the join in the background — we only need it
            // running long enough to commit the Learner membership entry and receive a heartbeat from
            // the P0 leader, which triggers the SystemState snapshot.  Full Voter promotion is not
            // required for this assertion and would block on P1 lag (n4 has no P1 partition).
            using CancellationTokenSource joinCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task joinTask = Task.Run(() => n4.JoinCluster(["localhost:8511"], joinCts.Token), ct);

            // Wait for the SystemState snapshot to be delivered to n4 and ImportPartitionState to fire.
            await WaitForAsync(() => transfer.ImportCount > 0, ct, timeoutMs: 15_000);

            await joinCts.CancelAsync();
            try { await joinTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken); } catch { /* cancellation / timeout expected */ }

            Assert.True(transfer.ImportCount > 0,
                "ImportPartitionState must have been called at least once on the learner");
            Assert.True(innerWal4.GetLastCheckpoint(0) > 0,
                "n4's P0 WAL must have a CommittedCheckpoint after snapshot install");
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
    /// list for any start index at or below the checkpoint so the snapshot-install trigger fires.
    /// </summary>
    private sealed class CompactableWAL : IWAL
    {
        private readonly InMemoryWAL inner;
        private readonly ConcurrentDictionary<int, long> _floors = new();

        public CompactableWAL(InMemoryWAL inner) => this.inner = inner;

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            RaftOperationStatus result = inner.Write(logs);
            foreach ((int partId, List<RaftLog> partitionLogs) in logs)
                foreach (RaftLog log in partitionLogs)
                    if (log.Type == RaftLogType.CommittedCheckpoint)
                        _floors.AddOrUpdate(partId, log.Id, (_, cur) => Math.Max(cur, log.Id));
            return result;
        }

        public long GetLastCheckpoint(int partitionId) =>
            _floors.TryGetValue(partitionId, out long cp) ? cp : inner.GetLastCheckpoint(partitionId);

        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue)
        {
            if (_floors.TryGetValue(partitionId, out long floor) && startLogIndex <= floor)
                return [];
            return inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        }

        public List<RaftLog> ReadLogs(int partitionId)              => inner.ReadLogs(partitionId);
        public long GetMaxLog(int partitionId)                       => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId)                  => inner.GetCurrentTerm(partitionId);
        public int CountPersistedLogs(int partitionId)               => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId)               => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key)                       => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value)            => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
            int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) =>
            inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() => inner.Dispose();
    }
}
