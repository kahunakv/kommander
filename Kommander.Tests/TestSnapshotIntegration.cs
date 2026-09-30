
using System.Collections.Concurrent;
using System.Security.Cryptography;
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
                    // Terminal chunk, so it must carry the digest a real sender would compute over
                    // the staged bytes — here the single chunk is the whole snapshot.
                    SnapshotChecksum = Convert.ToHexString(SHA256.HashData(new byte[] { 0xFF })),
                }, ct);
            Assert.True(idempotentResp.Success, "Re-install of same index must return success");
            Assert.Equal(importsBefore, transfer.ImportCallCount);
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();
        }
    }

    // ── snapshot staged on disk ─────────────────────────────────────────────────

    /// <summary>
    /// A learner whose snapshot staging memory budget is zero stages the whole multi-chunk snapshot in a spill
    /// file, installs it from there — the importer reads the exact exported bytes twice — and is promoted; the
    /// spill file is gone afterwards.
    /// </summary>
    [Fact]
    public async Task Learner_BelowCompactionFloor_InstallsASnapshotStagedOnDisk()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();
        string stagingDirectory = Path.Combine(Path.GetTempPath(), "kommander-staging-" + Guid.NewGuid().ToString("N"));

        CompactableWAL wal1 = new(new InMemoryWAL(logger));
        CompactableWAL wal2 = new(new InMemoryWAL(logger));
        CompactableWAL wal3 = new(new InMemoryWAL(logger));

        RaftManager n1 = BuildNode(comm, "localhost", 8411, 1, ["localhost:8412", "localhost:8413"], wal1, logger);
        RaftManager n2 = BuildNode(comm, "localhost", 8412, 2, ["localhost:8411", "localhost:8413"], wal2, logger);
        RaftManager n3 = BuildNode(comm, "localhost", 8413, 3, ["localhost:8411", "localhost:8412"], wal3, logger);
        RaftManager n4 = BuildNode(comm, "localhost", 8414, 4,
            ["localhost:8411", "localhost:8412", "localhost:8413"],
            new InMemoryWAL(logger), logger, initialPartitions: 0,
            stagingDirectory: stagingDirectory, stagingMemoryBytes: 0);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8411"] = n1,
            ["localhost:8412"] = n2,
            ["localhost:8413"] = n3,
            ["localhost:8414"] = n4,
        });

        // Larger than one 3 MB chunk, so the session spans several chunks appended to the file.
        byte[] payload = new byte[7 * 1024 * 1024 + 123];
        new Random(42).NextBytes(payload);

        RecordingTransfer transfer = new(payload);
        n1.RegisterStateMachineTransfer(transfer);
        n2.RegisterStateMachineTransfer(transfer);
        n3.RegisterStateMachineTransfer(transfer);
        n4.RegisterStateMachineTransfer(transfer);

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager leader = await FindLeaderAsync([n1, n2, n3], ct);
            int userPartitionId = leader.Partitions.Keys.FirstOrDefault(k => k != 0);
            Assert.NotEqual(0, userPartitionId);

            for (int i = 0; i < 5; i++)
                await leader.ReplicateLogs(userPartitionId, "test", [1, 2, 3], cancellationToken: ct);

            Assert.Equal(RaftOperationStatus.Success, (await leader.ReplicateCheckpoint(userPartitionId, ct)).Status);

            await WaitForAsync(() =>
                wal1.GetLastCheckpoint(userPartitionId) > 0 ||
                wal2.GetLastCheckpoint(userPartitionId) > 0 ||
                wal3.GetLastCheckpoint(userPartitionId) > 0,
                ct);

            await n4.JoinCluster(["localhost:8411"], ct);

            Assert.Equal(ClusterMemberRole.Voter, n4.LocalRole);
            Assert.True(transfer.VerifiedImportCount >= 1, "the learner's import did not read the exported bytes back from the staged file");
            Assert.Empty(Directory.GetFiles(stagingDirectory));
        }
        finally
        {
            n1.Dispose(); n2.Dispose(); n3.Dispose(); n4.Dispose();

            try { Directory.Delete(stagingDirectory, recursive: true); } catch { /* best-effort temp cleanup */ }
        }
    }

    // ── an install slower than a chunk acknowledgement ──────────────────────────

    /// <summary>
    /// The learner's import takes several times <see cref="RaftConfiguration.SnapshotChunkAckTimeout"/>.
    /// The leader must wait for it: one export, one import, and the learner is promoted. When the
    /// terminal chunk's call was held open for the install, that call expired, the attempt was
    /// recorded as failed, and each retry exported the partition again and queued another import
    /// behind the one still running (the retry cache is off here so every re-export would show).
    /// </summary>
    [Fact]
    public async Task Learner_WhoseInstallOutlastsTheChunkAckTimeout_IsSeededByOneExportAndOneImport()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        InMemoryCommunication comm = new();

        CompactableWAL wal1 = new(new InMemoryWAL(logger));
        CompactableWAL wal2 = new(new InMemoryWAL(logger));
        CompactableWAL wal3 = new(new InMemoryWAL(logger));

        void Configure(RaftConfiguration c)
        {
            c.SnapshotChunkAckTimeout = TimeSpan.FromMilliseconds(200);
            c.SnapshotExportRetryCacheMaxBytes = 0;
        }

        RaftManager n1 = BuildNode(comm, "localhost", 8421, 1, ["localhost:8422", "localhost:8423"], wal1, logger, configure: Configure);
        RaftManager n2 = BuildNode(comm, "localhost", 8422, 2, ["localhost:8421", "localhost:8423"], wal2, logger, configure: Configure);
        RaftManager n3 = BuildNode(comm, "localhost", 8423, 3, ["localhost:8421", "localhost:8422"], wal3, logger, configure: Configure);
        RaftManager n4 = BuildNode(comm, "localhost", 8424, 4,
            ["localhost:8421", "localhost:8422", "localhost:8423"],
            new InMemoryWAL(logger), logger, initialPartitions: 0, configure: Configure);

        comm.SetNodes(new Dictionary<string, IRaft>
        {
            ["localhost:8421"] = n1,
            ["localhost:8422"] = n2,
            ["localhost:8423"] = n3,
            ["localhost:8424"] = n4,
        });

        SlowImportTransfer transfer = new(importTime: TimeSpan.FromMilliseconds(1500));
        n1.RegisterStateMachineTransfer(transfer);
        n2.RegisterStateMachineTransfer(transfer);
        n3.RegisterStateMachineTransfer(transfer);
        n4.RegisterStateMachineTransfer(transfer);

        try
        {
            await Task.WhenAll(n1.JoinCluster(ct), n2.JoinCluster(ct), n3.JoinCluster(ct));
            await WaitForAsync(() => n1.IsInitialized && n2.IsInitialized && n3.IsInitialized, ct);

            RaftManager leader = await FindLeaderAsync([n1, n2, n3], ct);
            int userPartitionId = leader.Partitions.Keys.FirstOrDefault(k => k != 0);
            Assert.NotEqual(0, userPartitionId);

            for (int i = 0; i < 5; i++)
                await leader.ReplicateLogs(userPartitionId, "test", [1, 2, 3], cancellationToken: ct);

            Assert.Equal(RaftOperationStatus.Success, (await leader.ReplicateCheckpoint(userPartitionId, ct)).Status);

            await WaitForAsync(() =>
                wal1.GetLastCheckpoint(userPartitionId) > 0 ||
                wal2.GetLastCheckpoint(userPartitionId) > 0 ||
                wal3.GetLastCheckpoint(userPartitionId) > 0,
                ct);

            await n4.JoinCluster(["localhost:8421"], ct);

            Assert.Equal(ClusterMemberRole.Voter, n4.LocalRole);
            Assert.Equal(1, transfer.Imports(userPartitionId));
            Assert.Equal(1, transfer.Exports(userPartitionId));
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
        int initialPartitions = 1,
        string? stagingDirectory = null,
        long stagingMemoryBytes = 64L * 1024 * 1024,
        Action<RaftConfiguration>? configure = null)
    {
        RaftConfiguration cfg = new()
        {
            SnapshotStagingDirectory = stagingDirectory,
            SnapshotStagingMemoryBytes = stagingMemoryBytes,
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
        configure?.Invoke(cfg);
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
    private sealed class RecordingTransfer(byte[]? payload = null) : IRaftStateMachineTransfer
    {
        private int _importCount;
        private int _verifiedImports;

        public bool ImportWasCalled => _importCount > 0;
        public int ImportCallCount => _importCount;

        /// <summary>Imports whose stream carried exactly the exported payload on two reads from the start.</summary>
        public int VerifiedImportCount => _verifiedImports;

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream(payload ?? [0xDE, 0xAD, 0xBE, 0xEF]));

        public async Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            Interlocked.Increment(ref _importCount);

            if (payload is null)
                return;

            // An importer may verify before it applies, reading the stream twice.
            for (int pass = 0; pass < 2; pass++)
            {
                snapshot.Position = 0;
                using MemoryStream copy = new();
                await snapshot.CopyToAsync(copy, ct);
                if (!copy.ToArray().AsSpan().SequenceEqual(payload))
                    return;
            }

            Interlocked.Increment(ref _verifiedImports);
        }
    }

    /// <summary>
    /// A transfer whose import takes <paramref name="importTime"/>, and which counts exports and
    /// imports per partition.
    /// </summary>
    private sealed class SlowImportTransfer(TimeSpan importTime) : IRaftStateMachineTransfer
    {
        private readonly ConcurrentDictionary<int, int> exports = new();
        private readonly ConcurrentDictionary<int, int> imports = new();

        public int Exports(int partitionId) => exports.GetValueOrDefault(partitionId);
        public int Imports(int partitionId) => imports.GetValueOrDefault(partitionId);

        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct)
        {
            exports.AddOrUpdate(plan.TargetPartitionId, 1, static (_, count) => count + 1);
            return Task.FromResult<Stream>(new MemoryStream([0xDE, 0xAD, 0xBE, 0xEF]));
        }

        public async Task ImportRange(int targetPartitionId, Stream snapshot, CancellationToken ct)
        {
            imports.AddOrUpdate(targetPartitionId, 1, static (_, count) => count + 1);
            await Task.Delay(importTime, CancellationToken.None);
            await snapshot.CopyToAsync(Stream.Null, CancellationToken.None);
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
