using System.Collections.Concurrent;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.WAL;

/// <summary>
/// Task 9 of the WAL double-fsync spec: proves the recovered committed prefix is correct under the
/// single-fsync fast path, with the crash injected at every window the lazy commit marker opens.
///
/// <para>A real process kill can't be staged deterministically in-process (a graceful dispose flushes
/// RocksDB/SQLite, which would persist the lazy markers and erase the very window under test). Instead
/// each case <b>materialises the exact on-disk shape</b> a crash in that window leaves — a durable
/// Proposed prefix whose per-entry <c>Committed</c> markers were lost — by writing it straight to a
/// persistent WAL, then starts a node on that WAL and observes recovery. This is a faithful, fully
/// deterministic stand-in for the crash: the lost-marker entries are exactly the <c>Proposed</c> rows
/// the fsync skip would leave behind.</para>
///
/// <para>The invariants asserted (spec Task 9, step 3): the recovered committed prefix matches the
/// fast-path-off baseline; no acknowledged write is lost (the durable tail is retained, never
/// overwritten); and no unacknowledged-but-not-durable write is treated as committed (the Proposed
/// tail above the contiguous committed prefix is not delivered as committed on restore).</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class RecoveryReconstructionCrashMatrixTests
{
    private readonly ILogger<IRaft> logger;

    public RecoveryReconstructionCrashMatrixTests()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    public enum WalBackend { RocksDb, Sqlite }

    private const int UserPartition = 1;

    /// <summary>
    /// The clean (fast-path-off equivalent) on-disk shape: every entry's commit marker survived, so all
    /// N entries are durably Committed. Recovery must restore the full prefix and proposeIndex must land
    /// past the last entry. This is the baseline the crash shapes are compared against.
    /// </summary>
    [Theory]
    [InlineData(WalBackend.RocksDb)]
    [InlineData(WalBackend.Sqlite)]
    public async Task CleanShape_AllMarkersDurable_RecoversFullCommittedPrefix(WalBackend backend)
    {
        const int total = 12;
        string path = CreateTempWalPath();
        try
        {
            // Seed: ids 1..12 all Committed (no marker loss).
            SeedPartition(backend, path, committedThrough: total, proposedTailThrough: total);

            (List<long> restored, long maxLog, long nextId) = await RecoverAndProbeAsync(backend, path, fastPath: true);

            // Full committed prefix delivered, nothing lost, propose cursor past the tail.
            Assert.Equal(Enumerable.Range(1, total).Select(i => (long)i), restored);
            Assert.Equal(total, maxLog);
            Assert.Equal(total + 1, nextId);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// The crash-after-lazy-commit shape: the tail's lazy commit markers were lost, so entries
    /// K+1..N are durable only as Proposed. Recovery must restore exactly the contiguous committed
    /// prefix 1..K, retain the durable tail (no acked write lost, no overwrite of its ids), and must
    /// NOT deliver the Proposed tail as committed. Asserted for kills at three windows (the tail length
    /// = how many markers the crash dropped): just the last (between writes), a few (after a burst), and
    /// the whole tail (before any marker flushed).
    /// </summary>
    [Theory]
    [InlineData(WalBackend.RocksDb, 11)] // one marker lost (kill between the last two writes)
    [InlineData(WalBackend.RocksDb, 8)]  // a few markers lost
    [InlineData(WalBackend.RocksDb, 0)]  // every marker lost (kill before any flush)
    [InlineData(WalBackend.Sqlite, 11)]
    [InlineData(WalBackend.Sqlite, 8)]
    [InlineData(WalBackend.Sqlite, 0)]
    public async Task CrashShape_LostTailMarkers_RecoversContiguousPrefix_RetainsDurableTail(WalBackend backend, int committedThrough)
    {
        const int total = 12;
        string path = CreateTempWalPath();
        try
        {
            // Seed: ids 1..committedThrough Committed, committedThrough+1..12 Proposed (markers lost).
            SeedPartition(backend, path, committedThrough: committedThrough, proposedTailThrough: total);

            (List<long> restored, long maxLog, long nextId) = await RecoverAndProbeAsync(backend, path, fastPath: true);

            // Only the contiguous committed prefix is delivered as committed.
            Assert.Equal(Enumerable.Range(1, committedThrough).Select(i => (long)i), restored);

            // The durable Proposed tail is retained (no acked write lost) ...
            Assert.Equal(total, maxLog);
            // ... and a new propose lands strictly past it, never reusing — and so never overwriting —
            // the durable tail's ids.
            Assert.Equal(total + 1, nextId);
        }
        finally
        {
            DeleteTempWalPath(path);
        }
    }

    /// <summary>
    /// Equivalence: for the clean shape the fast-path-on reconstruction and the fast-path-off legacy
    /// recovery deliver the <b>identical</b> committed prefix. (The shapes diverge only when markers are
    /// actually lost, which the legacy path cannot produce.)
    /// </summary>
    [Theory]
    [InlineData(WalBackend.RocksDb)]
    [InlineData(WalBackend.Sqlite)]
    public async Task CleanShape_FastPathOnAndOff_RecoverIdenticalPrefix(WalBackend backend)
    {
        const int total = 9;

        string pathOn = CreateTempWalPath();
        string pathOff = CreateTempWalPath();
        try
        {
            SeedPartition(backend, pathOn, committedThrough: total, proposedTailThrough: total);
            SeedPartition(backend, pathOff, committedThrough: total, proposedTailThrough: total);

            (List<long> restoredOn, _, _) = await RecoverAndProbeAsync(backend, pathOn, fastPath: true);
            (List<long> restoredOff, _, _) = await RecoverAndProbeAsync(backend, pathOff, fastPath: false);

            Assert.Equal(restoredOff, restoredOn);
            Assert.Equal(Enumerable.Range(1, total).Select(i => (long)i), restoredOn);
        }
        finally
        {
            DeleteTempWalPath(pathOn);
            DeleteTempWalPath(pathOff);
        }
    }

    // ── Harness ────────────────────────────────────────────────────────────

    /// <summary>
    /// Writes a single partition's on-disk state directly: ids 1..<paramref name="committedThrough"/> as
    /// <see cref="RaftLogType.Committed"/>, then <paramref name="committedThrough"/>+1..<paramref name="proposedTailThrough"/>
    /// as <see cref="RaftLogType.Proposed"/> (the rows a lost lazy marker leaves). Disposes the WAL so the
    /// file handle is free for the node to reopen.
    /// </summary>
    private void SeedPartition(WalBackend backend, string path, int committedThrough, int proposedTailThrough)
    {
        using IWAL wal = CreateWal(backend, path);

        List<RaftLog> logs = [];
        for (int id = 1; id <= proposedTailThrough; id++)
        {
            logs.Add(new RaftLog
            {
                Id = id,
                Term = 1,
                Type = id <= committedThrough ? RaftLogType.Committed : RaftLogType.Proposed,
                LogType = "Greeting",
                LogData = "Hello World"u8.ToArray(),
            });
        }

        RaftOperationStatus status = wal.Write([(UserPartition, logs)]);
        Assert.Equal(RaftOperationStatus.Success, status);
    }

    /// <summary>
    /// Starts a single node on the seeded WAL, captures the committed entries delivered on restore for
    /// <see cref="UserPartition"/>, then probes the reconstructed propose cursor by issuing one write and
    /// reading back its assigned index. Returns (restoredIds, maxLog, nextProposedId).
    /// </summary>
    private async Task<(List<long> restored, long maxLog, long nextId)> RecoverAndProbeAsync(WalBackend backend, string path, bool fastPath)
    {
        ConcurrentBag<long> restored = [];

        IRaft node = BuildNode(CreateWal(backend, path), fastPath);
        node.OnLogRestored += (partitionId, log) =>
        {
            if (partitionId == UserPartition)
                restored.Add(log.Id);
            return Task.FromResult(true);
        };

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        try
        {
            await node.JoinCluster(cts.Token);
            Assert.True(node.IsInitialized);
            await WaitForLeader(node, UserPartition, cts.Token);

            long maxLog = node.WalAdapter.GetMaxLog(UserPartition);

            RaftReplicationResult result = await node.ReplicateLogs(
                UserPartition, "Greeting", "probe"u8.ToArray(), cancellationToken: cts.Token);
            Assert.True(result.Success, $"probe replicate failed: {result.Status}");

            List<long> ordered = restored.OrderBy(x => x).ToList();
            return (ordered, maxLog, result.LogIndex);
        }
        finally
        {
            await node.LeaveCluster(true, CancellationToken.None);
        }
    }

    private IRaft BuildNode(IWAL wal, bool fastPath)
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
            EnableQuiescence = false,
            EndElectionTimeout = 250,
            WalSingleFsyncCommit = fastPath,
        };

        return new RaftManager(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            logger);
    }

    private static async Task WaitForLeader(IRaft node, int partitionId, CancellationToken cancellationToken)
    {
        for (int i = 0; i < 600; i++)
        {
            if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                return;
            await Task.Delay(25, cancellationToken);
        }
        throw new TimeoutException($"No leader for partition {partitionId} within 15 seconds.");
    }

    private static IWAL CreateWal(WalBackend backend, string path) => backend switch
    {
        WalBackend.RocksDb => new RocksDbWAL(path, "wal", NullLoggerInstance, syncWrites: true),
        WalBackend.Sqlite => new SqliteWAL(path, "wal", NullLoggerInstance, syncWrites: true),
        _ => throw new ArgumentOutOfRangeException(nameof(backend)),
    };

    private static readonly ILogger<IRaft> NullLoggerInstance = Microsoft.Extensions.Logging.Abstractions.NullLogger<IRaft>.Instance;

    private static string CreateTempWalPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"kommander-recovery-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempWalPath(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }
}
