using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using RocksDbSharp;

namespace Kommander.Tests.WAL;

/// <summary>
/// Regression tests for the field incident of 2026-09-01: a node whose data volume filled never
/// recovered after the operator freed the space, because RocksDB had latched a storage error it never
/// clears on its own. The latch is native state, so these tests run against a genuinely full
/// filesystem (<see cref="EnospcVolume"/>) and skip where none is available.
///
/// <para>The sequence that latches the engine: an append fails on the full volume (recoverable on its
/// own), then a memtable switch tries to create a fresh WAL file and that creation fails ("While open a
/// file for appending"), which RocksDB treats as unrecoverable. The switch is forced here through a
/// test hook; in production ordinary load reaches it (a memtable fills, or the shared write-buffer
/// manager asks for a flush).</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRocksDbWalEnospcRecovery
{
    private const int Partition = 1;

    /// <summary>A partition no Raft node hosts; the cluster test fills the engine through it.</summary>
    private const int FillerPartition = 99;

    private const string WalCreationFailureMarker = "open a file for appending";

    private readonly ILogger<IRaft> logger;

    public TestRocksDbWalEnospcRecovery()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// The WAL-level contract: writes fail while the volume is full, hard-state persistence reports the
    /// failure as <c>false</c> instead of throwing, and once space is back the WAL accepts writes again
    /// in the same process with every earlier entry intact. When the engine latched (the field shape),
    /// the recovery must have gone through an engine reopen.
    /// </summary>
    [Fact]
    public void Wal_RecoversAfterSpaceReturns_InTheSameProcess()
    {
        using EnospcVolume? volume = EnospcVolume.TryCreate();
        Assert.SkipWhen(volume is null, EnospcVolume.SkipReason);

        const int entries = 25;

        using RocksDbWAL wal = new(volume.Root, "wal", logger, syncWrites: true);
        IWAL hardState = wal; // PersistHardState / TryGetHardState are interface default members

        for (long id = 1; id <= entries; id++)
            Assert.Equal(RaftOperationStatus.Success, wal.Write([(Partition, [Entry(id)])]));

        Assert.True(hardState.PersistHardState(Partition, currentTerm: 3, votedFor: "node-a"));

        volume.Fill();

        // Full volume: writes fail as a status, never as an escaping native exception. The first
        // failure is an append failure, which RocksDB could still recover from on its own. Entries
        // that still succeeded (RocksDB preallocates WAL file space) are durable and must survive.
        long failedId = WriteUntilErrored(wal, Partition, entries + 1);
        Assert.False(hardState.PersistHardState(Partition, currentTerm: 4, votedFor: "node-a"));

        // Force the memtable switch that creates a fresh WAL file. On a volume that refuses file
        // creation this latches the engine with the unrecoverable error from the field incident.
        string? flushError = null;
        try
        {
            wal.FlushMemTablesForTesting();
        }
        catch (RocksDbException ex)
        {
            flushError = ex.Message;
        }

        Assert.Equal(RaftOperationStatus.Errored, wal.Write([(Partition, [Entry(failedId)])]));
        bool latched = flushError?.Contains(WalCreationFailureMarker, StringComparison.Ordinal) == true
                       || wal.LastStorageFailureMessage?.Contains(WalCreationFailureMarker, StringComparison.Ordinal) == true;

        volume.Free();
        Assert.True(volume.AvailableFreeSpace > 100L * 1024 * 1024, "the filler was not released");

        // The node keeps retrying on its own cadence; here the test is the retry loop. RocksDB's own
        // resume (for the append-failure shape) takes about 5 s; the reopen (for the latched shape) is
        // immediate once the filesystem probe passes.
        RaftOperationStatus status = RaftOperationStatus.Errored;
        DateTime deadline = DateTime.UtcNow + TestTimeouts.Scale(TimeSpan.FromSeconds(60));

        while (status != RaftOperationStatus.Success && DateTime.UtcNow < deadline)
        {
            status = wal.Write([(Partition, [Entry(failedId)])]);
            if (status != RaftOperationStatus.Success)
                Thread.Sleep(250);
        }

        Assert.Equal(RaftOperationStatus.Success, status);

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"latched={latched} reopens={wal.EngineReopenCount} flushError={flushError}");

        if (latched)
            Assert.Equal(1, wal.EngineReopenCount);

        // Hard state persists again and reads back; the earlier vote survived the episode.
        Assert.True(hardState.PersistHardState(Partition, currentTerm: 5, votedFor: "node-b"));
        Assert.True(hardState.TryGetHardState(Partition, out long term, out string? votedFor));
        Assert.Equal(5, term);
        Assert.Equal("node-b", votedFor);

        // Nothing acknowledged before or during the episode was lost, and the retried entry landed.
        List<RaftLog> logs = wal.ReadLogs(Partition);
        Assert.Equal(failedId, logs.Count);
        for (int i = 0; i < logs.Count; i++)
            Assert.Equal(i + 1, logs[i].Id);

        Assert.Equal(failedId, wal.GetMaxLog(Partition));
    }

    /// <summary>
    /// The acceptance criterion of the incident: a cluster whose WALs hit ENOSPC completes an election
    /// on its own after free space returns, with no process restart. Three nodes share the volume; the
    /// volume fills and every engine latches; the leader is crashed so the survivors must elect. While
    /// the volume is full no election can complete (hard state cannot be persisted, and a vote that
    /// cannot be recorded is withheld). Once the filler is removed, the survivors elect a leader and
    /// accept a new write.
    /// </summary>
    [Fact]
    public async Task Cluster_ElectsLeaderAfterSpaceReturns_WithoutRestart()
    {
        using EnospcVolume? volume = EnospcVolume.TryCreate();
        Assert.SkipWhen(volume is null, EnospcVolume.SkipReason);

        CancellationToken ct = TestContext.Current.CancellationToken;

        InMemoryCommunication comm = new();
        RaftManager?[] live = new RaftManager?[3];
        RocksDbWAL[] wals = new RocksDbWAL[3];
        string[][] peersById =
        [
            ["localhost:8002", "localhost:8003"],
            ["localhost:8001", "localhost:8003"],
            ["localhost:8001", "localhost:8002"],
        ];

        try
        {
            for (int i = 0; i < 3; i++)
            {
                wals[i] = new RocksDbWAL(volume.Root, $"node{i + 1}", logger, syncWrites: true);
                live[i] = MakeNode(i + 1, comm, wals[i], peersById[i]);
            }

            SetNetwork(comm, live);

            await Task.WhenAll(live.Select(n => n!.UpdateNodes()));
            await Task.WhenAll(live.Select(n => n!.JoinCluster(ct)));

            await WaitForAnyLeader(live!, ct);
            int leaderIdx = await LeaderIndex(live!);
            Assert.InRange(leaderIdx, 0, 2);

            byte[] data = "before"u8.ToArray();
            for (int i = 1; i <= 5; i++)
            {
                RaftReplicationResult r = await live[leaderIdx]!.ReplicateLogs(Partition, "Greeting", data, cancellationToken: ct);
                Assert.True(r.Success, $"replicate {i} failed: {r.Status}");
            }

            await WaitForConditionAsync(() => live.All(n => n!.WalAdapter.GetMaxLog(Partition) >= 5), ct);

            volume.Fill();

            // Every engine hits the append failure, then the WAL-creation failure that latches it. The
            // filler entries go to a partition Raft never reads, so the Raft log itself stays untouched.
            for (int i = 0; i < 3; i++)
            {
                long failedId = WriteUntilErrored(wals[i], FillerPartition, 1);
                try { wals[i].FlushMemTablesForTesting(); }
                catch (RocksDbException) { }
                Assert.Equal(RaftOperationStatus.Errored, wals[i].Write([(FillerPartition, [Entry(failedId)])]));
            }

            bool allLatched = wals.All(w => w.LastStorageFailureMessage?.Contains(WalCreationFailureMarker, StringComparison.Ordinal) == true);

            // Crash the leader. The two survivors now have to elect, and cannot: hard state cannot be
            // made durable, so every election attempt is abandoned and every vote is withheld.
            live[leaderIdx]!.Dispose();
            live[leaderIdx] = null;
            SetNetwork(comm, live);

            if (allLatched)
            {
                await Task.Delay(TestTimeouts.Scale(1500), ct);
                Assert.Equal(-1, await LeaderIndex(live!));
            }

            volume.Free();

            // No restart: the survivors reopen their engines on the next failed write and elect.
            await WaitForAnyLeader(live!, ct, timeoutSeconds: 60);
            int newLeaderIdx = await LeaderIndex(live!);
            Assert.NotEqual(leaderIdx, newLeaderIdx);

            RaftReplicationResult after = await live[newLeaderIdx]!.ReplicateLogs(Partition, "Greeting", "after"u8.ToArray(), cancellationToken: ct);
            Assert.True(after.Success, $"replicate after recovery failed: {after.Status}");

            TestContext.Current.TestOutputHelper?.WriteLine(
                $"allLatched={allLatched} reopens={string.Join(",", wals.Select(w => w.EngineReopenCount))} newLeader=node{newLeaderIdx + 1}");

            if (allLatched)
            {
                for (int i = 0; i < 3; i++)
                {
                    if (i != leaderIdx)
                        Assert.True(wals[i].EngineReopenCount >= 1, $"node{i + 1} recovered without reopening its engine");
                }
            }
        }
        finally
        {
            foreach (RaftManager? n in live)
            {
                try { n?.Dispose(); }
                catch (Exception) { }
            }

            // The filler must be gone before the ramdisk is discarded, and the WALs must be closed
            // before their directories are deleted.
            volume.Free();

            foreach (RocksDbWAL wal in wals)
            {
                try { wal?.Dispose(); }
                catch (Exception) { }
            }
        }
    }

    // ── Harness ────────────────────────────────────────────────────────────

    private static RaftLog Entry(long id) => new()
    {
        Id = id,
        Term = 1,
        Type = RaftLogType.Committed,
        LogType = "Greeting",
        LogData = new byte[4096],
        Time = HLCTimestamp.Zero,
    };

    /// <summary>
    /// Writes entries from <paramref name="firstId"/> upward until the WAL reports Errored and returns
    /// the id that failed. RocksDB preallocates space for its WAL file, so the first writes after the
    /// volume fills can still succeed; those entries are durable and are expected to survive.
    /// </summary>
    private static long WriteUntilErrored(RocksDbWAL wal, int partition, long firstId)
    {
        const int maxEntries = 8192;

        for (long id = firstId; id < firstId + maxEntries; id++)
        {
            if (wal.Write([(partition, [Entry(id)])]) == RaftOperationStatus.Errored)
                return id;
        }

        throw new InvalidOperationException("The WAL never reported Errored on a full volume.");
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
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            wal,
            communication,
            new HybridLogicalClock(),
            logger);
    }

    private static async Task<int> LeaderIndex(IRaft?[] nodes)
    {
        for (int i = 0; i < nodes.Length; i++)
        {
            if (nodes[i] is not null && await nodes[i]!.AmILeaderQuick(Partition).ConfigureAwait(false))
                return i;
        }

        return -1;
    }

    private static async Task WaitForAnyLeader(IRaft?[] nodes, CancellationToken cancellationToken, int timeoutSeconds = 15)
    {
        int rounds = TestTimeouts.Scale(timeoutSeconds) * 40;

        for (int i = 0; i < rounds; i++)
        {
            if (await LeaderIndex(nodes).ConfigureAwait(false) >= 0)
                return;
            await Task.Delay(25, cancellationToken);
        }

        throw new TimeoutException($"No leader for partition {Partition} within {timeoutSeconds}s.");
    }

    private static async Task WaitForConditionAsync(Func<bool> condition, CancellationToken cancellationToken, int timeoutSeconds = 15)
    {
        int rounds = TestTimeouts.Scale(timeoutSeconds) * 40;

        for (int i = 0; i < rounds; i++)
        {
            if (condition())
                return;
            await Task.Delay(25, cancellationToken);
        }

        throw new TimeoutException("Condition not met in time.");
    }
}
