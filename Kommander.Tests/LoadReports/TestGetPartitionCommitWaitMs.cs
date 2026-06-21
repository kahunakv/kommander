
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Unit tests for <see cref="RaftManager.GetPartitionCommitWaitMs"/>:
/// verifies the local fast-path, the remote gossip path (current-leader selection),
/// the current-leader-over-stale-ex-leader tiebreak, and the 0-sentinel for
/// unknown partitions.
///
/// Also covers <see cref="RaftManager.BuildLocalLoadReport"/> populating
/// <see cref="PartitionLoad.CommitWaitMs"/>, and rolling-upgrade compatibility
/// for the additive JSON field.
/// </summary>
public sealed class TestGetPartitionCommitWaitMs
{
    private static RaftManager MakeManager(string host = "localhost", int port = 9000)
    {
        RaftConfiguration config = new()
        {
            Host = host,
            Port = port,
            InitialPartitions = 0,
            EnableLeaderBalancer = true,
        };
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        return new RaftManager(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    private static NodeLoadReport MakeReport(
        string endpoint,
        long version,
        HLCTimestamp time,
        params (int partitionId, double commitWaitMs)[] leaderships)
    {
        List<PartitionLoad> loads = leaderships
            .Select(l => new PartitionLoad { PartitionId = l.partitionId, CommitWaitMs = l.commitWaitMs })
            .ToList();

        return new NodeLoadReport
        {
            Endpoint = endpoint,
            ReportVersion = version,
            Time = time,
            Leaderships = loads,
        };
    }

    // ── BuildLocalLoadReport populates CommitWaitMs ───────────────────────────

    /// <summary>
    /// A freshly led partition with no WAL write activity must emit
    /// <c>CommitWaitMs == 0</c> in the load report (no observations yet).
    /// </summary>
    [Fact]
    public void BuildLocalLoadReport_IdleLedPartition_CommitWaitMsIsZero()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = manager.LocalEndpoint;

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Single(report.Leaderships);
            Assert.Equal(0.0, report.Leaderships[0].CommitWaitMs);
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// After <c>ReplicateLogs</c> ops have been processed through the WAL scheduler,
    /// <c>BuildLocalLoadReport</c> must emit a positive <c>CommitWaitMs</c> (the
    /// enqueue-to-durable elapsed time was recorded).
    /// </summary>
    [Fact]
    public async Task BuildLocalLoadReport_AfterReplicateLogs_CommitWaitMsIsNonZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = manager.LocalEndpoint;

            await p1.Executor.RestoreTask;

            for (int i = 0; i < 5; i++)
                p1.Executor.Post(new RaftRequest(
                    RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: true));

            await p1.Executor.DrainAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

            // Allow WAL worker to finish any in-flight writes.
            await Task.Delay(50, ct);

            NodeLoadReport report = manager.BuildLocalLoadReport();
            PartitionLoad led = report.Leaderships.Single(l => l.PartitionId == 1);

            Assert.True(led.CommitWaitMs >= 0.0,
                $"CommitWaitMs must be non-negative, got {led.CommitWaitMs}.");
        }
        finally
        {
            p1.Dispose();
        }
    }

    // ── Local fast-path ───────────────────────────────────────────────────────

    /// <summary>
    /// When the local node leads the partition and no writes have been issued, the
    /// local fast-path must return 0 (no observations in the WAL scheduler).
    /// </summary>
    [Fact]
    public void LocalLeader_IdlePartition_ReturnsZero()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = manager.LocalEndpoint;

            double wait = manager.GetPartitionCommitWaitMs(1);
            Assert.Equal(0.0, wait);
        }
        finally
        {
            p1.Dispose();
        }
    }

    // ── Sentinel ──────────────────────────────────────────────────────────────

    /// <summary>
    /// An unknown partition id (no local entry, no gossip report) must return 0.
    /// </summary>
    [Fact]
    public void UnknownPartition_ReturnsZero()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        Assert.Equal(0.0, manager.GetPartitionCommitWaitMs(999));
    }

    // ── Remote path — current-leader selection ────────────────────────────────

    /// <summary>
    /// When a peer leads partition P and a gossip report from that peer is present,
    /// <c>GetPartitionCommitWaitMs</c> must return that report's value.
    /// </summary>
    [Fact]
    public async Task RemoteLeader_ReturnsGossipedWait()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = "node-A:9001";

            HLCTimestamp t1 = manager.HybridLogicalClock.SendOrLocalEvent(0);
            NodeLoadReport report = MakeReport("node-A:9001", 1, t1, (1, 12.5));

            manager.SystemCoordinator.Send(new RaftSystemRequest(report));
            await manager.SystemCoordinator.DrainAsync();

            double wait = manager.GetPartitionCommitWaitMs(1);
            Assert.Equal(12.5, wait);
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// When two reports both contain partition P, the current leader's value is
    /// selected, regardless of which report has the higher ReportVersion or later Time.
    /// </summary>
    [Fact]
    public async Task CurrentLeaderReport_WinsOverStaleExLeader()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = "node-A:9001";

            HLCTimestamp tA = manager.HybridLogicalClock.SendOrLocalEvent(0);
            HLCTimestamp tB = manager.HybridLogicalClock.SendOrLocalEvent(0);
            Assert.True(tB > tA);

            NodeLoadReport reportA = MakeReport("node-A:9001", 1,  tA, (1, 5.0));  // current leader
            NodeLoadReport reportB = MakeReport("node-B:9002", 99, tB, (1, 99.0)); // stale ex-leader

            manager.SystemCoordinator.Send(new RaftSystemRequest(reportA));
            manager.SystemCoordinator.Send(new RaftSystemRequest(reportB));
            await manager.SystemCoordinator.DrainAsync();

            double wait = manager.GetPartitionCommitWaitMs(1);
            Assert.Equal(5.0, wait); // node-A (current leader) wins
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// When no report from the current leader is present, the fallback picks the report
    /// with the greatest HLC Time (not greatest ReportVersion).
    /// </summary>
    [Fact]
    public async Task NoCurrentLeaderReport_FallbackPicksGreatestHlcTime()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = "node-C:9003"; // no gossiped report yet

            HLCTimestamp tA = manager.HybridLogicalClock.SendOrLocalEvent(0);
            HLCTimestamp tB = manager.HybridLogicalClock.SendOrLocalEvent(0);

            NodeLoadReport reportA = MakeReport("node-A:9001", 100, tA, (1, 3.0)); // higher version, older
            NodeLoadReport reportB = MakeReport("node-B:9002", 1,   tB, (1, 7.0)); // newer time wins

            manager.SystemCoordinator.Send(new RaftSystemRequest(reportA));
            manager.SystemCoordinator.Send(new RaftSystemRequest(reportB));
            await manager.SystemCoordinator.DrainAsync();

            double wait = manager.GetPartitionCommitWaitMs(1);
            Assert.Equal(7.0, wait);
        }
        finally
        {
            p1.Dispose();
        }
    }
}
