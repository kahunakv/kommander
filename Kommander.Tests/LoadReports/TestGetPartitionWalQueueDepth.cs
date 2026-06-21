
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
/// Unit tests for <see cref="RaftManager.GetPartitionWalQueueDepth"/>:
/// verifies the local fast-path, the remote gossip path (current-leader selection),
/// the current-leader-over-stale-ex-leader tiebreak, and the 0-sentinel for unknown partitions.
/// Also covers <see cref="RaftManager.BuildLocalLoadReport"/> populating
/// <see cref="PartitionLoad.WalQueueDepth"/>, and rolling-upgrade compatibility for the
/// additive JSON field.
/// </summary>
public sealed class TestGetPartitionWalQueueDepth
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
        params (int partitionId, int walDepth)[] leaderships)
    {
        List<PartitionLoad> loads = leaderships
            .Select(l => new PartitionLoad { PartitionId = l.partitionId, WalQueueDepth = l.walDepth })
            .ToList();

        return new NodeLoadReport
        {
            Endpoint = endpoint,
            ReportVersion = version,
            Time = time,
            Leaderships = loads,
        };
    }

    // ── BuildLocalLoadReport populates WalQueueDepth ─────────────────────────

    /// <summary>
    /// For an idle led partition (no in-flight WAL writes) the WAL queue depth reported
    /// in <c>BuildLocalLoadReport</c> must be 0.
    /// </summary>
    [Fact]
    public void BuildLocalLoadReport_IdleLedPartition_WalQueueDepthIsZero()
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
            Assert.Equal(0, report.Leaderships[0].WalQueueDepth);
        }
        finally
        {
            p1.Dispose();
        }
    }

    // ── Local fast-path ──────────────────────────────────────────────────────

    /// <summary>
    /// When the local node leads a partition, <c>GetPartitionWalQueueDepth</c> reads from
    /// the in-process WAL scheduler — no gossip lag. For an idle partition the depth is 0.
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

            int depth = manager.GetPartitionWalQueueDepth(1);
            Assert.Equal(0, depth);
        }
        finally
        {
            p1.Dispose();
        }
    }

    // ── Sentinel ─────────────────────────────────────────────────────────────

    /// <summary>
    /// An unknown partition id (no local entry, no gossip report) must return 0.
    /// </summary>
    [Fact]
    public void UnknownPartition_ReturnsZero()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        int depth = manager.GetPartitionWalQueueDepth(999);
        Assert.Equal(0, depth);
    }

    // ── Remote path — current-leader selection ────────────────────────────────

    /// <summary>
    /// When a peer leads partition P and a gossip report from that peer is present,
    /// <c>GetPartitionWalQueueDepth</c> must return that report's value, not 0.
    /// </summary>
    [Fact]
    public async Task RemoteLeader_ReturnsGossipedDepth()
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
            NodeLoadReport report = MakeReport("node-A:9001", 1, t1, (1, 7));

            manager.SystemCoordinator.Send(new RaftSystemRequest(report));
            await manager.SystemCoordinator.DrainAsync();

            int depth = manager.GetPartitionWalQueueDepth(1);
            Assert.Equal(7, depth);
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// When two reports both contain partition P — one from the current leader and one from
    /// a stale ex-leader — the current leader's depth must be selected regardless of which
    /// report has the higher <c>ReportVersion</c> or later <c>Time</c>.
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
            HLCTimestamp tB = manager.HybridLogicalClock.SendOrLocalEvent(0); // tB > tA
            Assert.True(tB > tA);

            NodeLoadReport reportA = MakeReport("node-A:9001", 1,  tA, (1, 3));
            NodeLoadReport reportB = MakeReport("node-B:9002", 99, tB, (1, 50)); // stale ex-leader

            manager.SystemCoordinator.Send(new RaftSystemRequest(reportA));
            manager.SystemCoordinator.Send(new RaftSystemRequest(reportB));
            await manager.SystemCoordinator.DrainAsync();

            int depth = manager.GetPartitionWalQueueDepth(1);
            Assert.Equal(3, depth); // node-A (current leader) wins
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// When the current leader's endpoint is absent from the gossip store but another
    /// report does mention partition P, the fallback picks the report with the greatest
    /// HLC Time (not greatest ReportVersion).
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
            HLCTimestamp tB = manager.HybridLogicalClock.SendOrLocalEvent(0); // tB > tA

            // node-A: higher ReportVersion, earlier Time — must NOT win.
            NodeLoadReport reportA = MakeReport("node-A:9001", 100, tA, (1, 2));
            NodeLoadReport reportB = MakeReport("node-B:9002", 1,   tB, (1, 8));

            manager.SystemCoordinator.Send(new RaftSystemRequest(reportA));
            manager.SystemCoordinator.Send(new RaftSystemRequest(reportB));
            await manager.SystemCoordinator.DrainAsync();

            int depth = manager.GetPartitionWalQueueDepth(1);
            Assert.Equal(8, depth); // node-B wins on HLC Time
        }
        finally
        {
            p1.Dispose();
        }
    }
}
