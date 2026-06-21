
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
/// Unit tests for <see cref="RaftManager.GetPartitionLogOpsPerSecond"/>:
/// verifies the local fast-path, the remote gossip path (current-leader selection),
/// the current-leader-over-stale-ex-leader tiebreak, and the 0-sentinel for unknown partitions.
/// </summary>
public sealed class TestGetPartitionLogOpsPerSecond
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
        params (int partitionId, double logOps)[] leaderships)
    {
        List<PartitionLoad> loads = leaderships
            .Select(l => new PartitionLoad { PartitionId = l.partitionId, LogOpsPerSecond = l.logOps })
            .ToList();

        return new NodeLoadReport
        {
            Endpoint = endpoint,
            ReportVersion = version,
            Time = time,
            Leaderships = loads,
        };
    }

    // ── Local fast-path ─────────────────────────────────────────────────────

    /// <summary>
    /// When the local node is the current leader of a partition and that partition has
    /// seen <c>ReplicateLogs</c> activity, <c>GetPartitionLogOpsPerSecond</c> must
    /// return the live executor EWMA — no gossip lag.
    /// </summary>
    [Fact]
    public async Task LocalLeader_ReturnsLocalEwma()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = manager.LocalEndpoint;

            await p1.Executor.RestoreTask;

            for (int i = 0; i < 5; i++)
                p1.Executor.Post(new RaftRequest(RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: true));

            await p1.Executor.DrainAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct);

            double rate = manager.GetPartitionLogOpsPerSecond(1);
            Assert.True(rate > 0.0, $"Expected local EWMA > 0 for led partition, got {rate}.");
        }
        finally
        {
            p1.Dispose();
        }
    }

    // ── Sentinel ────────────────────────────────────────────────────────────

    /// <summary>
    /// An unknown partition id (no local entry, no gossip report) must return 0.
    /// </summary>
    [Fact]
    public void UnknownPartition_ReturnsZero()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        double rate = manager.GetPartitionLogOpsPerSecond(999);
        Assert.Equal(0.0, rate);
    }

    // ── Remote path — current-leader selection ───────────────────────────────

    /// <summary>
    /// When a peer leads partition P and a gossip report from that peer is present,
    /// <c>GetPartitionLogOpsPerSecond</c> must return that report's value, not 0.
    /// </summary>
    [Fact]
    public async Task RemoteLeader_ReturnsGossipedValue()
    {
        using RaftManager manager = MakeManager();
        ((FairReadScheduler)manager.ReadScheduler).Start();

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = "node-A:9001"; // peer leads P1

            HLCTimestamp t1 = manager.HybridLogicalClock.SendOrLocalEvent(0);
            NodeLoadReport report = MakeReport("node-A:9001", 1, t1, (1, 42.5));

            manager.SystemCoordinator.Send(new RaftSystemRequest(report));
            await manager.SystemCoordinator.DrainAsync();

            double rate = manager.GetPartitionLogOpsPerSecond(1);
            Assert.Equal(42.5, rate);
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// When two reports both contain partition P — one from the current leader and one from
    /// a stale ex-leader — the current leader's value must be selected, regardless of
    /// which report has the higher <c>ReportVersion</c> or later <c>Time</c>.
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
            p1.Leader = "node-A:9001"; // node-A is the CURRENT leader

            // node-B is a stale ex-leader: its report has a later Time and higher version,
            // but it no longer leads P1 per the local partition map.
            HLCTimestamp tA = manager.HybridLogicalClock.SendOrLocalEvent(0);
            HLCTimestamp tB = manager.HybridLogicalClock.SendOrLocalEvent(0); // tB > tA
            Assert.True(tB > tA, "Precondition: tB must be later than tA.");

            NodeLoadReport reportA = MakeReport("node-A:9001", 1, tA, (1, 10.0));
            NodeLoadReport reportB = MakeReport("node-B:9002", 99, tB, (1, 99.0));

            manager.SystemCoordinator.Send(new RaftSystemRequest(reportA));
            manager.SystemCoordinator.Send(new RaftSystemRequest(reportB));
            await manager.SystemCoordinator.DrainAsync();

            // Must return node-A's value (10.0), not node-B's (99.0).
            double rate = manager.GetPartitionLogOpsPerSecond(1);
            Assert.Equal(10.0, rate);
        }
        finally
        {
            p1.Dispose();
        }
    }

    /// <summary>
    /// When the current leader's endpoint is unknown in the gossip store but another
    /// report does contain partition P, the fallback picks the report with the greatest
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
            p1.Leader = "node-C:9003"; // node-C leads, but has no gossiped report yet

            HLCTimestamp tA = manager.HybridLogicalClock.SendOrLocalEvent(0);
            HLCTimestamp tB = manager.HybridLogicalClock.SendOrLocalEvent(0); // tB > tA

            // Two reports from ex-leaders, both listing P1. Higher Time wins.
            // node-A has higher ReportVersion but earlier Time — must NOT win.
            NodeLoadReport reportA = MakeReport("node-A:9001", 100, tA, (1, 5.0));
            NodeLoadReport reportB = MakeReport("node-B:9002", 1,   tB, (1, 7.0));

            manager.SystemCoordinator.Send(new RaftSystemRequest(reportA));
            manager.SystemCoordinator.Send(new RaftSystemRequest(reportB));
            await manager.SystemCoordinator.DrainAsync();

            double rate = manager.GetPartitionLogOpsPerSecond(1);
            Assert.Equal(7.0, rate); // node-B wins on HLC Time
        }
        finally
        {
            p1.Dispose();
        }
    }
}
