
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Unit tests for <see cref="IRaft.GetPartitionLeaderHint"/>: the best-effort leader hint a
/// non-replica node uses to forward an operation straight to a range's leader instead of routing
/// blind through an arbitrary voter. Verifies the local-belief fast path, the gossip reduction
/// (newest fresh claim wins), the TTL filter that compensates for eviction only running on the
/// P0 leader's balancer pass, the null contract for unknown partitions, and — the coupling fix —
/// that a placement-without-balancer configuration still ingests reports
/// (<see cref="RaftConfiguration.LoadReportsEnabled"/>).
/// </summary>
public sealed class TestGetPartitionLeaderHint
{
    private const int Partition = 7;

    private static RaftManager MakeManager(Action<RaftConfiguration>? configure = null)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            EnableLeaderBalancer = true,
        };
        configure?.Invoke(config);

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
        ((FairReadScheduler)manager.ReadScheduler).Start();
        return manager;
    }

    private static NodeLoadReport MakeReport(string endpoint, long version, HLCTimestamp time, params int[] ledPartitions) =>
        new()
        {
            Endpoint = endpoint,
            ReportVersion = version,
            Time = time,
            Leaderships = ledPartitions.Select(p => new PartitionLoad { PartitionId = p }).ToList(),
        };

    // ── Local fast path ──────────────────────────────────────────────────────

    [Fact]
    public void LocallyHostedPartition_ReturnsLocalLeaderBelief()
    {
        using RaftManager manager = MakeManager();
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        RaftPartition p1 = new(manager, wal, Partition, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[Partition] = p1;
            p1.Leader = "node-A:9001";

            // Local belief wins with no gossip present at all.
            Assert.Equal("node-A:9001", manager.GetPartitionLeaderHint(Partition));
        }
        finally
        {
            p1.Dispose();
        }
    }

    // ── Gossip reduction for non-hosted partitions ───────────────────────────

    [Fact]
    public async Task NonHostedPartition_ReturnsGossipedClaim()
    {
        using RaftManager manager = MakeManager();

        HLCTimestamp now = manager.HybridLogicalClock.SendOrLocalEvent(0);
        manager.SystemCoordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 1, now, Partition)));
        await manager.SystemCoordinator.DrainAsync();

        Assert.Equal("node-A:9001", manager.GetPartitionLeaderHint(Partition));
    }

    [Fact]
    public async Task TwoClaims_NewestHlcTimeWins_NotHighestVersion()
    {
        using RaftManager manager = MakeManager();

        HLCTimestamp tA = manager.HybridLogicalClock.SendOrLocalEvent(0);
        HLCTimestamp tB = manager.HybridLogicalClock.SendOrLocalEvent(0); // tB > tA
        Assert.True(tB > tA);

        // node-A has the higher ReportVersion but the older claim — an ex-leader whose
        // counter simply ran longer. The newest HLC time must win.
        manager.SystemCoordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 100, tA, Partition)));
        manager.SystemCoordinator.Send(new RaftSystemRequest(MakeReport("node-B:9002", 1, tB, Partition)));
        await manager.SystemCoordinator.DrainAsync();

        Assert.Equal("node-B:9002", manager.GetPartitionLeaderHint(Partition));
    }

    /// <summary>
    /// Store eviction (<c>LoadReportStore.EvictStale</c>) runs only inside the P0 leader's
    /// balancer pass, so on every other node reports accumulate forever — the hint must apply
    /// its own freshness filter or it would return a dead node's endpoint indefinitely.
    /// </summary>
    [Fact]
    public async Task StaleReport_YieldsNull()
    {
        using RaftManager manager = MakeManager();

        HLCTimestamp now = manager.HybridLogicalClock.SendOrLocalEvent(0);
        TimeSpan ttl = manager.Configuration.LeaderBalancerReportTtl;
        HLCTimestamp stale = new(now.N, now.L - (long)(ttl.TotalMilliseconds * 3), 0);

        manager.SystemCoordinator.Send(new RaftSystemRequest(MakeReport("dead-node:9009", 1, stale, Partition)));
        await manager.SystemCoordinator.DrainAsync();

        Assert.Null(manager.GetPartitionLeaderHint(Partition));
    }

    [Fact]
    public void UnknownPartition_ReturnsNull_NoThrow()
    {
        using RaftManager manager = MakeManager();

        Assert.Null(manager.GetPartitionLeaderHint(999));
    }

    // ── The balancer-coupling fix ────────────────────────────────────────────

    /// <summary>
    /// A deployment running replica placement without the leader balancer must still exchange
    /// load reports, or the hint would be silently null forever. The receive gate is now
    /// <see cref="RaftConfiguration.LoadReportsEnabled"/> (implied by a non-zero replication
    /// factor), not <see cref="RaftConfiguration.EnableLeaderBalancer"/> alone.
    /// </summary>
    [Fact]
    public async Task PlacementWithoutBalancer_StillIngestsGossipedReports()
    {
        using RaftManager manager = MakeManager(c =>
        {
            c.EnableLeaderBalancer = false;
            c.ReplicationFactor = 3;
        });

        Assert.True(manager.Configuration.LoadReportsEnabled);

        HLCTimestamp now = manager.HybridLogicalClock.SendOrLocalEvent(0);
        GossipMessage digest = new("node-A:9001", 0, null)
        {
            LoadReport = MakeReport("node-A:9001", 1, now, Partition),
        };

        manager.ReceiveGossip(digest);
        await manager.SystemCoordinator.DrainAsync();

        Assert.Equal("node-A:9001", manager.GetPartitionLeaderHint(Partition));
    }

    /// <summary>
    /// Control for the gate: with nothing that consumes load reports enabled, an incoming
    /// report is still dropped at the gossip receiver — vanilla clusters pay nothing.
    /// </summary>
    [Fact]
    public async Task EverythingOff_GossipedReportIsNotIngested()
    {
        using RaftManager manager = MakeManager(c => c.EnableLeaderBalancer = false);

        Assert.False(manager.Configuration.LoadReportsEnabled);

        HLCTimestamp now = manager.HybridLogicalClock.SendOrLocalEvent(0);
        GossipMessage digest = new("node-A:9001", 0, null)
        {
            LoadReport = MakeReport("node-A:9001", 1, now, Partition),
        };

        manager.ReceiveGossip(digest);
        await manager.SystemCoordinator.DrainAsync();

        Assert.Null(manager.GetPartitionLeaderHint(Partition));
    }

    // ── Cross-thread safety ──────────────────────────────────────────────────

    /// <summary>
    /// Hint reads race the coordinator loop's report ingestion by design (the store is now a
    /// concurrent dictionary; it was previously a plain one, making every cross-thread snapshot
    /// a torn-enumeration hazard). Hammer both sides and require only sane results.
    /// </summary>
    [Fact]
    public async Task ConcurrentReadsDuringIngestion_AreSafe()
    {
        using RaftManager manager = MakeManager();
        CancellationToken ct = TestContext.Current.CancellationToken;

        using CancellationTokenSource readers = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task readerTask = Task.Run(() =>
        {
            while (!readers.IsCancellationRequested)
            {
                string? hint = manager.GetPartitionLeaderHint(Partition);
                if (hint is not null)
                    Assert.StartsWith("node-", hint);
            }
        }, ct);

        for (int i = 1; i <= 500; i++)
        {
            HLCTimestamp t = manager.HybridLogicalClock.SendOrLocalEvent(0);
            manager.SystemCoordinator.Send(new RaftSystemRequest(
                MakeReport($"node-{i % 7}:900{i % 7}", i, t, Partition)));
        }

        await manager.SystemCoordinator.DrainAsync();
        readers.Cancel();
        await readerTask;

        Assert.NotNull(manager.GetPartitionLeaderHint(Partition));
    }
}
