
using System.Text.Json;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Verifies the zone-gossip plumbing that makes zone-aware placement work on multi-process
/// clusters: the local node's <see cref="RaftConfiguration.Zone"/> travels on its
/// <see cref="NodeLoadReport"/> (JSON on the wire — no proto change), the coordinator's
/// load-report store answers remote-zone lookups for the placement planner, and
/// <see cref="RaftConfiguration.Validate"/> normalizes a whitespace-only zone to null so it
/// cannot silently disable the hint while looking configured. Previously only the local node's
/// zone ever reached the planner, so zone anti-affinity was inert outside in-process deployments.
/// </summary>
public sealed class TestZoneGossip
{
    private static RaftManager MakeManager(string? zone = null)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            EnableLeaderBalancer = true,
            Zone = zone,
        };
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

    [Fact]
    public void LocalReport_CarriesConfiguredZone()
    {
        using RaftManager manager = MakeManager(zone: "zone-a");

        NodeLoadReport report = manager.BuildLocalLoadReport();

        Assert.Equal("zone-a", report.Zone);
    }

    [Fact]
    public void Report_JsonRoundTrip_PreservesZone_AndLegacyReadsAsNull()
    {
        NodeLoadReport report = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 3,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Zone = "rack-7",
            Leaderships = [],
        };

        string json = JsonSerializer.Serialize(report);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize<NodeLoadReport>(json);

        Assert.NotNull(deserialized);
        Assert.Equal("rack-7", deserialized.Zone);

        // A report from an older sender simply has no "Zone" property — it must deserialize
        // with a null zone, not fail (the wire is JSON inside the gossip envelope).
        NodeLoadReport? legacy = JsonSerializer.Deserialize<NodeLoadReport>(
            """{"Endpoint":"old:7000","ReportVersion":1,"Leaderships":[]}""");
        Assert.NotNull(legacy);
        Assert.Null(legacy.Zone);
    }

    /// <summary>
    /// The coordinator-side lookup the placement candidates are built from: a gossiped report's
    /// zone must be answerable by endpoint, and — unlike the load figures — must survive past
    /// the freshness TTL, because a zone is topology and effectively immutable for a node's run.
    /// </summary>
    [Fact]
    public async Task GossipedReport_ZoneReachesCoordinatorLookup_EvenWhenAgedPastTtl()
    {
        using RaftManager manager = MakeManager();

        HLCTimestamp now = manager.HybridLogicalClock.SendOrLocalEvent(0);
        TimeSpan ttl = manager.Configuration.LeaderBalancerReportTtl;
        HLCTimestamp aged = new(now.N, now.L - (long)(ttl.TotalMilliseconds * 3), 0);

        manager.SystemCoordinator.Send(new RaftSystemRequest(new NodeLoadReport
        {
            Endpoint = "node-A:9001",
            ReportVersion = 1,
            Time = aged,
            Zone = "zone-b",
            Leaderships = [],
        }));
        await manager.SystemCoordinator.DrainAsync();

        Assert.Equal("zone-b", manager.SystemCoordinator.GetNodeZone("node-A:9001"));
        Assert.Null(manager.SystemCoordinator.GetNodeZone("never-reported:9009"));
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("", null)]
    [InlineData("   ", null)]
    [InlineData("  zone-a  ", "zone-a")]
    [InlineData("zone-a", "zone-a")]
    public void Validate_NormalizesZone(string? configured, string? expected)
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 1,
            Zone = configured,
        };

        config.Validate();

        Assert.Equal(expected, config.Zone);
    }
}
