using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Unit tests for the gossip load-report store in <see cref="RaftSystemCoordinator"/>.
/// Verifies that reports delivered via <see cref="RaftSystemRequestType.ApplyGossipLoadReport"/>
/// are retained with last-version-wins semantics, and that
/// <see cref="RaftSystemCoordinator.GetLoadReports"/> returns a snapshot of what was stored.
/// </summary>
public sealed class TestGossipLoadReportWiring
{
    private static RaftManager MakeManager()
    {
        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
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
            NullLogger<IRaft>.Instance
        );
    }

    private static NodeLoadReport MakeReport(string endpoint, long version) => new()
    {
        Endpoint = endpoint,
        ReportVersion = version,
        Leaderships = [],
    };

    [Fact]
    public async Task ApplyGossipLoadReport_StoresReport()
    {
        using RaftManager manager = MakeManager();
        RaftSystemCoordinator coordinator = manager.SystemCoordinator;

        coordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 1)));
        await coordinator.DrainAsync();

        IReadOnlyList<NodeLoadReport> stored = coordinator.GetLoadReports();
        Assert.Single(stored);
        Assert.Equal("node-A:9001", stored[0].Endpoint);
        Assert.Equal(1, stored[0].ReportVersion);
    }

    [Fact]
    public async Task ApplyGossipLoadReport_KeepsHigherVersion()
    {
        using RaftManager manager = MakeManager();
        RaftSystemCoordinator coordinator = manager.SystemCoordinator;

        coordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 1)));
        coordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 3)));
        coordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 2)));
        await coordinator.DrainAsync();

        IReadOnlyList<NodeLoadReport> stored = coordinator.GetLoadReports();
        Assert.Single(stored);
        Assert.Equal(3, stored[0].ReportVersion);
    }

    [Fact]
    public async Task ApplyGossipLoadReport_SeparateEndpointsStoredIndependently()
    {
        using RaftManager manager = MakeManager();
        RaftSystemCoordinator coordinator = manager.SystemCoordinator;

        coordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 1)));
        coordinator.Send(new RaftSystemRequest(MakeReport("node-B:9002", 5)));
        await coordinator.DrainAsync();

        IReadOnlyList<NodeLoadReport> stored = coordinator.GetLoadReports();
        Assert.Equal(2, stored.Count);

        HashSet<string> endpoints = stored.Select(r => r.Endpoint).ToHashSet();
        Assert.Contains("node-A:9001", endpoints);
        Assert.Contains("node-B:9002", endpoints);
    }

    [Fact]
    public async Task ApplyGossipLoadReport_DropsNullEndpoint()
    {
        using RaftManager manager = MakeManager();
        RaftSystemCoordinator coordinator = manager.SystemCoordinator;

        NodeLoadReport badReport = new() { Endpoint = "", ReportVersion = 1, Leaderships = [] };
        coordinator.Send(new RaftSystemRequest(badReport));
        await coordinator.DrainAsync();

        Assert.Empty(coordinator.GetLoadReports());
    }

    [Fact]
    public async Task GetLoadReports_ReturnsSnapshotNotLiveView()
    {
        using RaftManager manager = MakeManager();
        RaftSystemCoordinator coordinator = manager.SystemCoordinator;

        coordinator.Send(new RaftSystemRequest(MakeReport("node-A:9001", 1)));
        await coordinator.DrainAsync();

        IReadOnlyList<NodeLoadReport> snapshot1 = coordinator.GetLoadReports();

        coordinator.Send(new RaftSystemRequest(MakeReport("node-B:9002", 2)));
        await coordinator.DrainAsync();

        // Earlier snapshot must not grow.
        Assert.Single(snapshot1);

        // New snapshot reflects both reports.
        IReadOnlyList<NodeLoadReport> snapshot2 = coordinator.GetLoadReports();
        Assert.Equal(2, snapshot2.Count);
    }
}
