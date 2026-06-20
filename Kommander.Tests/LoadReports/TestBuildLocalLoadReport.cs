
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Unit tests for <see cref="RaftManager.BuildLocalLoadReport"/>: verifies that the
/// builder enumerates only partitions this node currently leads, stamps monotonically
/// increasing <c>ReportVersion</c> values, and populates the expected fields.
/// </summary>
public sealed class TestBuildLocalLoadReport
{
    [Fact]
    public void BuildLocalLoadReport_ListsOnlyLedPartitions()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
            EnableLeaderBalancer = true,
            LeaderBalancerOpsWeight = 1.0,
            LeaderBalancerQueueWeight = 0.5,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        // This node leads partitions 1 and 3; partition 2 is led by a peer.
        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);
        RaftPartition p2 = new(manager, wal, 2, 0, 0, NullLogger<IRaft>.Instance);
        RaftPartition p3 = new(manager, wal, 3, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            manager.Partitions[2] = p2;
            manager.Partitions[3] = p3;

            p1.Leader = manager.LocalEndpoint;
            p2.Leader = "peer:9001";
            p3.Leader = manager.LocalEndpoint;

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Equal(manager.LocalEndpoint, report.Endpoint);
            Assert.Equal(1L, report.ReportVersion);
            Assert.Equal(2, report.Leaderships.Count);

            global::System.Collections.Generic.HashSet<int> led =
                [.. report.Leaderships.Select(l => l.PartitionId)];

            Assert.Contains(1, led);
            Assert.Contains(3, led);
            Assert.DoesNotContain(2, led);
        }
        finally
        {
            p1.Dispose();
            p2.Dispose();
            p3.Dispose();
        }
    }

    [Fact]
    public void BuildLocalLoadReport_ReportVersion_IsMonotonicallyIncreasing()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        NodeLoadReport r1 = manager.BuildLocalLoadReport();
        NodeLoadReport r2 = manager.BuildLocalLoadReport();
        NodeLoadReport r3 = manager.BuildLocalLoadReport();

        Assert.True(r1.ReportVersion < r2.ReportVersion);
        Assert.True(r2.ReportVersion < r3.ReportVersion);
        Assert.Equal(r1.ReportVersion + 1, r2.ReportVersion);
        Assert.Equal(r2.ReportVersion + 1, r3.ReportVersion);
    }

    [Fact]
    public void BuildLocalLoadReport_NoLedPartitions_YieldsEmptyLeaderships()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = "other:9000"; // this node doesn't lead it

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Equal(manager.LocalEndpoint, report.Endpoint);
            Assert.Empty(report.Leaderships);
        }
        finally
        {
            p1.Dispose();
        }
    }

    [Fact]
    public void BuildLocalLoadReport_LedPartition_HasNonNegativeLeaderSinceMs()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9000,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        RaftPartition p1 = new(manager, wal, 1, 0, 0, NullLogger<IRaft>.Instance);

        try
        {
            manager.Partitions[1] = p1;
            p1.Leader = manager.LocalEndpoint;

            NodeLoadReport report = manager.BuildLocalLoadReport();

            Assert.Single(report.Leaderships);
            Assert.True(report.Leaderships[0].LeaderSinceMs >= 0,
                "LeaderSinceMs must never be negative");
        }
        finally
        {
            p1.Dispose();
        }
    }

    [Fact]
    public void BuildLocalLoadReport_EndpointMatchesManagerLocalEndpoint()
    {
        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftConfiguration config = new()
        {
            Host = "mynode",
            Port = 7777,
            InitialPartitions = 0,
        };

        using RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();

        NodeLoadReport report = manager.BuildLocalLoadReport();

        Assert.Equal("mynode:7777", report.Endpoint);
    }
}
