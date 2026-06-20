
using System.Text.Json;
using Kommander.System;
using Kommander.Time;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Verifies that <see cref="NodeLoadReport"/> and <see cref="PartitionLoad"/> serialize
/// and deserialize correctly via <see cref="JsonSerializer"/> (the same serializer used
/// throughout the system coordinator).
/// </summary>
public sealed class TestNodeLoadReport
{
    [Fact]
    public void RoundTrip_EmptyLeaderships()
    {
        NodeLoadReport report = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Leaderships = [],
        };

        string json = JsonSerializer.Serialize(report);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize<NodeLoadReport>(json);

        Assert.NotNull(deserialized);
        Assert.Equal(report.Endpoint, deserialized.Endpoint);
        Assert.Equal(report.ReportVersion, deserialized.ReportVersion);
        Assert.Equal(report.Time, deserialized.Time);
        Assert.Empty(deserialized.Leaderships);
    }

    [Fact]
    public void RoundTrip_MultiplePartitionLoads()
    {
        NodeLoadReport report = new()
        {
            Endpoint = "node2:7001",
            ReportVersion = 42,
            Time = new HLCTimestamp(2, 200_000L, 7),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 12.5, LeaderSinceMs = 30_000 },
                new PartitionLoad { PartitionId = 3, Load = 0.0,  LeaderSinceMs = 1_500  },
                new PartitionLoad { PartitionId = 7, Load = 99.9, LeaderSinceMs = 120_000 },
            ],
        };

        string json = JsonSerializer.Serialize(report);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize<NodeLoadReport>(json);

        Assert.NotNull(deserialized);
        Assert.Equal(report.Endpoint, deserialized.Endpoint);
        Assert.Equal(report.ReportVersion, deserialized.ReportVersion);
        Assert.Equal(report.Time, deserialized.Time);
        Assert.Equal(3, deserialized.Leaderships.Count);

        Assert.Equal(1, deserialized.Leaderships[0].PartitionId);
        Assert.Equal(12.5, deserialized.Leaderships[0].Load);
        Assert.Equal(30_000L, deserialized.Leaderships[0].LeaderSinceMs);

        Assert.Equal(3, deserialized.Leaderships[1].PartitionId);
        Assert.Equal(0.0, deserialized.Leaderships[1].Load);
        Assert.Equal(1_500L, deserialized.Leaderships[1].LeaderSinceMs);

        Assert.Equal(7, deserialized.Leaderships[2].PartitionId);
        Assert.Equal(99.9, deserialized.Leaderships[2].Load);
        Assert.Equal(120_000L, deserialized.Leaderships[2].LeaderSinceMs);
    }

    [Fact]
    public void ReportVersion_IsPreservedExactly()
    {
        NodeLoadReport report = new()
        {
            Endpoint = "node3:7002",
            ReportVersion = long.MaxValue,
            Time = HLCTimestamp.Zero,
            Leaderships = [],
        };

        string json = JsonSerializer.Serialize(report);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize<NodeLoadReport>(json);

        Assert.NotNull(deserialized);
        Assert.Equal(long.MaxValue, deserialized.ReportVersion);
    }

    [Fact]
    public void DefaultEndpoint_IsEmptyString()
    {
        NodeLoadReport report = new();
        Assert.Equal("", report.Endpoint);
        Assert.Equal(0L, report.ReportVersion);
        Assert.Equal(HLCTimestamp.Zero, report.Time);
        Assert.Empty(report.Leaderships);
    }
}
