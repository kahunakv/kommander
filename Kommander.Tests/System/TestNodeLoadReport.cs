
using System.Text.Json;
using System.Text.Json.Nodes;
using Kommander.Communication;
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

    // ── Rolling-upgrade / additive-field compatibility (Task 5) ─────────────
    // LogOpsPerSecond was added to PartitionLoad in a later version. Older nodes
    // will emit JSON that omits the field. Both deserialization paths (plain
    // JsonSerializer used by the gRPC LoadReportJson wire, and the source-generated
    // RestJsonContext used by the REST path) must default the missing field to 0.

    /// <summary>
    /// gRPC path (plain <c>JsonSerializer</c>): a <c>PartitionLoad</c> payload omitting
    /// <c>LogOpsPerSecond</c> must deserialize without error and yield <c>0.0</c>.
    /// Uses the serializer to generate current-format JSON, then strips the new field via
    /// <c>JsonNode</c> — this avoids hard-coding property casing or the <c>HLCTimestamp</c>
    /// JSON shape, both of which can drift.
    /// </summary>
    [Fact]
    public void GrpcPath_MissingLogOpsPerSecond_DefaultsToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Leaderships = [new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, LogOpsPerSecond = 99.0 }],
        };

        // Strip LogOpsPerSecond to simulate an older node's payload.
        string fullJson = JsonSerializer.Serialize(source);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root["Leaderships"]![0]!.AsObject().Remove("LogOpsPerSecond");
        string legacyJson = root.ToJsonString();

        NodeLoadReport? report = JsonSerializer.Deserialize<NodeLoadReport>(legacyJson);

        Assert.NotNull(report);
        Assert.Single(report.Leaderships);
        Assert.Equal(0.0, report.Leaderships[0].LogOpsPerSecond);
    }

    /// <summary>
    /// gRPC path (plain <c>JsonSerializer</c>): a payload including <c>LogOpsPerSecond</c>
    /// must round-trip the value exactly.
    /// </summary>
    [Fact]
    public void GrpcPath_LogOpsPerSecond_RoundTrips()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, LogOpsPerSecond = 42.5 },
            ],
        };

        string json = JsonSerializer.Serialize(original);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize<NodeLoadReport>(json);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.Leaderships);
        Assert.Equal(42.5, deserialized.Leaderships[0].LogOpsPerSecond);
    }

    /// <summary>
    /// REST path (<c>RestJsonContext</c>): a payload omitting <c>LogOpsPerSecond</c>
    /// must deserialize without error and yield <c>0.0</c>.
    /// </summary>
    [Fact]
    public void RestPath_MissingLogOpsPerSecond_DefaultsToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Leaderships = [new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, LogOpsPerSecond = 99.0 }],
        };

        // Strip logOpsPerSecond (camelCase under RestJsonContext) to simulate an older node.
        string fullJson = JsonSerializer.Serialize(source, RestJsonContext.Default.NodeLoadReport);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root["leaderships"]![0]!.AsObject().Remove("logOpsPerSecond");
        string legacyJson = root.ToJsonString();

        NodeLoadReport? report = JsonSerializer.Deserialize(legacyJson, RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(report);
        Assert.Single(report.Leaderships);
        Assert.Equal(0.0, report.Leaderships[0].LogOpsPerSecond);
    }

    /// <summary>
    /// REST path (<c>RestJsonContext</c>): a payload including <c>LogOpsPerSecond</c>
    /// must round-trip the value exactly.
    /// </summary>
    [Fact]
    public void RestPath_LogOpsPerSecond_RoundTrips()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, LogOpsPerSecond = 17.3 },
            ],
        };

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.NodeLoadReport);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize(json, RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.Leaderships);
        Assert.Equal(17.3, deserialized.Leaderships[0].LogOpsPerSecond);
    }
}
