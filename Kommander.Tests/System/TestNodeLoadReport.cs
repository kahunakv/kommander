
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

    // ── Rolling-upgrade / additive-field compatibility ──────────────────────
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
    /// Source-gen context (<c>RestJsonContext</c>) additive-field safety: a payload omitting
    /// <c>LogOpsPerSecond</c> must deserialize without error and yield <c>0.0</c>.
    /// Note: REST gossip uses the plain serializer (same path as gRPC); this test covers
    /// the source-gen context in case any future REST endpoint serializes these types directly.
    /// </summary>
    [Fact]
    public void SourceGenContext_MissingLogOpsPerSecond_DefaultsToZero()
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
    /// Source-gen context (<c>RestJsonContext</c>) additive-field safety: a payload including
    /// <c>LogOpsPerSecond</c> must round-trip the value exactly through the source-generated context.
    /// </summary>
    [Fact]
    public void SourceGenContext_LogOpsPerSecond_RoundTrips()
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

    // ── WalQueueDepth rolling-upgrade / additive-field compatibility ──────────
    // WalQueueDepth was added to PartitionLoad in a later version. Older nodes will
    // omit the field. Both deserialization paths must default the missing field to 0.

    /// <summary>
    /// gRPC path (plain <c>JsonSerializer</c>): a payload omitting <c>WalQueueDepth</c>
    /// must deserialize without error and yield <c>0</c>.
    /// </summary>
    [Fact]
    public void GrpcPath_MissingWalQueueDepth_DefaultsToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Leaderships = [new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, WalQueueDepth = 42 }],
        };

        string fullJson = JsonSerializer.Serialize(source);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root["Leaderships"]![0]!.AsObject().Remove("WalQueueDepth");
        string legacyJson = root.ToJsonString();

        NodeLoadReport? report = JsonSerializer.Deserialize<NodeLoadReport>(legacyJson);

        Assert.NotNull(report);
        Assert.Single(report.Leaderships);
        Assert.Equal(0, report.Leaderships[0].WalQueueDepth);
    }

    /// <summary>
    /// gRPC path (plain <c>JsonSerializer</c>): a payload including <c>WalQueueDepth</c>
    /// must round-trip the value exactly.
    /// </summary>
    [Fact]
    public void GrpcPath_WalQueueDepth_RoundTrips()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, WalQueueDepth = 17 },
            ],
        };

        string json = JsonSerializer.Serialize(original);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize<NodeLoadReport>(json);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.Leaderships);
        Assert.Equal(17, deserialized.Leaderships[0].WalQueueDepth);
    }

    /// <summary>
    /// Source-gen context (<c>RestJsonContext</c>) additive-field safety: a payload omitting
    /// <c>WalQueueDepth</c> must deserialize without error and yield <c>0</c>.
    /// </summary>
    [Fact]
    public void SourceGenContext_MissingWalQueueDepth_DefaultsToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Leaderships = [new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, WalQueueDepth = 42 }],
        };

        string fullJson = JsonSerializer.Serialize(source, RestJsonContext.Default.NodeLoadReport);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root["leaderships"]![0]!.AsObject().Remove("walQueueDepth");
        string legacyJson = root.ToJsonString();

        NodeLoadReport? report = JsonSerializer.Deserialize(legacyJson, RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(report);
        Assert.Single(report.Leaderships);
        Assert.Equal(0, report.Leaderships[0].WalQueueDepth);
    }

    /// <summary>
    /// Source-gen context (<c>RestJsonContext</c>) additive-field safety: a payload including
    /// <c>WalQueueDepth</c> must round-trip the value exactly.
    /// </summary>
    [Fact]
    public void SourceGenContext_WalQueueDepth_RoundTrips()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, WalQueueDepth = 99 },
            ],
        };

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.NodeLoadReport);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize(json, RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.Leaderships);
        Assert.Equal(99, deserialized.Leaderships[0].WalQueueDepth);
    }

    // ── CommitWaitMs rolling-upgrade / additive-field compatibility ───────────
    // CommitWaitMs was added to PartitionLoad in a later version. Older nodes will
    // omit the field. Both deserialization paths must default the missing field to 0.

    /// <summary>
    /// gRPC path (plain <c>JsonSerializer</c>): a payload omitting <c>CommitWaitMs</c>
    /// must deserialize without error and yield <c>0.0</c>.
    /// </summary>
    [Fact]
    public void GrpcPath_MissingCommitWaitMs_DefaultsToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Leaderships = [new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, CommitWaitMs = 42.5 }],
        };

        string fullJson = JsonSerializer.Serialize(source);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root["Leaderships"]![0]!.AsObject().Remove("CommitWaitMs");
        string legacyJson = root.ToJsonString();

        NodeLoadReport? report = JsonSerializer.Deserialize<NodeLoadReport>(legacyJson);

        Assert.NotNull(report);
        Assert.Single(report.Leaderships);
        Assert.Equal(0.0, report.Leaderships[0].CommitWaitMs);
    }

    /// <summary>
    /// gRPC path (plain <c>JsonSerializer</c>): a payload including <c>CommitWaitMs</c>
    /// must round-trip the value exactly.
    /// </summary>
    [Fact]
    public void GrpcPath_CommitWaitMs_RoundTrips()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, CommitWaitMs = 15.75 },
            ],
        };

        string json = JsonSerializer.Serialize(original);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize<NodeLoadReport>(json);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.Leaderships);
        Assert.Equal(15.75, deserialized.Leaderships[0].CommitWaitMs);
    }

    /// <summary>
    /// Source-gen context (<c>RestJsonContext</c>): a payload omitting <c>CommitWaitMs</c>
    /// must deserialize without error and yield <c>0.0</c>.
    /// </summary>
    [Fact]
    public void SourceGenContext_MissingCommitWaitMs_DefaultsToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Leaderships = [new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, CommitWaitMs = 42.5 }],
        };

        string fullJson = JsonSerializer.Serialize(source, RestJsonContext.Default.NodeLoadReport);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root["leaderships"]![0]!.AsObject().Remove("commitWaitMs");
        string legacyJson = root.ToJsonString();

        NodeLoadReport? report = JsonSerializer.Deserialize(legacyJson, RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(report);
        Assert.Single(report.Leaderships);
        Assert.Equal(0.0, report.Leaderships[0].CommitWaitMs);
    }

    /// <summary>
    /// Source-gen context (<c>RestJsonContext</c>): a payload including <c>CommitWaitMs</c>
    /// must round-trip the value exactly.
    /// </summary>
    [Fact]
    public void SourceGenContext_CommitWaitMs_RoundTrips()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            Leaderships =
            [
                new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000, CommitWaitMs = 8.3 },
            ],
        };

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.NodeLoadReport);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize(json, RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.Leaderships);
        Assert.Equal(8.3, deserialized.Leaderships[0].CommitWaitMs);
    }

    // ── Node-level health fields: rolling-upgrade / additive compatibility ────
    // NodeCommitWaitMs/Samples/AgeMs were added to NodeLoadReport for degraded-node leader
    // avoidance. An older peer omits all three. The zero they default to must read as "unknown",
    // which is why the sample count travels beside the figure.

    /// <summary>
    /// gRPC path (plain <c>JsonSerializer</c>): a payload omitting the three node-health fields
    /// must deserialize without error and yield zeros — a zero sample count, which the detector
    /// treats as unknown rather than healthy.
    /// </summary>
    [Fact]
    public void GrpcPath_MissingNodeHealthFields_DefaultToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            NodeCommitWaitMs = 42.5,
            NodeCommitWaitSamples = 128,
            NodeCommitWaitAgeMs = 900,
            Leaderships = [new PartitionLoad { PartitionId = 1, Load = 5.0, LeaderSinceMs = 10_000 }],
        };

        string fullJson = JsonSerializer.Serialize(source);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root.Remove("NodeCommitWaitMs");
        root.Remove("NodeCommitWaitSamples");
        root.Remove("NodeCommitWaitAgeMs");

        NodeLoadReport? report = JsonSerializer.Deserialize<NodeLoadReport>(root.ToJsonString());

        Assert.NotNull(report);
        Assert.Equal(0.0, report.NodeCommitWaitMs);
        Assert.Equal(0L, report.NodeCommitWaitSamples);
        Assert.Equal(0L, report.NodeCommitWaitAgeMs);
        Assert.Single(report.Leaderships);
    }

    /// <summary>
    /// gRPC path: the three node-health fields must round-trip exactly.
    /// </summary>
    [Fact]
    public void GrpcPath_NodeHealthFields_RoundTrip()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            Time = new HLCTimestamp(1, 100_000L, 0),
            NodeCommitWaitMs = 15.75,
            NodeCommitWaitSamples = 64,
            NodeCommitWaitAgeMs = 1_250,
        };

        NodeLoadReport? deserialized =
            JsonSerializer.Deserialize<NodeLoadReport>(JsonSerializer.Serialize(original));

        Assert.NotNull(deserialized);
        Assert.Equal(15.75, deserialized.NodeCommitWaitMs);
        Assert.Equal(64L, deserialized.NodeCommitWaitSamples);
        Assert.Equal(1_250L, deserialized.NodeCommitWaitAgeMs);
    }

    /// <summary>
    /// Source-gen context (<c>RestJsonContext</c>): a payload omitting the three node-health fields
    /// must deserialize without error and yield zeros.
    /// </summary>
    [Fact]
    public void SourceGenContext_MissingNodeHealthFields_DefaultToZero()
    {
        NodeLoadReport source = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            NodeCommitWaitMs = 42.5,
            NodeCommitWaitSamples = 128,
            NodeCommitWaitAgeMs = 900,
        };

        // The source-gen context is configured for camelCase, so the wire names differ from the
        // property names — strip what an older peer would actually have omitted.
        string fullJson = JsonSerializer.Serialize(source, RestJsonContext.Default.NodeLoadReport);
        JsonObject root = JsonNode.Parse(fullJson)!.AsObject();
        root.Remove("nodeCommitWaitMs");
        root.Remove("nodeCommitWaitSamples");
        root.Remove("nodeCommitWaitAgeMs");

        NodeLoadReport? report = JsonSerializer.Deserialize(
            root.ToJsonString(), RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(report);
        Assert.Equal(0.0, report.NodeCommitWaitMs);
        Assert.Equal(0L, report.NodeCommitWaitSamples);
        Assert.Equal(0L, report.NodeCommitWaitAgeMs);
    }

    /// <summary>
    /// Source-gen context: the three node-health fields must round-trip exactly. The REST context
    /// is source-generated, so a new public property is only carried if the generator picked it up.
    /// </summary>
    [Fact]
    public void SourceGenContext_NodeHealthFields_RoundTrip()
    {
        NodeLoadReport original = new()
        {
            Endpoint = "node1:7000",
            ReportVersion = 1,
            NodeCommitWaitMs = 8.5,
            NodeCommitWaitSamples = 33,
            NodeCommitWaitAgeMs = 700,
        };

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.NodeLoadReport);
        NodeLoadReport? deserialized = JsonSerializer.Deserialize(json, RestJsonContext.Default.NodeLoadReport);

        Assert.NotNull(deserialized);
        Assert.Equal(8.5, deserialized.NodeCommitWaitMs);
        Assert.Equal(33L, deserialized.NodeCommitWaitSamples);
        Assert.Equal(700L, deserialized.NodeCommitWaitAgeMs);
    }
}
