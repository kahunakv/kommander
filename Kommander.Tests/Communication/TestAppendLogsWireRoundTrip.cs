
using Google.Protobuf;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Wire correctness: <c>PrevLogIndex</c> and <c>PrevLogTerm</c> (the Log Matching Property
/// anchor on <c>GrpcAppendLogsRequest</c>) must survive proto3 serialization.
/// Proto3 int64 defaults to 0, so an un-upgraded peer that never sets the fields must
/// deserialize with both values as 0 — preserving backward compatibility.
/// </summary>
public class TestAppendLogsWireRoundTrip
{
    [Fact]
    public void GrpcAppendLogsRequest_PrevLogFields_SurviveRoundTrip()
    {
        GrpcAppendLogsRequest original = new()
        {
            Partition = 2,
            Term = 5,
            Endpoint = "localhost:9000",
            PrevLogIndex = 42,
            PrevLogTerm = 3
        };

        GrpcAppendLogsRequest parsed = GrpcAppendLogsRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(42, parsed.PrevLogIndex);
        Assert.Equal(3, parsed.PrevLogTerm);
        Assert.Equal(2, parsed.Partition);
        Assert.Equal(5, parsed.Term);
    }

    [Fact]
    public void GrpcAppendLogsRequest_UnsetPrevLogFields_DefaultToZero()
    {
        // Simulates an un-upgraded peer that never wrote fields 8 and 9.
        GrpcAppendLogsRequest original = new()
        {
            Partition = 1,
            Term = 4,
            Endpoint = "localhost:9001"
        };

        GrpcAppendLogsRequest parsed = GrpcAppendLogsRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(0, parsed.PrevLogIndex);
        Assert.Equal(0, parsed.PrevLogTerm);
    }

    /// <summary>
    /// The leader's live-replica retention floor and budget (fields 11 and 12) must reach the
    /// follower, which holds its own WAL compaction there. A floor of <see cref="long.MaxValue"/> is
    /// "no replica constrains retention" and must survive as itself; an absent field reads 0, which
    /// is "no statement" — what a leader that predates the fields sends.
    /// </summary>
    [Fact]
    public void GrpcAppendLogsRequest_RetentionFields_SurviveRoundTrip()
    {
        GrpcAppendLogsRequest original = new()
        {
            Partition = 2,
            Term = 5,
            Endpoint = "localhost:9000",
            RetentionFloor = 53_696_438,
            RetentionBudget = 1_000_000,
        };

        GrpcAppendLogsRequest parsed = GrpcAppendLogsRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(53_696_438, parsed.RetentionFloor);
        Assert.Equal(1_000_000, parsed.RetentionBudget);

        original.RetentionFloor = long.MaxValue;
        Assert.Equal(long.MaxValue, GrpcAppendLogsRequest.Parser.ParseFrom(original.ToByteArray()).RetentionFloor);

        Assert.Equal(11, GrpcAppendLogsRequest.RetentionFloorFieldNumber);
        Assert.Equal(12, GrpcAppendLogsRequest.RetentionBudgetFieldNumber);
    }

    [Fact]
    public void GrpcAppendLogsRequest_UnsetRetentionFields_DefaultToZero()
    {
        GrpcAppendLogsRequest parsed = GrpcAppendLogsRequest.Parser.ParseFrom(
            new GrpcAppendLogsRequest { Partition = 1, Term = 4, Endpoint = "localhost:9001" }.ToByteArray());

        Assert.Equal(0, parsed.RetentionFloor);
        Assert.Equal(0, parsed.RetentionBudget);
    }

    [Fact]
    public void RestAppendLogsRequest_RetentionFields_SurviveRoundTrip()
    {
        Kommander.Data.AppendLogsRequest original = new(2, 5, new Kommander.Time.HLCTimestamp(1, 100, 5), "localhost:9000")
        {
            RetentionFloor = long.MaxValue,
            RetentionBudget = 1_000_000,
        };

        Kommander.Data.AppendLogsRequest? parsed = global::System.Text.Json.JsonSerializer.Deserialize(
            global::System.Text.Json.JsonSerializer.Serialize(original, Kommander.Communication.RestJsonContext.Default.AppendLogsRequest),
            Kommander.Communication.RestJsonContext.Default.AppendLogsRequest);

        Assert.NotNull(parsed);
        Assert.Equal(long.MaxValue, parsed!.RetentionFloor);
        Assert.Equal(1_000_000, parsed.RetentionBudget);

        // A body from a leader that predates the fields.
        Kommander.Data.AppendLogsRequest? legacy = global::System.Text.Json.JsonSerializer.Deserialize(
            """{"partition":2,"term":5,"time":{"n":1,"l":100,"c":5},"endpoint":"localhost:9000"}""",
            Kommander.Communication.RestJsonContext.Default.AppendLogsRequest);

        Assert.NotNull(legacy);
        Assert.Equal(0, legacy!.RetentionFloor);
    }
}
