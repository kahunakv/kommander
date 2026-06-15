
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
}
