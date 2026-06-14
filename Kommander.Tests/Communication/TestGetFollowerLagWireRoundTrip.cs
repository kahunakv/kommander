
using Google.Protobuf;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Wire correctness: the <c>GetFollowerLag</c> RPC messages must survive proto3 serialization,
/// including the nullable-value encoding (<c>HasValue + Value</c>).
/// </summary>
public class TestGetFollowerLagWireRoundTrip
{
    [Fact]
    public void GrpcGetFollowerLagRequest_SurvivesRoundTrip()
    {
        GrpcGetFollowerLagRequest original = new()
        {
            PartitionId = 3,
            FollowerEndpoint = "localhost:8205"
        };

        GrpcGetFollowerLagRequest parsed = GrpcGetFollowerLagRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(3, parsed.PartitionId);
        Assert.Equal("localhost:8205", parsed.FollowerEndpoint);
    }

    [Fact]
    public void GrpcGetFollowerLagResponse_WithValue_SurvivesRoundTrip()
    {
        GrpcGetFollowerLagResponse original = new() { HasValue = true, Value = 42 };

        GrpcGetFollowerLagResponse parsed = GrpcGetFollowerLagResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.True(parsed.HasValue);
        Assert.Equal(42, parsed.Value);
    }

    [Fact]
    public void GrpcGetFollowerLagResponse_NoValue_SurvivesRoundTrip()
    {
        GrpcGetFollowerLagResponse original = new() { HasValue = false };

        GrpcGetFollowerLagResponse parsed = GrpcGetFollowerLagResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.False(parsed.HasValue);
        Assert.Equal(0, parsed.Value); // proto3 default
    }
}
