
using Google.Protobuf;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Wire correctness: the <c>Join</c> RPC messages must survive proto3 serialization with
/// all fields preserved including the <c>LeaderHint</c> redirect and <c>MembershipVersion</c>.
/// </summary>
public class TestJoinWireRoundTrip
{
    [Fact]
    public void GrpcJoinRequest_SurvivesRoundTrip()
    {
        GrpcJoinRequest original = new() { Endpoint = "localhost:8004", NodeId = 4 };

        GrpcJoinRequest parsed = GrpcJoinRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal("localhost:8004", parsed.Endpoint);
        Assert.Equal(4, parsed.NodeId);
    }

    [Fact]
    public void GrpcJoinResponse_Success_SurvivesRoundTrip()
    {
        GrpcJoinResponse original = new() { Success = true, MembershipVersion = 5 };

        GrpcJoinResponse parsed = GrpcJoinResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.True(parsed.Success);
        Assert.Equal(5, parsed.MembershipVersion);
        Assert.Equal("", parsed.LeaderHint); // proto3 default
    }

    [Fact]
    public void GrpcJoinResponse_NotLeader_LeaderHintSurvivesRoundTrip()
    {
        GrpcJoinResponse original = new() { Success = false, LeaderHint = "localhost:8001" };

        GrpcJoinResponse parsed = GrpcJoinResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.False(parsed.Success);
        Assert.Equal("localhost:8001", parsed.LeaderHint);
        Assert.Equal(0, parsed.MembershipVersion); // proto3 default
    }
}
