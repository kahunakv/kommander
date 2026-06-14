
using Google.Protobuf;
using System.Text.Json;
using Kommander.Communication;
using Kommander.Communication.Grpc;
using Kommander.Data;

namespace Kommander.Tests.Communication;

/// <summary>
/// Wire correctness: the <c>Leave</c> RPC messages must survive proto3 serialization with all
/// fields preserved, including the <c>LeaderHint</c> redirect path.
/// REST JSON round-trips are validated separately in <see cref="TestRestJsonContext"/>.
/// </summary>
public class TestLeaveWireRoundTrip
{
    // ── gRPC / proto3 ─────────────────────────────────────────────────────────

    [Fact]
    public void GrpcLeaveRequest_SurvivesRoundTrip()
    {
        GrpcLeaveRequest original = new() { Endpoint = "localhost:8204", NodeId = 4 };

        GrpcLeaveRequest parsed = GrpcLeaveRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal("localhost:8204", parsed.Endpoint);
        Assert.Equal(4, parsed.NodeId);
    }

    [Fact]
    public void GrpcLeaveResponse_Success_SurvivesRoundTrip()
    {
        GrpcLeaveResponse original = new() { Success = true };

        GrpcLeaveResponse parsed = GrpcLeaveResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.True(parsed.Success);
        Assert.Equal("", parsed.LeaderHint); // proto3 default
    }

    [Fact]
    public void GrpcLeaveResponse_NotLeader_LeaderHintSurvivesRoundTrip()
    {
        GrpcLeaveResponse original = new() { Success = false, LeaderHint = "localhost:8201" };

        GrpcLeaveResponse parsed = GrpcLeaveResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.False(parsed.Success);
        Assert.Equal("localhost:8201", parsed.LeaderHint);
    }

    // ── REST / JSON ───────────────────────────────────────────────────────────

    [Fact]
    public void LeaveRequest_SurvivesRestJsonRoundTrip()
    {
        LeaveRequest original = new("localhost:8204", 4);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.LeaveRequest);
        LeaveRequest? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.LeaveRequest);

        Assert.NotNull(parsed);
        Assert.Equal("localhost:8204", parsed.Endpoint);
        Assert.Equal(4, parsed.NodeId);
    }

    [Fact]
    public void LeaveResponse_Success_SurvivesRestJsonRoundTrip()
    {
        LeaveResponse original = new(true);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.LeaveResponse);
        LeaveResponse? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.LeaveResponse);

        Assert.NotNull(parsed);
        Assert.True(parsed.Success);
        Assert.Null(parsed.LeaderHint);
    }

    [Fact]
    public void LeaveResponse_NotLeader_LeaderHintSurvivesRestJsonRoundTrip()
    {
        LeaveResponse original = new(false, "localhost:8201");

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.LeaveResponse);
        LeaveResponse? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.LeaveResponse);

        Assert.NotNull(parsed);
        Assert.False(parsed.Success);
        Assert.Equal("localhost:8201", parsed.LeaderHint);
    }
}
