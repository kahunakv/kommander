
using Google.Protobuf;
using System.Text.Json;
using Kommander.Communication;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.System;

namespace Kommander.Tests.Communication;

/// <summary>
/// Wire correctness: <c>Gossip</c>, <c>Ping</c>, and <c>PingReq</c> RPC messages must survive
/// proto3 and JSON serialization with all fields preserved.
/// </summary>
public class TestGossipWireRoundTrip
{
    // ── gRPC / proto3 ─────────────────────────────────────────────────────────

    [Fact]
    public void GrpcGossipRequest_NoRoster_SurvivesRoundTrip()
    {
        GrpcGossipRequest original = new()
        {
            SenderEndpoint = "localhost:8201",
            MembershipVersion = 42,
            RosterJson = ByteString.Empty
        };

        GrpcGossipRequest parsed = GrpcGossipRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
        Assert.Equal(42, parsed.MembershipVersion);
        Assert.True(parsed.RosterJson.IsEmpty);
    }

    [Fact]
    public void GrpcGossipRequest_WithRoster_SurvivesRoundTrip()
    {
        string rosterJson = "{\"membershipVersion\":42}";
        GrpcGossipRequest original = new()
        {
            SenderEndpoint = "localhost:8201",
            MembershipVersion = 42,
            RosterJson = ByteString.CopyFromUtf8(rosterJson)
        };

        GrpcGossipRequest parsed = GrpcGossipRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
        Assert.Equal(42, parsed.MembershipVersion);
        Assert.Equal(rosterJson, parsed.RosterJson.ToStringUtf8());
    }

    [Fact]
    public void GrpcGossipResponse_NoRoster_SurvivesRoundTrip()
    {
        GrpcGossipResponse original = new()
        {
            MembershipVersion = 99,
            RosterJson = ByteString.Empty
        };

        GrpcGossipResponse parsed = GrpcGossipResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(99, parsed.MembershipVersion);
        Assert.True(parsed.RosterJson.IsEmpty);
    }

    [Fact]
    public void GrpcGossipResponse_WithRoster_SurvivesRoundTrip()
    {
        string rosterJson = "{\"membershipVersion\":99}";
        GrpcGossipResponse original = new()
        {
            MembershipVersion = 99,
            RosterJson = ByteString.CopyFromUtf8(rosterJson)
        };

        GrpcGossipResponse parsed = GrpcGossipResponse.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(99, parsed.MembershipVersion);
        Assert.Equal(rosterJson, parsed.RosterJson.ToStringUtf8());
    }

    // ── REST / JSON ───────────────────────────────────────────────────────────

    [Fact]
    public void GossipRequest_NoRoster_SurvivesRestJsonRoundTrip()
    {
        GossipRequest original = new("localhost:8201", 42, null);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.GossipRequest);
        GossipRequest? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.GossipRequest);

        Assert.NotNull(parsed);
        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
        Assert.Equal(42, parsed.MembershipVersion);
        Assert.Null(parsed.RosterJson);
    }

    [Fact]
    public void GossipRequest_WithRoster_SurvivesRestJsonRoundTrip()
    {
        string rosterJson = "{\"membershipVersion\":42}";
        GossipRequest original = new("localhost:8201", 42, rosterJson);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.GossipRequest);
        GossipRequest? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.GossipRequest);

        Assert.NotNull(parsed);
        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
        Assert.Equal(42, parsed.MembershipVersion);
        Assert.Equal(rosterJson, parsed.RosterJson);
    }

    [Fact]
    public void GossipResponse_NoRoster_SurvivesRestJsonRoundTrip()
    {
        GossipResponse original = new(99, null);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.GossipResponse);
        GossipResponse? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.GossipResponse);

        Assert.NotNull(parsed);
        Assert.Equal(99, parsed.MembershipVersion);
        Assert.Null(parsed.RosterJson);
    }

    [Fact]
    public void GossipResponse_WithRoster_SurvivesRestJsonRoundTrip()
    {
        string rosterJson = "{\"membershipVersion\":99}";
        GossipResponse original = new(99, rosterJson);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.GossipResponse);
        GossipResponse? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.GossipResponse);

        Assert.NotNull(parsed);
        Assert.Equal(99, parsed.MembershipVersion);
        Assert.Equal(rosterJson, parsed.RosterJson);
    }

    // ── gRPC / proto3 – Ping ─────────────────────────────────────────────────

    [Fact]
    public void GrpcPingRequest_SurvivesRoundTrip()
    {
        GrpcPingRequest original = new() { SenderEndpoint = "localhost:8201" };
        GrpcPingRequest parsed = GrpcPingRequest.Parser.ParseFrom(original.ToByteArray());
        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
    }

    [Fact]
    public void GrpcPingResponse_SurvivesRoundTrip()
    {
        GrpcPingResponse original = new() { Alive = true, Incarnation = 7 };
        GrpcPingResponse parsed = GrpcPingResponse.Parser.ParseFrom(original.ToByteArray());
        Assert.True(parsed.Alive);
        Assert.Equal(7, parsed.Incarnation);
    }

    [Fact]
    public void GrpcPingReqRequest_SurvivesRoundTrip()
    {
        GrpcPingReqRequest original = new()
        {
            SenderEndpoint = "localhost:8201",
            TargetEndpoint = "localhost:8203"
        };
        GrpcPingReqRequest parsed = GrpcPingReqRequest.Parser.ParseFrom(original.ToByteArray());
        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
        Assert.Equal("localhost:8203", parsed.TargetEndpoint);
    }

    [Fact]
    public void GrpcPingReqResponse_SurvivesRoundTrip()
    {
        GrpcPingReqResponse original = new() { Reached = true };
        GrpcPingReqResponse parsed = GrpcPingReqResponse.Parser.ParseFrom(original.ToByteArray());
        Assert.True(parsed.Reached);
    }

    // ── REST / JSON – Ping ────────────────────────────────────────────────────

    [Fact]
    public void PingRequest_SurvivesRestJsonRoundTrip()
    {
        PingRequest original = new("localhost:8201");
        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.PingRequest);
        PingRequest? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.PingRequest);
        Assert.NotNull(parsed);
        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
    }

    [Fact]
    public void PingResponse_SurvivesRestJsonRoundTrip()
    {
        PingResponse original = new(true, 7);
        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.PingResponse);
        PingResponse? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.PingResponse);
        Assert.NotNull(parsed);
        Assert.True(parsed.Alive);
        Assert.Equal(7, parsed.Incarnation);
    }

    [Fact]
    public void PingReqRequest_SurvivesRestJsonRoundTrip()
    {
        PingReqRequest original = new("localhost:8201", "localhost:8203");
        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.PingReqRequest);
        PingReqRequest? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.PingReqRequest);
        Assert.NotNull(parsed);
        Assert.Equal("localhost:8201", parsed.SenderEndpoint);
        Assert.Equal("localhost:8203", parsed.TargetEndpoint);
    }

    [Fact]
    public void PingReqResponse_SurvivesRestJsonRoundTrip()
    {
        PingReqResponse original = new(true);
        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.PingReqResponse);
        PingReqResponse? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.PingReqResponse);
        Assert.NotNull(parsed);
        Assert.True(parsed.Reached);
    }

    // ── End-to-end JSON codec (ClusterMembership ↔ RosterJson) ───────────────

    [Fact]
    public void GossipRequest_ClusterMembership_SerializesAndDeserializesCorrectly()
    {
        ClusterMembership membership = new()
        {
            MembershipVersion = 7,
            Members =
            [
                new ClusterMember { Endpoint = "localhost:8201", NodeId = 1, Role = ClusterMemberRole.Voter },
                new ClusterMember { Endpoint = "localhost:8202", NodeId = 2, Role = ClusterMemberRole.Learner }
            ]
        };

        string rosterJson = JsonSerializer.Serialize(membership);
        GossipRequest request = new("localhost:8201", 7, rosterJson);

        string wireJson = JsonSerializer.Serialize(request, RestJsonContext.Default.GossipRequest);
        GossipRequest? parsed = JsonSerializer.Deserialize(wireJson, RestJsonContext.Default.GossipRequest);

        Assert.NotNull(parsed);
        ClusterMembership? roundTripped = JsonSerializer.Deserialize<ClusterMembership>(parsed.RosterJson!);
        Assert.NotNull(roundTripped);
        Assert.Equal(7, roundTripped.MembershipVersion);
        Assert.Equal(2, roundTripped.Members.Count);
        Assert.Equal("localhost:8201", roundTripped.Members[0].Endpoint);
        Assert.Equal(ClusterMemberRole.Learner, roundTripped.Members[1].Role);
    }
}
