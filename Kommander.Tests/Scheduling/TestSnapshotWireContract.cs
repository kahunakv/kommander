
using System.Text.Json;
using Google.Protobuf;
using Kommander.Communication;
using Kommander.Data;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Wire-contract tests for the snapshot session-metadata fields
/// (<c>LeaderTerm</c>, <c>LeaderEndpoint</c>, <c>LastIncludedTerm</c>). Verifies that both
/// serialized transport representations — the gRPC protobuf message (field numbers 9/10/11) and the
/// REST JSON body (source-generated <see cref="RestJsonContext"/>) — carry the fields through a
/// full round-trip so a real transport propagates them to the receiver.
/// </summary>
public class TestSnapshotWireContract
{
    [Fact]
    public void GrpcInstallSnapshotRequest_RoundTripsNewMetadataFields()
    {
        GrpcInstallSnapshotRequest original = new()
        {
            SessionId = "sess-1",
            PartitionId = 7,
            SnapshotIndex = 4242,
            FollowerEndpoint = "follower:9002",
            ChunkIndex = 3,
            IsLast = true,
            Data = ByteString.CopyFrom([1, 2, 3]),
            Kind = (int)SnapshotKind.SystemState,
            LeaderTerm = 11,
            LeaderEndpoint = "leader:9001",
            LastIncludedTerm = 9,
        };

        // Serialize through protobuf and parse back — exercises the generated field 9/10/11 codecs.
        byte[] wire = original.ToByteArray();
        GrpcInstallSnapshotRequest parsed = GrpcInstallSnapshotRequest.Parser.ParseFrom(wire);

        Assert.Equal(11, parsed.LeaderTerm);
        Assert.Equal("leader:9001", parsed.LeaderEndpoint);
        Assert.Equal(9, parsed.LastIncludedTerm);
        // Pre-existing fields still intact alongside the additions.
        Assert.Equal("sess-1", parsed.SessionId);
        Assert.Equal(4242, parsed.SnapshotIndex);
        Assert.Equal((int)SnapshotKind.SystemState, parsed.Kind);
    }

    [Fact]
    public void GrpcInstallSnapshotRequest_FieldNumbersAreStable()
    {
        // Guard against accidental renumbering that would break rolling upgrades.
        Assert.Equal(9, GrpcInstallSnapshotRequest.LeaderTermFieldNumber);
        Assert.Equal(10, GrpcInstallSnapshotRequest.LeaderEndpointFieldNumber);
        Assert.Equal(11, GrpcInstallSnapshotRequest.LastIncludedTermFieldNumber);
    }

    [Fact]
    public void RestSnapshotRequest_RoundTripsNewMetadataFields()
    {
        SnapshotRequest original = new()
        {
            SessionId = "sess-2",
            PartitionId = 4,
            SnapshotIndex = 500,
            FollowerEndpoint = "follower:9003",
            ChunkIndex = 1,
            IsLast = false,
            Data = new byte[] { 9, 8, 7 },
            Kind = SnapshotKind.Range,
            LeaderTerm = 13,
            LeaderEndpoint = "leader:9004",
            LastIncludedTerm = 12,
        };

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.SnapshotRequest);
        SnapshotRequest? parsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.SnapshotRequest);

        Assert.NotNull(parsed);
        Assert.Equal(13, parsed!.LeaderTerm);
        Assert.Equal("leader:9004", parsed.LeaderEndpoint);
        Assert.Equal(12, parsed.LastIncludedTerm);
        Assert.Equal("sess-2", parsed.SessionId);
        Assert.Equal(500, parsed.SnapshotIndex);
    }

    [Theory]
    [InlineData(SnapshotKind.Range)]
    [InlineData(SnapshotKind.SystemState)]
    public void GrpcAndRest_SnapshotKind_RoundTrips(SnapshotKind kind)
    {
        // gRPC protobuf round-trip.
        GrpcInstallSnapshotRequest grpc = new()
        {
            SessionId = "k", PartitionId = 0, SnapshotIndex = 1,
            IsLast = true, Data = ByteString.CopyFrom([1]), Kind = (int)kind,
        };
        GrpcInstallSnapshotRequest grpcParsed = GrpcInstallSnapshotRequest.Parser.ParseFrom(grpc.ToByteArray());
        Assert.Equal((int)kind, grpcParsed.Kind);

        // REST JSON round-trip.
        SnapshotRequest rest = new()
        {
            SessionId = "k", PartitionId = 0, SnapshotIndex = 1, IsLast = true,
            Data = new byte[] { 1 }, Kind = kind,
        };
        string json = JsonSerializer.Serialize(rest, RestJsonContext.Default.SnapshotRequest);
        SnapshotRequest? restParsed = JsonSerializer.Deserialize(json, RestJsonContext.Default.SnapshotRequest);
        Assert.NotNull(restParsed);
        Assert.Equal(kind, restParsed!.Kind);
    }

    [Fact]
    public void LegacyRequest_HasZeroEmptyMetadataDefaults()
    {
        // A request that omits the new fields (a legacy sender) parses with zero/empty defaults,
        // which is the sentinel the receiver uses to recognise legacy senders.
        SnapshotRequest legacy = new()
        {
            SessionId = "old",
            PartitionId = 1,
            SnapshotIndex = 10,
            IsLast = true,
        };

        Assert.Equal(0, legacy.LeaderTerm);
        Assert.Equal("", legacy.LeaderEndpoint);
        Assert.Equal(0, legacy.LastIncludedTerm);
    }
}
