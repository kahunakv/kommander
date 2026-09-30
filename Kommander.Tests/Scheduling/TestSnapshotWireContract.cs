
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

    [Fact]
    public void SnapshotKind_ValuesAreStable()
    {
        // The kind travels as a raw int (gRPC field 8 / REST "kind"); renumbering breaks
        // rolling upgrades, so pin every value.
        Assert.Equal(0, (int)SnapshotKind.Range);
        Assert.Equal(1, (int)SnapshotKind.SystemState);
        Assert.Equal(2, (int)SnapshotKind.PartitionState);
    }

    [Theory]
    [InlineData(SnapshotKind.Range)]
    [InlineData(SnapshotKind.SystemState)]
    [InlineData(SnapshotKind.PartitionState)]
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

    // ── the install as a step of its own: InstallPolling / StatusQuery and the install an answer names ──

    [Fact]
    public void GrpcInstallSnapshot_InstallStepFields_RoundTrip()
    {
        GrpcInstallSnapshotRequest request = new()
        {
            SessionId = "sess-3",
            PartitionId = 7,
            SnapshotIndex = 4242,
            ChunkIndex = -1,
            InstallPolling = true,
            StatusQuery = true,
        };

        GrpcInstallSnapshotRequest parsedRequest = GrpcInstallSnapshotRequest.Parser.ParseFrom(request.ToByteArray());
        Assert.True(parsedRequest.InstallPolling);
        Assert.True(parsedRequest.StatusQuery);
        Assert.Equal(-1, parsedRequest.ChunkIndex);

        GrpcInstallSnapshotResponse response = new()
        {
            Outcome = (int)SnapshotInstallOutcome.InstallPending,
            InstallSessionId = "sess-3",
            InstallIndex = 4242,
            InstallLeaderTerm = 11,
            InstallLeaderEndpoint = "leader:9001",
            InstallProgress = 1_048_576,
        };

        GrpcInstallSnapshotResponse parsedResponse = GrpcInstallSnapshotResponse.Parser.ParseFrom(response.ToByteArray());
        Assert.Equal((int)SnapshotInstallOutcome.InstallPending, parsedResponse.Outcome);
        Assert.Equal("sess-3", parsedResponse.InstallSessionId);
        Assert.Equal(4242, parsedResponse.InstallIndex);
        Assert.Equal(11, parsedResponse.InstallLeaderTerm);
        Assert.Equal("leader:9001", parsedResponse.InstallLeaderEndpoint);
        Assert.Equal(1_048_576, parsedResponse.InstallProgress);
    }

    [Fact]
    public void GrpcInstallSnapshot_InstallStepFieldNumbersAreStable()
    {
        Assert.Equal(14, GrpcInstallSnapshotRequest.InstallPollingFieldNumber);
        Assert.Equal(15, GrpcInstallSnapshotRequest.StatusQueryFieldNumber);

        Assert.Equal(3, GrpcInstallSnapshotResponse.InstallSessionIdFieldNumber);
        Assert.Equal(4, GrpcInstallSnapshotResponse.InstallIndexFieldNumber);
        Assert.Equal(5, GrpcInstallSnapshotResponse.InstallLeaderTermFieldNumber);
        Assert.Equal(6, GrpcInstallSnapshotResponse.InstallLeaderEndpointFieldNumber);
        Assert.Equal(7, GrpcInstallSnapshotResponse.InstallProgressFieldNumber);
    }

    [Fact]
    public void RestSnapshot_InstallStepFields_RoundTrip()
    {
        SnapshotRequest query = new()
        {
            SessionId = "sess-4",
            PartitionId = 4,
            SnapshotIndex = 500,
            ChunkIndex = -1,
            InstallPolling = true,
            StatusQuery = true,
        };

        SnapshotRequest? parsedQuery = JsonSerializer.Deserialize(
            JsonSerializer.Serialize(query, RestJsonContext.Default.SnapshotRequest),
            RestJsonContext.Default.SnapshotRequest);

        Assert.NotNull(parsedQuery);
        Assert.True(parsedQuery!.InstallPolling);
        Assert.True(parsedQuery.StatusQuery);
        Assert.Equal(-1, parsedQuery.ChunkIndex);

        SnapshotResponse pending = new(SnapshotInstallOutcome.InstallPending)
        {
            InstallSessionId = "sess-4",
            InstallIndex = 500,
            InstallLeaderTerm = 13,
            InstallLeaderEndpoint = "leader:9004",
            InstallProgress = 77,
        };

        SnapshotResponse? parsed = JsonSerializer.Deserialize(
            JsonSerializer.Serialize(pending, RestJsonContext.Default.SnapshotResponse),
            RestJsonContext.Default.SnapshotResponse);

        Assert.NotNull(parsed);
        Assert.Equal(SnapshotInstallOutcome.InstallPending, parsed!.Outcome);
        Assert.Equal("sess-4", parsed.InstallSessionId);
        Assert.Equal(500, parsed.InstallIndex);
        Assert.Equal(13, parsed.InstallLeaderTerm);
        Assert.Equal("leader:9004", parsed.InstallLeaderEndpoint);
        Assert.Equal(77, parsed.InstallProgress);
    }

    [Fact]
    public void SnapshotInstallOutcome_ValuesAreStable()
    {
        // The outcome travels as a raw int; a sender that reads 0 where a newer peer meant
        // something else would treat an install as refused, or the reverse.
        Assert.Equal(0, (int)SnapshotInstallOutcome.Rejected);
        Assert.Equal(1, (int)SnapshotInstallOutcome.ChunkAccepted);
        Assert.Equal(2, (int)SnapshotInstallOutcome.Installed);
        Assert.Equal(3, (int)SnapshotInstallOutcome.SkippedAlreadyCovered);
        Assert.Equal(4, (int)SnapshotInstallOutcome.InstallPending);
        Assert.Equal(5, (int)SnapshotInstallOutcome.NoInstall);
    }

    /// <summary>
    /// The legacy bit says "staged or installed". A pending install and "no install on record" are
    /// neither, so a peer that reads only the bit never takes them for a seeded follower.
    /// </summary>
    [Theory]
    [InlineData(SnapshotInstallOutcome.Rejected, false)]
    [InlineData(SnapshotInstallOutcome.ChunkAccepted, true)]
    [InlineData(SnapshotInstallOutcome.Installed, true)]
    [InlineData(SnapshotInstallOutcome.SkippedAlreadyCovered, true)]
    [InlineData(SnapshotInstallOutcome.InstallPending, false)]
    [InlineData(SnapshotInstallOutcome.NoInstall, false)]
    public void SuccessBit_FollowsTheOutcome(SnapshotInstallOutcome outcome, bool success)
    {
        SnapshotResponse response = new(outcome);
        Assert.Equal(success, response.Success);

        SnapshotResponse? parsed = JsonSerializer.Deserialize(
            JsonSerializer.Serialize(response, RestJsonContext.Default.SnapshotResponse),
            RestJsonContext.Default.SnapshotResponse);

        Assert.NotNull(parsed);
        Assert.Equal(outcome, parsed!.Outcome);
        Assert.Equal(success, parsed.Success);
    }

    [Fact]
    public void RequestFromASenderThatPredatesTheFields_IsAChunkFromASenderThatDoesNotPoll()
    {
        SnapshotRequest? legacy = JsonSerializer.Deserialize(
            """{"sessionId":"old","partitionId":1,"snapshotIndex":10,"chunkIndex":0,"isLast":true}""",
            RestJsonContext.Default.SnapshotRequest);

        Assert.NotNull(legacy);
        Assert.False(legacy!.InstallPolling);
        Assert.False(legacy.StatusQuery);

        GrpcInstallSnapshotRequest grpcLegacy = GrpcInstallSnapshotRequest.Parser.ParseFrom(
            new GrpcInstallSnapshotRequest { SessionId = "old", PartitionId = 1, SnapshotIndex = 10, IsLast = true }.ToByteArray());
        Assert.False(grpcLegacy.InstallPolling);
        Assert.False(grpcLegacy.StatusQuery);
    }
}
