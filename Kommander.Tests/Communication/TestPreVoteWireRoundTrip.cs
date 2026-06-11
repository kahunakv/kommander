
using Google.Protobuf;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Task 7 wire correctness: the <c>PreVote</c> flag (Raft §9.6) must survive proto3 serialization
/// on the gRPC vote payloads, and — because proto3 <c>bool</c> defaults to <c>false</c> — an
/// un-upgraded peer that never sets the field must deserialize as a normal (non-pre-vote) request,
/// preserving backward compatibility.
/// </summary>
public class TestPreVoteWireRoundTrip
{
    [Fact]
    public void GrpcVoteRequest_PreVote_SurvivesRoundTrip()
    {
        GrpcVoteRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a", PreVote = true };

        GrpcVoteRequest parsed = GrpcVoteRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.True(parsed.PreVote);
        Assert.Equal(7, parsed.Term);
    }

    [Fact]
    public void GrpcVoteRequest_UnsetPreVote_DefaultsToFalse()
    {
        // Simulates an un-upgraded peer that never wrote field 8.
        GrpcVoteRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a" };

        GrpcVoteRequest parsed = GrpcVoteRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.False(parsed.PreVote);
    }

    [Fact]
    public void GrpcRequestVotesRequest_PreVote_SurvivesRoundTrip()
    {
        GrpcRequestVotesRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a", PreVote = true };

        GrpcRequestVotesRequest parsed = GrpcRequestVotesRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.True(parsed.PreVote);
        Assert.Equal(7, parsed.Term);
    }

    [Fact]
    public void GrpcRequestVotesRequest_UnsetPreVote_DefaultsToFalse()
    {
        GrpcRequestVotesRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a" };

        GrpcRequestVotesRequest parsed = GrpcRequestVotesRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.False(parsed.PreVote);
    }
}
