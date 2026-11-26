
using Google.Protobuf;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Wire correctness: the <c>PreVote</c> flag (Raft §9.6) must survive proto3 serialization
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

    // ── The leaderless-notice flag must survive the wire, and a peer that never sets it must be
    //    read as an ordinary (pre-)vote request: a notice mistaken for a request is harmless, a
    //    request mistaken for a notice would be dropped unanswered.

    [Fact]
    public void GrpcRequestVotesRequest_NoticeOnly_SurvivesRoundTrip()
    {
        GrpcRequestVotesRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a", PreVote = true, NoticeOnly = true };

        GrpcRequestVotesRequest parsed = GrpcRequestVotesRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.True(parsed.NoticeOnly);
        Assert.True(parsed.PreVote);
    }

    [Fact]
    public void GrpcRequestVotesRequest_UnsetNoticeOnly_DefaultsToFalse()
    {
        GrpcRequestVotesRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a", PreVote = true };

        GrpcRequestVotesRequest parsed = GrpcRequestVotesRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.False(parsed.NoticeOnly);
    }

    [Fact]
    public void RestRequestVotesRequest_NoticeOnly_SurvivesRoundTrip_AndDefaultsToFalseWhenAbsent()
    {
        global::Kommander.Data.RequestVotesRequest original = new(1, 7, 42, 3, new global::Kommander.Time.HLCTimestamp(1, 100, 2), "node-a", preVote: true, noticeOnly: true);

        string json = global::System.Text.Json.JsonSerializer.Serialize(original, global::Kommander.Communication.RestJsonContext.Default.RequestVotesRequest);
        global::Kommander.Data.RequestVotesRequest? parsed = global::System.Text.Json.JsonSerializer.Deserialize(json, global::Kommander.Communication.RestJsonContext.Default.RequestVotesRequest);

        Assert.NotNull(parsed);
        Assert.True(parsed!.NoticeOnly);
        Assert.True(parsed.PreVote);
        Assert.Equal(7, parsed.Term);

        // A peer that predates the field sends no such property.
        global::System.Text.Json.Nodes.JsonObject legacy = global::System.Text.Json.Nodes.JsonNode.Parse(json)!.AsObject();
        Assert.True(legacy.Remove("NoticeOnly") || legacy.Remove("noticeOnly"));

        global::Kommander.Data.RequestVotesRequest? fromLegacy = global::System.Text.Json.JsonSerializer.Deserialize(legacy.ToJsonString(), global::Kommander.Communication.RestJsonContext.Default.RequestVotesRequest);

        Assert.NotNull(fromLegacy);
        Assert.False(fromLegacy!.NoticeOnly);
        Assert.True(fromLegacy.PreVote);
    }

    // ── The §5.4.1 last-log term must survive the wire, and an un-upgraded peer that never
    //        writes field 9 must deserialize as 0 (→ voter falls back to index-only comparison). ──

    [Fact]
    public void GrpcRequestVotesRequest_LastLogTerm_SurvivesRoundTrip()
    {
        GrpcRequestVotesRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, LastLogTerm = 5, Endpoint = "node-a" };

        GrpcRequestVotesRequest parsed = GrpcRequestVotesRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(5, parsed.LastLogTerm);
        Assert.Equal(42, parsed.MaxLogId);
    }

    [Fact]
    public void GrpcRequestVotesRequest_UnsetLastLogTerm_DefaultsToZero()
    {
        // Simulates an un-upgraded peer that never wrote field 9.
        GrpcRequestVotesRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a" };

        GrpcRequestVotesRequest parsed = GrpcRequestVotesRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(0, parsed.LastLogTerm);
    }

    [Fact]
    public void GrpcVoteRequest_LastLogTerm_SurvivesRoundTrip()
    {
        GrpcVoteRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, LastLogTerm = 5, Endpoint = "node-a" };

        GrpcVoteRequest parsed = GrpcVoteRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(5, parsed.LastLogTerm);
    }

    [Fact]
    public void GrpcVoteRequest_UnsetLastLogTerm_DefaultsToZero()
    {
        GrpcVoteRequest original = new() { Partition = 1, Term = 7, MaxLogId = 42, Endpoint = "node-a" };

        GrpcVoteRequest parsed = GrpcVoteRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(0, parsed.LastLogTerm);
    }
}
