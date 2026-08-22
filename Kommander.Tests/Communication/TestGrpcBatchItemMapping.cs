using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Time;

namespace Kommander.Tests.Communication;

/// <summary>
/// Pins the client-side mapping from transport-neutral batch items onto their gRPC wire shape
/// (<see cref="GrpcCommunication.TryMapBatchItem"/>).
///
/// <para>
/// The regression this guards (Caraxes "BatchRequests: NullReferenceException" finding): the old
/// mapping keyed on payload-field presence, and its if-chain lacked a
/// TransferLeadershipSuggestion branch. Every leadership balance suggestion went out with
/// <c>Type = TRANSFER_LEADERSHIP_SUGGESTION</c> and a null payload — the receiver dereferenced
/// the payload and threw, on the 30-second balancer cadence, and the suggestion itself was
/// silently lost. The mapping is now keyed on the item's declared type, so every payload-bearing
/// type must round-trip its payload here; a type this test does not cover cannot be added to the
/// enum without also extending the mapper (the default returns null and the sender drops loudly).
/// </para>
/// </summary>
public sealed class TestGrpcBatchItemMapping
{
    private static readonly HLCTimestamp Time = new(1, 100, 5);

    [Fact]
    public void Ping_MapsWithNoPayload()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(
            new BatchRequestsRequestItem { Type = BatchRequestsRequestType.Ping });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.Ping, item!.Type);
    }

    [Fact]
    public void Handshake_PayloadSurvivesMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.Handshake,
            Handshake = new HandshakeRequest(7, 3, 42, "node-a:9001"),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.Handshake, item!.Type);
        Assert.NotNull(item.Handshake);
        Assert.Equal(3, item.Handshake.Partition);
        Assert.Equal(42, item.Handshake.MaxLogId);
        Assert.Equal("node-a:9001", item.Handshake.Endpoint);
    }

    [Fact]
    public void Vote_PayloadSurvivesMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.Vote,
            Vote = new VoteRequest(3, 9, 42, 8, Time, "node-a:9001", preVote: true),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.Vote, item!.Type);
        Assert.NotNull(item.Vote);
        Assert.Equal(9, item.Vote.Term);
        Assert.True(item.Vote.PreVote);
    }

    [Fact]
    public void RequestVote_PayloadSurvivesMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.RequestVote,
            RequestVotes = new RequestVotesRequest(3, 9, 42, 8, Time, "node-a:9001", preVote: false),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.RequestVotes, item!.Type);
        Assert.NotNull(item.RequestVotes);
        Assert.Equal(42, item.RequestVotes.MaxLogId);
    }

    [Fact]
    public void StepDownNotice_PayloadSurvivesMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.StepDownNotice,
            StepDownNotice = new StepDownNoticeRequest(3, 9, Time, "node-a:9001"),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.StepDownNotice, item!.Type);
        Assert.NotNull(item.StepDownNotice);
        Assert.Equal("node-a:9001", item.StepDownNotice.Endpoint);
    }

    [Fact]
    public void TransferLeadership_PayloadSurvivesMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.TransferLeadership,
            TransferLeadership = new TransferLeadershipRequest(3, 9, Time, "node-a:9001", "node-b:9002"),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.TransferLeadership, item!.Type);
        Assert.NotNull(item.TransferLeadership);
        Assert.Equal("node-b:9002", item.TransferLeadership.TargetEndpoint);
    }

    /// <summary>
    /// The regression case. On the payload-presence if-chain this item mapped to a typed item
    /// with a NULL payload: the wire shape behind the Caraxes 30-second
    /// NullReferenceException cadence, and a leadership balancer whose suggestions never
    /// arrived over gRPC.
    /// </summary>
    [Fact]
    public void TransferLeadershipSuggestion_PayloadSurvivesMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.TransferLeadershipSuggestion,
            TransferLeadershipSuggestion = new TransferLeadershipSuggestionRequest(
                3, 9, Time, "p0-leader:9000", "node-b:9002"),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.TransferLeadershipSuggestion, item!.Type);
        Assert.NotNull(item.TransferLeadershipSuggestion);
        Assert.Equal(3, item.TransferLeadershipSuggestion.Partition);
        Assert.Equal(9, item.TransferLeadershipSuggestion.Term);
        Assert.Equal("p0-leader:9000", item.TransferLeadershipSuggestion.SuggestedBy);
        Assert.Equal("node-b:9002", item.TransferLeadershipSuggestion.TargetEndpoint);
    }

    [Fact]
    public void AppendLogs_PayloadAndEntriesSurviveMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.AppendLogs,
            AppendLogs = new AppendLogsRequest(3, 9, Time, "node-a:9001",
                [new RaftLog { Id = 5, Term = 9, Type = RaftLogType.Committed, LogType = "t", LogData = [1, 2] }],
                prevLogIndex: 4, prevLogTerm: 9),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.AppendLogs, item!.Type);
        Assert.NotNull(item.AppendLogs);
        Assert.Equal(4, item.AppendLogs.PrevLogIndex);
        Assert.Single(item.AppendLogs.Logs);
        Assert.Equal(5, item.AppendLogs.Logs[0].Id);
    }

    [Fact]
    public void CompleteAppendLogs_PayloadSurvivesMapping()
    {
        GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(new BatchRequestsRequestItem
        {
            Type = BatchRequestsRequestType.CompleteAppendLogs,
            CompleteAppendLogs = new CompleteAppendLogsRequest(3, 9, Time, "node-a:9001",
                RaftOperationStatus.Success, 42),
        });

        Assert.NotNull(item);
        Assert.Equal(GrpcBatchRequestsRequestType.CompleteAppendLogs, item!.Type);
        Assert.NotNull(item.CompleteAppendLogs);
        Assert.Equal(42, item.CompleteAppendLogs.CommitIndex);
        Assert.Equal(GrpcRaftOperationStatus.Success, item.CompleteAppendLogs.Status);
    }

    /// <summary>
    /// A typed item whose payload is missing must not be shipped: the mapper returns null and the
    /// sender drops it with a log line, instead of emitting the typed-but-empty wire shape the
    /// receiver can only fail on.
    /// </summary>
    [Fact]
    public void MissingPayload_ReturnsNull()
    {
        foreach (BatchRequestsRequestType type in Enum.GetValues<BatchRequestsRequestType>())
        {
            if (type == BatchRequestsRequestType.Ping)
                continue;

            GrpcBatchRequestsRequestItem? item = GrpcCommunication.TryMapBatchItem(
                new BatchRequestsRequestItem { Type = type });

            Assert.Null(item);
        }
    }

    /// <summary>
    /// Every enum value must be handled by the mapper — a payload-bearing type added to the enum
    /// without a mapper branch reproduces the original defect. This loop fails for any new type
    /// until the mapper (and a payload property wired here) covers it.
    /// </summary>
    [Fact]
    public void EveryPayloadBearingType_HasAMapperBranch()
    {
        foreach (BatchRequestsRequestType type in Enum.GetValues<BatchRequestsRequestType>())
        {
            BatchRequestsRequestItem item = new() { Type = type };

            switch (type)
            {
                case BatchRequestsRequestType.Ping:
                    continue;
                case BatchRequestsRequestType.Handshake:
                    item.Handshake = new HandshakeRequest(1, 1, 1, "e");
                    break;
                case BatchRequestsRequestType.Vote:
                    item.Vote = new VoteRequest(1, 1, 1, 1, Time, "e", false);
                    break;
                case BatchRequestsRequestType.RequestVote:
                    item.RequestVotes = new RequestVotesRequest(1, 1, 1, 1, Time, "e", false);
                    break;
                case BatchRequestsRequestType.StepDownNotice:
                    item.StepDownNotice = new StepDownNoticeRequest(1, 1, Time, "e");
                    break;
                case BatchRequestsRequestType.TransferLeadership:
                    item.TransferLeadership = new TransferLeadershipRequest(1, 1, Time, "e", "t");
                    break;
                case BatchRequestsRequestType.TransferLeadershipSuggestion:
                    item.TransferLeadershipSuggestion = new TransferLeadershipSuggestionRequest(1, 1, Time, "e", "t");
                    break;
                case BatchRequestsRequestType.AppendLogs:
                    item.AppendLogs = new AppendLogsRequest(1, 1, Time, "e", [], 0, 0);
                    break;
                case BatchRequestsRequestType.CompleteAppendLogs:
                    item.CompleteAppendLogs = new CompleteAppendLogsRequest(1, 1, Time, "e", RaftOperationStatus.Success, 1);
                    break;
                default:
                    Assert.Fail($"BatchRequestsRequestType.{type} has no payload wiring in this test — " +
                                "add it here AND to GrpcCommunication.TryMapBatchItem.");
                    break;
            }

            Assert.NotNull(GrpcCommunication.TryMapBatchItem(item));
        }
    }
}
