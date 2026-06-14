using System.Text.Json;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Time;

namespace Kommander.Tests.Communication;

public sealed class TestRestJsonContext
{
    [Fact]
    public void BatchRequestsRequest_SerializesStepDownNotice_WithRestJsonContext()
    {
        BatchRequestsRequest request = new()
        {
            Requests =
            [
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.StepDownNotice,
                    StepDownNotice = new StepDownNoticeRequest(
                        partition: 7,
                        term: 11,
                        time: new HLCTimestamp(3, 1234, 2),
                        endpoint: "node-b")
                }
            ]
        };

        string json = JsonSerializer.Serialize(request, RestJsonContext.Default.BatchRequestsRequest);
        BatchRequestsRequest? deserialized = JsonSerializer.Deserialize(
            json,
            RestJsonContext.Default.BatchRequestsRequest);

        Assert.NotNull(deserialized);
        BatchRequestsRequestItem item = Assert.Single(deserialized.Requests!);
        Assert.Equal(BatchRequestsRequestType.StepDownNotice, item.Type);
        Assert.NotNull(item.StepDownNotice);
        Assert.Equal(7, item.StepDownNotice.Partition);
        Assert.Equal(11, item.StepDownNotice.Term);
        Assert.Equal(new HLCTimestamp(3, 1234, 2), item.StepDownNotice.Time);
        Assert.Equal("node-b", item.StepDownNotice.Endpoint);
    }

    [Fact]
    public void BatchRequestsRequest_SerializesTransferLeadership_WithRestJsonContext()
    {
        BatchRequestsRequest request = new()
        {
            Requests =
            [
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.TransferLeadership,
                    TransferLeadership = new TransferLeadershipRequest(
                        partition: 9,
                        term: 12,
                        time: new HLCTimestamp(4, 5678, 3),
                        endpoint: "node-a",
                        targetEndpoint: "node-c")
                }
            ]
        };

        string json = JsonSerializer.Serialize(request, RestJsonContext.Default.BatchRequestsRequest);
        BatchRequestsRequest? deserialized = JsonSerializer.Deserialize(
            json,
            RestJsonContext.Default.BatchRequestsRequest);

        Assert.NotNull(deserialized);
        BatchRequestsRequestItem item = Assert.Single(deserialized.Requests!);
        Assert.Equal(BatchRequestsRequestType.TransferLeadership, item.Type);
        Assert.NotNull(item.TransferLeadership);
        Assert.Equal(9, item.TransferLeadership.Partition);
        Assert.Equal(12, item.TransferLeadership.Term);
        Assert.Equal(new HLCTimestamp(4, 5678, 3), item.TransferLeadership.Time);
        Assert.Equal("node-a", item.TransferLeadership.Endpoint);
        Assert.Equal("node-c", item.TransferLeadership.TargetEndpoint);
    }

    [Fact]
    public void GetFollowerLagRequest_SurvivesRestJsonRoundTrip()
    {
        GetFollowerLagRequest original = new(PartitionId: 2, FollowerEndpoint: "localhost:8205");

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.GetFollowerLagRequest);
        GetFollowerLagRequest? deserialized = JsonSerializer.Deserialize(json, RestJsonContext.Default.GetFollowerLagRequest);

        Assert.NotNull(deserialized);
        Assert.Equal(2, deserialized.PartitionId);
        Assert.Equal("localhost:8205", deserialized.FollowerEndpoint);
    }

    [Fact]
    public void GetFollowerLagResponse_WithValue_SurvivesRestJsonRoundTrip()
    {
        GetFollowerLagResponse original = new(HasValue: true, Value: 99);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.GetFollowerLagResponse);
        GetFollowerLagResponse? deserialized = JsonSerializer.Deserialize(json, RestJsonContext.Default.GetFollowerLagResponse);

        Assert.NotNull(deserialized);
        Assert.True(deserialized.HasValue);
        Assert.Equal(99, deserialized.Value);
    }

    [Fact]
    public void GetFollowerLagResponse_NoValue_SurvivesRestJsonRoundTrip()
    {
        GetFollowerLagResponse original = new(HasValue: false);

        string json = JsonSerializer.Serialize(original, RestJsonContext.Default.GetFollowerLagResponse);
        GetFollowerLagResponse? deserialized = JsonSerializer.Deserialize(json, RestJsonContext.Default.GetFollowerLagResponse);

        Assert.NotNull(deserialized);
        Assert.False(deserialized.HasValue);
        Assert.Equal(0, deserialized.Value);
    }
}
