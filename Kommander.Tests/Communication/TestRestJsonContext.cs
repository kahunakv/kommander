
using System.Text;
using System.Text.Json;
using Kommander.Communication;
using Kommander.Communication.Rest;
using Kommander.Data;
using Kommander.Time;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Kommander.Tests.Communication;

/// <summary>
/// Guards the REST transport in a host that is trimmed with reflection-based JSON off.
/// </summary>
/// <remarks>
/// <para>
/// Such a host resolves every JSON body through source-generated metadata only. A route type that
/// <see cref="RestJsonContext"/> does not list makes the endpoint build fail, and ASP.NET Core builds
/// all endpoints together at the first request, so every route of the host answered 500. A response
/// type the client reads through reflection made every REST Raft call fail.
/// </para>
/// <para>
/// A test process cannot turn reflection off (the switch is read once per process), so these tests
/// give the serializer <see cref="RestJsonContext"/> as its only resolver. A missing type then fails
/// the same way it fails in the trimmed host.
/// </para>
/// </remarks>
public sealed class TestRestJsonContext
{
    [Fact]
    public void EveryRaftRoute_Builds_WithOnlyTheRestJsonContext()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();

        builder.Services.AddSingleton<IRaft>(
            _ => throw new NotSupportedException("Endpoint construction must not invoke handlers."));

        builder.Services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.TypeInfoResolverChain.Clear();
            options.SerializerOptions.TypeInfoResolverChain.Add(RestJsonContext.Default);
        });

        WebApplication app = builder.Build();

        app.MapRestRaftRoutes();

        IEndpointRouteBuilder routeBuilder = app;

        // Reading Endpoints builds each request delegate, which resolves the JSON metadata of every
        // body parameter and return type. This is where the trimmed host failed.
        List<RouteEndpoint> endpoints =
        [
            .. routeBuilder.DataSources
                .SelectMany(source => source.Endpoints)
                .OfType<RouteEndpoint>()
        ];

        Assert.True(endpoints.Count >= 10, $"only {endpoints.Count} endpoints built");
        Assert.All(endpoints, endpoint => Assert.NotNull(endpoint.RequestDelegate));
    }

    [Fact]
    public void ResponseSerializer_ReadsEveryResponseType_ThroughTheContext()
    {
        AssertReads<HandshakeResponse>("""{"nodeId":3,"maxLogId":7,"endpoint":"n1"}""");
        AssertReads<AppendLogsResponse>("{}");
        AssertReads<CompleteAppendLogsResponse>("{}");
        AssertReads<BatchRequestsResponse>("{}");
        AssertReads<RequestVotesResponse>("{}");
        AssertReads<VoteResponse>("{}");
        AssertReads<LeaveResponse>("""{"success":true}""");
        AssertReads<SetMemberRoleResponse>("""{"success":true}""");
        AssertReads<GossipResponse>("""{"membershipVersion":1}""");
        AssertReads<PingResponse>("""{"alive":true,"incarnation":2}""");
        AssertReads<PingReqResponse>("""{"reached":true}""");
        AssertReads<GetFollowerLagResponse>("""{"found":true}""");
        AssertReads<SnapshotResponse>("""{"success":true}""");
        AssertReads<JoinResponse>("{}");

        // Names are matched case-insensitively, as with Flurl's default serializer.
        HandshakeResponse? pascal = Read<HandshakeResponse>("""{"NodeId":3,"MaxLogId":7,"Endpoint":"n1"}""");
        Assert.NotNull(pascal);
        Assert.Equal(3, pascal.NodeId);
        Assert.Equal(7, pascal.MaxLogId);
        Assert.Equal("n1", pascal.Endpoint);
    }

    [Fact]
    public void ResponseSerializer_ReadsAnEmptyBody_AsNull()
    {
        Assert.Null(Read<HandshakeResponse>(""));
    }

    /// <summary>
    /// Null log data and empty log data are different values. The source-generated writer must keep
    /// a null <see cref="RaftLog.LogData"/> as JSON null, and must not write it as an empty string.
    /// </summary>
    [Fact]
    public void AppendLogsRequest_KeepsNullLogData_Null()
    {
        AppendLogsRequest request = new(
            1,
            2,
            HLCTimestamp.Zero,
            "n1",
            [new RaftLog { Id = 1, LogData = null }, new RaftLog { Id = 2, LogData = [] }]);

        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.AppendLogsRequest);

        AppendLogsRequest? read = JsonSerializer.Deserialize(payload, RestJsonContext.Default.AppendLogsRequest);

        Assert.NotNull(read?.Logs);
        Assert.Null(read.Logs[0].LogData);
        Assert.NotNull(read.Logs[1].LogData);
        Assert.Empty(read.Logs[1].LogData!);
    }

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

    private static void AssertReads<T>(string json) where T : class
    {
        Assert.NotNull(Read<T>(json));
    }

    private static T? Read<T>(string json)
    {
        using MemoryStream stream = new(Encoding.UTF8.GetBytes(json));
        return RestCommunication.ResponseSerializer.Deserialize<T>(stream);
    }
}
