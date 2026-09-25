using System.Text.Json.Serialization;
using Kommander.Data;
using Kommander.System;

namespace Kommander.Communication;

/// <summary>
/// Source-generated JSON metadata for every request and response body of the REST transport.
/// </summary>
/// <remarks>
/// <para>
/// Every route parameter and every route return type in <c>RestCommunicationExtensions</c> must be
/// listed here, and so must every type that <see cref="Rest.RestCommunication"/> reads. A host that
/// is trimmed with reflection-based JSON off resolves all bodies through this context. ASP.NET Core
/// builds all endpoints together at the first request, so one missing type makes every route of the
/// host answer 500, not only the Kommander route. <c>TestRestJsonContext</c> checks the list.
/// </para>
/// <para>
/// The camel-case policy matches the ASP.NET Core web defaults that the server side used before, so
/// the wire shape does not change. The client reads responses case-insensitively.
/// </para>
/// <para>
/// The context generates metadata only, not the fast-path writer. The fast-path writer writes a null
/// <c>byte[]</c> as <c>""</c>, so a null <see cref="RaftLog.LogData"/> arrived at the peer as an empty
/// array. <c>TestRestJsonContext.AppendLogsRequest_KeepsNullLogData_Null</c> checks this.
/// </para>
/// </remarks>
[JsonSerializable(typeof(HandshakeResponse))]
[JsonSerializable(typeof(AppendLogsResponse))]
[JsonSerializable(typeof(AppendLogsBatchResponse))]
[JsonSerializable(typeof(CompleteAppendLogsResponse))]
[JsonSerializable(typeof(CompleteAppendLogsBatchResponse))]
[JsonSerializable(typeof(RequestVotesResponse))]
[JsonSerializable(typeof(VoteResponse))]
[JsonSerializable(typeof(BatchRequestsResponse))]
[JsonSerializable(typeof(AppendLogsRequest))]
[JsonSerializable(typeof(AppendLogsBatchRequest))]
[JsonSerializable(typeof(RequestVotesRequest))]
[JsonSerializable(typeof(VoteRequest))]
[JsonSerializable(typeof(CompleteAppendLogsRequest))]
[JsonSerializable(typeof(CompleteAppendLogsBatchRequest))]
[JsonSerializable(typeof(BatchRequestsRequest))]
[JsonSerializable(typeof(BatchRequestsRequestItem))]
[JsonSerializable(typeof(StepDownNoticeRequest))]
[JsonSerializable(typeof(TransferLeadershipRequest))]
[JsonSerializable(typeof(TransferLeadershipSuggestionRequest))]
[JsonSerializable(typeof(ReseedRequest))]
[JsonSerializable(typeof(NodeLoadReport))]
[JsonSerializable(typeof(PartitionLoad))]
[JsonSerializable(typeof(HandshakeRequest))]
[JsonSerializable(typeof(JoinRequest))]
[JsonSerializable(typeof(JoinResponse))]
[JsonSerializable(typeof(LeaveRequest))]
[JsonSerializable(typeof(LeaveResponse))]
[JsonSerializable(typeof(SetMemberRoleRequest))]
[JsonSerializable(typeof(SetMemberRoleResponse))]
[JsonSerializable(typeof(GetFollowerLagRequest))]
[JsonSerializable(typeof(GetFollowerLagResponse))]
[JsonSerializable(typeof(SnapshotRequest))]
[JsonSerializable(typeof(SnapshotResponse))]
[JsonSerializable(typeof(SnapshotKind))]
[JsonSerializable(typeof(GossipRequest))]
[JsonSerializable(typeof(GossipResponse))]
[JsonSerializable(typeof(PingRequest))]
[JsonSerializable(typeof(PingResponse))]
[JsonSerializable(typeof(PingReqRequest))]
[JsonSerializable(typeof(PingReqResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, GenerationMode = JsonSourceGenerationMode.Metadata)]
public sealed partial class RestJsonContext : JsonSerializerContext
{

}
