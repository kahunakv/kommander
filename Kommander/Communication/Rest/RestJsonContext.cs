using System.Text.Json.Serialization;
using Kommander.Data;

namespace Kommander.Communication;

/// <summary>
/// Generates JSON serialization via source code generation for the specified types.
/// </summary>
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
[JsonSerializable(typeof(HandshakeRequest))]
[JsonSerializable(typeof(JoinRequest))]
[JsonSerializable(typeof(JoinResponse))]
[JsonSerializable(typeof(LeaveRequest))]
[JsonSerializable(typeof(LeaveResponse))]
[JsonSerializable(typeof(GetFollowerLagRequest))]
[JsonSerializable(typeof(GetFollowerLagResponse))]
[JsonSerializable(typeof(SnapshotRequest))]
[JsonSerializable(typeof(SnapshotResponse))]
[JsonSerializable(typeof(GossipRequest))]
[JsonSerializable(typeof(GossipResponse))]
[JsonSerializable(typeof(PingRequest))]
[JsonSerializable(typeof(PingResponse))]
[JsonSerializable(typeof(PingReqRequest))]
[JsonSerializable(typeof(PingReqResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class RestJsonContext : JsonSerializerContext
{

}
