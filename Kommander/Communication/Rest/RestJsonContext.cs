using System.Text.Json.Serialization;
using Kommander.Data;

namespace Kommander.Communication;

/// <summary>
/// Generates JSON serialization via source code generation for the specified types.
/// </summary>
[JsonSerializable(typeof(AppendLogsRequest))]
[JsonSerializable(typeof(RequestVotesRequest))]
[JsonSerializable(typeof(VoteRequest))]
[JsonSerializable(typeof(CompleteAppendLogsRequest))]
[JsonSerializable(typeof(HandshakeRequest))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class RestJsonContext : JsonSerializerContext
{

}