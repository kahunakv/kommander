using System.Text.Json.Serialization;
using Kommander.Data;

namespace Kommander.Communication;

[JsonSerializable(typeof(AppendLogsRequest))]
[JsonSerializable(typeof(RequestVotesRequest))]
[JsonSerializable(typeof(VoteRequest))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class RaftJsonContext : JsonSerializerContext
{

}