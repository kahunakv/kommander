using System.Text.Json.Serialization;
using Kommander.Discovery.Data;

namespace Kommander.System;

/// <summary>
/// Source-generated JSON metadata for the documents that Kommander stores in the system partition
/// (the partition map and the cluster roster), carries inside gossip/join payloads (roster and load
/// report JSON blobs), and sends over multicast discovery. Using this context instead of the
/// reflection-based <c>JsonSerializer</c> overloads keeps these paths trim- and AOT-safe (no IL2026/IL3050).
/// </summary>
/// <remarks>
/// The context deliberately uses the default serializer options (PascalCase property names, numeric
/// enums), unlike <c>RestJsonContext</c> which is camelCase. These documents are persisted in the
/// system partition log and checkpoints and exchanged between nodes of mixed versions, so the wire
/// shape must stay byte-compatible with what the reflection serializer wrote before. Do not add a
/// naming policy or enum converter here.
/// </remarks>
[JsonSerializable(typeof(RaftPartitionMap))]
[JsonSerializable(typeof(ClusterMembership))]
[JsonSerializable(typeof(NodeLoadReport))]
[JsonSerializable(typeof(MulticastDiscoveryPayload))]
internal sealed partial class SystemJsonContext : JsonSerializerContext
{

}
