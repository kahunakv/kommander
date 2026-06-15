
namespace Kommander.Data;

/// <summary>
/// Wire envelope for the REST SWIM direct-probe endpoint (<c>POST /v1/raft/ping</c>).
/// Mirrors <see cref="Gossip.PingRequest"/> for JSON serialization without importing
/// the Gossip namespace into <see cref="RestJsonContext"/>.
/// </summary>
public sealed record PingRequest(string SenderEndpoint);
