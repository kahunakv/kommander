
namespace Kommander.Data;

/// <summary>
/// Sent by a joining node to a seed to be admitted into the cluster roster.
/// The seed forwards to the P0 leader if it is not the leader itself.
/// </summary>
public sealed record JoinRequest(string Endpoint, int NodeId);
