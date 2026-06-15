
namespace Kommander.Data;

/// <summary>
/// Wire envelope for the REST SWIM indirect-probe endpoint (<c>POST /v1/raft/ping-req</c>).
/// The receiving node relays a direct ping to <see cref="TargetEndpoint"/> on the
/// originator's behalf, isolating path failures from dead nodes.
/// </summary>
public sealed record PingReqRequest(string SenderEndpoint, string TargetEndpoint);
