
namespace Kommander.Data;

/// <summary>
/// Wire reply returned by the REST SWIM indirect-probe endpoint (<c>POST /v1/raft/ping-req</c>).
/// <see cref="Reached"/> is <see langword="true"/> when the relay successfully pinged
/// the <c>TargetEndpoint</c> within its <c>PingTimeout</c>.
/// </summary>
public sealed record PingReqResponse(bool Reached);
