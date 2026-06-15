
namespace Kommander.Data;

/// <summary>
/// Wire reply returned by the REST SWIM direct-probe endpoint (<c>POST /v1/raft/ping</c>).
/// <see cref="Alive"/> is <see langword="true"/> whenever the receiver processes the request.
/// <see cref="Incarnation"/> lets the probing node record a fresh liveness entry.
/// </summary>
public sealed record PingResponse(bool Alive, long Incarnation);
