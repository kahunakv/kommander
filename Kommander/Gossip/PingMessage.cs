
namespace Kommander.Gossip;

/// <summary>
/// Direct SWIM probe sent to a single peer.
/// The receiver replies with <see cref="PingResponse"/> carrying its current incarnation so
/// the sender can record an up-to-date <see cref="MemberLivenessState.Alive"/> entry.
/// </summary>
public sealed record PingRequest(
    string SenderEndpoint
);

/// <summary>
/// Reply to a <see cref="PingRequest"/>.
/// <see cref="Alive"/> is true whenever the receiver processes the message (i.e., always on a
/// reachable node; it is false only when the transport returns a failure object rather than
/// an exception, which never happens in the current in-memory implementation).
/// </summary>
public sealed record PingResponse(
    bool Alive,
    long Incarnation
);

/// <summary>
/// Indirect probe request: the sender asks an intermediary to relay a direct
/// <see cref="PingRequest"/> to <see cref="TargetEndpoint"/> on its behalf.
/// Used when a direct probe times out, to rule out false positives caused by
/// a transient path failure between the sender and the target.
/// </summary>
public sealed record PingReqRequest(
    string SenderEndpoint,
    string TargetEndpoint
);

/// <summary>
/// Reply to a <see cref="PingReqRequest"/>.
/// <see cref="Reached"/> is true if the intermediary was able to contact
/// <see cref="PingReqRequest.TargetEndpoint"/> within <c>PingTimeout</c>.
/// </summary>
public sealed record PingReqResponse(
    bool Reached
);
