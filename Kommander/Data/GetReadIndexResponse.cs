namespace Kommander.Data;

/// <summary>
/// Answer to <see cref="GetReadIndexRequest"/>. <see cref="Success"/> is <c>true</c> only when
/// the responder proved it was still the partition leader with a same-term quorum ack round
/// started after the request arrived; <see cref="ReadIndex"/> is then the commit frontier that
/// round captured — every entry committed cluster-wide before the request began is at or below
/// it. Any failure (responder not the leader, quorum round failed or timed out, transport
/// error) is <see cref="Success"/> = <c>false</c>, and the caller must not act destructively
/// on locally-applied state.
/// </summary>
public sealed record GetReadIndexResponse(bool Success, long ReadIndex = -1);
