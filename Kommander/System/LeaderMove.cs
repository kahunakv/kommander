
namespace Kommander.System;

/// <summary>
/// A single advisory leadership-transfer suggestion produced by <see cref="LeaderBalancePlanner"/>.
/// The move is sent as a suggestion to <see cref="FromEndpoint"/>; the recipient validates
/// current leadership and target liveness before acting.  Dropping or ignoring a
/// <see cref="LeaderMove"/> never violates Raft safety.
/// </summary>
/// <param name="PartitionId">The partition whose leadership should move.</param>
/// <param name="FromEndpoint">The endpoint currently believed to lead the partition.</param>
/// <param name="ToEndpoint">The endpoint that should receive leadership.</param>
/// <param name="IsDrain">
/// True when the move exists to evacuate a node classified slow, rather than to even out load.
/// Only the metric outcome tag distinguishes the two — the recipient validates and executes both
/// identically — but the distinction matters in an incident: a burst of drain moves means a device
/// was judged degraded, while a burst of ordinary moves means the cluster was merely uneven.
/// Defaults to false so existing construction sites are unaffected.
/// </param>
public sealed record LeaderMove(int PartitionId, string FromEndpoint, string ToEndpoint, bool IsDrain = false);
