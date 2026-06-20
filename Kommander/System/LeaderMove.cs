
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
public sealed record LeaderMove(int PartitionId, string FromEndpoint, string ToEndpoint);
