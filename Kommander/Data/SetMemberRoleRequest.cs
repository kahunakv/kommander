
using Kommander.System;

namespace Kommander.Data;

/// <summary>
/// Asks the P0 leader to commit a roster role transition for <paramref name="Endpoint"/>.
/// Used by the graceful-decommission drain: <see cref="ClusterMemberRole.Voter"/> →
/// <see cref="ClusterMemberRole.Leaving"/> to start a drain, and the reverse to roll back a
/// drain that timed out or was cancelled. Learner promotion deliberately does not travel this
/// path — it stays on the existing <c>PromoteMember</c> flow driven by the P0 leader itself.
/// </summary>
public sealed record SetMemberRoleRequest(string Endpoint, int NodeId, ClusterMemberRole TargetRole);
