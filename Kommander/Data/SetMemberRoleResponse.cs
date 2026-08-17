
namespace Kommander.Data;

/// <summary>
/// Returned by <see cref="ICommunication.SendSetMemberRole"/> after a role-transition attempt.
/// <para>
/// <see cref="Status"/> carries the coordinator's verdict so the caller can distinguish the
/// outcomes that need different reactions: <see cref="RaftOperationStatus.ConcurrentMembershipChange"/>
/// (another drain or membership change is in flight — retry later),
/// <see cref="RaftOperationStatus.InsufficientVoters"/> (permanent refusal), and
/// <see cref="RaftOperationStatus.MemberNotFound"/> (the member is already out of the roster —
/// for a rollback this means the removal won the race and the caller must treat the node as
/// departed, not retry).
/// </para>
/// </summary>
/// <param name="Success">True when the transition committed (or was an idempotent no-op).</param>
/// <param name="LeaderHint">When the receiver is not the P0 leader, its current leader guess.</param>
/// <param name="Status">The coordinator's verdict; <see cref="RaftOperationStatus.Success"/> when <paramref name="Success"/>.</param>
/// <param name="MembershipVersion">The roster version after the commit (0 when nothing committed).</param>
public sealed record SetMemberRoleResponse(
    bool Success,
    string? LeaderHint = null,
    RaftOperationStatus Status = RaftOperationStatus.Success,
    long MembershipVersion = 0);
