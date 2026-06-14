
namespace Kommander.Data;

/// <summary>
/// Returned by <see cref="ICommunication.SendLeave"/> after a leave attempt.
/// <para>
/// When <see cref="Success"/> is <c>true</c> the removal has been committed (or the node
/// was already absent from the roster — idempotent).
/// When <see cref="Success"/> is <c>false</c> the contacted node was not the P0 leader;
/// <see cref="LeaderHint"/> carries the current leader endpoint so the caller can retry.
/// A <see langword="null"/> hint means the leader is not yet known — the caller should
/// pause and retry rather than give up, unless <see cref="Terminal"/> is also set.
/// </para>
/// <para>
/// <see cref="Terminal"/> distinguishes a permanently-blocked removal (e.g.,
/// <c>InsufficientVoters</c>) from a transient "leader unknown" response that shares the
/// same null-hint shape.  When <c>true</c>, the caller must not retry.
/// </para>
/// </summary>
public sealed record LeaveResponse(bool Success, string? LeaderHint = null, bool Terminal = false);
