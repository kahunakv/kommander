
namespace Kommander.Data;

/// <summary>
/// Returns the last committed log index the partition leader has seen from the queried
/// follower, or <see cref="HasValue"/> = <c>false</c> when the follower is unknown to
/// that partition (never connected, or already promoted to voter on this partition).
/// </summary>
public sealed record GetFollowerLagResponse(bool HasValue, long Value = 0);
