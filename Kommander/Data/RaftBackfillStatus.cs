
namespace Kommander.Data;

/// <summary>
/// Leader-side diagnostic view of one follower the leader is refusing to backfill, returned by
/// <see cref="IRaft.GetBackfillStatuses"/>. An entry exists while the leader's committed-range read
/// at that follower's anchor comes back starting <b>above</b> the anchor, so no anchored batch can
/// be shipped without landing entries over the follower's gap — a healthy partition reports an empty
/// list.
/// <para>
/// Two causes produce the same shape and the guard cannot distinguish them: an uncommitted
/// (Proposed) run sitting exactly at the anchor, which the inherited-tail re-commit repairs, or a
/// WAL compaction floor above the anchor, where only a snapshot install can seed the follower.
/// <see cref="LastCheckpoint"/> is what separates them — a checkpoint at or above
/// <see cref="FirstAvailableIndex"/> points at the compaction floor.
/// </para>
/// <para>
/// This is the queryable form of a condition that previously existed only as one leader Warning per
/// heartbeat, forever. Diagnostic only: values are point-in-time reads of state mutated on the
/// executor thread, and the entry disappears once a contiguous batch ships or the follower is seeded
/// by snapshot. Never gate correctness on it.
/// </para>
/// </summary>
public sealed class RaftBackfillStatus
{
    /// <summary>The follower endpoint whose backfill is being refused.</summary>
    public required string FollowerEndpoint { get; init; }

    /// <summary>
    /// The index the batch would have been anchored at — the follower's next needed entry.
    /// </summary>
    public long AnchorIndex { get; init; }

    /// <summary>
    /// Id of the first committed entry the leader can actually read at or above
    /// <see cref="AnchorIndex"/>. Always greater than the anchor while this entry exists; the
    /// distance between the two is the range the follower cannot be served by log shipping.
    /// </summary>
    public long FirstAvailableIndex { get; init; }

    /// <summary>
    /// The partition's last checkpoint when the episode opened. At or above
    /// <see cref="FirstAvailableIndex"/> means the anchor was compacted away (snapshot install is
    /// the only recovery); below the anchor means the entries exist but are uncommitted, and the
    /// inherited-tail re-commit is the repair to wait on.
    /// </summary>
    public long LastCheckpoint { get; init; }

    /// <summary>
    /// How many times this refusal has fired since the episode opened. Only the first logs at
    /// Warning, so this is the count the log line no longer repeats.
    /// </summary>
    public int Occurrences { get; init; }

    /// <summary>When this episode's first refusal was recorded (UTC).</summary>
    public DateTimeOffset FirstRefusedAt { get; init; }

    /// <summary>When the most recent refusal was recorded (UTC).</summary>
    public DateTimeOffset LastRefusedAt { get; init; }
}
