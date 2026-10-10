namespace Kommander.Data;

/// <summary>
/// Why the leader started a snapshot transfer to a follower. Carried on the transfer-start log
/// line and on <see cref="RaftSnapshotStatus.Trigger"/>, so a run can be read for which path
/// escalated instead of inferring it from a message that asserted the compaction floor for
/// every path (the CamusDB rl6 soak: three installs whose Warning blamed a floor the follower was
/// never below, when the no-progress probe had fired on a follower that was closing its gap).
/// </summary>
public enum SnapshotTransferTrigger
{
    /// <summary>
    /// A backfill read anchored at the follower's next index came back starting above it: no entry
    /// exists at the anchor on this leader (compacted away, or a truncated hole), so no batch can be
    /// anchored there and only a snapshot can seed the follower.
    /// </summary>
    NonContiguousBackfill = 1,

    /// <summary>
    /// A backfill read anchored at the follower's next index came back empty: the leader's log
    /// holds nothing at or above the anchor it can ship.
    /// </summary>
    EmptyBackfillRead = 2,

    /// <summary>
    /// The no-progress probe: batches anchored at the frontier the follower itself reported were
    /// shipped and acknowledged, and none of the follower's frontiers (commit, durable, contiguous
    /// presence) moved across the streak. Log shipping demonstrably cannot converge the follower.
    /// </summary>
    NoProgressProbe = 3,

    /// <summary>
    /// The follower kept rejecting batches anchored on an entry this leader compacted, so the
    /// anchor's term can never be verified and the batch can never be accepted.
    /// </summary>
    CompactedAnchorRefusal = 4,

    /// <summary>The follower asked to be re-seeded (<see cref="ReseedRequest"/>).</summary>
    FollowerReseedRequest = 5,
}
