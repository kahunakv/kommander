
namespace Kommander.System;

/// <summary>
/// JSON serialization wrapper for the partition map stored under
/// <see cref="RaftSystemConfigKeys.Partitions"/>. The flat list was replaced by
/// this envelope so that <see cref="MapVersion"/> travels with the data and any
/// node can detect that its in-memory copy is stale by comparing versions.
/// </summary>
public sealed class RaftPartitionMap
{
    /// <summary>
    /// Monotonically increasing counter bumped on every write to the partition map.
    /// Initialized to 1 when the map is first created; never decremented or reset.
    /// </summary>
    public long MapVersion { get; set; }

    public List<RaftPartitionRange> Partitions { get; set; } = [];

    /// <summary>
    /// The highest partition id this map ever handed out, in any lifecycle state. It only grows:
    /// <see cref="RecordPartitionId"/> raises it when a partition is created or a split mints a
    /// child, and nothing lowers it. A removed partition keeps a tombstone entry in
    /// <see cref="Partitions"/> and can never be recreated, so its id is spent; this field keeps
    /// the id spent even if the tombstone itself is ever lost from the map — a whole-map rewrite
    /// from a stale base once dropped one, and the id was reissued with a different voter set.
    /// <para>
    /// Maps written before the field existed deserialize it as 0, which disables the floor until
    /// the first id is minted on the new version. The allocator does not depend on the field
    /// alone: <see cref="NextAvailablePartitionId(IEnumerable{RaftPartitionRange}, int)"/> takes
    /// the maximum of this floor and of every entry in the map.
    /// </para>
    /// </summary>
    public int HighestPartitionIdEver { get; set; }

    /// <summary>
    /// One past the highest partition id in <paramref name="ranges"/>, counting entries in
    /// <b>every</b> lifecycle state, floored just above the system partition and just above
    /// <paramref name="highestPartitionIdEver"/>. A removed partition keeps its entry forever and
    /// can never be recreated, so its id is spent and must be skipped — which is why this counts
    /// tombstones while routing views filter them out. The floor covers an id whose entry is no
    /// longer in the map. This is the single definition of "next id" for every allocator, inside
    /// the library and out.
    /// </summary>
    public static int NextAvailablePartitionId(IEnumerable<RaftPartitionRange> ranges, int highestPartitionIdEver = 0)
    {
        int maxPartitionId = Math.Max(RaftSystemConfig.SystemPartition, highestPartitionIdEver);

        foreach (RaftPartitionRange range in ranges)
        {
            if (range.PartitionId > maxPartitionId)
                maxPartitionId = range.PartitionId;
        }

        return maxPartitionId + 1;
    }

    /// <summary>
    /// The next id this map can hand out: see
    /// <see cref="NextAvailablePartitionId(IEnumerable{RaftPartitionRange}, int)"/>.
    /// </summary>
    public int NextAvailablePartitionId() => NextAvailablePartitionId(Partitions, HighestPartitionIdEver);

    /// <summary>
    /// True when <paramref name="partitionId"/> was handed out before, whether or not its entry is
    /// still in <see cref="Partitions"/>. A spent id must be refused by every path that mints a
    /// partition. Entries still present are refused by their own state check; this answers for the
    /// ids whose entry is gone.
    /// </summary>
    public bool IsPartitionIdSpent(int partitionId) => partitionId <= HighestPartitionIdEver;

    /// <summary>
    /// Marks <paramref name="partitionId"/> as handed out. Must be called by every path that adds
    /// a new entry to <see cref="Partitions"/> (create, split child, initial map), before the map
    /// is serialized for replication, so the floor commits in the same log entry as the entry it
    /// protects. Never lowers the floor.
    /// </summary>
    public void RecordPartitionId(int partitionId)
    {
        if (partitionId > HighestPartitionIdEver)
            HighestPartitionIdEver = partitionId;
    }
}
