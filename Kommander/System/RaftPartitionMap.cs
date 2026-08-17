
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
    /// One past the highest partition id in <paramref name="ranges"/>, counting entries in
    /// <b>every</b> lifecycle state, floored just above the system partition. A removed partition
    /// keeps its entry forever and can never be recreated, so its id is spent and must be skipped —
    /// which is why this counts tombstones while routing views filter them out. This is the single
    /// definition of "next id" for every allocator, inside the library and out.
    /// </summary>
    public static int NextAvailablePartitionId(IEnumerable<RaftPartitionRange> ranges)
    {
        int maxPartitionId = RaftSystemConfig.SystemPartition;

        foreach (RaftPartitionRange range in ranges)
        {
            if (range.PartitionId > maxPartitionId)
                maxPartitionId = range.PartitionId;
        }

        return maxPartitionId + 1;
    }
}
