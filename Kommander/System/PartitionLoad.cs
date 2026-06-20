
namespace Kommander.System;

/// <summary>
/// Per-partition leadership load snapshot, embedded in a <see cref="NodeLoadReport"/>.
/// Carries the composite load scalar and the leadership age so the planner can apply
/// its stability gate without a separate clock call.
/// </summary>
public sealed class PartitionLoad
{
    /// <summary>Partition this entry describes.</summary>
    public int PartitionId { get; set; }

    /// <summary>
    /// Advisory composite load scalar: <c>wOps × OpsPerSecond + wQueue × (clientDepth + walDepth)</c>.
    /// Computed locally by the partition's leader; never authoritative — used only to rank
    /// candidate moves in the load-balancing planner.
    /// </summary>
    public double Load { get; set; }

    /// <summary>
    /// Milliseconds elapsed since this node became leader of the partition.
    /// Used by the planner's stability gate: partitions with a short leadership tenure
    /// (<c>LeaderSinceMs &lt; MinLeaderStabilityMs</c>) are not eligible for transfer.
    /// </summary>
    public long LeaderSinceMs { get; set; }
}
