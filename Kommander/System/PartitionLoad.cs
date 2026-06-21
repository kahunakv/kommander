
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

    /// <summary>
    /// EWMA rate of <c>ReplicateLogs</c> operations per second on this partition at the
    /// time the report was built. Advisory and path-not-label: keyed on request type, not
    /// node role, so it is naturally 0 on followers. A missing field in older JSON payloads
    /// deserializes to 0 (additive, no serializer registration required).
    /// </summary>
    public double LogOpsPerSecond { get; set; }

    /// <summary>
    /// Per-partition WAL backlog depth at the time this report was built: the number of
    /// write operations queued for this partition's share of the shared <see cref="FairWalScheduler"/>
    /// fsync group. Advisory saturation signal — a rising depth while <see cref="LogOpsPerSecond"/>
    /// plateaus indicates the partition is fsync-bound. A missing field in older JSON payloads
    /// deserializes to 0 (additive, no serializer registration required).
    /// </summary>
    public int WalQueueDepth { get; set; }

    /// <summary>
    /// EWMA of the enqueue-to-durable commit-wait latency in milliseconds for this
    /// partition at the time this report was built. Complements <see cref="WalQueueDepth"/>:
    /// depth counts operations; this field measures how long each operation waits.
    /// Under fsync pressure both rise together while <see cref="LogOpsPerSecond"/> plateaus.
    /// Advisory — a missing field in older JSON payloads deserializes to 0 (additive).
    /// </summary>
    public double CommitWaitMs { get; set; }
}
