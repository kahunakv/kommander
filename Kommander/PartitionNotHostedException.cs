
namespace Kommander;

/// <summary>
/// Thrown when an operation targets a data partition that exists in the committed partition map
/// but is not materialized on this node. Under replica placement
/// (<see cref="RaftConfiguration.ReplicationFactor"/> &gt; 0) this is a normal, expected routing
/// condition — most ranges live on other nodes — not a caller error, so it gets its own type:
/// consumers should retry on a node that hosts the range (<see cref="IRaft.GetPartitionReplicas"/>)
/// instead of matching the exception message.
/// <para>
/// Retryable by construction: the committed map can name this node as a replica moments before the
/// system coordinator materializes the partition locally (map application runs on the coordinator's
/// channel), so a guard such as <see cref="IRaft.HostsPartition"/> and this throw inherently race.
/// Treat the exception as the authoritative answer and the guard as advisory. Derives from
/// <see cref="RaftException"/> so existing catch-all handling keeps working; the message keeps the
/// historical "Invalid partition" prefix for consumers that still match on it.
/// </para>
/// </summary>
public sealed class PartitionNotHostedException : RaftException
{
    /// <summary>
    /// The partition id that is not hosted on this node.
    /// </summary>
    public int PartitionId { get; }

    public PartitionNotHostedException(int partitionId)
        : base("Invalid partition: " + partitionId + " (committed range not hosted on this node)")
    {
        PartitionId = partitionId;
    }
}
