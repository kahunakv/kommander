
namespace Kommander;

/// <summary>
/// Narrow read seam over the node's partition registry, implemented by <see cref="RaftManager"/>.
/// <para>
/// Collaborators extracted out of <see cref="RaftManager"/> resolve partitions through this
/// interface instead of holding a back-pointer to the manager or — worse — a reference to the
/// live <c>ConcurrentDictionary&lt;int, RaftPartition&gt;</c> it owns. That keeps the registry a
/// single-writer structure owned by the manager, and makes each collaborator's partition
/// dependency visible from its constructor rather than from its body.
/// </para>
/// <para>
/// The system partition (<c>RaftSystemConfig.SystemPartition</c>) is deliberately kept out
/// of the data-partition members: it is stored in a separate field, never in the data registry,
/// and several callers must operate on user ranges only. Members that include it say so.
/// </para>
/// </summary>
internal interface IPartitionProvider
{
    /// <summary>
    /// The system partition, or <see langword="null"/> before the node has assembled it.
    /// </summary>
    RaftPartition? SystemPartition { get; }

    /// <summary>
    /// The user data partitions currently materialized on this node. Excludes the system
    /// partition. Enumeration is a moment-in-time view of a concurrent registry: entries may be
    /// added or removed by a concurrent map application while the caller iterates.
    /// </summary>
    IEnumerable<RaftPartition> DataPartitions { get; }

    /// <summary>
    /// Number of user data partitions materialized on this node. Excludes the system partition.
    /// </summary>
    int DataPartitionCount { get; }

    /// <summary>
    /// Resolves a user data partition. Returns <see langword="false"/> for the system partition
    /// id and for any range this node does not host.
    /// </summary>
    bool TryGetDataPartition(int partitionId, out RaftPartition? partition);

    /// <summary>
    /// Resolves any partition, including the system partition. Returns <see langword="false"/>
    /// rather than throwing when the partition has not been materialized here yet — the contract
    /// inbound peer messages rely on so that one not-yet-created range cannot poison sibling
    /// messages sharing a coalesced endpoint batch.
    /// </summary>
    bool TryGetPartition(int partitionId, out RaftPartition? partition);

    /// <summary>
    /// Resolves any partition, including the system partition, throwing when it is not hosted
    /// here. Used on the local proposal path, where an unresolvable partition is a routing error
    /// the caller must see rather than a message to drop.
    /// </summary>
    RaftPartition GetPartition(int partitionId);

    /// <summary>
    /// Whether the partition is materialized on this node. Authoritative at read time; a
    /// concurrent replica move can invalidate it immediately afterwards, so callers must keep
    /// treating <see cref="PartitionNotHostedException"/> as retryable.
    /// </summary>
    bool HostsPartition(int partitionId);
}
