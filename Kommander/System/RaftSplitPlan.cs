
namespace Kommander.System;

/// <summary>
/// Describes a requested partition split.
/// Passed from <see cref="IRaft.SplitPartitionAsync"/> through
/// <see cref="RaftSystemCoordinator"/> to <c>TrySplitPartition</c>.
/// </summary>
public sealed class RaftSplitPlan
{
    /// <summary>
    /// Partition id to assign to the newly-created right-half partition.
    /// When zero the coordinator auto-assigns <c>max(existing ids) + 1</c>.
    /// </summary>
    public int TargetPartitionId { get; init; }

    /// <summary>
    /// Routing mode for the new partition.
    /// Defaults to <see cref="RaftRoutingMode.HashRange"/> when not set.
    /// </summary>
    public RaftRoutingMode TargetRoutingMode { get; init; } = RaftRoutingMode.HashRange;

    /// <summary>
    /// Hash-range boundary: the new partition owns
    /// <c>[HashBoundary, source.EndRange]</c> while the source is shrunk to
    /// <c>[source.StartRange, HashBoundary - 1]</c>.
    /// When null the coordinator computes the midpoint automatically.
    /// Ignored when <see cref="TargetRoutingMode"/> is <see cref="RaftRoutingMode.Unrouted"/>.
    /// </summary>
    public int? HashBoundary { get; init; }

    /// <summary>
    /// When true the coordinator immediately enqueues
    /// <see cref="RaftSystemRequestType.SplitPartitionCommit"/> to itself after
    /// Phase 1 succeeds, completing the two-phase split without external intervention.
    /// Set by the legacy <c>SplitPartition(int)</c> path.
    /// </summary>
    public bool AutoCommit { get; init; }
}
