
namespace Kommander.System;

/// <summary>
/// Describes a partition merge operation.
/// Passed from <see cref="IRaft.MergePartitionsAsync"/> through
/// <see cref="RaftSystemCoordinator"/> to <c>TryMergePartitions</c>.
/// </summary>
public sealed class RaftMergePlan
{
    /// <summary>
    /// The partition that survives the merge and absorbs the source's range.
    /// </summary>
    public int SurvivorPartitionId { get; init; }

    /// <summary>
    /// The partition whose data is drained into the survivor and then removed.
    /// </summary>
    public int SourcePartitionId { get; init; }
}
