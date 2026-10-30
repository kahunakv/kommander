using Kommander.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Kommander.System;

/// <summary>
/// Enforces <see cref="RaftConfiguration.MaxPartitions"/> on the paths that add a partition to the
/// committed map: <see cref="PartitionMapService.TryCreatePartition"/> and
/// <see cref="SplitMergeController.TrySplitPartition"/>. The bootstrap layout is not checked here;
/// <see cref="RaftConfiguration.Validate"/> covers it, because the operator chose that count.
/// <para>
/// The check reads the map the handler is about to change, on the coordinator's single-reader
/// channel loop, so two creators cannot both pass it: the second one sees the first one's entry.
/// Callers must check before they change the map, so a refusal commits nothing and leaves
/// <see cref="RaftPartitionMap.HighestPartitionIdEver"/> where it was.
/// </para>
/// <para>
/// A consumer at the bound tends to retry. Every refusal is counted, but the warning is logged once
/// per episode: the episode ends when a later check finds room.
/// </para>
/// </summary>
internal sealed class PartitionCountLimit
{
    internal const string CreatePath = "create";

    internal const string SplitPath = "split";

    private readonly int maxPartitions;

    private readonly ILogger<IRaft> logger;

    private int refusing;

    internal PartitionCountLimit(int maxPartitions, ILogger<IRaft> logger)
    {
        this.maxPartitions = maxPartitions;
        this.logger = logger;
    }

    /// <summary>
    /// Data partitions that hold resources: every entry that is not <see cref="RaftPartitionState.Removed"/>,
    /// excluding the system partition. <c>Splitting</c> and <c>Draining</c> entries count.
    /// </summary>
    internal static int CountPartitions(RaftPartitionMap map)
    {
        int count = 0;

        foreach (RaftPartitionRange range in map.Partitions)
        {
            if (range.PartitionId != RaftSystemConfig.SystemPartition && range.State != RaftPartitionState.Removed)
                count++;
        }

        return count;
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="map"/> has room for one more partition.
    /// On a refusal it counts the refusal on <paramref name="path"/> and logs the first refusal of
    /// an episode.
    /// </summary>
    internal bool TryAdmit(RaftPartitionMap map, string path, int partitionId)
    {
        if (maxPartitions <= 0)
            return true;

        int count = CountPartitions(map);

        if (count < maxPartitions)
        {
            if (Interlocked.Exchange(ref refusing, 0) == 1)
                logger.LogInformation(
                    "PartitionCountLimit: partition count {Count} is below the bound {Max} again; creates resume",
                    count, maxPartitions);

            return true;
        }

        KommanderMetrics.PartitionCreateRefusedTotal.Add(1, new KeyValuePair<string, object?>("path", path));

        if (Interlocked.Exchange(ref refusing, 1) == 0)
            logger.LogWarning(
                "PartitionCountLimit: refused {Path} of partition {Id}: the map holds {Count} partitions and MaxPartitions is {Max}. Later refusals are counted in raft.partitions.create_refused_total until a slot frees",
                path, partitionId, count, maxPartitions);

        return false;
    }
}
