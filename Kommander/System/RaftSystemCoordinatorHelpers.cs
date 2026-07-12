
using Google.Protobuf;
using Kommander.System.Protos;

namespace Kommander.System;

/// <summary>
/// Pure static helpers extracted from <see cref="RaftSystemCoordinator"/> as part of the
/// large-class decomposition warm-up. These methods have no instance-state dependency and
/// are called on the coordinator's single-consumer loop thread, but the helpers themselves
/// are stateless and safe to call from any context.
/// </summary>
internal static class RaftSystemCoordinatorHelpers
{
    /// <summary>
    /// Divides the int32 hash space evenly into <paramref name="numberOfRanges"/> contiguous,
    /// non-overlapping <see cref="RaftPartitionRange"/> entries. Partition IDs are assigned
    /// starting from <see cref="RaftSystemConfig.SystemPartition"/> + 1. The remainder
    /// (when the space is not evenly divisible) is distributed one extra slot to the first
    /// <c>remainder</c> buckets.
    /// </summary>
    internal static List<RaftPartitionRange> DivideIntoRanges(int numberOfRanges)
    {
        int monotonicId = RaftSystemConfig.SystemPartition + 1;

        List<RaftPartitionRange> ranges = new(numberOfRanges);

        const long totalCount = (long)int.MaxValue + 1;
        long baseRangeSize = totalCount / numberOfRanges;
        long remainder = totalCount % numberOfRanges;

        long currentStart = 0;

        for (int i = 0; i < numberOfRanges; i++)
        {
            long currentRangeSize = baseRangeSize + (i < remainder ? 1 : 0);
            long currentEnd = currentStart + currentRangeSize - 1;

            ranges.Add(new()
            {
                PartitionId = monotonicId++,
                StartRange = (int)currentStart,
                EndRange = (int)currentEnd,
                Generation = 1,
                State = RaftPartitionState.Active,
                RoutingMode = RaftRoutingMode.HashRange
            });

            currentStart = currentEnd + 1;
        }

        return ranges;
    }

    /// <summary>
    /// Serializes a <see cref="RaftSystemMessage"/> to its Protobuf wire encoding.
    /// </summary>
    internal static byte[] Serialize(RaftSystemMessage message)
    {
        // Exact-size write (same pattern as RocksDbWAL): allocate one buffer of the computed size and
        // write straight into it — no MemoryStream, no growable backing array, no final ToArray copy.
        int size = message.CalculateSize();
        byte[] result = GC.AllocateUninitializedArray<byte>(size);
        message.WriteTo(result.AsSpan());
        return result;
    }

    /// <summary>
    /// Deserializes a <see cref="RaftSystemMessage"/> from its Protobuf wire encoding.
    /// </summary>
    internal static RaftSystemMessage Unserialize(byte[] serializedData)
    {
        // Parse directly from the span — no wrapping MemoryStream.
        return RaftSystemMessage.Parser.ParseFrom(serializedData.AsSpan());
    }

    /// <summary>
    /// Updates the <see cref="Diagnostics.KommanderMetrics"/> imbalance gauges from the
    /// current <paramref name="view"/> snapshot. Count-imbalance is max-leaders-on-one-node
    /// minus the floor target; load-imbalance is (maxLoad / meanLoad) − 1.
    /// A zero-voter or zero-load view sets both gauges to 0.0.
    /// </summary>
    internal static void UpdateImbalanceGauges(GlobalLeadershipView view)
    {
        if (view.LiveVoters.Count == 0)
        {
            Diagnostics.KommanderMetrics.BalancerCountImbalance = 0.0;
            Diagnostics.KommanderMetrics.BalancerLoadImbalance = 0.0;
            return;
        }

        int totalLeaders = 0;
        foreach (KeyValuePair<string, List<int>> kv in view.LeadersByNode)
            totalLeaders += kv.Value.Count;

        double target = (double)totalLeaders / view.LiveVoters.Count;
        double maxCount = 0;
        foreach (KeyValuePair<string, List<int>> kv in view.LeadersByNode)
        {
            if (kv.Value.Count > maxCount)
                maxCount = kv.Value.Count;
        }
        Diagnostics.KommanderMetrics.BalancerCountImbalance = Math.Max(0.0, maxCount - target);

        if (view.LoadByNode.Count == 0)
        {
            Diagnostics.KommanderMetrics.BalancerLoadImbalance = 0.0;
            return;
        }

        double sumLoad = 0;
        double maxLoad = 0;
        foreach (KeyValuePair<string, double> kv in view.LoadByNode)
        {
            sumLoad += kv.Value;
            if (kv.Value > maxLoad)
                maxLoad = kv.Value;
        }
        double meanLoad = sumLoad / view.LoadByNode.Count;
        Diagnostics.KommanderMetrics.BalancerLoadImbalance = meanLoad > 0.0 ? (maxLoad / meanLoad) - 1.0 : 0.0;
    }
}
