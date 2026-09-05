
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Owns the per-node activity timestamp table for <see cref="RaftManager"/>.
/// Thread-safe by construction: the table is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// written and read concurrently without external locking. Updates are
/// intentionally racy-but-benign: two concurrent writers may briefly interleave, but the
/// read-compare-write only ever moves a timestamp forward from the value it observed, and
/// readers tolerate slightly stale values.
///
/// <para>Keyed endpoint → (partition → timestamp) rather than by a flat
/// <c>(endpoint, partition)</c> tuple: the update path runs per inbound ack,
/// and the tuple key hashed the endpoint string on every probe (2–3 of them per
/// update). The nested layout hashes the string once and the partition id (an int) for the rest,
/// and lets the per-endpoint aggregations (<see cref="GetLastNodeActivity(string)"/>,
/// <see cref="GetActiveNodes"/>) read one endpoint's bucket instead of scanning the whole table.</para>
///
/// <para>The last-heartbeat-sent table that used to live beside this one moved to the
/// per-partition <c>ReplicationTracker</c> as monotonic ticks: its only reader was the
/// heartbeat de-dup skip, and comparing HLC timestamps against a wall-clock window there
/// silenced heartbeats for the length of any received clock skew (DST FINDING 4).</para>
/// </summary>
internal sealed class NodeActivityTracker
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, HLCTimestamp>> lastActivity = new();
    private readonly Func<HLCTimestamp> getNow;
    private readonly string localEndpoint;

    internal NodeActivityTracker(Func<HLCTimestamp> getNow, string localEndpoint)
    {
        this.getNow = getNow;
        this.localEndpoint = localEndpoint;
    }

    internal HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) =>
        lastActivity.TryGetValue(endpoint, out ConcurrentDictionary<int, HLCTimestamp>? partitions)
        && partitions.TryGetValue(partitionId, out HLCTimestamp ts)
            ? ts
            : HLCTimestamp.Zero;

    internal HLCTimestamp GetLastNodeActivity(string endpoint)
    {
        HLCTimestamp max = HLCTimestamp.Zero;

        if (lastActivity.TryGetValue(endpoint, out ConcurrentDictionary<int, HLCTimestamp>? partitions))
        {
            foreach (KeyValuePair<int, HLCTimestamp> kv in partitions)
            {
                if (kv.Value > max)
                    max = kv.Value;
            }
        }

        return max;
    }

    internal void UpdateLastNodeActivity(string nodeId, int partitionId, HLCTimestamp lastTimestamp) =>
        Update(lastActivity, nodeId, partitionId, lastTimestamp);

    private static void Update(
        ConcurrentDictionary<string, ConcurrentDictionary<int, HLCTimestamp>> table,
        string nodeId,
        int partitionId,
        HLCTimestamp lastTimestamp)
    {
        // TryGetValue fast path: after the first heartbeat the endpoint bucket always exists, and
        // GetOrAdd alone would evaluate its (non-capturing, but still) factory path on every call.
        if (!table.TryGetValue(nodeId, out ConcurrentDictionary<int, HLCTimestamp>? partitions))
            partitions = table.GetOrAdd(nodeId, static _ => new ConcurrentDictionary<int, HLCTimestamp>());

        // Same monotonic read-compare-write the flat table used: only move forward from the value
        // observed; a concurrent interleaving is benign (see class remarks).
        if (partitions.TryGetValue(partitionId, out HLCTimestamp currentTimestamp))
        {
            if (lastTimestamp > currentTimestamp)
                partitions[partitionId] = lastTimestamp;
        }
        else
            partitions.TryAdd(partitionId, lastTimestamp);
    }

    /// <summary>
    /// Returns all endpoints (excluding <see cref="localEndpoint"/>) seen within
    /// <paramref name="within"/>, sorted for deterministic output. Each endpoint appears
    /// at most once even if it leads multiple partitions.
    /// </summary>
    internal IReadOnlyList<string> GetActiveNodes(TimeSpan within)
    {
        HLCTimestamp now = getNow();
        List<string> active = [];

        foreach (KeyValuePair<string, ConcurrentDictionary<int, HLCTimestamp>> node in lastActivity)
        {
            if (node.Key == localEndpoint)
                continue;

            foreach (KeyValuePair<int, HLCTimestamp> kv in node.Value)
            {
                if ((now - kv.Value) <= within)
                {
                    active.Add(node.Key);
                    break;
                }
            }
        }

        active.Sort(StringComparer.Ordinal);
        return active;
    }
}
