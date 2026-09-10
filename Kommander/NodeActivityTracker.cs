
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Owns the per-node activity table for <see cref="RaftManager"/>.
/// Thread-safe by construction: the table is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// written and read concurrently without external locking. Updates are
/// intentionally racy-but-benign: two concurrent writers may briefly interleave, but the
/// read-compare-write only ever moves the HLC component forward from the value it observed, and
/// readers tolerate slightly stale values.
///
/// <para><b>Two clocks per entry, on purpose.</b> Each entry carries the peer's last HLC
/// timestamp (for event ordering and the public <see cref="IRaft"/> timestamp APIs) AND the
/// local monotonic tick at which that activity was received. Every freshness or elapsed-time
/// decision must read the tick, never subtract the HLC: an HLC absorbed from a skewed peer can
/// sit ahead of wall time, freezing <c>now - lastActivity</c> at zero for the whole skew and
/// silencing election back-off gates (the HLC drift review's failover blocker). The tick is
/// stamped by this tracker at update time, so it is always a genuine local receipt time.</para>
///
/// <para>Keyed endpoint → (partition → activity) rather than by a flat
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
    /// <summary>One activity observation: the peer's HLC timestamp plus the local monotonic
    /// tick at which it was recorded. <c>Ticks</c> is 0 only for the never-written default.</summary>
    private readonly record struct NodeActivity(HLCTimestamp Timestamp, long Ticks);

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, NodeActivity>> lastActivity = new();
    private readonly Func<long> getMonotonicTicks;
    private readonly string localEndpoint;

    internal NodeActivityTracker(Func<long> getMonotonicTicks, string localEndpoint)
    {
        this.getMonotonicTicks = getMonotonicTicks;
        this.localEndpoint = localEndpoint;
    }

    internal HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) =>
        lastActivity.TryGetValue(endpoint, out ConcurrentDictionary<int, NodeActivity>? partitions)
        && partitions.TryGetValue(partitionId, out NodeActivity activity)
            ? activity.Timestamp
            : HLCTimestamp.Zero;

    /// <summary>
    /// Local monotonic tick at which activity from <paramref name="endpoint"/> on
    /// <paramref name="partitionId"/> was last recorded, or 0 when the peer was never heard.
    /// This — not the HLC value — is what elapsed-time freshness gates must measure against.
    /// </summary>
    internal long GetLastNodeActivityTicks(string endpoint, int partitionId) =>
        lastActivity.TryGetValue(endpoint, out ConcurrentDictionary<int, NodeActivity>? partitions)
        && partitions.TryGetValue(partitionId, out NodeActivity activity)
            ? activity.Ticks
            : 0;

    internal HLCTimestamp GetLastNodeActivity(string endpoint)
    {
        HLCTimestamp max = HLCTimestamp.Zero;

        if (lastActivity.TryGetValue(endpoint, out ConcurrentDictionary<int, NodeActivity>? partitions))
        {
            foreach (KeyValuePair<int, NodeActivity> kv in partitions)
            {
                if (kv.Value.Timestamp > max)
                    max = kv.Value.Timestamp;
            }
        }

        return max;
    }

    internal void UpdateLastNodeActivity(string nodeId, int partitionId, HLCTimestamp lastTimestamp)
    {
        // TryGetValue fast path: after the first heartbeat the endpoint bucket always exists, and
        // GetOrAdd alone would evaluate its (non-capturing, but still) factory path on every call.
        if (!lastActivity.TryGetValue(nodeId, out ConcurrentDictionary<int, NodeActivity>? partitions))
            partitions = lastActivity.GetOrAdd(nodeId, static _ => new ConcurrentDictionary<int, NodeActivity>());

        long nowTicks = getMonotonicTicks();

        // The receipt tick always refreshes — any message from the peer is activity now — while
        // the HLC component only moves forward, matching the flat table's monotonic
        // read-compare-write. A concurrent interleaving is benign (see class remarks).
        if (partitions.TryGetValue(partitionId, out NodeActivity current))
            partitions[partitionId] = new(lastTimestamp > current.Timestamp ? lastTimestamp : current.Timestamp, nowTicks);
        else
            partitions.TryAdd(partitionId, new(lastTimestamp, nowTicks));
    }

    /// <summary>
    /// Returns all endpoints (excluding <see cref="localEndpoint"/>) heard within
    /// <paramref name="within"/>, sorted for deterministic output. Each endpoint appears
    /// at most once even if it leads multiple partitions. Measured on local receipt ticks,
    /// never HLC subtraction, so a skewed peer's timestamps cannot pin it "active".
    /// </summary>
    internal IReadOnlyList<string> GetActiveNodes(TimeSpan within)
    {
        long nowTicks = getMonotonicTicks();
        List<string> active = [];

        foreach (KeyValuePair<string, ConcurrentDictionary<int, NodeActivity>> node in lastActivity)
        {
            if (node.Key == localEndpoint)
                continue;

            foreach (KeyValuePair<int, NodeActivity> kv in node.Value)
            {
                if (kv.Value.Ticks != 0 && Consensus.RaftMonotonic.Elapsed(kv.Value.Ticks, nowTicks) <= within)
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
