
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kommander;

/// <summary>
/// Owns the per-node activity and heartbeat timestamp tables for <see cref="RaftManager"/>.
/// Thread-safe by construction: both tables are <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// instances that are written and read concurrently without external locking.
/// </summary>
internal sealed class NodeActivityTracker
{
    private readonly ConcurrentDictionary<(string endpoint, int partitionId), HLCTimestamp> lastActivity = new();
    private readonly ConcurrentDictionary<(string endpoint, int partitionId), HLCTimestamp> lastHearthBeat = new();
    private readonly Func<HLCTimestamp> getNow;
    private readonly string localEndpoint;

    internal NodeActivityTracker(Func<HLCTimestamp> getNow, string localEndpoint)
    {
        this.getNow = getNow;
        this.localEndpoint = localEndpoint;
    }

    internal HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) =>
        lastActivity.TryGetValue((endpoint, partitionId), out HLCTimestamp ts) ? ts : HLCTimestamp.Zero;

    internal HLCTimestamp GetLastNodeActivity(string endpoint)
    {
        HLCTimestamp max = HLCTimestamp.Zero;
        foreach (((string ep, int _) key, HLCTimestamp ts) in lastActivity)
        {
            if (key.ep == endpoint && ts > max)
                max = ts;
        }
        return max;
    }

    internal void UpdateLastNodeActivity(string nodeId, int partitionId, HLCTimestamp lastTimestamp)
    {
        var key = (nodeId, partitionId);
        if (lastActivity.TryGetValue(key, out HLCTimestamp currentTimestamp))
        {
            if (lastTimestamp > currentTimestamp)
                lastActivity[key] = lastTimestamp;
        }
        else
            lastActivity.TryAdd(key, lastTimestamp);
    }

    internal HLCTimestamp GetLastNodeHearthbeat(string nodeId, int partitionId) =>
        lastHearthBeat.TryGetValue((nodeId, partitionId), out HLCTimestamp ts) ? ts : HLCTimestamp.Zero;

    internal void UpdateLastHeartbeat(string nodeId, int partitionId, HLCTimestamp lastTimestamp)
    {
        var key = (nodeId, partitionId);
        if (lastHearthBeat.TryGetValue(key, out HLCTimestamp currentTimestamp))
        {
            if (lastTimestamp > currentTimestamp)
                lastHearthBeat[key] = lastTimestamp;
        }
        else
            lastHearthBeat.TryAdd(key, lastTimestamp);
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

        foreach (((string endpoint, int _) key, HLCTimestamp lastSeen) in lastActivity)
        {
            if (key.endpoint == localEndpoint)
                continue;

            if ((now - lastSeen) <= within && !active.Contains(key.endpoint))
                active.Add(key.endpoint);
        }

        active.Sort(StringComparer.Ordinal);
        return active;
    }
}
