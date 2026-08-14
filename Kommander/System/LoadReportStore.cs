
using System.Collections.Concurrent;

namespace Kommander.System;

/// <summary>
/// Owns and manages the per-node load-report cache on behalf of
/// <see cref="RaftSystemCoordinator"/>. Mutations (<see cref="Apply"/>, <see cref="EvictStale"/>)
/// are invoked exclusively on the coordinator's single-consumer channel loop, so writes never
/// race each other — but <see cref="GetAll"/> is called from arbitrary threads (load metrics,
/// leader hints), so the backing map is a <see cref="ConcurrentDictionary{TKey,TValue}"/> to make
/// those cross-thread enumerations safe. (It was previously a plain dictionary, which made every
/// external snapshot a torn-enumeration hazard against the loop's writes.)
/// </summary>
internal sealed class LoadReportStore
{
    private readonly ConcurrentDictionary<string, NodeLoadReport> _loadReports = new(StringComparer.Ordinal);

    /// <summary>
    /// Returns a point-in-time snapshot of all current load reports as a new list,
    /// safe to hand to callers on other threads.
    /// </summary>
    internal IReadOnlyList<NodeLoadReport> GetAll()
    {
        List<NodeLoadReport> snapshot = new(_loadReports.Count);
        foreach (NodeLoadReport r in _loadReports.Values)
            snapshot.Add(r);
        return snapshot;
    }

    /// <summary>
    /// Removes entries whose HLC age exceeds <paramref name="ttl"/> × 3. Called by the
    /// balancer pass before consuming store contents to avoid planning moves based on stale data.
    /// Note this runs only on the P0 leader with the balancer enabled — every other consumer of
    /// <see cref="GetAll"/> must apply its own freshness filter rather than rely on eviction.
    /// </summary>
    internal void EvictStale(TimeSpan ttl, Time.HLCTimestamp now)
    {
        TimeSpan maxAge = ttl * 3;
        List<string>? stale = null;
        foreach (NodeLoadReport r in _loadReports.Values)
        {
            if ((now - r.Time) > maxAge)
                (stale ??= []).Add(r.Endpoint);
        }
        if (stale is null)
            return;
        foreach (string endpoint in stale)
            _loadReports.TryRemove(endpoint, out _);
    }

    /// <summary>
    /// Returns the zone the given endpoint last advertised on its load report, or null when the
    /// endpoint has never reported (or reported no zone). Deliberately not TTL-filtered: a zone
    /// is topology, effectively immutable for a node's lifetime, so a report too old for load
    /// planning still carries a valid zone — filtering would randomly blind zone-aware placement
    /// during gossip gaps.
    /// </summary>
    internal string? GetNodeZone(string endpoint) =>
        _loadReports.TryGetValue(endpoint, out NodeLoadReport? report) ? report.Zone : null;

    /// <summary>
    /// Ingests a gossiped load report, retaining only the entry with the highest
    /// <see cref="NodeLoadReport.ReportVersion"/> per sender endpoint. The check-then-set is safe
    /// without a compare-exchange because the coordinator loop is the only writer.
    /// </summary>
    internal void Apply(RaftSystemRequest request)
    {
        NodeLoadReport? report = request.GossipedLoadReport;
        if (report is null || string.IsNullOrEmpty(report.Endpoint))
            return;
        if (_loadReports.TryGetValue(report.Endpoint, out NodeLoadReport? existing) &&
            report.ReportVersion <= existing.ReportVersion)
            return;
        _loadReports[report.Endpoint] = report;
    }
}
