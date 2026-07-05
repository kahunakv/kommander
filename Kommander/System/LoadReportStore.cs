
namespace Kommander.System;

/// <summary>
/// Owns and manages the per-node load-report cache on behalf of
/// <see cref="RaftSystemCoordinator"/>. All methods are invoked exclusively on the
/// coordinator's single-consumer channel loop — no locking is required.
/// </summary>
internal sealed class LoadReportStore
{
    private readonly Dictionary<string, NodeLoadReport> _loadReports = new(StringComparer.Ordinal);

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
            _loadReports.Remove(endpoint);
    }

    /// <summary>
    /// Ingests a gossiped load report, retaining only the entry with the highest
    /// <see cref="NodeLoadReport.ReportVersion"/> per sender endpoint.
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
