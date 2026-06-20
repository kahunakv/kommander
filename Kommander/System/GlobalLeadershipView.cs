
using Kommander.Time;

namespace Kommander.System;

/// <summary>
/// Immutable snapshot of the cluster's leadership placement reduced from a set of
/// <see cref="NodeLoadReport"/> gossip messages.  Consumed by the planner to compute
/// transfer suggestions; never committed to the Raft log.
///
/// <para><b>Invariants:</b></para>
/// <list type="bullet">
///   <item>Only the highest <see cref="NodeLoadReport.ReportVersion"/> per endpoint is retained;
///   out-of-order arrivals are silently discarded.</item>
///   <item>Reports whose <see cref="NodeLoadReport.Time"/> is older than the configured TTL
///   (evaluated at <see cref="Build"/> time) are excluded.  Stale exclusion means the planner
///   simply sees fewer covered partitions and may abort via <see cref="IsComplete"/>.</item>
///   <item>When two fresh reports both claim leadership of the same partition the claim from the
///   report with the larger <see cref="NodeLoadReport.Time"/> wins; ties favour the first seen.</item>
/// </list>
/// </summary>
public sealed class GlobalLeadershipView
{
    /// <summary>Maps each live endpoint to the list of partition IDs it is believed to lead.</summary>
    public IReadOnlyDictionary<string, List<int>> LeadersByNode { get; }

    /// <summary>Maps each live endpoint to its aggregate load (sum of owned partition loads).</summary>
    public IReadOnlyDictionary<string, double> LoadByNode { get; }

    /// <summary>Maps each claimed partition ID to the endpoint that is believed to lead it.</summary>
    public IReadOnlyDictionary<int, string> PartitionOwner { get; }

    /// <summary>Maps each claimed partition ID to its reported load score.</summary>
    public IReadOnlyDictionary<int, double> PartitionLoad { get; }

    /// <summary>Maps each claimed partition ID to how long (ms) the current leader has held it.</summary>
    public IReadOnlyDictionary<int, long> PartitionLeaderSinceMs { get; }

    /// <summary>
    /// The set of cluster members with <see cref="ClusterMemberRole.Voter"/> role that
    /// appeared alive (in <c>aliveEndpoints</c>) at <see cref="Build"/> time.
    /// Only voters in this set are eligible as transfer targets.
    /// </summary>
    public IReadOnlySet<string> LiveVoters { get; }

    /// <summary>
    /// The set of endpoints from which a fresh (non-expired) report was received at
    /// <see cref="Build"/> time.  Used by <see cref="IsComplete"/> to detect silent
    /// live voters whose state is masked by a dead node's lingering report.
    /// </summary>
    public IReadOnlySet<string> FreshReportEndpoints { get; }

    /// <summary>Number of distinct endpoints whose reports were fresh at <see cref="Build"/> time.</summary>
    public int FreshReportCount => FreshReportEndpoints.Count;

    private GlobalLeadershipView(
        Dictionary<string, List<int>> leadersByNode,
        Dictionary<string, double> loadByNode,
        Dictionary<int, string> partitionOwner,
        Dictionary<int, double> partitionLoad,
        Dictionary<int, long> partitionLeaderSinceMs,
        HashSet<string> liveVoters,
        HashSet<string> freshReportEndpoints)
    {
        LeadersByNode   = leadersByNode;
        LoadByNode      = loadByNode;
        PartitionOwner  = partitionOwner;
        PartitionLoad   = partitionLoad;
        PartitionLeaderSinceMs = partitionLeaderSinceMs;
        LiveVoters      = liveVoters;
        FreshReportEndpoints = freshReportEndpoints;
    }

    /// <summary>
    /// Returns <see langword="true"/> when every live voter has a fresh report in this view.
    /// Uses set-membership rather than a count comparison so that a dead node's lingering
    /// report cannot substitute for a silent live one: a fresh report from endpoint X only
    /// satisfies completeness if X is itself a live voter.
    /// The planner calls this at the start of every pass and emits zero moves when false —
    /// an incomplete view is a safety valve, not an error.
    /// </summary>
    public bool IsComplete()
    {
        foreach (string voter in LiveVoters)
        {
            if (!FreshReportEndpoints.Contains(voter))
                return false;
        }
        return LiveVoters.Count > 0;
    }

    /// <summary>
    /// Reduces <paramref name="reports"/> into a <see cref="GlobalLeadershipView"/> by:
    /// deduplicating to the highest <see cref="NodeLoadReport.ReportVersion"/> per endpoint,
    /// expiring reports older than <paramref name="ttl"/>, and resolving conflicting partition
    /// claims by newest <see cref="NodeLoadReport.Time"/>.
    /// </summary>
    /// <param name="reports">Advisory gossip snapshots from all cluster nodes.</param>
    /// <param name="members">Committed cluster roster used to determine eligible voter set.</param>
    /// <param name="aliveEndpoints">Endpoints currently considered alive by the SWIM detector.</param>
    /// <param name="ttl">Maximum age before a report is discarded.</param>
    /// <param name="now">Current logical time used for TTL evaluation.</param>
    public static GlobalLeadershipView Build(
        IEnumerable<NodeLoadReport> reports,
        IEnumerable<ClusterMember> members,
        IReadOnlySet<string> aliveEndpoints,
        TimeSpan ttl,
        HLCTimestamp now)
    {
        // Step 1: deduplicate to highest ReportVersion per endpoint.
        Dictionary<string, NodeLoadReport> best = new(StringComparer.Ordinal);
        foreach (NodeLoadReport report in reports)
        {
            if (!best.TryGetValue(report.Endpoint, out NodeLoadReport? existing) ||
                report.ReportVersion > existing.ReportVersion)
            {
                best[report.Endpoint] = report;
            }
        }

        // Step 2: keep only fresh (non-expired) reports.
        List<NodeLoadReport> fresh = new(best.Count);
        HashSet<string> freshReportEndpoints = new(best.Count, StringComparer.Ordinal);
        foreach (NodeLoadReport report in best.Values)
        {
            if ((now - report.Time) <= ttl)
            {
                fresh.Add(report);
                freshReportEndpoints.Add(report.Endpoint);
            }
        }

        // Step 3: conflict-resolve partition ownership by newest report Time.
        // claimTime tracks which report's Time was used to win each partition.
        Dictionary<int, (string Endpoint, HLCTimestamp ClaimTime, double Load, long LeaderSinceMs)> claims = new();
        foreach (NodeLoadReport report in fresh)
        {
            foreach (PartitionLoad pl in report.Leaderships)
            {
                if (!claims.TryGetValue(pl.PartitionId, out var existing) ||
                    report.Time.CompareTo(existing.ClaimTime) > 0)
                {
                    claims[pl.PartitionId] = (report.Endpoint, report.Time, pl.Load, pl.LeaderSinceMs);
                }
            }
        }

        // Step 4: build output maps from the resolved claims.
        Dictionary<int, string> partitionOwner = new(claims.Count);
        Dictionary<int, double> partitionLoad = new(claims.Count);
        Dictionary<int, long> partitionLeaderSinceMs = new(claims.Count);
        Dictionary<string, List<int>> leadersByNode = new(StringComparer.Ordinal);
        Dictionary<string, double> loadByNode = new(StringComparer.Ordinal);

        foreach (KeyValuePair<int, (string Endpoint, HLCTimestamp ClaimTime, double Load, long LeaderSinceMs)> kv in claims)
        {
            int partitionId = kv.Key;
            string endpoint = kv.Value.Endpoint;
            double load = kv.Value.Load;
            long leaderSinceMs = kv.Value.LeaderSinceMs;

            partitionOwner[partitionId] = endpoint;
            partitionLoad[partitionId] = load;
            partitionLeaderSinceMs[partitionId] = leaderSinceMs;

            if (!leadersByNode.TryGetValue(endpoint, out List<int>? list))
                leadersByNode[endpoint] = list = [];
            list.Add(partitionId);

            loadByNode.TryGetValue(endpoint, out double currentLoad);
            loadByNode[endpoint] = currentLoad + load;
        }

        // Step 5: live voters = Voter-role members that are in aliveEndpoints.
        HashSet<string> liveVoters = new(StringComparer.Ordinal);
        foreach (ClusterMember member in members)
        {
            if (member.Role == ClusterMemberRole.Voter && aliveEndpoints.Contains(member.Endpoint))
                liveVoters.Add(member.Endpoint);
        }

        return new GlobalLeadershipView(
            leadersByNode,
            loadByNode,
            partitionOwner,
            partitionLoad,
            partitionLeaderSinceMs,
            liveVoters,
            freshReportEndpoints);
    }
}
