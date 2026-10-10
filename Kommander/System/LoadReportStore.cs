
using System.Collections.Concurrent;
using System.Collections.ObjectModel;

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
    /// Local monotonic tick source used to stamp <see cref="NodeLoadReport.ReceivedAtTicks"/> on
    /// accepted ingestion. Receipt ticks — not the sender-stamped HLC <see cref="NodeLoadReport.Time"/> —
    /// are what every freshness/TTL decision measures against, because a sender's clock skew makes
    /// the HLC age meaningless (a future-dated report otherwise never expires). Defaults to the
    /// process clock; the coordinator wires the configured tick source.
    /// </summary>
    private readonly Func<long> _getReceiptTicks;

    /// <summary>
    /// Receipt age past which a retained entry no longer blocks a report that loses the
    /// incarnation/version ordering (see <see cref="Apply"/>). Wired to
    /// <see cref="RaftConfiguration.LeaderBalancerReportTtl"/>: an entry older than the TTL is
    /// already invisible to every TTL-filtered consumer, so replacing it with any newer-received
    /// report is strictly better than keeping it.
    /// </summary>
    private readonly TimeSpan _staleAfter;

    internal LoadReportStore(Func<long>? getReceiptTicks = null, TimeSpan? staleAfter = null)
    {
        _getReceiptTicks = getReceiptTicks ?? Time.SystemMonotonicTickSource.Instance.GetTimestamp;
        _staleAfter = staleAfter ?? TimeSpan.FromSeconds(20);
    }

    /// <summary>
    /// Incremented after every mutation of <see cref="_loadReports"/>. A cached snapshot is valid only
    /// while this value is unchanged, which is what lets <see cref="GetAll"/> hand out the same
    /// collection to many readers without ever showing a torn or stale view.
    /// </summary>
    private long _version;

    /// <summary>
    /// The most recently published snapshot, stamped with the <see cref="_version"/> it was built from.
    /// Null until the first read.
    /// </summary>
    private volatile VersionedSnapshot? _snapshot;

    /// <summary>
    /// Returns a point-in-time snapshot of all current load reports, safe to hand to callers on other
    /// threads.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The result is cached per store version and shared between readers. Load metrics are polled per
    /// partition and there are three of them, so the previous "copy every value into a fresh list on
    /// every call" shape rebuilt the same collection many times per poll cycle — and did so twice over,
    /// because <see cref="ConcurrentDictionary{TKey,TValue}.Values"/> already materializes a snapshot
    /// collection of its own before the copy ran.
    /// </para>
    /// <para>
    /// The snapshot is published only when the version has not moved while it was being built, so a
    /// reader that raced a writer discards its own result rather than caching a view that is already
    /// behind. A losing race costs one wasted rebuild; it can never install a stale snapshot, because
    /// the version increases monotonically and the publish check re-reads it.
    /// </para>
    /// <para>
    /// Handed out as a <see cref="ReadOnlyCollection{T}"/> rather than the backing array: the same
    /// instance now reaches many callers, so a caller that cast the result back to a mutable collection
    /// would corrupt every other reader's view rather than just its own copy.
    /// </para>
    /// </remarks>
    internal IReadOnlyList<NodeLoadReport> GetAll()
    {
        long version = Volatile.Read(ref _version);

        VersionedSnapshot? cached = _snapshot;
        if (cached is not null && cached.Version == version)
            return cached.Reports;

        VersionedSnapshot fresh = new(version, BuildSnapshot());

        // Publish only if nothing changed while the snapshot was built. Two readers can still overwrite
        // each other here; the loser's snapshot is merely older, never newer than the version it claims,
        // so the next reader either reuses it or rebuilds.
        if (Volatile.Read(ref _version) == version)
            _snapshot = fresh;

        return fresh.Reports;
    }

    private ReadOnlyCollection<NodeLoadReport> BuildSnapshot()
    {
        ICollection<NodeLoadReport> values = _loadReports.Values;

        NodeLoadReport[] reports = new NodeLoadReport[values.Count];
        values.CopyTo(reports, 0);

        return new ReadOnlyCollection<NodeLoadReport>(reports);
    }

    /// <summary>
    /// Marks every cached snapshot stale. Must be called after — never before — the mutation it
    /// describes, so a reader that observes the new version also observes the new dictionary contents.
    /// <see cref="Interlocked.Increment(ref long)"/> supplies the ordering barrier.
    /// </summary>
    private void InvalidateSnapshot() => Interlocked.Increment(ref _version);

    /// <summary>Pairs a published snapshot with the store version it was built from.</summary>
    private sealed class VersionedSnapshot(long version, ReadOnlyCollection<NodeLoadReport> reports)
    {
        internal long Version { get; } = version;

        internal ReadOnlyCollection<NodeLoadReport> Reports { get; } = reports;
    }

    /// <summary>
    /// Removes entries whose local receipt age exceeds <paramref name="ttl"/> × 3. Called by the
    /// balancer pass before consuming store contents to avoid planning moves based on stale data.
    /// Age is measured against <see cref="NodeLoadReport.ReceivedAtTicks"/> (local monotonic) —
    /// never against the sender-stamped HLC, whose skew previously let a future-dated report
    /// survive eviction for the whole skew plus the TTL. A report that somehow carries no receipt
    /// tick reads as infinitely old and is evicted.
    /// Note this runs only on the P0 leader with the balancer enabled — every other consumer of
    /// <see cref="GetAll"/> must apply its own freshness filter rather than rely on eviction.
    /// </summary>
    internal void EvictStale(TimeSpan ttl, long nowTicks)
    {
        TimeSpan maxAge = ttl * 3;
        List<string>? stale = null;
        foreach (NodeLoadReport r in _loadReports.Values)
        {
            if (Consensus.RaftMonotonic.Elapsed(r.ReceivedAtTicks, nowTicks) > maxAge)
                (stale ??= []).Add(r.Endpoint);
        }
        if (stale is null)
            return;
        foreach (string endpoint in stale)
            _loadReports.TryRemove(endpoint, out _);

        InvalidateSnapshot();
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
    /// Ingests a gossiped load report, retaining one entry per sender endpoint. The check-then-set
    /// is safe without a compare-exchange because the coordinator loop is the only writer.
    /// <para>Acceptance order, per sender:</para>
    /// <list type="number">
    ///   <item>No retained entry: accept.</item>
    ///   <item>A newer <see cref="NodeLoadReport.Incarnation"/> than the retained entry: accept,
    ///   whatever the <see cref="NodeLoadReport.ReportVersion"/>. The sender restarted and its
    ///   version counter restarted with it; comparing versions across lifetimes rejected every
    ///   post-restart report and left the P0 placement controller without a leader hint for each
    ///   range the restarted node led (the 2026-10-09 Kahuna learner-promotion stall).</item>
    ///   <item>The same incarnation: accept only a strictly higher version, as before.</item>
    ///   <item>An older incarnation (an in-flight report of the dead lifetime, or a restart whose
    ///   clock stepped backwards): reject while the retained entry is fresh. Once the retained
    ///   entry's receipt age exceeds the TTL it blocks nothing — every freshness-filtered consumer
    ///   already ignores it — so any report then replaces it. This backstop bounds the damage of
    ///   any ordering failure to one TTL instead of forever.</item>
    /// </list>
    /// <para>Stamps <see cref="NodeLoadReport.ReceivedAtTicks"/> here, on acceptance only, so
    /// gossip that loses the ordering can never refresh a report's freshness.</para>
    /// </summary>
    internal void Apply(RaftSystemRequest request)
    {
        NodeLoadReport? report = request.GossipedLoadReport;
        if (report is null || string.IsNullOrEmpty(report.Endpoint))
            return;

        long nowTicks = _getReceiptTicks();

        if (_loadReports.TryGetValue(report.Endpoint, out NodeLoadReport? existing) &&
            !Supersedes(report, existing, nowTicks))
            return;

        report.ReceivedAtTicks = nowTicks;
        _loadReports[report.Endpoint] = report;

        InvalidateSnapshot();
    }

    /// <summary>
    /// The ordering rule of <see cref="Apply"/>, isolated so it can be read as one predicate:
    /// newer incarnation wins; same incarnation orders by version; an older incarnation wins only
    /// over a retained entry that has aged past the TTL.
    /// </summary>
    private bool Supersedes(NodeLoadReport candidate, NodeLoadReport existing, long nowTicks)
    {
        if (candidate.Incarnation > existing.Incarnation)
            return true;

        if (candidate.Incarnation == existing.Incarnation)
            return candidate.ReportVersion > existing.ReportVersion;

        return Consensus.RaftMonotonic.Elapsed(existing.ReceivedAtTicks, nowTicks) > _staleAfter;
    }
}
