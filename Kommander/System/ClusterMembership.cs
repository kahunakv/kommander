namespace Kommander.System;

/// <summary>
/// Versioned cluster roster stored under <see cref="RaftSystemConfigKeys.Members"/> on P0.
/// Mutated only through committed Raft entries — never by gossip or discovery snapshots —
/// so it inherits P0's linearizability. Each change increments <see cref="MembershipVersion"/>
/// by exactly 1; callers carry the version they read so the coordinator can detect stale writes.
/// <para>
/// Wire shape: serialized as JSON (<see cref="SystemJsonContext"/>) into the P0 log, checkpoints
/// and gossip payloads. A property added here must be optional with a default, so a roster
/// written by an older leader still deserializes and an older node ignores what it does not know.
/// </para>
/// </summary>
public sealed class ClusterMembership
{
    /// <summary>
    /// Upper bound on <see cref="Departures"/>. Entries are pruned when their endpoint is admitted
    /// again, so the list only grows with endpoints that left for good; the oldest are dropped
    /// past this bound. Generous on purpose: a record that was dropped reads as "reason unknown"
    /// and costs the departed node its eviction recovery.
    /// </summary>
    public const int MaxDepartures = 32;

    /// <summary>
    /// Monotonically increasing counter, bumped on every Add/Promote/Remove.
    /// Zero means no roster has been committed yet (pre-seed transient).
    /// </summary>
    public long MembershipVersion { get; set; }

    public List<ClusterMember> Members { get; set; } = [];

    /// <summary>
    /// The committed record of recent removals, oldest first: for each endpoint this roster no
    /// longer contains, the version that removed it and <see cref="ClusterDeparture.Reason">why</see>.
    /// A node that finds itself absent from a roster reads its own entry to learn whether it was
    /// evicted as a dead member (its leave request, if any, is void and it must auto-rejoin) or
    /// removed because it asked (it must stay out). The member list alone cannot tell the two
    /// apart, and inferring it from local state produced three distinct stalls in the Kahuna
    /// nightly (2026-10-11, 2026-10-13, 2026-10-27).
    /// <para>
    /// Pruned when the endpoint is admitted again, so an entry always describes the endpoint's
    /// most recent departure; capped at <see cref="MaxDepartures"/>. Empty on rosters committed
    /// by a leader that predates it — readers must treat a missing entry as "unknown", never as
    /// "not evicted".
    /// </para>
    /// </summary>
    public List<ClusterDeparture> Departures { get; set; } = [];

    /// <summary>The departure record for <paramref name="endpoint"/>, or null when this roster holds none.</summary>
    public ClusterDeparture? DepartureOf(string endpoint) =>
        Departures.FirstOrDefault(d => string.Equals(d.Endpoint, endpoint, StringComparison.Ordinal));

    /// <summary>
    /// The departure list for the roster that removes <paramref name="endpoint"/> at
    /// <paramref name="version"/>: any older record of the same endpoint is replaced, and the
    /// oldest records are dropped past <see cref="MaxDepartures"/>. Returns a new list; committed
    /// rosters are never mutated in place.
    /// </summary>
    public List<ClusterDeparture> DeparturesAfterRemoval(string endpoint, long version, ClusterDepartureReason reason)
    {
        List<ClusterDeparture> next = Departures
            .Where(d => !string.Equals(d.Endpoint, endpoint, StringComparison.Ordinal))
            .ToList();

        next.Add(new ClusterDeparture { Endpoint = endpoint, Version = version, Reason = reason });

        if (next.Count > MaxDepartures)
            next.RemoveRange(0, next.Count - MaxDepartures);

        return next;
    }

    /// <summary>
    /// The departure list for the roster that admits <paramref name="endpoint"/> again: its
    /// record is dropped, so a later exclusion is judged on its own. Returns the existing list
    /// when there is nothing to drop.
    /// </summary>
    public List<ClusterDeparture> DeparturesAfterAdmission(string endpoint) =>
        Departures.Any(d => string.Equals(d.Endpoint, endpoint, StringComparison.Ordinal))
            ? Departures.Where(d => !string.Equals(d.Endpoint, endpoint, StringComparison.Ordinal)).ToList()
            : Departures;
}
