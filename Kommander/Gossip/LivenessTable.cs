
namespace Kommander.Gossip;

/// <summary>
/// Thread-safe per-node SWIM liveness table.
///
/// <para>
/// Each <see cref="RaftManager"/> instance owns one <c>LivenessTable</c>.  Every node
/// maintains its own independent view of cluster health; the P0 leader's view is used
/// to drive eviction commits.
/// </para>
///
/// <para>
/// SWIM merge rule (§4.2):
/// <list type="bullet">
///   <item>Higher incarnation always wins.</item>
///   <item>For the same incarnation, state weight Dead &gt; Suspect &gt; Alive.</item>
///   <item>
///     Dead is terminal: once a node reaches Dead it stays Dead locally until evicted
///     from the roster (after which it is simply absent).
///   </item>
/// </list>
/// </para>
///
/// <para>
/// Self-refutation: when a <see cref="MemberLivenessState.Suspect"/> entry for the local
/// endpoint arrives via gossip, <see cref="RefuteSuspicion"/> bumps the self incarnation so
/// the next gossip round disseminates an Alive entry with a higher incarnation, overwriting
/// the Suspect across the cluster.
/// </para>
/// </summary>
internal sealed class LivenessTable
{
    private sealed record Entry(MemberLivenessState State, long Incarnation, DateTimeOffset ChangedAt);

    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _table = new();
    private long _selfIncarnation;  // always accessed under _lock

    // ── State queries ──────────────────────────────────────────────────────

    public MemberLivenessState GetState(string endpoint)
    {
        lock (_lock)
            return _table.TryGetValue(endpoint, out Entry? e) ? e.State : MemberLivenessState.Alive;
    }

    public long GetSelfIncarnation() { lock (_lock) return _selfIncarnation; }

    // ── Local probe outcomes ───────────────────────────────────────────────

    /// <summary>
    /// Records that <paramref name="endpoint"/> responded to a probe with
    /// <paramref name="incarnation"/>.  Clears Suspect if the response carries an
    /// incarnation that is at least as high as the Suspect entry — the responding node
    /// is demonstrably alive and its state is up-to-date.
    /// </summary>
    public void MarkAlive(string endpoint, long incarnation)
    {
        lock (_lock)
        {
            if (_table.TryGetValue(endpoint, out Entry? current))
            {
                // Don't downgrade Dead (terminal) or apply a stale incarnation.
                if (current.State == MemberLivenessState.Dead) return;
                if (incarnation < current.Incarnation) return;
            }
            _table[endpoint] = new(MemberLivenessState.Alive, incarnation, DateTimeOffset.UtcNow);
        }
    }

    /// <summary>
    /// Marks <paramref name="endpoint"/> as <see cref="MemberLivenessState.Suspect"/>
    /// after both direct and indirect probes timed out.  No-op if the node is already
    /// Suspect or Dead; preserves the existing incarnation.
    /// </summary>
    public void MarkSuspect(string endpoint)
    {
        lock (_lock)
        {
            if (_table.TryGetValue(endpoint, out Entry? current))
            {
                if (current.State != MemberLivenessState.Alive) return; // already worse
                // Preserve incarnation; a higher refutation will overwrite later.
                _table[endpoint] = new(MemberLivenessState.Suspect, current.Incarnation, DateTimeOffset.UtcNow);
            }
            else
            {
                _table[endpoint] = new(MemberLivenessState.Suspect, 0, DateTimeOffset.UtcNow);
            }
        }
    }

    /// <summary>
    /// Clears a <see cref="MemberLivenessState.Suspect"/> entry, resetting the node to
    /// <see cref="MemberLivenessState.Alive"/> while preserving the current incarnation.
    ///
    /// <para>
    /// Used on the indirect-probe (PingReq) success path: a relay confirmed the node is
    /// reachable, but <see cref="PingReqResponse"/> does not carry the target's incarnation,
    /// so passing incarnation 0 to <see cref="MarkAlive"/> would be silently dropped for any
    /// node that has previously refuted a suspicion (incarnation &gt; 0).  Reachability via
    /// a relay is an authoritative proof that overrides Suspect regardless of incarnation.
    /// </para>
    ///
    /// <para>No-op when the node is Alive (nothing to clear) or Dead (terminal).</para>
    /// </summary>
    public void ClearSuspicion(string endpoint)
    {
        lock (_lock)
        {
            if (!_table.TryGetValue(endpoint, out Entry? current)) return;
            if (current.State != MemberLivenessState.Suspect) return;
            _table[endpoint] = new(MemberLivenessState.Alive, current.Incarnation, DateTimeOffset.UtcNow);
        }
    }

    // ── Time advancement ───────────────────────────────────────────────────

    /// <summary>
    /// Transitions all <see cref="MemberLivenessState.Suspect"/> entries whose
    /// suspicion age exceeds <paramref name="suspicionTimeout"/> to
    /// <see cref="MemberLivenessState.Dead"/>.  Should be called at the start of
    /// each <c>PingAsync</c> tick.
    /// </summary>
    public void AdvanceExpiry(DateTimeOffset now, TimeSpan suspicionTimeout)
    {
        lock (_lock)
        {
            foreach (string ep in _table.Keys.ToList())
            {
                Entry e = _table[ep];
                if (e.State == MemberLivenessState.Suspect && now - e.ChangedAt >= suspicionTimeout)
                    _table[ep] = new(MemberLivenessState.Dead, e.Incarnation, now);
            }
        }
    }

    // ── Eviction ───────────────────────────────────────────────────────────

    /// <summary>
    /// Returns endpoints that have been <see cref="MemberLivenessState.Dead"/> for at
    /// least <paramref name="grace"/>.  The P0 leader calls this to decide which nodes
    /// to evict via committed <c>RemoveMember</c>.
    /// </summary>
    public IReadOnlyList<string> GetEvictable(DateTimeOffset now, TimeSpan grace)
    {
        lock (_lock)
        {
            List<string> result = [];
            foreach ((string ep, Entry e) in _table)
            {
                if (e.State == MemberLivenessState.Dead && now - e.ChangedAt >= grace)
                    result.Add(ep);
            }
            return result;
        }
    }

    /// <summary>Removes the liveness entry for <paramref name="endpoint"/>, e.g. after eviction commits.</summary>
    public void Remove(string endpoint)
    {
        lock (_lock) _table.Remove(endpoint);
    }

    // ── Gossip integration ─────────────────────────────────────────────────

    /// <summary>
    /// Applies a batch of <see cref="MemberLivenessEntry"/> updates received via gossip
    /// using the SWIM merge rule.  Returns <c>true</c> when an entry for
    /// <paramref name="selfEndpoint"/> with state ≥ <see cref="MemberLivenessState.Suspect"/>
    /// was applied — the caller should then call <see cref="RefuteSuspicion"/> so the next
    /// gossip round disseminates the refutation.
    /// </summary>
    public bool ApplyUpdates(string selfEndpoint, IReadOnlyList<MemberLivenessEntry>? updates)
    {
        if (updates is null || updates.Count == 0)
            return false;

        bool selfSuspected = false;

        lock (_lock)
        {
            foreach (MemberLivenessEntry upd in updates)
            {
                if (_table.TryGetValue(upd.Endpoint, out Entry? current))
                {
                    if (!ShouldUpdate(current, upd)) continue;
                }

                _table[upd.Endpoint] = new(upd.State, upd.Incarnation, upd.StateChangedAt);

                if (upd.Endpoint == selfEndpoint && upd.State >= MemberLivenessState.Suspect)
                    selfSuspected = true;
            }
        }

        return selfSuspected;
    }

    /// <summary>
    /// Clears self-suspicion and bumps the self-incarnation counter so subsequent gossip
    /// rounds carry an <see cref="MemberLivenessState.Alive"/> entry that overwrites any
    /// <see cref="MemberLivenessState.Suspect"/> entries already in circulation.
    ///
    /// <para>
    /// The new incarnation is <c>max(currentSelfIncarnation, suspectedIncarnation) + 1</c>
    /// where <c>suspectedIncarnation</c> is read from the table entry that
    /// <see cref="ApplyUpdates"/> just wrote.  This guarantees the refutation incarnation
    /// strictly exceeds the incoming suspicion regardless of the counter's current value —
    /// a node whose counter is at 1 receiving <c>Suspect(inc=99)</c> refutes with inc=100,
    /// not 2.
    /// </para>
    /// </summary>
    public long RefuteSuspicion(string selfEndpoint)
    {
        lock (_lock)
        {
            // Read the suspicion incarnation that ApplyUpdates just stored.
            long suspectedInc = _table.TryGetValue(selfEndpoint, out Entry? e) ? e.Incarnation : 0;
            long newInc = Math.Max(_selfIncarnation, suspectedInc) + 1;
            _selfIncarnation = newInc;
            _table[selfEndpoint] = new(MemberLivenessState.Alive, newInc, DateTimeOffset.UtcNow);
            return newInc;
        }
    }

    // ── Gossip piggyback ───────────────────────────────────────────────────

    /// <summary>
    /// Returns a snapshot of all tracked liveness entries to piggyback on the next
    /// outgoing gossip message.
    /// </summary>
    public IReadOnlyList<MemberLivenessEntry> GetAll()
    {
        lock (_lock)
        {
            List<MemberLivenessEntry> list = new(_table.Count);
            foreach ((string ep, Entry e) in _table)
                list.Add(new(ep, e.State, e.Incarnation, e.ChangedAt));
            return list;
        }
    }

    // ── Private helpers ────────────────────────────────────────────────────

    private static bool ShouldUpdate(Entry current, MemberLivenessEntry update)
    {
        if (update.Incarnation > current.Incarnation) return true;
        if (update.Incarnation < current.Incarnation && update.State != MemberLivenessState.Dead) return false;
        // Same incarnation: higher state weight wins (Dead=2 > Suspect=1 > Alive=0).
        return (int)update.State > (int)current.State;
    }
}
