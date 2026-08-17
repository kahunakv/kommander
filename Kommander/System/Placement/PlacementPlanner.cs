
namespace Kommander.System.Placement;

/// <summary>
/// Pure replica-placement planner, modeled on the leader balancer's planner: given a
/// <see cref="PlacementView"/> it returns the bounded list of replica moves that brings every
/// range toward exactly <c>min(RF, |alive voters|)</c> replicas, evenly spread across nodes.
/// <para>
/// Priorities, highest first:
/// <list type="number">
///   <item><b>Repair under-replication</b> — a range with fewer healthy replicas than its RF is
///   a durability risk; repairs bypass the deadband.</item>
///   <item><b>Trim over-replication</b> — a range with more voters than RF (e.g. after a merge
///   union or an RF decrease) sheds the replica on the most-loaded node.</item>
///   <item><b>Balance replica-count skew</b> — nodes above the even-spread ceiling plus
///   <see cref="PlacementView.ReplicaCountDeadband"/> donate a range to the least-loaded node,
///   expressed as an add; the trim emerges on a later pass as over-replication once the learner
///   promotes, which keeps every step quorum-safe without a move saga.</item>
/// </list>
/// Stability rules: never more than one move per range per plan (single mover), never a move for
/// a range that already has a transitional replica, prefer the current placement on ties (churn
/// costs more than perfection), and cap the plan at <see cref="PlacementView.MaxMoves"/> total.
/// Moves are budgeted by class: repairs (priority 1, and priority-2 sheds of replicas on nodes
/// that left the roster) draw on <see cref="PlacementView.RepairBudget"/>; balance moves
/// (priority-2 cosmetic trims and priority-3 skew adds) draw on
/// <see cref="PlacementView.TransferBudget"/>, against which repairs emitted in the same plan
/// also count — so durability work is never throttled by the balance budget, while balance work
/// yields to an in-flight repair wave. The planner holds no state — determinism comes from
/// ordering by partition id and endpoint.
/// </para>
/// </summary>
public static class PlacementPlanner
{
    public static List<PlacementMove> Plan(PlacementView view)
    {
        List<PlacementMove> moves = [];

        // Per-class budgets: repairs consume repairBudget only; balance moves are additionally
        // capped so that repairs + balance emitted here never exceed TransferBudget — any
        // transfer consumes bandwidth regardless of class, so total in-flight transitions stay
        // bounded by max(MaxConcurrentReplicaRepairs, MaxConcurrentReplicaTransfers).
        int repairsUsed = 0;
        bool RepairAllowed() => moves.Count < view.MaxMoves && repairsUsed < view.RepairBudget;
        bool BalanceAllowed() => moves.Count < view.MaxMoves && moves.Count < view.TransferBudget;

        if (view.MaxMoves <= 0 || (view.RepairBudget <= 0 && view.TransferBudget <= 0))
            return moves;

        List<CandidateNode> alive = view.Nodes
            .Where(n => n.Alive)
            .OrderBy(n => n.Endpoint, StringComparer.Ordinal)
            .ToList();

        if (alive.Count == 0)
            return moves;

        HashSet<string> aliveEndpoints = alive.Select(n => n.Endpoint).ToHashSet(StringComparer.Ordinal);
        HashSet<string> rosterEndpoints = view.Nodes.Select(n => n.Endpoint).ToHashSet(StringComparer.Ordinal);

        // Current replica count per alive node (voters + learners), the load metric for spread.
        Dictionary<string, int> replicaCount = alive.ToDictionary(n => n.Endpoint, _ => 0, StringComparer.Ordinal);
        Dictionary<string, string?> zoneOf = view.Nodes.ToDictionary(n => n.Endpoint, n => n.Zone, StringComparer.Ordinal);

        foreach (RangePlacement range in view.Ranges)
        {
            foreach (string endpoint in range.VoterEndpoints.Concat(range.LearnerEndpoints))
            {
                if (replicaCount.TryGetValue(endpoint, out int count))
                    replicaCount[endpoint] = count + 1;
            }
        }

        // Ranges the planner may act on this pass, ordered for determinism.
        List<RangePlacement> actionable = view.Ranges
            .Where(r => !r.HasTransitionalReplica && r.ReplicationFactor > 0)
            .OrderBy(r => r.PartitionId)
            .ToList();

        HashSet<int> touched = [];

        // ── Priority 1: repair under-replication (repair budget) ─────────────
        foreach (RangePlacement range in actionable)
        {
            if (!RepairAllowed())
                break;

            int effectiveRf = Math.Min(range.ReplicationFactor, alive.Count);
            int healthyVoters = range.VoterEndpoints.Count(aliveEndpoints.Contains);

            if (healthyVoters >= effectiveRf)
                continue;

            string? target = PickAddTarget(range, alive, replicaCount, zoneOf);
            if (target is null)
                continue;

            moves.Add(new PlacementMove(range.PartitionId, PlacementMoveKind.AddReplica, target));
            repairsUsed++;
            replicaCount[target]++;
            touched.Add(range.PartitionId);
        }

        // ── Priority 2: trim over-replication (incl. replicas on dead/evicted nodes) ──
        foreach (RangePlacement range in actionable)
        {
            if (!RepairAllowed() && !BalanceAllowed())
                break;

            if (touched.Contains(range.PartitionId))
                continue;

            int effectiveRf = Math.Min(range.ReplicationFactor, alive.Count);
            int healthyVoters = range.VoterEndpoints.Count(aliveEndpoints.Contains);

            // A replica on a node no longer in the roster is dead weight; shed it as soon as
            // the healthy voters alone satisfy RF (otherwise priority 1 repairs first).
            // This shed completes a repair, so it draws on the repair budget.
            string? evicted = range.VoterEndpoints
                .Where(e => !rosterEndpoints.Contains(e))
                .OrderBy(e => e, StringComparer.Ordinal)
                .FirstOrDefault();

            if (evicted is not null && healthyVoters >= effectiveRf)
            {
                if (!RepairAllowed())
                    continue;

                moves.Add(new PlacementMove(range.PartitionId, PlacementMoveKind.RemoveReplica, evicted));
                repairsUsed++;
                touched.Add(range.PartitionId);
                continue;
            }

            if (healthyVoters <= effectiveRf)
                continue;

            // Shedding a cosmetically-excess healthy voter is balance work.
            if (!BalanceAllowed())
                continue;

            // Most-loaded healthy voter, preferring non-leader victims.
            string? victim = range.VoterEndpoints
                .Where(aliveEndpoints.Contains)
                .OrderBy(e => e == range.LeaderEndpoint ? 1 : 0)
                .ThenByDescending(e => replicaCount.GetValueOrDefault(e))
                .ThenBy(e => e, StringComparer.Ordinal)
                .FirstOrDefault();

            if (victim is null)
                continue;

            moves.Add(new PlacementMove(range.PartitionId, PlacementMoveKind.RemoveReplica, victim));
            replicaCount[victim] = Math.Max(0, replicaCount.GetValueOrDefault(victim) - 1);
            touched.Add(range.PartitionId);
        }

        // ── Priority 3: balance replica-count skew (deadband-gated, balance budget) ──
        int totalReplicas = replicaCount.Values.Sum();
        int ceiling = (totalReplicas + alive.Count - 1) / alive.Count; // even-spread ceiling

        foreach (CandidateNode donor in alive
                     .OrderByDescending(n => replicaCount.GetValueOrDefault(n.Endpoint))
                     .ThenBy(n => n.Endpoint, StringComparer.Ordinal))
        {
            if (!BalanceAllowed())
                return moves;

            if (replicaCount.GetValueOrDefault(donor.Endpoint) <= ceiling + view.ReplicaCountDeadband)
                break; // ordered descending: nobody further is above the deadband either

            // Donate one range hosted on the donor to the least-loaded node that lacks it.
            foreach (RangePlacement range in actionable)
            {
                if (touched.Contains(range.PartitionId) || !range.VoterEndpoints.Contains(donor.Endpoint))
                    continue;

                int effectiveRf = Math.Min(range.ReplicationFactor, alive.Count);
                if (effectiveRf >= alive.Count)
                    continue; // range must live everywhere; nothing to spread

                string? target = PickAddTarget(range, alive, replicaCount, zoneOf);
                if (target is null || replicaCount.GetValueOrDefault(target) + 1 >= replicaCount.GetValueOrDefault(donor.Endpoint))
                    continue; // move would not improve the spread

                moves.Add(new PlacementMove(range.PartitionId, PlacementMoveKind.AddReplica, target));
                replicaCount[target]++;
                touched.Add(range.PartitionId);
                break;
            }
        }

        return moves;
    }

    /// <summary>
    /// Least-loaded alive node not already hosting the range, preferring a zone not yet covered
    /// by the range's replicas (anti-affinity is absolute — one replica per node; zone spread is
    /// best-effort).
    /// </summary>
    private static string? PickAddTarget(
        RangePlacement range,
        List<CandidateNode> alive,
        Dictionary<string, int> replicaCount,
        Dictionary<string, string?> zoneOf)
    {
        HashSet<string> occupied = range.VoterEndpoints
            .Concat(range.LearnerEndpoints)
            .ToHashSet(StringComparer.Ordinal);

        HashSet<string> coveredZones = occupied
            .Select(e => zoneOf.GetValueOrDefault(e))
            .Where(z => !string.IsNullOrEmpty(z))
            .Select(z => z!)
            .ToHashSet(StringComparer.Ordinal);

        return alive
            .Where(n => !occupied.Contains(n.Endpoint))
            .OrderBy(n => !string.IsNullOrEmpty(n.Zone) && coveredZones.Contains(n.Zone!) ? 1 : 0)
            .ThenBy(n => replicaCount.GetValueOrDefault(n.Endpoint))
            .ThenBy(n => n.Endpoint, StringComparer.Ordinal)
            .Select(n => n.Endpoint)
            .FirstOrDefault();
    }

    /// <summary>
    /// Initial even-spread assignment used by <c>TrySetInitialPartitions</c> when
    /// <c>ReplicationFactor &gt; 0</c>: each range gets <paramref name="replicationFactor"/>
    /// distinct nodes with node loads differing by at most one, deterministically. When no
    /// candidate carries a zone the historical round-robin over the sorted node list is used
    /// unchanged; when zones are known the picks additionally prefer zones the range does not
    /// cover yet (same best-effort tiebreak as <see cref="PickAddTarget"/> — node
    /// anti-affinity stays absolute, zone spread bends to the load balance). Returns an empty
    /// assignment (legacy full replication) when there are fewer nodes than the factor — the RF
    /// floor degrades to full replication.
    /// </summary>
    public static Dictionary<int, List<CandidateNode>> AssignInitial(
        IReadOnlyList<int> partitionIds,
        IReadOnlyList<CandidateNode> nodes,
        int replicationFactor)
    {
        Dictionary<int, List<CandidateNode>> assignment = [];

        List<CandidateNode> sorted = nodes
            .OrderBy(n => n.Endpoint, StringComparer.Ordinal)
            .ToList();

        if (replicationFactor <= 0 || sorted.Count <= replicationFactor)
            return assignment; // sub-RF cluster: legacy full replication (empty replica sets)

        // No zones anywhere (single-zone deployments, or zones not yet gossiped at bootstrap):
        // keep the historical cursor round-robin bit-for-bit so existing deployments and their
        // deterministic expectations are untouched.
        if (sorted.All(n => string.IsNullOrEmpty(n.Zone)))
        {
            int cursor = 0;
            foreach (int partitionId in partitionIds.OrderBy(id => id))
            {
                List<CandidateNode> replicas = new(replicationFactor);
                for (int i = 0; i < replicationFactor; i++)
                    replicas.Add(sorted[(cursor + i) % sorted.Count]);

                cursor = (cursor + replicationFactor) % sorted.Count;
                assignment[partitionId] = replicas;
            }

            return assignment;
        }

        // Zone-aware greedy: pick one replica at a time, preferring an uncovered zone, then the
        // least-assigned node, then endpoint order — the same preference stack as PickAddTarget,
        // so bootstrap and later rebalancing agree on what "well placed" means.
        Dictionary<string, int> assignedCount = sorted.ToDictionary(n => n.Endpoint, _ => 0, StringComparer.Ordinal);

        foreach (int partitionId in partitionIds.OrderBy(id => id))
        {
            List<CandidateNode> replicas = new(replicationFactor);
            HashSet<string> coveredZones = new(StringComparer.Ordinal);

            for (int i = 0; i < replicationFactor; i++)
            {
                CandidateNode pick = sorted
                    .Where(n => !replicas.Contains(n))
                    .OrderBy(n => !string.IsNullOrEmpty(n.Zone) && coveredZones.Contains(n.Zone!) ? 1 : 0)
                    .ThenBy(n => assignedCount[n.Endpoint])
                    .ThenBy(n => n.Endpoint, StringComparer.Ordinal)
                    .First();

                replicas.Add(pick);
                assignedCount[pick.Endpoint]++;
                if (!string.IsNullOrEmpty(pick.Zone))
                    coveredZones.Add(pick.Zone!);
            }

            assignment[partitionId] = replicas;
        }

        return assignment;
    }
}
