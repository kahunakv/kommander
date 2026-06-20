
namespace Kommander.System;

/// <summary>
/// Stateless, pure advisory planner that produces a list of <see cref="LeaderMove"/>
/// suggestions from a <see cref="GlobalLeadershipView"/>, a partition map, and
/// configuration.  No cluster I/O or side effects — all mutable state (cooldowns,
/// in-flight tracking) is passed in by the caller (Phase 4 controller) and never held
/// here.  Because every suggested move is ultimately executed through the recipient's
/// own <c>TransferLeadershipAsync</c> validation, an incorrect or stale plan is always
/// safe: the worst outcome is a wasted or skipped move.
///
/// <para><b>Two-tier strategy (run in order, stop when caps are hit):</b></para>
/// <list type="number">
///   <item><b>Count tier</b> — redistributes leaders from over-loaded nodes to
///   under-loaded nodes until the per-node count is within
///   <see cref="RaftConfiguration.CountDeadband"/> of the ideal value.
///   Chooses the <i>hottest</i> source partition and the <i>coolest</i> under-node.</item>
///   <item><b>Load tier</b> — when counts are already balanced but aggregate load skew
///   across nodes exceeds <see cref="RaftConfiguration.LoadImbalanceThreshold"/>, emits
///   count-neutral hot↔cold swaps (one partition in each direction per swap) to reduce
///   skew without disturbing the count distribution.</item>
/// </list>
///
/// <para><b>Per-move filters (all must pass):</b></para>
/// <list type="bullet">
///   <item>Partition <see cref="RaftPartitionState.Active"/> in <paramref name="partitionMap"/>.</item>
///   <item><c>LeaderSinceMs ≥ MinLeaderStabilityMs</c> — the leader must be stable.</item>
///   <item>Target endpoint is a live voter in <see cref="GlobalLeadershipView.LiveVoters"/>.</item>
///   <item>Partition not in <paramref name="cooldownState"/> with an unexpired entry.</item>
/// </list>
/// </summary>
public static class LeaderBalancePlanner
{
    /// <summary>
    /// Plans a set of advisory leadership moves for a single balancer pass.
    /// Returns an empty list if the view is incomplete, no live voters exist,
    /// or all partitions are filtered out.
    /// </summary>
    /// <param name="view">Reduced global leadership snapshot from recent gossip reports.</param>
    /// <param name="partitionMap">Authoritative partition ranges used to filter non-Active partitions.</param>
    /// <param name="config">Balancer tuning knobs.</param>
    /// <param name="cooldownState">
    /// Map of partition ID → expiry time for the post-move cooldown.
    /// Partitions with a future expiry time are excluded from this pass.
    /// </param>
    /// <param name="now">Wall-clock time used for cooldown expiry evaluation.</param>
    public static IReadOnlyList<LeaderMove> Plan(
        GlobalLeadershipView view,
        RaftPartitionMap partitionMap,
        RaftConfiguration config,
        IReadOnlyDictionary<int, global::System.DateTimeOffset> cooldownState,
        global::System.DateTimeOffset now)
    {
        int liveNodeCount = view.LiveVoters.Count;
        if (liveNodeCount < 2)
            return [];

        // The planner requires a complete view; abort if any live voter is silent.
        if (!view.IsComplete())
            return [];

        // Pre-build partition-state lookup for fast Active checks.
        Dictionary<int, RaftPartitionState> stateById = new(partitionMap.Partitions.Count);
        foreach (RaftPartitionRange range in partitionMap.Partitions)
            stateById[range.PartitionId] = range.State;

        List<LeaderMove> moves = new(config.MaxMovesPerPass);

        // ── Count tier ────────────────────────────────────────────────────────
        RunCountTier(view, config, stateById, cooldownState, now, moves);

        // ── Load tier (only when counts are balanced) ─────────────────────────
        if (moves.Count == 0)
            RunLoadTier(view, config, stateById, cooldownState, now, moves);

        return moves;
    }

    // ── Count tier ────────────────────────────────────────────────────────────

    private static void RunCountTier(
        GlobalLeadershipView view,
        RaftConfiguration config,
        Dictionary<int, RaftPartitionState> stateById,
        IReadOnlyDictionary<int, global::System.DateTimeOffset> cooldownState,
        global::System.DateTimeOffset now,
        List<LeaderMove> moves)
    {
        int liveNodeCount = view.LiveVoters.Count;
        int totalLeaders = view.PartitionOwner.Count;

        // Natural ideal: floor and ceiling of total/nodes.
        int floor = totalLeaders / liveNodeCount;
        int ceil  = (totalLeaders + liveNodeCount - 1) / liveNodeCount;

        // Nodes are "over" when they exceed ceil + CountDeadband - 1 (i.e., ceil when deadband=1).
        // Nodes are "under" when they are below floor.
        int overThreshold  = ceil  + config.CountDeadband - 1;
        int underThreshold = floor;

        // Build mutable per-node counts from the view.
        Dictionary<string, int> countByNode = new(liveNodeCount, StringComparer.Ordinal);
        foreach (string voter in view.LiveVoters)
            countByNode[voter] = view.LeadersByNode.TryGetValue(voter, out List<int>? led) ? led.Count : 0;

        // Build mutable per-node partition lists (sorted hottest-first for each over-node).
        Dictionary<string, List<int>> leadersByNode = new(StringComparer.Ordinal);
        foreach (KeyValuePair<string, List<int>> kv in view.LeadersByNode)
        {
            if (view.LiveVoters.Contains(kv.Key))
                leadersByNode[kv.Key] = new List<int>(kv.Value);
        }

        while (moves.Count < config.MaxMovesPerPass)
        {
            // Find over-node with most leaders; endpoint string breaks ties for determinism.
            string? overNode = null;
            int overCount = 0;
            foreach (KeyValuePair<string, int> kv in countByNode)
            {
                if (kv.Value > overThreshold &&
                    (kv.Value > overCount ||
                     (kv.Value == overCount && global::System.StringComparer.Ordinal.Compare(kv.Key, overNode) < 0)))
                {
                    overNode = kv.Key;
                    overCount = kv.Value;
                }
            }

            if (overNode is null) break; // no over-nodes

            // Find under-node with fewest leaders; endpoint string breaks ties for determinism.
            string? underNode = null;
            int underCount = int.MaxValue;
            foreach (KeyValuePair<string, int> kv in countByNode)
            {
                if (kv.Key != overNode && kv.Value < underThreshold &&
                    (kv.Value < underCount ||
                     (kv.Value == underCount && global::System.StringComparer.Ordinal.Compare(kv.Key, underNode) < 0)))
                {
                    underNode = kv.Key;
                    underCount = kv.Value;
                }
            }

            if (underNode is null) break; // no under-nodes

            // Pick the hottest eligible partition from overNode.
            int? chosenPartition = PickHottestEligible(
                overNode, underNode, view, stateById, cooldownState, now, config.MinLeaderStabilityMs,
                leadersByNode.TryGetValue(overNode, out List<int>? overPartitions) ? overPartitions : []);

            if (chosenPartition is null) break; // nothing movable

            moves.Add(new LeaderMove(chosenPartition.Value, overNode, underNode));

            // Update local accounting to reflect the simulated move.
            countByNode[overNode]--;
            countByNode[underNode]++;

            if (leadersByNode.TryGetValue(overNode, out List<int>? src))
                src.Remove(chosenPartition.Value);
            if (!leadersByNode.TryGetValue(underNode, out List<int>? dst))
                leadersByNode[underNode] = dst = [];
            dst.Add(chosenPartition.Value);
        }
    }

    // ── Load tier ─────────────────────────────────────────────────────────────

    private static void RunLoadTier(
        GlobalLeadershipView view,
        RaftConfiguration config,
        Dictionary<int, RaftPartitionState> stateById,
        IReadOnlyDictionary<int, global::System.DateTimeOffset> cooldownState,
        global::System.DateTimeOffset now,
        List<LeaderMove> moves)
    {
        if (view.LiveVoters.Count < 2) return;

        // Compute load skew across live voters.
        double maxLoad = double.MinValue;
        double minLoad = double.MaxValue;
        string? hotNode = null;
        string? coldNode = null;

        foreach (string voter in view.LiveVoters)
        {
            double load = view.LoadByNode.TryGetValue(voter, out double l) ? l : 0.0;
            if (load > maxLoad || (load == maxLoad && global::System.StringComparer.Ordinal.Compare(voter, hotNode) < 0))
                { maxLoad = load; hotNode = voter; }
            if (load < minLoad || (load == minLoad && global::System.StringComparer.Ordinal.Compare(voter, coldNode) < 0))
                { minLoad = load; coldNode = voter; }
        }

        if (hotNode is null || coldNode is null || hotNode == coldNode) return;

        double skew = maxLoad > 0.0 ? (maxLoad - minLoad) / maxLoad : 0.0;
        if (skew <= config.LoadImbalanceThreshold) return;

        // At most one swap per pass — re-evaluate node loads on the next balancer interval
        // once the transferred partition has settled and emitted a fresh report.
        if (moves.Count + 2 > config.MaxMovesPerPass) return;

        // Hot partition from hotNode → coldNode.
        List<int> hotPartitions = view.LeadersByNode.TryGetValue(hotNode, out List<int>? hp) ? hp : [];
        int? hotPartition = PickHottestEligible(
            hotNode, coldNode, view, stateById, cooldownState, now, config.MinLeaderStabilityMs,
            hotPartitions);

        if (hotPartition is null) return;

        // Cold partition from coldNode → hotNode (to maintain count neutrality).
        List<int> coldPartitions = view.LeadersByNode.TryGetValue(coldNode, out List<int>? cp) ? cp : [];
        int? coldPartition = PickColdestEligible(
            coldNode, hotNode, view, stateById, cooldownState, now, config.MinLeaderStabilityMs,
            coldPartitions, excludePartition: hotPartition.Value);

        if (coldPartition is null) return; // no swap partner — skip this pass

        // Guard: only emit the swap if it strictly reduces the load spread.
        // After the swap: hotNode loses pLoad and gains qLoad; coldNode gains pLoad and loses qLoad.
        // New spread = |(maxLoad - pLoad + qLoad) - (minLoad + pLoad - qLoad)|.
        // If the spread does not decrease the swap is non-improving (e.g. when loads are equal)
        // and emitting it would produce churn without benefit.
        double pLoad = view.PartitionLoad.TryGetValue(hotPartition.Value, out double pl) ? pl : 0.0;
        double qLoad = view.PartitionLoad.TryGetValue(coldPartition.Value, out double ql) ? ql : 0.0;
        double newSpread = global::System.Math.Abs((maxLoad - pLoad + qLoad) - (minLoad + pLoad - qLoad));
        if (newSpread >= maxLoad - minLoad) return;

        moves.Add(new LeaderMove(hotPartition.Value, hotNode, coldNode));
        moves.Add(new LeaderMove(coldPartition.Value, coldNode, hotNode));
    }

    // ── Partition-selection helpers ───────────────────────────────────────────

    private static int? PickHottestEligible(
        string fromNode,
        string toNode,
        GlobalLeadershipView view,
        Dictionary<int, RaftPartitionState> stateById,
        IReadOnlyDictionary<int, global::System.DateTimeOffset> cooldownState,
        global::System.DateTimeOffset now,
        long minStabilityMs,
        List<int> candidatePartitions)
    {
        int? best = null;
        double bestLoad = double.MinValue;

        foreach (int pid in candidatePartitions)
        {
            if (!IsEligible(pid, fromNode, toNode, view, stateById, cooldownState, now, minStabilityMs))
                continue;

            double load = view.PartitionLoad.TryGetValue(pid, out double l) ? l : 0.0;
            if (load > bestLoad) { bestLoad = load; best = pid; }
        }

        return best;
    }

    private static int? PickColdestEligible(
        string fromNode,
        string toNode,
        GlobalLeadershipView view,
        Dictionary<int, RaftPartitionState> stateById,
        IReadOnlyDictionary<int, global::System.DateTimeOffset> cooldownState,
        global::System.DateTimeOffset now,
        long minStabilityMs,
        List<int> candidatePartitions,
        int excludePartition)
    {
        int? best = null;
        double bestLoad = double.MaxValue;

        foreach (int pid in candidatePartitions)
        {
            if (pid == excludePartition) continue;
            if (!IsEligible(pid, fromNode, toNode, view, stateById, cooldownState, now, minStabilityMs))
                continue;

            double load = view.PartitionLoad.TryGetValue(pid, out double l) ? l : 0.0;
            if (load < bestLoad) { bestLoad = load; best = pid; }
        }

        return best;
    }

    /// <summary>
    /// Checks all per-move safety filters for a candidate partition move.
    /// </summary>
    private static bool IsEligible(
        int partitionId,
        string fromNode,
        string toNode,
        GlobalLeadershipView view,
        Dictionary<int, RaftPartitionState> stateById,
        IReadOnlyDictionary<int, global::System.DateTimeOffset> cooldownState,
        global::System.DateTimeOffset now,
        long minStabilityMs)
    {
        // Partition must exist and be Active.
        if (!stateById.TryGetValue(partitionId, out RaftPartitionState state) ||
            state != RaftPartitionState.Active)
            return false;

        // This node must currently be believed to lead this partition.
        if (!view.PartitionOwner.TryGetValue(partitionId, out string? owner) ||
            !string.Equals(owner, fromNode, global::System.StringComparison.Ordinal))
            return false;

        // Leader must have been stable long enough.
        if (!view.PartitionLeaderSinceMs.TryGetValue(partitionId, out long sinceMs) ||
            sinceMs < minStabilityMs)
            return false;

        // Target must be a live voter.
        if (!view.LiveVoters.Contains(toNode))
            return false;

        // Partition must not be in cooldown.
        if (cooldownState.TryGetValue(partitionId, out global::System.DateTimeOffset expiry) &&
            now < expiry)
            return false;

        return true;
    }
}
