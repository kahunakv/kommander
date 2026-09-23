using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.WAL;

namespace Kommander.Tests.Simulation.Invariants;

/// <summary>
/// Rule <c>applied-state-agrees</c>: at the end of a run, every live node's application state holds
/// every committed entry it is owed, and no two nodes applied different entries at one id.
///
/// <para><b>Why a rule about the application and not the log.</b> The log rules cannot see an
/// install that was acknowledged and imported nothing: the receiver's WAL boundary moves, its
/// frontiers are right, and every log check passes. Only the state the application holds shows the
/// hole. The simulated application (<see cref="SimulatedPartitionStateTransfer.Apply"/>) records
/// every entry the library hands it, and a snapshot carries that state, so a skipped import leaves
/// the receiver without the entries below the boundary (<c>14b564a</c> item 6, DST-20).</para>
///
/// <para><b>What a node is owed.</b> Every entry the library delivers: type <c>Committed</c>, not the
/// leadership-barrier no-op (the two filters in <c>LogApplicator</c>). A node is owed such an entry
/// when it is at or below the node's own commit index and either the node's log still holds it, or
/// another node applied it — the second case is the one a skipped import produces, because the
/// receiver's log no longer holds what the snapshot was meant to carry.</para>
///
/// <para><b>Soundness.</b> An applied entry is committed, and committed entries are never truncated,
/// so a node that applied an id holds a fact every node with that commit index must share. The
/// application state survives a crash (it models an application that persists what it applied), and
/// a wiped node starts empty, so neither a crash nor a blank restart leaves a stale entry
/// behind.</para>
///
/// <para>Application runs after commit, so the rule first gives the cluster a bounded number of steps
/// to finish delivering before it judges.</para>
/// </summary>
public static class AppliedStateRule
{
    /// <summary>Steps allowed for delivery to catch up with the commit index before the verdict.</summary>
    private const int DeliveryStepBudget = 100;

    public static async Task CheckAsync(
        SimulationCluster cluster,
        int partitionId,
        long advanceMilliseconds,
        CancellationToken cancellationToken)
    {
        List<string> problems = [];

        await cluster.RunUntilAsync(
            async () =>
            {
                problems = await FindProblemsAsync(cluster, partitionId, cancellationToken).ConfigureAwait(false);
                return problems.Count == 0;
            },
            DeliveryStepBudget,
            advanceMilliseconds,
            cancellationToken).ConfigureAwait(false);

        if (problems.Count == 0)
            return;

        throw new InvariantViolationException(
            "applied-state-agrees",
            $"applied-state-agrees: after {DeliveryStepBudget} steps for delivery, the application state is wrong: " +
            string.Join(" | ", problems.Take(8)) +
            (problems.Count > 8 ? $" (+{problems.Count - 8} more)" : string.Empty) +
            ". Imports: " + string.Join(", ", cluster.Nodes.Select(node =>
                $"{node.Endpoint} applied={node.StateTransfer.ImportsApplied} skipped={node.StateTransfer.ImportsSkipped} " +
                $"boundary={node.StateTransfer.LastImportBoundary}")),
            cluster.StepNumber,
            selectedEvent: null,
            lastValidSnapshot: null,
            failingSnapshot: null);
    }

    private static async Task<List<string>> FindProblemsAsync(
        SimulationCluster cluster,
        int partitionId,
        CancellationToken cancellationToken)
    {
        List<string> problems = [];
        List<SimulationNode> live = cluster.Nodes.Where(node => node.HasLiveManager).ToList();

        Dictionary<string, IReadOnlyDictionary<long, SimulatedPartitionStateTransfer.AppliedEntry>> appliedByNode =
            live.ToDictionary(node => node.Endpoint, node => node.StateTransfer.GetApplied(partitionId));

        // Every id anybody applied, with the entry applied there first. Two different entries at one
        // id is the most serious answer this rule can give, so it is looked for first.
        Dictionary<long, (string Endpoint, SimulatedPartitionStateTransfer.AppliedEntry Entry)> union = [];

        foreach ((string endpoint, IReadOnlyDictionary<long, SimulatedPartitionStateTransfer.AppliedEntry> state) in appliedByNode)
        {
            foreach ((long id, SimulatedPartitionStateTransfer.AppliedEntry entry) in state)
            {
                if (!union.TryGetValue(id, out (string Endpoint, SimulatedPartitionStateTransfer.AppliedEntry Entry) seen))
                    union[id] = (endpoint, entry);
                else if (seen.Entry != entry)
                    problems.Add($"{seen.Endpoint} and {endpoint} applied different entries at {id}");
            }
        }

        foreach (SimulationNode node in live)
        {
            RaftPartitionView? view = await node.GetPartitionViewAsync(partitionId, cancellationToken).ConfigureAwait(false);
            if (view is null)
            {
                problems.Add($"{node.Endpoint} answered no view");
                continue;
            }

            IReadOnlyDictionary<long, SimulatedPartitionStateTransfer.AppliedEntry> state = appliedByNode[node.Endpoint];

            Dictionary<long, RaftLog> held = node.Wal
                .ReadLogsRange(partitionId, 0)
                .ToDictionary(log => log.Id);

            List<long> missing = [];

            // Owed because the node's own log holds it, committed and deliverable.
            foreach (RaftLog log in held.Values)
            {
                if (log.Id <= view.CommitIndex && IsDelivered(log) && !state.ContainsKey(log.Id))
                    missing.Add(log.Id);
            }

            // Owed because another node applied it and this node committed that far, while this
            // node's log does not hold it as something that is never delivered.
            foreach (long id in union.Keys)
            {
                if (id > view.CommitIndex || state.ContainsKey(id))
                    continue;

                if (held.TryGetValue(id, out RaftLog? own) && !IsDelivered(own))
                    continue;

                if (!missing.Contains(id))
                    missing.Add(id);
            }

            if (missing.Count > 0)
            {
                missing.Sort();
                problems.Add(
                    $"{node.Endpoint} (commit {view.CommitIndex}, log from {(held.Count > 0 ? held.Keys.Min() : -1)}) " +
                    $"never applied [{string.Join(",", missing.Take(12))}{(missing.Count > 12 ? ",…" : string.Empty)}]");
            }
        }

        return problems;
    }

    /// <summary>The two filters the library applies before it hands an entry to the application.</summary>
    private static bool IsDelivered(RaftLog log) =>
        log.Type == RaftLogType.Committed
        && log.LogType != Kommander.System.RaftSystemConfig.LeadershipBarrierLogType;
}
