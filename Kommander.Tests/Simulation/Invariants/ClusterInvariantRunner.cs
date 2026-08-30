using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;

namespace Kommander.Tests.Simulation.Invariants;

/// <summary>
/// Runs the invariant set against a live simulated cluster and carries the history that the
/// history-dependent checks need.
///
/// <para>Most of these checks are not functions of a single state. "Committed ids never decrease",
/// "committed entries agree", "committed terms never decrease", and leader completeness all
/// compare the current reading against what the run has already seen, so the runner owns that
/// memory for the whole run rather than rebuilding it per step.</para>
///
/// <para>One runner belongs to one run. Reusing it across runs would carry one run's history into
/// another and report a violation that neither run committed.</para>
/// </summary>
public sealed class ClusterInvariantRunner
{
    /// <summary>
    /// Committed entries read from each node per step, counted back from that node's commit
    /// index. The whole committed prefix is not re-read every step: an entry that already agreed
    /// is recorded and can only be contradicted by a node that rewrites it, and a rewrite lands
    /// near the frontier. A larger window costs time on every step of every run and buys only
    /// the detection of a rewrite deep in a settled prefix, which no observed defect produced.
    /// </summary>
    private const int CommittedWindow = 64;

    private readonly Dictionary<string, long> highestCommitByNode = [];
    private readonly Dictionary<long, CommittedEntryFingerprint> recordedByCommittedIndex = [];

    /// <summary>Number of settled states checked so far.</summary>
    public int ChecksRun { get; private set; }

    /// <summary>Number of distinct committed indices the run has fingerprinted.</summary>
    public int CommittedIndicesSeen => recordedByCommittedIndex.Count;

    /// <summary>
    /// Checks every per-step invariant against the current settled state of
    /// <paramref name="partitionId"/>. Throws <see cref="InvariantViolationException"/> on the
    /// first rule that breaks.
    /// </summary>
    public async Task CheckAsync(SimulationCluster cluster, int partitionId, CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(partitionId, cancellationToken).ConfigureAwait(false);

        IReadOnlyList<ClusterInvariantSet.NodeCommittedWindow> windows =
            CollectCommittedWindows(cluster, views, partitionId);

        ClusterInvariantSet.CheckOneLeaderPerTerm(cluster.StepNumber, views);
        ClusterInvariantSet.CheckCommittedIdsMonotonic(cluster.StepNumber, views, highestCommitByNode);

        ClusterInvariantSet.CheckCommittedEntriesAgree(
            cluster.StepNumber,
            Flatten(windows),
            recordedByCommittedIndex);

        ClusterInvariantSet.CheckCommittedTermsNonDecreasing(cluster.StepNumber, recordedByCommittedIndex);

        // Runs after the agreement check on purpose: that check is what populates the recorded
        // history this one measures a leader against.
        ClusterInvariantSet.CheckLeaderCompleteness(
            cluster.StepNumber,
            views,
            windows,
            recordedByCommittedIndex);

        ChecksRun++;
    }

    /// <summary>
    /// Checks that the cluster converged. Call this at the end of a run, after faults have stopped
    /// and enough simulated time has passed for every timeout to expire.
    ///
    /// <para>Kept separate from <see cref="CheckAsync"/> because convergence is a promise about
    /// where a run ends, not about any single moment. Asserting it mid-run would report a
    /// disagreement the protocol is entitled to have.</para>
    /// </summary>
    public async Task CheckConvergedAsync(
        SimulationCluster cluster,
        int partitionId,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(partitionId, cancellationToken).ConfigureAwait(false);

        ClusterInvariantSet.CheckQuiescentConvergence(
            cluster.StepNumber,
            views,
            CollectCommittedWindows(cluster, views, partitionId));
    }

    private static List<CommittedEntryFingerprint> Flatten(
        IReadOnlyList<ClusterInvariantSet.NodeCommittedWindow> windows)
    {
        List<CommittedEntryFingerprint> fingerprints = [];

        foreach (ClusterInvariantSet.NodeCommittedWindow window in windows)
            fingerprints.AddRange(window.ByIndex.Values);

        return fingerprints;
    }

    /// <summary>
    /// Reads the tail of each node's committed prefix and turns it into a window.
    ///
    /// <para>Only entries at or below the node's own commit index are read. An entry above it is
    /// proposed, not committed, and two nodes are entitled to disagree about a proposed tail —
    /// treating one as evidence would report a violation where the protocol is behaving
    /// correctly.</para>
    ///
    /// <para>The range each read covered is carried alongside the entries, because a missing index
    /// means two different things. Inside the range it is a hole. Below it, the node may simply
    /// have compacted the entry away, which is correct behavior.</para>
    /// </summary>
    private static List<ClusterInvariantSet.NodeCommittedWindow> CollectCommittedWindows(
        SimulationCluster cluster,
        IReadOnlyList<RaftPartitionView> views,
        int partitionId)
    {
        List<ClusterInvariantSet.NodeCommittedWindow> windows = [];

        foreach (RaftPartitionView view in views)
        {
            if (view.CommitIndex <= 0)
                continue;

            SimulationNode? node = cluster.Nodes.FirstOrDefault(
                candidate => string.Equals(candidate.Endpoint, view.Endpoint, StringComparison.Ordinal));

            if (node is null)
                continue;

            long from = Math.Max(1, view.CommitIndex - CommittedWindow + 1);
            Dictionary<long, CommittedEntryFingerprint> byIndex = [];

            foreach (RaftLog log in node.Wal.ReadLogsRange(partitionId, from, CommittedWindow))
            {
                if (log.Id > view.CommitIndex || !IsCommitted(log.Type))
                    continue;

                byIndex[log.Id] = CommittedEntryFingerprint.From(view.Endpoint, log);
            }

            if (byIndex.Count == 0)
                continue;

            windows.Add(new ClusterInvariantSet.NodeCommittedWindow(
                view.Endpoint,
                from,
                view.CommitIndex,
                byIndex));
        }

        return windows;
    }

    /// <summary>
    /// Both committed forms count. A <see cref="RaftLogType.CommittedCheckpoint"/> occupies a log
    /// id like any other committed entry, so two nodes disagreeing about one is the same
    /// divergence as two nodes disagreeing about a client entry. Proposed and rolled-back forms
    /// are excluded: nodes are entitled to differ there.
    /// </summary>
    private static bool IsCommitted(RaftLogType type) =>
        type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint;
}
