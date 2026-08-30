using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;

namespace Kommander.Tests.Simulation.Invariants;

/// <summary>
/// Runs the per-step invariant set against a live simulated cluster and carries the history that
/// the history-dependent checks need.
///
/// <para>Two of the three checks are not functions of a single state. "Committed ids never
/// decrease" and "committed entries agree" both compare the current reading against what the run
/// has already seen, so the runner owns that memory for the whole run rather than rebuilding it
/// per step.</para>
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
    /// Checks every invariant against the current settled state of <paramref name="partitionId"/>.
    /// Throws <see cref="InvariantViolationException"/> on the first rule that breaks.
    /// </summary>
    public async Task CheckAsync(SimulationCluster cluster, int partitionId, CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(partitionId, cancellationToken).ConfigureAwait(false);

        ClusterInvariantSet.CheckOneLeaderPerTerm(cluster.StepNumber, views);
        ClusterInvariantSet.CheckCommittedIdsMonotonic(cluster.StepNumber, views, highestCommitByNode);
        ClusterInvariantSet.CheckCommittedEntriesAgree(
            cluster.StepNumber,
            CollectCommittedFingerprints(cluster, views, partitionId),
            recordedByCommittedIndex);

        ChecksRun++;
    }

    /// <summary>
    /// Reads the tail of each node's committed prefix and turns it into fingerprints.
    ///
    /// <para>Only entries at or below the node's own commit index are read. An entry above it is
    /// proposed, not committed, and two nodes are entitled to disagree about a proposed tail —
    /// treating one as evidence would report a violation where the protocol is behaving
    /// correctly.</para>
    /// </summary>
    private static List<CommittedEntryFingerprint> CollectCommittedFingerprints(
        SimulationCluster cluster,
        IReadOnlyList<RaftPartitionView> views,
        int partitionId)
    {
        List<CommittedEntryFingerprint> fingerprints = [];

        foreach (RaftPartitionView view in views)
        {
            if (view.CommitIndex <= 0)
                continue;

            SimulationNode? node = cluster.Nodes.FirstOrDefault(
                candidate => string.Equals(candidate.Endpoint, view.Endpoint, StringComparison.Ordinal));

            if (node is null)
                continue;

            long from = Math.Max(1, view.CommitIndex - CommittedWindow + 1);

            foreach (RaftLog log in node.Wal.ReadLogsRange(partitionId, from, CommittedWindow))
            {
                if (log.Id > view.CommitIndex || !IsCommitted(log.Type))
                    continue;

                fingerprints.Add(CommittedEntryFingerprint.From(view.Endpoint, log));
            }
        }

        return fingerprints;
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
