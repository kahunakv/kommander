using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Diagnostics;
using Kommander.Tests.Simulation.WAL;

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

    /// <summary>Crash count last seen per node, so no crash is missed between two checks.</summary>
    private readonly Dictionary<string, int> crashCountByNode = [];

    /// <summary>Number of settled states checked so far.</summary>
    public int ChecksRun { get; private set; }

    /// <summary>
    /// Where the time these checks take is recorded, when somebody is measuring.
    ///
    /// <para>Optional on purpose. The checker is correct without a collector, and a run that only
    /// wants the rules should not have to build one. When it is set, the share of a run spent here
    /// is what says whether the search is paying for oracles rather than for exploration.</para>
    /// </summary>
    public SimulationMetricsCollector? Metrics { get; set; }

    /// <summary>
    /// Commit high-water marks actually dropped because their node crashed. Counting the marks
    /// rather than the crashes is deliberate: a reset that matched no mark would otherwise report
    /// success while changing nothing.
    /// </summary>
    public int CrashResets { get; private set; }

    /// <summary>Number of distinct committed indices the run has fingerprinted.</summary>
    public int CommittedIndicesSeen => recordedByCommittedIndex.Count;

    /// <summary>
    /// Checks every per-step invariant against the current settled state of
    /// <paramref name="partitionId"/>. Throws <see cref="InvariantViolationException"/> on the
    /// first rule that breaks.
    /// </summary>
    public async Task CheckAsync(SimulationCluster cluster, int partitionId, CancellationToken cancellationToken)
    {
        // The timer opens here, not at the first rule. Reading every node's view and every node's
        // store is most of what a check costs, and a measurement that started after the reads
        // reported four milliseconds for four hundred checks — a number that would have sent a
        // reader looking for the run's cost anywhere but here.
        using IDisposable? timer = Metrics?.TimeInvariantCheck();

        // First, before any view is read: a node whose own invariant broke may no longer answer a
        // view at all, and every rule below would then be judging a cluster minus that node.
        ClusterInvariantSet.CheckNoLibraryInvariantViolation(
            cluster.StepNumber, cluster.LibraryInvariantViolations);

        // Judged when the import began, so it is a fact about a past step and needs no view.
        ClusterInvariantSet.CheckNoUnnecessarySnapshot(
            cluster.StepNumber, cluster.UnnecessarySnapshotImports);

        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(partitionId, cancellationToken).ConfigureAwait(false);

        IReadOnlyDictionary<string, SimulatedWalPartitionSnapshot> stores =
            CollectStores(cluster, partitionId);

        IReadOnlyList<ClusterInvariantSet.NodeCommittedWindow> windows =
            CollectCommittedWindows(cluster, views, partitionId, stores);

        ForgetCrashedNodes(cluster);

        ClusterInvariantSet.CheckOneLeaderPerTerm(cluster.StepNumber, views);
        CheckStaleLeaderBounded(cluster, views);
        ClusterInvariantSet.CheckCommittedIdsMonotonic(cluster.StepNumber, views, highestCommitByNode);

        ClusterInvariantSet.CheckCommittedEntriesAgree(
            cluster.StepNumber,
            Flatten(windows),
            recordedByCommittedIndex);

        ClusterInvariantSet.CheckCommittedTermsNonDecreasing(cluster.StepNumber, windows);

        ClusterInvariantSet.CheckCommittedPrefixPresent(cluster.StepNumber, views, stores);
        ClusterInvariantSet.CheckCompactionFloorRespected(cluster.StepNumber, stores);

        // Runs after the agreement check on purpose: that check is what populates the recorded
        // history this one measures a leader against.
        ClusterInvariantSet.CheckLeaderCompleteness(
            cluster.StepNumber,
            views,
            windows,
            recordedByCommittedIndex);

        ClusterInvariantSet.CheckCurrentLeaderHoldsCommittedEntries(
            cluster.StepNumber,
            views,
            CollectLeaderLogs(cluster, views, partitionId, stores),
            recordedByCommittedIndex);

        ChecksRun++;
    }

    /// <summary>
    /// Simulated time at which each node was first seen leading in a term below another node's
    /// leadership, while it has kept doing so without a break.
    /// </summary>
    private readonly Dictionary<string, long> staleLeaderSince = [];

    /// <summary>
    /// Rule <c>stale-leader-bounded</c>: with check-quorum on, a running node does not keep
    /// reporting itself leader for more than two check-quorum windows after another node leads in a
    /// higher term.
    ///
    /// <para><b>Why this rule and not a lost write.</b> One leader per term is never broken by a
    /// stale leader, and the stale leader cannot commit, so no log check sees it. The harm is
    /// outside the log: a consumer that acts on the belief of leadership (Kahuna's actor-only
    /// mutations, <c>39bf62e2</c>) acts twice. Check-quorum exists to bound that belief, and the
    /// bound is time, so the rule measures time. The quiesced-leader defect fixed in
    /// <c>14b564a</c> kept the belief for the whole length of a network cut.</para>
    ///
    /// <para><b>Why two windows.</b> A correct leader steps down one window after its last
    /// majority contact, which comes before the cut, while the other side needs a failure verdict
    /// and an election timeout before it has a leader at all. The measured overlap is therefore
    /// close to zero. The second window absorbs tick granularity and the half-window probe of a
    /// quiesced leader, so the rule fires only on a leader that did not step down.</para>
    ///
    /// <para>Time counts only while the stale node is running: a paused or crashed process cannot
    /// step down, and its belief is not the defect.</para>
    /// </summary>
    private void CheckStaleLeaderBounded(SimulationCluster cluster, IReadOnlyList<RaftPartitionView> views)
    {
        long now = cluster.Clock.LogicalMilliseconds;
        HashSet<string> stale = [];

        foreach (RaftPartitionView view in views)
        {
            if (view.Role != RaftNodeState.Leader)
                continue;

            SimulationNode? node = cluster.Nodes.FirstOrDefault(candidate => candidate.Endpoint == view.Endpoint);

            if (node is null
                || node.LifecycleStatus != SimulationNodeLifecycleStatus.Running
                || !node.Manager.Configuration.EnableCheckQuorum)
                continue;

            RaftPartitionView? newer = views.FirstOrDefault(other =>
                other.Role == RaftNodeState.Leader && other.Term > view.Term);

            if (newer is null)
                continue;

            stale.Add(view.Endpoint);

            if (!staleLeaderSince.TryGetValue(view.Endpoint, out long since))
            {
                staleLeaderSince[view.Endpoint] = now;
                continue;
            }

            long bound = 2 * (long)node.Manager.Configuration.CheckQuorumWindow.TotalMilliseconds;

            if (now - since > bound)
            {
                throw new InvariantViolationException(
                    "stale-leader-bounded",
                    $"stale-leader-bounded: {view.Endpoint} has reported itself leader of term {view.Term} for " +
                    $"{now - since} ms of simulated time while {newer.Endpoint} leads term {newer.Term}. " +
                    $"Check-quorum is on with a {node.Manager.Configuration.CheckQuorumWindow.TotalMilliseconds} ms " +
                    $"window, so it must step down within {bound} ms. Quiesced={view.Quiesced}.",
                    cluster.StepNumber,
                    selectedEvent: null,
                    lastValidSnapshot: null,
                    failingSnapshot: null);
            }
        }

        foreach (string endpoint in staleLeaderSince.Keys.ToList())
        {
            if (!stale.Contains(endpoint))
                staleLeaderSince.Remove(endpoint);
        }
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
        ClusterInvariantSet.CheckNoUnnecessarySnapshot(
            cluster.StepNumber, cluster.UnnecessarySnapshotImports);

        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(partitionId, cancellationToken).ConfigureAwait(false);

        ClusterInvariantSet.CheckQuiescentConvergence(
            cluster.StepNumber,
            views,
            CollectCommittedWindows(cluster, views, partitionId, CollectStores(cluster, partitionId)));
    }

    /// <summary>
    /// Drops the commit high-water mark of any node that has crashed since the last check.
    ///
    /// <para><b>Why the invariant needs this.</b> "A node's committed index never decreases" is
    /// true of a running node and false of one that crashed: a crash takes back everything inside
    /// the fsync window, so the node comes up having genuinely committed less than it once
    /// reported. Raft is not violated — the entry survives on the majority that fsynced it — but
    /// the per-node rule is, and without this the first crash scenario would report a defect that
    /// is really the durability model working.</para>
    ///
    /// <para><b>Why only a crash.</b> A paused node keeps its memory and must stay monotonic; a
    /// stopped one is being torn down. Resetting on either would blunt the rule for no reason. The
    /// mark is dropped once per crash, not once per step, so a node that keeps running after its
    /// restart is held to the rule again from its new baseline. <see cref="CrashResets"/> counts
    /// the drops, so a scenario can prove the reset engaged rather than assume it.</para>
    /// </summary>
    private void ForgetCrashedNodes(SimulationCluster cluster)
    {
        foreach (SimulationNode node in cluster.Nodes)
        {
            int seen = crashCountByNode.GetValueOrDefault(node.Endpoint);
            if (node.CrashCount <= seen)
                continue;

            // The count, not the current status. A node can crash and restart between two checks,
            // and a status test would then see it running and hold it to a mark it no longer has.
            crashCountByNode[node.Endpoint] = node.CrashCount;

            // Every partition, because the crash took the whole process. The marks are keyed by
            // endpoint and partition together, so removing the bare endpoint removes nothing —
            // which is how the first version of this reset counted itself as done while leaving
            // every mark in place. CrashResets therefore counts marks actually dropped.
            string prefix = node.Endpoint + "/p";

            List<string> keys = highestCommitByNode.Keys
                .Where(key => key.StartsWith(prefix, StringComparison.Ordinal))
                .ToList();

            foreach (string key in keys)
            {
                highestCommitByNode.Remove(key);
                CrashResets++;
            }
        }
    }

    /// <summary>
    /// Reads one partition's store state from every node that has a simulated one.
    ///
    /// <para>A node running a plain in-memory log contributes nothing and is simply absent from the
    /// map. That is deliberate: the store rules skip what they cannot read rather than treat a
    /// missing reading as an empty log, which would report a hole on every node the harness cannot
    /// see into.</para>
    /// </summary>
    private static IReadOnlyDictionary<string, SimulatedWalPartitionSnapshot> CollectStores(
        SimulationCluster cluster,
        int partitionId)
    {
        Dictionary<string, SimulatedWalPartitionSnapshot> stores = new();

        foreach ((string endpoint, SimulatedWalSnapshot snapshot) in cluster.GetWalSnapshots())
        {
            SimulatedWalPartitionSnapshot? partition = snapshot.Partition(partitionId);

            if (partition is not null)
                stores[endpoint] = partition;
        }

        return stores;
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
        int partitionId,
        IReadOnlyDictionary<string, SimulatedWalPartitionSnapshot> stores)
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

            // The window must start above whatever this node compacted away, not at whatever the
            // read asked for. Compaction removes committed entries on purpose, and a window that
            // claims to cover an index the node deliberately discarded turns every compacted prefix
            // into a reported hole — which is exactly what the leader-completeness rule did the
            // first time a generated run ever compacted. The rule was never wrong before; it was
            // only sound because nothing compacted. An installed snapshot covers its prefix the same
            // way, so the window starts above that too.
            long compactedThrough = stores.TryGetValue(view.Endpoint, out SimulatedWalPartitionSnapshot? store)
                ? store.CoveredThrough
                : -1;

            long from = Math.Max(Math.Max(1, view.CommitIndex - CommittedWindow + 1), compactedThrough + 1);

            if (from > view.CommitIndex)
                continue;
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
    /// Reads the log of each node that reports itself leader, over the tail the recorded commit
    /// history reaches, with every entry type.
    ///
    /// <para>The read starts above whatever the leader compacted or holds only as a snapshot, for
    /// the same reason as the committed window: a compacted entry is not a hole. It ends
    /// <see cref="CommittedWindow"/> entries past the highest committed index recorded, so an entry
    /// the leader lacks at the top of the committed history is still inside the read.</para>
    /// </summary>
    private List<ClusterInvariantSet.NodeLogWindow> CollectLeaderLogs(
        SimulationCluster cluster,
        IReadOnlyList<RaftPartitionView> views,
        int partitionId,
        IReadOnlyDictionary<string, SimulatedWalPartitionSnapshot> stores)
    {
        List<ClusterInvariantSet.NodeLogWindow> logs = [];

        if (recordedByCommittedIndex.Count == 0)
            return logs;

        long highestRecorded = recordedByCommittedIndex.Keys.Max();

        foreach (RaftPartitionView view in views)
        {
            if (view.Role != RaftNodeState.Leader)
                continue;

            SimulationNode? node = cluster.Nodes.FirstOrDefault(
                candidate => string.Equals(candidate.Endpoint, view.Endpoint, StringComparison.Ordinal));

            if (node is null)
                continue;

            long compactedThrough = stores.TryGetValue(view.Endpoint, out SimulatedWalPartitionSnapshot? store)
                ? store.CoveredThrough
                : -1;

            long from = Math.Max(Math.Max(1, highestRecorded - CommittedWindow + 1), compactedThrough + 1);
            long to = highestRecorded;

            if (from > to)
                continue;

            Dictionary<long, CommittedEntryFingerprint> byIndex = [];

            foreach (RaftLog log in node.Wal.ReadLogsRange(partitionId, from, (int)(to - from + 1)))
            {
                if (log.Id <= to)
                    byIndex[log.Id] = CommittedEntryFingerprint.From(view.Endpoint, log);
            }

            logs.Add(new ClusterInvariantSet.NodeLogWindow(view.Endpoint, from, to, byIndex));
        }

        return logs;
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
