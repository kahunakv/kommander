using System.Security.Cryptography;
using Kommander.Data;
using Kommander.Tests.Simulation.WAL;

namespace Kommander.Tests.Simulation.Invariants;

/// <summary>
/// One committed entry as the invariant checker compares it: enough to detect divergence,
/// small enough to keep for the whole run.
/// </summary>
/// <param name="LogId">Index of the entry within its partition.</param>
/// <param name="Term">Term in which the entry was created.</param>
/// <param name="LogType">Application-declared type string, or the empty string.</param>
/// <param name="PayloadHash">Hash of the payload bytes. Comparing hashes rather than payloads
/// keeps the recorded history small while still catching two different values at one index.</param>
/// <param name="Endpoint">The node this reading came from, so a failure can name both sides.</param>
public sealed record CommittedEntryFingerprint(
    long LogId,
    long Term,
    string LogType,
    string PayloadHash,
    string Endpoint)
{
    /// <summary>Builds a fingerprint from a write-ahead-log entry read on one node.</summary>
    public static CommittedEntryFingerprint From(string endpoint, RaftLog log) =>
        new(log.Id, log.Term, log.LogType ?? string.Empty, HashPayload(log.LogData), endpoint);

    private static string HashPayload(byte[]? payload)
    {
        if (payload is null || payload.Length == 0)
            return "empty";

        return Convert.ToHexString(SHA256.HashData(payload))[..16].ToLowerInvariant();
    }

    /// <summary>True when two readings describe the same entry. The endpoint is not compared.</summary>
    public bool DescribesSameEntryAs(CommittedEntryFingerprint other) =>
        LogId == other.LogId
        && Term == other.Term
        && string.Equals(LogType, other.LogType, StringComparison.Ordinal)
        && string.Equals(PayloadHash, other.PayloadHash, StringComparison.Ordinal);
}

/// <summary>
/// Consensus invariants that the harness checks after every step of a simulated run.
///
/// <para><b>Why after every step.</b> A scripted test asserts at the points its author thought
/// to assert. A defect that appears for three steps and repairs itself passes such a test. These
/// checks run on every settled state, so a violation is caught at the step that caused it and
/// the failure report names that step.</para>
///
/// <para><b>Input.</b> The first two checks take the per-node partition views, read on each
/// node's executor thread, so a view is a settled read and never a torn one. The third reads the
/// committed entries themselves, because a frontier alone cannot show that two nodes hold
/// different values at one index.</para>
///
/// <para><b>What a violation means.</b> Each check throws
/// <see cref="InvariantViolationException"/> with the invariant name, so a failure report can
/// say which rule broke rather than only that an assertion failed.</para>
/// </summary>
public static class ClusterInvariantSet
{
    /// <summary>
    /// At most one leader exists per term per partition.
    ///
    /// <para>Two leaders in one term means a node granted a second vote for a term it had
    /// already voted in, or a candidate counted a vote it did not receive. Both break the
    /// election-safety property that every later guarantee rests on.</para>
    /// </summary>
    public const string OneLeaderPerTerm = "one-leader-per-term";

    /// <summary>
    /// Committed log ids never decrease on a node.
    ///
    /// <para>The commit index is the promise a client already received. A node that lowers it has
    /// un-committed acknowledged data, which is a lost write however the value is later
    /// repaired.</para>
    /// </summary>
    public const string CommittedIdsMonotonic = "committed-ids-monotonic";

    /// <summary>
    /// Two nodes that both hold a committed entry at one index agree on its term, its type, and
    /// its payload.
    ///
    /// <para>This is the log-matching property. Disagreement at an index means two different
    /// values committed at the same position, so the two state machines have diverged
    /// permanently and no later repair can reconcile them.</para>
    /// </summary>
    public const string CommittedEntriesAgree = "committed-entries-agree";

    /// <summary>
    /// Checks <see cref="OneLeaderPerTerm"/> against one partition's views.
    /// </summary>
    public static void CheckOneLeaderPerTerm(int stepNumber, IReadOnlyList<RaftPartitionView> views)
    {
        Dictionary<long, string> leaderByTerm = [];

        foreach (RaftPartitionView view in views)
        {
            if (view.Role != RaftNodeState.Leader)
                continue;

            if (leaderByTerm.TryGetValue(view.Term, out string? existing)
                && !string.Equals(existing, view.Endpoint, StringComparison.Ordinal))
            {
                throw Violation(
                    OneLeaderPerTerm,
                    stepNumber,
                    $"Partition {view.Partition} has two leaders in term {view.Term}: " +
                    $"'{existing}' and '{view.Endpoint}'.");
            }

            leaderByTerm[view.Term] = view.Endpoint;
        }
    }

    /// <summary>
    /// Checks <see cref="CommittedIdsMonotonic"/> by comparing each node's commit index against
    /// the highest value that node has ever reported.
    /// <paramref name="highestCommitByNode"/> is updated in place, so the caller keeps one
    /// dictionary for the whole run.
    /// </summary>
    public static void CheckCommittedIdsMonotonic(
        int stepNumber,
        IReadOnlyList<RaftPartitionView> views,
        Dictionary<string, long> highestCommitByNode)
    {
        foreach (RaftPartitionView view in views)
        {
            string key = $"{view.Endpoint}/p{view.Partition}";

            if (highestCommitByNode.TryGetValue(key, out long highest) && view.CommitIndex < highest)
            {
                throw Violation(
                    CommittedIdsMonotonic,
                    stepNumber,
                    $"Node '{view.Endpoint}' partition {view.Partition} reported commit index " +
                    $"{view.CommitIndex} after previously reporting {highest}.");
            }

            highestCommitByNode[key] = Math.Max(highest, view.CommitIndex);
        }
    }

    /// <summary>
    /// Committed terms never decrease as the log index rises.
    ///
    /// <para>Raft assigns each entry the term of the leader that created it, and terms only ever
    /// rise. A committed log whose terms dip has an entry from an older term sitting above one
    /// from a newer term, which means a stale leader's entry survived into the committed prefix.
    /// </para>
    /// </summary>
    public const string CommittedTermsNonDecreasing = "committed-terms-non-decreasing";

    /// <summary>
    /// A leader holds every entry the cluster has already committed.
    ///
    /// <para>This is Raft's leader-completeness property, and it is the one the election
    /// restriction exists to protect. A leader missing a committed entry cannot replicate it, so
    /// the entry is lost for the rest of that term however the leader later behaves. It is also
    /// the shape of the hole-in-the-log defect: a candidate whose log has a gap below its
    /// high-water mark wins the term and then serves an incomplete projection.</para>
    /// </summary>
    public const string LeaderCompleteness = "leader-completeness";

    /// <summary>
    /// Once faults stop and time advances past every timeout, live nodes hold identical committed
    /// prefixes.
    ///
    /// <para>A run-level check, not a per-step one: convergence is a promise about where the
    /// cluster ends up, not about any single moment. Its absence is the signature of a wedge —
    /// every replica alive, no fault left, and the frontiers still disagree.</para>
    /// </summary>
    public const string QuiescentConvergence = "quiescent-convergence";

    /// <summary>
    /// A node's durable log holds every id at or below its own committed frontier, except those it
    /// compacted.
    ///
    /// <para>A node that says it committed up to N is asserting that N and everything under it is
    /// on its disk. Losing one of those entries while still advertising N is how a replica serves a
    /// prefix it does not have, and it is the durable half of the applied-hole shape that
    /// <c>9ba52729</c> produced.</para>
    ///
    /// <para><b>What it is not.</b> This is not the per-step form of DST FINDING 1, and it is worth
    /// being exact about why. The stranded node in that defect held entry 2 with entry 1 absent and
    /// its frontier honestly at 0 — it never claimed the missing entry, so no per-node rule was
    /// broken. Only comparing nodes at rest exposed it. A rule that appears to cover a known defect
    /// but does not is worse than no rule, because it stops anyone looking for the one that would.</para>
    ///
    /// <para><b>Physically absent, not merely uncommitted.</b> The rule reads ids with no row at
    /// all. An id present but still marked proposed is legal: the single-fsync fast path can lose a
    /// commit marker without losing the entry, and calling that a hole would fail every crash
    /// scenario for modelling exactly what it is meant to model.</para>
    ///
    /// <para><b>Two ways to be absent.</b> An id can be missing inside the retained range, or the
    /// whole head can be gone. The second needs the store's compaction record to judge: a log whose
    /// lowest id is 5 either compacted 1 through 4 or never received them, and nothing observable
    /// afterwards separates those. Compaction is legal, never receiving them and claiming to have
    /// committed them is not.</para>
    /// </summary>
    public const string CommittedPrefixPresent = "committed-prefix-present";

    /// <summary>
    /// Compaction is never asked to remove an entry above the certified checkpoint floor.
    ///
    /// <para>The floor is the promise that everything above it is still readable. A compaction that
    /// passed it would leave a leader unable to serve a follower from a position that follower is
    /// entitled to ask for, and the failure surfaces far away — as a backfill that refuses, or a
    /// snapshot rescue for a peer that was nearly in sync.</para>
    ///
    /// <para><b>The request, not the result.</b> An earlier version of this rule compared the lowest
    /// retained id against the floor, and it was unsound: a log that starts above its first id may
    /// have been compacted, or may simply never have received its head. A follower whose write was
    /// refused produces the second, and the rule fired on it every time. Only the request separates
    /// the two, so the store records each request against the checkpoint certified at that instant
    /// and this rule reads the count.</para>
    /// </summary>
    public const string CompactionFloorRespected = "compaction-floor-respected";

    /// <summary>
    /// Checks <see cref="CommittedPrefixPresent"/> for every node whose store the harness can read.
    /// </summary>
    /// <param name="storeByEndpoint">
    /// One partition's store state per endpoint. Nodes absent from the map are skipped rather than
    /// treated as empty: a scenario may have asked for a plain in-memory log, and a missing reading
    /// is not evidence of anything.
    /// </param>
    public static void CheckCommittedPrefixPresent(
        int stepNumber,
        IReadOnlyList<RaftPartitionView> views,
        IReadOnlyDictionary<string, SimulatedWalPartitionSnapshot> storeByEndpoint)
    {
        foreach (RaftPartitionView view in views)
        {
            if (!storeByEndpoint.TryGetValue(view.Endpoint, out SimulatedWalPartitionSnapshot? store))
                continue;

            foreach (long missing in store.MissingIds)
            {
                if (missing > view.CommitIndex)
                    continue;

                throw Violation(
                    CommittedPrefixPresent,
                    stepNumber,
                    $"Node '{view.Endpoint}' partition {view.Partition} is committed to " +
                    $"{view.CommitIndex} but holds no entry at {missing}. Retained range is " +
                    $"[{store.FirstLogId}, {store.MaxLogId}].");
            }

            // The head. The lowest id the node should still hold is one above whatever it compacted;
            // anything between that and its first retained id was never received, and the node may
            // not claim to have committed it.
            long expectedFirst = store.CompactedThrough + 1;

            if (store.EntryCount > 0
                && store.FirstLogId > expectedFirst
                && view.CommitIndex >= expectedFirst)
            {
                throw Violation(
                    CommittedPrefixPresent,
                    stepNumber,
                    $"Node '{view.Endpoint}' partition {view.Partition} is committed to " +
                    $"{view.CommitIndex} but its log starts at {store.FirstLogId} and it compacted " +
                    $"only through {store.CompactedThrough}, so ids {expectedFirst} to " +
                    $"{store.FirstLogId - 1} were never received.");
            }
        }
    }

    /// <summary>
    /// Checks <see cref="CompactionFloorRespected"/> for every node whose store the harness can read.
    /// </summary>
    public static void CheckCompactionFloorRespected(
        int stepNumber,
        IReadOnlyDictionary<string, SimulatedWalPartitionSnapshot> storeByEndpoint)
    {
        foreach ((string endpoint, SimulatedWalPartitionSnapshot store) in storeByEndpoint)
        {
            if (store.CompactionsAboveFloor == 0)
                continue;

            throw Violation(
                CompactionFloorRespected,
                stepNumber,
                $"Node '{endpoint}' partition {store.PartitionId} was asked " +
                $"{store.CompactionsAboveFloor} time(s) to compact below " +
                $"{store.WorstCompactionRequest} while its certified checkpoint was " +
                $"{store.WorstCompactionCertifiedFloor}.");
        }
    }

    /// <summary>
    /// One node's readable committed window: the entries it returned, plus the index range that
    /// read actually covered. The range matters. An index missing from the map is a hole only if
    /// the read covered it; below the range the node may simply have compacted the entry away.
    /// </summary>
    /// <param name="Endpoint">The node.</param>
    /// <param name="RangeStart">Lowest index the read covered.</param>
    /// <param name="RangeEnd">Highest index the read covered.</param>
    /// <param name="ByIndex">Committed entries the node actually holds, keyed by index.</param>
    public sealed record NodeCommittedWindow(
        string Endpoint,
        long RangeStart,
        long RangeEnd,
        IReadOnlyDictionary<long, CommittedEntryFingerprint> ByIndex);

    /// <summary>
    /// Checks <see cref="CommittedTermsNonDecreasing"/> over everything the run has recorded.
    /// </summary>
    public static void CheckCommittedTermsNonDecreasing(
        int stepNumber,
        IReadOnlyDictionary<long, CommittedEntryFingerprint> recordedByIndex)
    {
        CommittedEntryFingerprint? previous = null;

        foreach (long index in recordedByIndex.Keys.OrderBy(key => key))
        {
            CommittedEntryFingerprint current = recordedByIndex[index];

            if (previous is not null && current.Term < previous.Term)
            {
                throw Violation(
                    CommittedTermsNonDecreasing,
                    stepNumber,
                    $"Committed index {current.LogId} carries term {current.Term}, below index " +
                    $"{previous.LogId} at term {previous.Term}. An older term sits above a newer one.");
            }

            previous = current;
        }
    }

    /// <summary>
    /// Checks <see cref="LeaderCompleteness"/> for every node currently claiming leadership.
    ///
    /// <para>Only indices inside the leader's own read range are judged. Below that range the
    /// leader may have compacted the entry, and calling that a violation would report a defect
    /// where the protocol did exactly what it should.</para>
    /// </summary>
    public static void CheckLeaderCompleteness(
        int stepNumber,
        IReadOnlyList<RaftPartitionView> views,
        IReadOnlyList<NodeCommittedWindow> windows,
        IReadOnlyDictionary<long, CommittedEntryFingerprint> recordedByIndex)
    {
        foreach (RaftPartitionView view in views)
        {
            if (view.Role != RaftNodeState.Leader)
                continue;

            NodeCommittedWindow? window = windows.FirstOrDefault(
                candidate => string.Equals(candidate.Endpoint, view.Endpoint, StringComparison.Ordinal));

            if (window is null || window.ByIndex.Count == 0)
                continue;

            foreach (KeyValuePair<long, CommittedEntryFingerprint> recorded in recordedByIndex)
            {
                if (recorded.Key < window.RangeStart || recorded.Key > window.RangeEnd)
                    continue;

                if (!window.ByIndex.TryGetValue(recorded.Key, out CommittedEntryFingerprint? held))
                {
                    throw Violation(
                        LeaderCompleteness,
                        stepNumber,
                        $"Leader '{view.Endpoint}' (term {view.Term}) has no entry at committed index " +
                        $"{recorded.Key}, which '{recorded.Value.Endpoint}' committed at term " +
                        $"{recorded.Value.Term}. The read covered [{window.RangeStart}, " +
                        $"{window.RangeEnd}], so this is a hole rather than a compacted prefix.");
                }

                if (!held.DescribesSameEntryAs(recorded.Value))
                {
                    throw Violation(
                        LeaderCompleteness,
                        stepNumber,
                        $"Leader '{view.Endpoint}' holds a different entry at committed index " +
                        $"{recorded.Key}: term {held.Term} payload {held.PayloadHash}, against " +
                        $"term {recorded.Value.Term} payload {recorded.Value.PayloadHash} from " +
                        $"'{recorded.Value.Endpoint}'.");
                }
            }
        }
    }

    /// <summary>
    /// Checks <see cref="QuiescentConvergence"/> across the nodes that are still running.
    ///
    /// <para>Call this at the end of a run, after faults have stopped and enough simulated time
    /// has passed for every timeout to expire. Calling it mid-run reports a disagreement that the
    /// protocol is entitled to have.</para>
    /// </summary>
    public static void CheckQuiescentConvergence(
        int stepNumber,
        IReadOnlyList<RaftPartitionView> views,
        IReadOnlyList<NodeCommittedWindow> windows)
    {
        if (views.Count < 2)
            return;

        RaftPartitionView reference = views[0];

        foreach (RaftPartitionView view in views.Skip(1))
        {
            if (view.CommitIndex != reference.CommitIndex)
            {
                throw Violation(
                    QuiescentConvergence,
                    stepNumber,
                    $"After quiescence '{reference.Endpoint}' is committed to {reference.CommitIndex} " +
                    $"and '{view.Endpoint}' to {view.CommitIndex}. The cluster did not converge.");
            }
        }

        // Frontiers can agree while the entries behind them do not. Compare the entries too,
        // over the index range every node's read actually covered.
        if (windows.Count < 2)
            return;

        long start = windows.Max(window => window.RangeStart);
        long end = windows.Min(window => window.RangeEnd);

        for (long index = start; index <= end; index++)
        {
            CommittedEntryFingerprint? first = null;

            foreach (NodeCommittedWindow window in windows)
            {
                if (!window.ByIndex.TryGetValue(index, out CommittedEntryFingerprint? held))
                    continue;

                if (first is null)
                {
                    first = held;
                    continue;
                }

                if (!first.DescribesSameEntryAs(held))
                {
                    throw Violation(
                        QuiescentConvergence,
                        stepNumber,
                        $"After quiescence index {index} differs: '{first.Endpoint}' holds term " +
                        $"{first.Term} payload {first.PayloadHash}, '{held.Endpoint}' holds term " +
                        $"{held.Term} payload {held.PayloadHash}.");
                }
            }
        }
    }

    /// <summary>
    /// Checks <see cref="CommittedEntriesAgree"/> over the committed entries themselves.
    ///
    /// <para>Each reading is compared against the first reading recorded at that index, so the
    /// check covers history and not only the current step: an entry that was committed twenty
    /// steps ago and has since been overwritten on one node still fails here.
    /// <paramref name="recordedByIndex"/> is updated in place and belongs to one run.</para>
    /// </summary>
    public static void CheckCommittedEntriesAgree(
        int stepNumber,
        IReadOnlyList<CommittedEntryFingerprint> fingerprints,
        Dictionary<long, CommittedEntryFingerprint> recordedByIndex)
    {
        foreach (CommittedEntryFingerprint fingerprint in fingerprints)
        {
            if (recordedByIndex.TryGetValue(fingerprint.LogId, out CommittedEntryFingerprint? recorded))
            {
                if (!recorded.DescribesSameEntryAs(fingerprint))
                {
                    throw Violation(
                        CommittedEntriesAgree,
                        stepNumber,
                        $"Committed index {fingerprint.LogId} disagrees across nodes. " +
                        $"'{recorded.Endpoint}' holds term {recorded.Term} type " +
                        $"'{recorded.LogType}' payload {recorded.PayloadHash}; " +
                        $"'{fingerprint.Endpoint}' holds term {fingerprint.Term} type " +
                        $"'{fingerprint.LogType}' payload {fingerprint.PayloadHash}.");
                }

                continue;
            }

            recordedByIndex[fingerprint.LogId] = fingerprint;
        }
    }

    private static InvariantViolationException Violation(string name, int stepNumber, string message) =>
        new(name, message, stepNumber, selectedEvent: null, lastValidSnapshot: null, failingSnapshot: null);
}
