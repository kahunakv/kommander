using System.Security.Cryptography;
using Kommander.Data;

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
