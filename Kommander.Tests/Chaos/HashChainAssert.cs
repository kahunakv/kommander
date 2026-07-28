
using System.Text;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Static oracle over a set of <see cref="HashChainStateMachine"/> observers. Compares the recorded
/// applied histories across nodes to detect state-machine divergence, non-convergence, and duplicate
/// apply. Failures throw with a report that localises the <b>first</b> offending index by walking the
/// stored prefix hashes (the current/last hash alone cannot say <em>where</em> two histories diverged).
/// </summary>
public static class HashChainAssert
{
    /// <summary>
    /// Fails if any two nodes disagree on the prefix digest at a shared index, or if one node has a hole
    /// (a missing index) below the greatest index they share. Passing means every pair agrees on every
    /// index they both applied, with no gaps beneath their common frontier.
    /// </summary>
    public static void NoDivergence(IEnumerable<HashChainStateMachine> nodes, int partitionId, long? seed = null)
    {
        List<HashChainSnapshot> snaps = Snapshots(nodes, partitionId);

        for (int i = 0; i < snaps.Count; i++)
        for (int j = i + 1; j < snaps.Count; j++)
        {
            HashChainSnapshot a = snaps[i];
            HashChainSnapshot b = snaps[j];

            List<long> common = a.PrefixHashByIndex.Keys
                .Where(b.PrefixHashByIndex.ContainsKey)
                .OrderBy(x => x)
                .ToList();

            if (common.Count == 0)
                continue;

            long maxCommon = common[^1];

            // Hole: an index at/below the shared frontier that only one node applied.
            long? hole = a.PrefixHashByIndex.Keys
                .Union(b.PrefixHashByIndex.Keys)
                .Where(k => k <= maxCommon
                            && !(a.PrefixHashByIndex.ContainsKey(k) && b.PrefixHashByIndex.ContainsKey(k)))
                .OrderBy(k => k)
                .Select(k => (long?)k)
                .FirstOrDefault();

            if (hole is long holeIndex)
                Fail(BuildReport(a, b, holeIndex, "hole (index present on only one node) below the shared frontier", seed));

            // First index where the prefix digests disagree.
            foreach (long k in common)
            {
                if (a.PrefixHashByIndex[k] != b.PrefixHashByIndex[k])
                    Fail(BuildReport(a, b, k, "prefix-hash divergence", seed));
            }
        }
    }

    /// <summary>
    /// Fails unless every node has applied through <paramref name="expectedIndex"/> and they all agree on
    /// the prefix digest at that index.
    /// </summary>
    public static void ConvergedToIndex(
        IEnumerable<HashChainStateMachine> nodes, int partitionId, long expectedIndex, long? seed = null)
    {
        List<HashChainSnapshot> snaps = Snapshots(nodes, partitionId);
        if (snaps.Count == 0)
            Fail($"ConvergedToIndex: no hash-chain nodes for partition {partitionId}.");

        foreach (HashChainSnapshot s in snaps)
        {
            if (s.LastAppliedIndex < expectedIndex || !s.PrefixHashByIndex.ContainsKey(expectedIndex))
                Fail($"ConvergedToIndex: node {s.Endpoint} (partition {partitionId}) has not applied through index " +
                     $"{expectedIndex} (lastApplied={s.LastAppliedIndex}, hasIndex={s.PrefixHashByIndex.ContainsKey(expectedIndex)}). " +
                     SeedSuffix(seed));
        }

        HashChainSnapshot reference = snaps[0];
        ulong expectedHash = reference.PrefixHashByIndex[expectedIndex];
        foreach (HashChainSnapshot s in snaps.Skip(1))
        {
            if (s.PrefixHashByIndex[expectedIndex] != expectedHash)
                Fail(BuildReport(reference, s, expectedIndex, $"digest disagreement at target index {expectedIndex}", seed));
        }
    }

    /// <summary>
    /// Fails only if any node applied a <b>conflicting</b> duplicate — the same index delivered twice with
    /// different content, which is a true state-machine-safety violation. Tolerates identical duplicate
    /// delivery, so it is the right check when a scenario cannot guarantee exactly-once (e.g. under active
    /// faults). Consumer delivery is normally exactly-once, so <see cref="NoDuplicateApply"/> is the
    /// stricter default for a healthy run.
    /// </summary>
    public static void NoConflictingDuplicate(IEnumerable<HashChainStateMachine> nodes, int partitionId, long? seed = null)
    {
        foreach (HashChainSnapshot s in Snapshots(nodes, partitionId))
        {
            if (s.ConflictingDuplicates.Count > 0)
            {
                ConflictingDuplicate d = s.ConflictingDuplicates[0];
                Fail($"NoConflictingDuplicate: node {s.Endpoint} (partition {partitionId}) applied a CONFLICTING duplicate at " +
                     $"index {d.Index}. first=[{d.First}] second=[{d.Second}]. {SeedSuffix(seed)}");
            }
        }
    }

    /// <summary>
    /// Fails if any node observed the same index applied more than once (identical or conflicting) — the
    /// strict exactly-once check. Kommander gates consumer delivery on <c>log.Id &gt; lastAppliedIndex</c>
    /// (in <c>ApplyLogToConsumerAsync</c> and <c>CompleteFollowerAppend</c>), so a healthy run delivers
    /// each committed index exactly once. Use <see cref="NoConflictingDuplicate"/> when a scenario under
    /// active faults cannot guarantee exactly-once and you only want to catch true divergence.
    /// </summary>
    public static void NoDuplicateApply(IEnumerable<HashChainStateMachine> nodes, int partitionId, long? seed = null)
    {
        foreach (HashChainSnapshot s in Snapshots(nodes, partitionId))
        {
            if (s.ConflictingDuplicates.Count > 0)
            {
                ConflictingDuplicate d = s.ConflictingDuplicates[0];
                Fail($"NoDuplicateApply: node {s.Endpoint} (partition {partitionId}) applied a CONFLICTING duplicate at " +
                     $"index {d.Index}. first=[{d.First}] second=[{d.Second}]. {SeedSuffix(seed)}");
            }

            if (s.IdempotencyViolations.Count > 0)
                Fail($"NoDuplicateApply: node {s.Endpoint} (partition {partitionId}) applied an IDENTICAL duplicate at " +
                     $"index(es) {string.Join(", ", s.IdempotencyViolations)}. {SeedSuffix(seed)}");
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static List<HashChainSnapshot> Snapshots(IEnumerable<HashChainStateMachine> nodes, int partitionId) =>
        nodes.Where(n => n.PartitionId == partitionId).Select(n => n.Snapshot()).ToList();

    private static string SeedSuffix(long? seed) => seed is long s ? $"[seed={s}]" : "";

    private static string BuildReport(HashChainSnapshot a, HashChainSnapshot b, long index, string kind, long? seed)
    {
        StringBuilder sb = new();
        sb.AppendLine($"Hash-chain {kind} on partition {a.PartitionId} at index {index}. {SeedSuffix(seed)}");
        sb.AppendLine($"  node A = {a.Endpoint} (lastApplied={a.LastAppliedIndex}, count={a.AppliedCount})");
        sb.AppendLine($"  node B = {b.Endpoint} (lastApplied={b.LastAppliedIndex}, count={b.AppliedCount})");

        sb.AppendLine($"  entry@{index} A: {(a.MetaByIndex.TryGetValue(index, out EntryMeta? ma) ? ma.ToString() : "<absent>")}");
        sb.AppendLine($"  entry@{index} B: {(b.MetaByIndex.TryGetValue(index, out EntryMeta? mb) ? mb.ToString() : "<absent>")}");

        sb.AppendLine($"  prefixHash@{index} A: {Hash(a, index)}");
        sb.AppendLine($"  prefixHash@{index} B: {Hash(b, index)}");

        // A little surrounding context helps pinpoint where the chains split.
        for (long k = index - 2; k < index; k++)
        {
            if (k < 0) continue;
            sb.AppendLine($"  prefixHash@{k} A: {Hash(a, k)}  B: {Hash(b, k)}");
        }

        return sb.ToString();
    }

    private static string Hash(HashChainSnapshot s, long index) =>
        s.PrefixHashByIndex.TryGetValue(index, out ulong h) ? h.ToString("X16") : "<absent>";

    private static void Fail(string message) => Assert.Fail(message);
}
