
using Kommander;
using Kommander.Data;

namespace Kommander.Tests.Chaos;

/// <summary>An immutable invariant violation. Side-effect-free evaluation returns one of these or null.</summary>
public sealed record ClusterViolation(string Invariant, string Detail, bool RequiresConfirmation)
{
    public override string ToString() => $"[{Invariant}] {Detail}" + (RequiresConfirmation ? " (needs confirmation)" : "");
}

/// <summary>
/// A continuously-evaluated cluster safety property. <see cref="Evaluate"/> is a pure function of two
/// adjacent cluster views (the previous one may be <see langword="null"/> on the first sample) and returns
/// either <see langword="null"/> (holds) or an immutable <see cref="ClusterViolation"/>.
/// </summary>
public interface IClusterInvariant
{
    string Name { get; }
    ClusterViolation? Evaluate(ClusterView? previous, ClusterView current);
}

/// <summary>Pure predicates shared by the live invariants and the assertion helpers.</summary>
public static class InvariantPredicates
{
    /// <summary>Raft voter majority: <c>max(2, floor(n/2)+1)</c>, matching <c>RaftSafetyAssert.Quorum</c>.</summary>
    public static int VoterMajority(int voters) => Math.Max(2, voters / 2 + 1);
}

/// <summary>Invariant 1 — at most one leader per (partition, term). Transient: confirmed before failing.</summary>
public sealed class ElectionSafetyInvariant : IClusterInvariant
{
    public string Name => "ElectionSafety";

    public ClusterViolation? Evaluate(ClusterView? previous, ClusterView current)
    {
        var leadersByTerm = current.PartitionViews
            .Where(v => v.Role == RaftNodeState.Leader)
            .GroupBy(v => (v.Partition, v.Term));

        foreach (var g in leadersByTerm)
        {
            string[] endpoints = g.Select(v => v.Endpoint).Distinct().OrderBy(e => e).ToArray();
            if (endpoints.Length > 1)
                return new ClusterViolation(Name,
                    $"partition {g.Key.Partition} term {g.Key.Term} has {endpoints.Length} leaders: {string.Join(", ", endpoints)}",
                    RequiresConfirmation: true);
        }
        return null;
    }
}

/// <summary>Invariant 2 — state machine safety: no prefix divergence, hole, or conflicting duplicate apply.</summary>
public sealed class StateMachineSafetyInvariant : IClusterInvariant
{
    public string Name => "StateMachineSafety";

    public ClusterViolation? Evaluate(ClusterView? previous, ClusterView current)
    {
        foreach (IGrouping<int, HashChainSnapshot> byPartition in current.HashChains.GroupBy(h => h.PartitionId))
        {
            List<HashChainSnapshot> snaps = byPartition.ToList();

            foreach (HashChainSnapshot s in snaps)
                if (s.ConflictingDuplicates.Count > 0)
                    return new ClusterViolation(Name,
                        $"node {s.Endpoint} p{s.PartitionId} conflicting duplicate at index {s.ConflictingDuplicates[0].Index}",
                        RequiresConfirmation: false);

            for (int i = 0; i < snaps.Count; i++)
            for (int j = i + 1; j < snaps.Count; j++)
            {
                ClusterViolation? v = ComparePair(snaps[i], snaps[j], Name);
                if (v is not null)
                    return v;
            }
        }
        return null;
    }

    internal static ClusterViolation? ComparePair(HashChainSnapshot a, HashChainSnapshot b, string name)
    {
        List<long> common = a.PrefixHashByIndex.Keys.Where(b.PrefixHashByIndex.ContainsKey).OrderBy(x => x).ToList();
        if (common.Count == 0)
            return null;
        long maxCommon = common[^1];

        long? hole = a.PrefixHashByIndex.Keys.Union(b.PrefixHashByIndex.Keys)
            .Where(k => k <= maxCommon && !(a.PrefixHashByIndex.ContainsKey(k) && b.PrefixHashByIndex.ContainsKey(k)))
            .OrderBy(k => k).Select(k => (long?)k).FirstOrDefault();
        if (hole is long h)
            return new ClusterViolation(name, $"p{a.PartitionId} hole at index {h} between {a.Endpoint} and {b.Endpoint}", false);

        foreach (long k in common)
            if (a.PrefixHashByIndex[k] != b.PrefixHashByIndex[k])
                return new ClusterViolation(name, $"p{a.PartitionId} prefix divergence at index {k} between {a.Endpoint} and {b.Endpoint}", false);

        return null;
    }
}

/// <summary>
/// Invariant 3 — index uniqueness: retained entries at the same (partition, index) agree on term, Raft entry
/// type, application log type, and payload digest. A missing/compacted entry is unknown, not disagreement.
/// </summary>
public sealed class IndexUniquenessInvariant : IClusterInvariant
{
    public string Name => "IndexUniqueness";

    public ClusterViolation? Evaluate(ClusterView? previous, ClusterView current)
    {
        foreach (IGrouping<int, HashChainSnapshot> byPartition in current.HashChains.GroupBy(h => h.PartitionId))
        {
            Dictionary<long, (string Endpoint, EntryMeta Meta)> seen = new();
            foreach (HashChainSnapshot s in byPartition)
            foreach ((long index, EntryMeta meta) in s.MetaByIndex)
            {
                if (seen.TryGetValue(index, out var prior))
                {
                    if (prior.Meta.Term != meta.Term
                        || prior.Meta.Type != meta.Type
                        || prior.Meta.LogType != meta.LogType
                        || prior.Meta.EntryDigest != meta.EntryDigest)
                    {
                        return new ClusterViolation(Name,
                            $"p{s.PartitionId} index {index} disagrees: {prior.Endpoint}=[{prior.Meta}] vs {s.Endpoint}=[{meta}]",
                            RequiresConfirmation: false);
                    }
                }
                else
                {
                    seen[index] = (s.Endpoint, meta);
                }
            }
        }
        return null;
    }
}

/// <summary>
/// Invariant 4 — leader completeness: every entry previously observed committed is present in a later
/// leader's applied history (or covered by a snapshot boundary). Only checked once the leader has applied
/// through the index; a leader still catching up is a transient, not a violation.
/// </summary>
public sealed class LeaderCompletenessInvariant : IClusterInvariant
{
    public string Name => "LeaderCompleteness";

    public ClusterViolation? Evaluate(ClusterView? previous, ClusterView current)
    {
        foreach (CommitObservation c in current.ObservedCommits)
        {
            RaftPartitionView? leader = current.PartitionViews
                .FirstOrDefault(v => v.Partition == c.Partition && v.Role == RaftNodeState.Leader);
            if (leader is null || leader.LastAppliedIndex < c.Index)
                continue; // no current leader, or leader has not applied through this index yet — transient

            HashChainSnapshot? chain = current.HashChains
                .FirstOrDefault(h => h.PartitionId == c.Partition && h.Endpoint == leader.Endpoint);
            if (chain is null)
                continue;

            if (!chain.PrefixHashByIndex.ContainsKey(c.Index))
                return new ClusterViolation(Name,
                    $"p{c.Partition} leader {leader.Endpoint} (applied through {leader.LastAppliedIndex}) is missing committed index {c.Index}",
                    RequiresConfirmation: true);

            if (chain.MetaByIndex.TryGetValue(c.Index, out EntryMeta? meta) && meta.EntryDigest != c.Digest)
                return new ClusterViolation(Name,
                    $"p{c.Partition} leader {leader.Endpoint} has index {c.Index} with digest {meta.EntryDigest:X16} != committed {c.Digest:X16}",
                    RequiresConfirmation: false);
        }
        return null;
    }
}

/// <summary>
/// Invariant 5 — commit monotonicity: a node's commit index never decreases within one process lifetime.
///
/// <para><b>Confirmation-required.</b> The observable value is the WAL's <i>gap-aware</i> commit frontier
/// (<c>GetCommitIndex</c>), which stops at the first hole in the log. Under adversarial delivery an unanchored
/// live-propose or a misordered append can transiently open a hole above the committed prefix, so the frontier
/// can dip for a heartbeat or two before log-hole repair heals it — without any committed entry actually being
/// lost. A genuine regression (real data loss) persists across a resample; a transient observational dip does
/// not. Hence this is re-sampled before failing, unlike the strictly-historical prefix/rollback invariants
/// (<see cref="StateMachineSafetyInvariant"/>, <see cref="NoCommittedRollbackInvariant"/>) which fail at once.</para>
/// </summary>
public sealed class CommitMonotonicityInvariant : IClusterInvariant
{
    public string Name => "CommitMonotonicity";

    public ClusterViolation? Evaluate(ClusterView? previous, ClusterView current)
    {
        foreach (RaftPartitionView v in current.PartitionViews)
        {
            if (current.MaxCommitByNode.TryGetValue(v.Endpoint, out long maxSeen) && v.CommitIndex < maxSeen)
                return new ClusterViolation(Name,
                    $"node {v.Endpoint} p{v.Partition} commit index regressed: {v.CommitIndex} < {maxSeen}",
                    RequiresConfirmation: true);
        }
        return null;
    }
}

/// <summary>Invariant 6 — an entry observed committed is never later observed rolled back or replaced.</summary>
public sealed class NoCommittedRollbackInvariant : IClusterInvariant
{
    public string Name => "NoCommittedRollback";

    public ClusterViolation? Evaluate(ClusterView? previous, ClusterView current)
    {
        Dictionary<(int, long), ulong> committed = current.ObservedCommits.ToDictionary(c => (c.Partition, c.Index), c => c.Digest);

        foreach (HashChainSnapshot s in current.HashChains)
        foreach ((long index, EntryMeta meta) in s.MetaByIndex)
        {
            if (committed.TryGetValue((s.PartitionId, index), out ulong committedDigest) && committedDigest != meta.EntryDigest)
                return new ClusterViolation(Name,
                    $"p{s.PartitionId} index {index} on {s.Endpoint} replaced a committed entry: digest {meta.EntryDigest:X16} != committed {committedDigest:X16}",
                    RequiresConfirmation: false);
        }
        return null;
    }
}

/// <summary>
/// Invariant 7 — quorum discipline: each successful commit is acknowledged by a voter majority; learners
/// never count. Requires recorded per-entry acknowledgements with voter roles and the voter count at commit.
/// </summary>
public sealed class QuorumDisciplineInvariant : IClusterInvariant
{
    public string Name => "QuorumDiscipline";

    public ClusterViolation? Evaluate(ClusterView? previous, ClusterView current)
    {
        foreach (IGrouping<(int Partition, long Index), CommitAck> g in current.CommitAcks.GroupBy(a => (a.Partition, a.Index)))
        {
            int voters = g.Max(a => a.VotersTotal);
            int voterAcks = g.Where(a => a.AckerIsVoter).Select(a => a.Acker).Distinct().Count();
            int majority = InvariantPredicates.VoterMajority(voters);
            if (voterAcks < majority)
                return new ClusterViolation(Name,
                    $"p{g.Key.Partition} index {g.Key.Index} committed with only {voterAcks} voter acks (need {majority} of {voters})",
                    RequiresConfirmation: false);
        }
        return null;
    }
}

/// <summary>The default set of continuous invariants, in evaluation order.</summary>
public static class ClusterInvariants
{
    public static IReadOnlyList<IClusterInvariant> All { get; } =
    [
        new ElectionSafetyInvariant(),
        new StateMachineSafetyInvariant(),
        new IndexUniquenessInvariant(),
        new LeaderCompletenessInvariant(),
        new CommitMonotonicityInvariant(),
        new NoCommittedRollbackInvariant(),
        new QuorumDisciplineInvariant(),
    ];
}
