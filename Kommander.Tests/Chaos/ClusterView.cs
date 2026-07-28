
using Kommander.Data;

namespace Kommander.Tests.Chaos;

/// <summary>
/// An immutable, point-in-time view of the whole cluster assembled by the chaos harness for the continuous
/// invariant checker. It combines the per-(node,partition) executor snapshots (<see cref="RaftPartitionView"/>),
/// each node's applied history (<see cref="HashChainSnapshot"/>), and cumulative historical facts the harness
/// records — entries observed committed, per-entry commit acknowledgements (with voter roles), and the highest
/// commit index ever seen per node — so history-dependent invariants (leader completeness, no-rollback,
/// commit monotonicity, quorum discipline) can be evaluated as pure functions of a single view.
///
/// <para>Cluster snapshots are NOT globally atomic (each node is sampled independently through its executor),
/// so an apparent cross-node violation from transient skew is resampled before failing; see
/// <see cref="ClusterInvariantChecker"/>.</para>
/// </summary>
public sealed record ClusterView(
    long Sequence,
    IReadOnlyList<RaftPartitionView> PartitionViews,
    IReadOnlyList<HashChainSnapshot> HashChains,
    IReadOnlyList<CommitObservation> ObservedCommits,
    IReadOnlyList<CommitAck> CommitAcks,
    IReadOnlyDictionary<string, long> MaxCommitByNode)
{
    /// <summary>
    /// Highest applied index ever seen per node (injected by the checker's Augment, defaults empty). Unlike the
    /// gap-aware commit frontier, the applied index is the durable delivered prefix and never regresses, so
    /// commit-monotonicity compares against it — a commit-frontier dip that stays at/above what a node already
    /// applied is a benign observability artifact (e.g. a fault-rolled-back uncommitted entry), not a violation.
    /// </summary>
    public IReadOnlyDictionary<string, long> MaxAppliedByNode { get; init; } = new Dictionary<string, long>();

    public static ClusterView Empty { get; } =
        new(0, [], [], [], [], new Dictionary<string, long>());
}

/// <summary>An entry the harness observed committed (recorded once, cumulatively) for a partition.</summary>
public sealed record CommitObservation(int Partition, long Index, long Term, ulong Digest);

/// <summary>
/// One acknowledgement of a committed entry by a specific node, tagged with whether that node was a voter
/// (learners never count toward quorum) and how many voters the membership had at commit time.
/// </summary>
public sealed record CommitAck(int Partition, long Index, string Acker, bool AckerIsVoter, int VotersTotal);
