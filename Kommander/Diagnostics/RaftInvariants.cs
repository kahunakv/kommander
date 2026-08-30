using Microsoft.Extensions.Logging;

namespace Kommander.Diagnostics;

/// <summary>
/// The consensus invariants that hold at every transition, asserted in the product code.
///
/// <para><b>Why this type exists.</b> The rules about how a partition's term and its two frontiers
/// may move were written in XML comments. A comment cannot fail a build and cannot fire during a
/// chaos soak, so a regression stayed silent until an external checker noticed divergence long
/// after the transition that caused it. These helpers move the same rules into code at the one
/// place each value is written, which turns "the applied frontier went backwards" from a
/// downstream mystery into a named, timestamped event at its origin.</para>
///
/// <para><b>Cost.</b> Every helper is a comparison of two <see cref="long"/> values on a path that
/// already performs disk or network work. Nothing is allocated and no string is formatted unless
/// the invariant is actually violated, so the checks stay on in release builds. That is the point:
/// Jepsen and Caraxes run release builds.</para>
///
/// <para><b>Scope.</b> Only single-node, single-partition rules belong here — the ones a node can
/// check from state it owns. A cluster-wide rule such as "at most one leader per term" needs a
/// view of every node and belongs in the simulation harness and the chaos checker, not here.</para>
///
/// <para><b>Process-wide policy.</b> <see cref="Policy"/> and <see cref="Logger"/> are static
/// because a violation must be reportable from types that hold neither a configuration nor a
/// logger, such as <c>RaftPartitionCoreState</c>. A process that hosts several
/// <see cref="RaftManager"/> instances (the test suite does) shares one policy: the last manager
/// to start wins. That is deliberate — the policy is a diagnostic setting, never a behavioral one,
/// so a shared value cannot change what the protocol does.</para>
/// </summary>
public static class RaftInvariants
{
    /// <summary>
    /// The default reaction to a violation: throw in a debug build so the test suite stops at the
    /// offending transition, log in a release build so a soak keeps running while still recording
    /// the first divergence.
    /// </summary>
    public static RaftInvariantPolicy DefaultPolicy =>
#if DEBUG
        RaftInvariantPolicy.Throw;
#else
        RaftInvariantPolicy.Log;
#endif

    /// <summary>
    /// The active reaction to a violation. Set from <see cref="RaftConfiguration.InvariantChecks"/>
    /// when a <see cref="RaftManager"/> starts; assignable directly by a test that needs to observe
    /// a violation without an exception.
    /// </summary>
    public static RaftInvariantPolicy Policy { get; set; } = DefaultPolicy;

    /// <summary>
    /// Optional sink for violation reports. Set by <see cref="RaftManager"/> at startup so a
    /// violation reaches the host's normal log pipeline. A <see langword="null"/> logger still
    /// records the metric and still throws under <see cref="RaftInvariantPolicy.Throw"/>.
    /// </summary>
    public static ILogger? Logger { get; set; }

    // ── Invariant names (also the metric tag values) ──────────────────────────

    /// <summary>A node's term never decreases (Raft §5.1).</summary>
    public const string TermMonotonic = "term_monotonic";

    /// <summary>The leader's committed frontier only advances; it regresses only through the
    /// declared demotion reset.</summary>
    public const string CommittedFrontierMonotonic = "committed_frontier_monotonic";

    /// <summary>The applied frontier only advances. It has no declared reset at all: an entry
    /// delivered to the consumer cannot be undelivered.</summary>
    public const string AppliedFrontierMonotonic = "applied_frontier_monotonic";

    /// <summary>A leader never reports more applied than committed.</summary>
    public const string AppliedNotAboveCommitted = "applied_not_above_committed";

    /// <summary>The composed compaction floor is at or below every floor it composes.</summary>
    public const string CompactionFloorIsLowerBound = "compaction_floor_is_lower_bound";

    /// <summary>The composed compaction floor equals one of the floors it composes, so the source
    /// the diagnostics name is the source the truncation used.</summary>
    public const string CompactionFloorHasSource = "compaction_floor_has_source";

    // ── Checks ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Asserts that <paramref name="next"/> does not move below <paramref name="previous"/>.
    /// <para>Written as an early return on the healthy path so the message is composed only when
    /// the rule is already broken — this runs on every commit.</para>
    /// </summary>
    /// <param name="invariant">One of the name constants on this type.</param>
    /// <param name="previous">The value held before the write.</param>
    /// <param name="next">The value the caller is about to write.</param>
    /// <param name="partitionId">Partition the value belongs to, for the report.</param>
    /// <param name="localEndpoint">Node the value belongs to, for the report.</param>
    public static void RequireNoRegression(
        string invariant,
        long previous,
        long next,
        int partitionId,
        string? localEndpoint)
    {
        if (next >= previous)
            return;

        Violate(
            invariant,
            partitionId,
            localEndpoint,
            $"value regressed from {previous} to {next}");
    }

    /// <summary>
    /// Asserts <c>applied &lt;= committed</c>. The caller is responsible for only asking while the
    /// node is a leader with a live committed frontier: a follower keeps its leader-side committed
    /// frontier at -1 by design, so the comparison is meaningless there.
    /// </summary>
    public static void RequireAppliedNotAboveCommitted(
        long applied,
        long committed,
        int partitionId,
        string? localEndpoint)
    {
        if (applied <= committed)
            return;

        Violate(
            RaftInvariants.AppliedNotAboveCommitted,
            partitionId,
            localEndpoint,
            $"applied={applied} exceeds committed={committed}");
    }

    /// <summary>
    /// Asserts an arbitrary condition. Prefer a named helper above; use this for a rule that has
    /// exactly one call site, and pass an already-built <paramref name="detail"/> only from the
    /// failing branch where the cost does not matter.
    /// </summary>
    public static void Require(
        bool condition,
        string invariant,
        int partitionId,
        string? localEndpoint,
        string detail)
    {
        if (condition)
            return;

        Violate(invariant, partitionId, localEndpoint, detail);
    }

    /// <summary>
    /// Records a violation and reacts according to <see cref="Policy"/>. Public so a harness that
    /// checks a cluster-wide rule (the simulation runtime, the chaos checker) reports through the
    /// same counter and the same exception type as the in-process checks.
    /// </summary>
    /// <exception cref="RaftInvariantViolationException">
    /// When <see cref="Policy"/> is <see cref="RaftInvariantPolicy.Throw"/>.
    /// </exception>
    public static void Violate(string invariant, int partitionId, string? localEndpoint, string detail)
    {
        RaftInvariantPolicy policy = Policy;
        if (policy == RaftInvariantPolicy.Off)
            return;

        KommanderMetrics.RecordInvariantViolation(partitionId, invariant);

        string message =
            $"[{localEndpoint ?? "?"}/{partitionId}] Raft invariant '{invariant}' violated: {detail}";

        // LogError rather than a source-generated message: a violation is rare by construction, so
        // the allocation does not matter, and the message must carry the free-form detail.
        Logger?.LogError("{InvariantViolation}", message);

        if (policy == RaftInvariantPolicy.Throw)
            throw new RaftInvariantViolationException(invariant, message);
    }
}
