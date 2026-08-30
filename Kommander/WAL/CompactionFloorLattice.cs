using Kommander.Diagnostics;

namespace Kommander.WAL;

/// <summary>
/// The five retention floors that bound one WAL compaction pass, composed in one place.
///
/// <para><b>Why this type exists.</b> The floors used to be composed by nested
/// <see cref="Math.Min(long,long)"/> calls inside the compaction pass, while a separate method
/// re-derived which floor had won so the diagnostics could name it. Two derivations of the same
/// answer can disagree, and neither one stated the order relations that make the composition
/// correct. The fragility analysis names this shape directly: about twenty floor concepts across
/// the code base, each one a variable that another floor can interact with, and no owned type to
/// hold the relations. This struct is that type for the compaction pass — it computes the
/// effective floor and its source from the same comparison chain, and it asserts the relations
/// instead of assuming them.</para>
///
/// <para><b>The relations.</b> Compaction deletes entries strictly below
/// <see cref="Effective"/>, so the effective floor must be at or below every floor it composes —
/// truncating above any one of them would delete an entry that consumer still needs. It must also
/// equal one of them, because a floor that belongs to no consumer is a number nobody can act on
/// and nobody can raise.</para>
///
/// <para><b>Absent floors.</b> A consumer that imposes no floor contributes
/// <see cref="long.MaxValue"/>, never 0. A zero floor would collapse the composition and suppress
/// compaction entirely, which is the behavior a caller who has not yet computed a protected index
/// never wants.</para>
/// </summary>
internal readonly struct CompactionFloorLattice
{
    /// <summary>Source name for the last durable checkpoint.</summary>
    public const string CheckpointSource = "checkpoint";

    /// <summary>Source name for the application-durability floor.</summary>
    public const string DurabilitySource = "durability_floor";

    /// <summary>Source name for the composable retention holds (min of holds).</summary>
    public const string RetentionHoldSource = "retention_hold";

    /// <summary>Source name for the legacy single <c>SetMinRetainIndex</c> floor.</summary>
    public const string MinRetainIndexSource = "min_retain_index";

    /// <summary>Source name for the live-replica lag budget.</summary>
    public const string LiveReplicaSource = "live_replica_lag_budget";

    /// <summary>Highest id the WAL has durably checkpointed. Compaction never passes it.</summary>
    public long Checkpoint { get; }

    /// <summary>Lowest id the application has NOT durably applied, or <see cref="long.MaxValue"/>
    /// when no <see cref="IApplicationDurabilityProvider"/> is configured.</summary>
    public long Durability { get; }

    /// <summary>Minimum over the active composable retention holds, or <see cref="long.MaxValue"/>
    /// when no hold is active.</summary>
    public long RetentionHold { get; }

    /// <summary>The legacy single retention floor, or <see cref="long.MaxValue"/> when unset.</summary>
    public long MinRetainIndex { get; }

    /// <summary>Lowest id a live, acking follower still needs (already clamped to the lag budget
    /// by the caller), or <see cref="long.MaxValue"/> when no follower constrains retention.</summary>
    public long LiveReplica { get; }

    /// <summary>The floor compaction actually uses: the minimum over all five.</summary>
    public long Effective { get; }

    /// <summary>
    /// Which floor <see cref="Effective"/> came from, as one of the source-name constants on this
    /// type. Ties resolve checkpoint → durability → retention hold → min-retain → live replica:
    /// the earlier a source appears, the more likely it is the one an operator can act on, and one
    /// name is more actionable than five numbers.
    /// </summary>
    public string Source { get; }

    /// <summary>
    /// <see langword="true"/> when the application-durability floor sits below the checkpoint, so
    /// the pass is bounded by the application's flusher rather than by the WAL. A pass that is
    /// clamped AND removes nothing means the flusher has stalled, which grows the WAL without
    /// bound and must be reported.
    /// </summary>
    public bool IsClampedByDurabilityFloor { get; }

    private CompactionFloorLattice(
        long checkpoint,
        long durability,
        long retentionHold,
        long minRetainIndex,
        long liveReplica)
    {
        Checkpoint = checkpoint;
        Durability = durability;
        RetentionHold = retentionHold;
        MinRetainIndex = minRetainIndex;
        LiveReplica = liveReplica;

        long effective = checkpoint;
        string source = CheckpointSource;

        // One chain decides both the value and its name, so the two can never disagree. A strict
        // comparison keeps the tie-break order above: an equal floor does not displace the source
        // already chosen.
        if (durability < effective)
        {
            effective = durability;
            source = DurabilitySource;
        }

        if (retentionHold < effective)
        {
            effective = retentionHold;
            source = RetentionHoldSource;
        }

        if (minRetainIndex < effective)
        {
            effective = minRetainIndex;
            source = MinRetainIndexSource;
        }

        if (liveReplica < effective)
        {
            effective = liveReplica;
            source = LiveReplicaSource;
        }

        Effective = effective;
        Source = source;
        IsClampedByDurabilityFloor = durability < checkpoint;
    }

    /// <summary>
    /// Composes the five floors and asserts the lattice relations through
    /// <see cref="RaftInvariants"/>. Pass <see cref="long.MaxValue"/> for a floor no consumer
    /// imposes.
    /// </summary>
    /// <param name="checkpoint">Last durable checkpoint id.</param>
    /// <param name="durability">Application-durability floor.</param>
    /// <param name="retentionHold">Minimum over the active retention holds.</param>
    /// <param name="minRetainIndex">The legacy single retention floor.</param>
    /// <param name="liveReplica">Live-replica floor, already clamped to the lag budget.</param>
    /// <param name="partitionId">Partition, for an invariant violation report.</param>
    /// <param name="localEndpoint">Node, for an invariant violation report.</param>
    public static CompactionFloorLattice Compose(
        long checkpoint,
        long durability,
        long retentionHold,
        long minRetainIndex,
        long liveReplica,
        int partitionId,
        string? localEndpoint)
    {
        CompactionFloorLattice lattice = new(
            checkpoint, durability, retentionHold, minRetainIndex, liveReplica);

        long effective = lattice.Effective;

        RaftInvariants.Require(
            effective <= checkpoint
            && effective <= durability
            && effective <= retentionHold
            && effective <= minRetainIndex
            && effective <= liveReplica,
            RaftInvariants.CompactionFloorIsLowerBound,
            partitionId,
            localEndpoint,
            lattice.Describe());

        RaftInvariants.Require(
            effective == checkpoint
            || effective == durability
            || effective == retentionHold
            || effective == minRetainIndex
            || effective == liveReplica,
            RaftInvariants.CompactionFloorHasSource,
            partitionId,
            localEndpoint,
            lattice.Describe());

        return lattice;
    }

    /// <summary>All five floors and the composed result, for a log line or a violation report.</summary>
    public string Describe() =>
        $"effective={Effective} source={Source} checkpoint={Checkpoint} durability={Durability} " +
        $"hold={RetentionHold} minRetain={MinRetainIndex} liveReplica={LiveReplica}";
}
