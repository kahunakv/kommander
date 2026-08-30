using Kommander.Diagnostics;
using Kommander.WAL;

namespace Kommander.Tests.WAL;

/// <summary>
/// Covers <see cref="CompactionFloorLattice"/> (fragility analysis, recommendation 3).
///
/// <para>These are property tests, not example tests: the lattice replaces a hand-written
/// composition whose failure mode was a disagreement between the value and the name reported for
/// it, and an example test only proves the examples chosen. The randomized cases enumerate several
/// thousand floor tuples, including the ties and the absent-floor sentinel, and check the three
/// rules that make the composition correct — the effective floor is the minimum, it names a real
/// source, and the named source carries that value.</para>
/// </summary>
public sealed class TestCompactionFloorLattice
{
    private const long Absent = long.MaxValue;

    private static CompactionFloorLattice Compose(
        long checkpoint, long durability, long hold, long minRetain, long liveReplica) =>
        CompactionFloorLattice.Compose(checkpoint, durability, hold, minRetain, liveReplica, 1, "node-a");

    [Fact]
    public void NoFloorMeansTheCheckpointWins()
    {
        CompactionFloorLattice floors = Compose(100, Absent, Absent, Absent, Absent);

        Assert.Equal(100, floors.Effective);
        Assert.Equal(CompactionFloorLattice.CheckpointSource, floors.Source);
        Assert.False(floors.IsClampedByDurabilityFloor);
    }

    [Theory]
    [InlineData(40, CompactionFloorLattice.DurabilitySource)]
    [InlineData(41, CompactionFloorLattice.RetentionHoldSource)]
    [InlineData(42, CompactionFloorLattice.MinRetainIndexSource)]
    [InlineData(43, CompactionFloorLattice.LiveReplicaSource)]
    public void TheLowestFloorNamesItself(long winner, string expectedSource)
    {
        // Each call puts `winner` on one input and holds the others above it.
        long durability = expectedSource == CompactionFloorLattice.DurabilitySource ? winner : 90;
        long hold = expectedSource == CompactionFloorLattice.RetentionHoldSource ? winner : 91;
        long minRetain = expectedSource == CompactionFloorLattice.MinRetainIndexSource ? winner : 92;
        long liveReplica = expectedSource == CompactionFloorLattice.LiveReplicaSource ? winner : 93;

        CompactionFloorLattice floors = Compose(100, durability, hold, minRetain, liveReplica);

        Assert.Equal(winner, floors.Effective);
        Assert.Equal(expectedSource, floors.Source);
    }

    [Fact]
    public void ATieKeepsTheEarlierSource()
    {
        // Checkpoint first, then durability, hold, min-retain, live replica. An operator can act
        // on the earlier name, so an equal floor must not displace it.
        Assert.Equal(
            CompactionFloorLattice.CheckpointSource,
            Compose(50, 50, 50, 50, 50).Source);

        Assert.Equal(
            CompactionFloorLattice.DurabilitySource,
            Compose(60, 50, 50, 50, 50).Source);

        Assert.Equal(
            CompactionFloorLattice.RetentionHoldSource,
            Compose(60, 60, 50, 50, 50).Source);

        Assert.Equal(
            CompactionFloorLattice.MinRetainIndexSource,
            Compose(60, 60, 60, 50, 50).Source);
    }

    [Fact]
    public void TheDurabilityClampIsReportedOnlyBelowTheCheckpoint()
    {
        Assert.True(Compose(100, 40, Absent, Absent, Absent).IsClampedByDurabilityFloor);
        Assert.False(Compose(100, 100, Absent, Absent, Absent).IsClampedByDurabilityFloor);
        Assert.False(Compose(100, 140, Absent, Absent, Absent).IsClampedByDurabilityFloor);
    }

    [Fact]
    public void TheEffectiveFloorIsTheMinimumAndNamesItsOwnSource()
    {
        // A fixed seed keeps a failure reproducible; the range deliberately includes ties, zero,
        // and the absent-floor sentinel.
        Random random = new(20260829);

        for (int i = 0; i < 20_000; i++)
        {
            long checkpoint = Draw(random);
            long durability = Draw(random);
            long hold = Draw(random);
            long minRetain = Draw(random);
            long liveReplica = Draw(random);

            CompactionFloorLattice floors = Compose(checkpoint, durability, hold, minRetain, liveReplica);

            long expected = Math.Min(
                Math.Min(Math.Min(checkpoint, durability), Math.Min(hold, minRetain)), liveReplica);

            Assert.Equal(expected, floors.Effective);

            long named = floors.Source switch
            {
                CompactionFloorLattice.CheckpointSource => checkpoint,
                CompactionFloorLattice.DurabilitySource => durability,
                CompactionFloorLattice.RetentionHoldSource => hold,
                CompactionFloorLattice.MinRetainIndexSource => minRetain,
                CompactionFloorLattice.LiveReplicaSource => liveReplica,
                _ => throw new InvalidOperationException($"unknown source '{floors.Source}'")
            };

            Assert.Equal(floors.Effective, named);
        }
    }

    [Fact]
    public void AnImpossibleCompositionIsReportedAsAnInvariantViolation()
    {
        // The lattice cannot produce a floor above one of its inputs, so this drives the checker
        // directly — the point is that the relation is asserted, not merely intended.
        RaftInvariantPolicy original = RaftInvariants.Policy;
        try
        {
            RaftInvariants.Policy = RaftInvariantPolicy.Throw;

            RaftInvariantViolationException ex = Assert.Throws<RaftInvariantViolationException>(() =>
                RaftInvariants.Require(
                    condition: false,
                    RaftInvariants.CompactionFloorIsLowerBound,
                    partitionId: 1,
                    localEndpoint: "node-a",
                    detail: "effective=9 checkpoint=5"));

            Assert.Equal(RaftInvariants.CompactionFloorIsLowerBound, ex.Invariant);
        }
        finally
        {
            RaftInvariants.Policy = original;
        }
    }

    /// <summary>Draws a floor value biased toward collisions, plus the absent sentinel.</summary>
    private static long Draw(Random random)
    {
        int roll = random.Next(0, 10);
        if (roll == 0)
            return Absent;

        return random.Next(0, 12);
    }
}
