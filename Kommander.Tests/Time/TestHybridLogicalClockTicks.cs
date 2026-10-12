using Kommander.Time;

namespace Kommander.Tests.Time;

/// <summary>
/// Hybrid logical clock behavior under an injected physical-tick source: counter-only
/// advancement on a still wall clock, counter reset when the wall clock advances, tolerance of
/// a wall clock that runs backwards, receive-event merging for messages from the past and the
/// future, and the corruption guards on the packed representation.
/// </summary>
public sealed class TestHybridLogicalClockTicks
{
    private sealed class FixedPhysicalTime(long milliseconds)
    {
        private long current = milliseconds;

        public long Read() => current;

        public void Set(long milliseconds) => current = milliseconds;
    }

    [Fact]
    public void StillWallTick_AdvancesOnlyTheCounter_Strictly()
    {
        FixedPhysicalTime wall = new(1_000);
        HybridLogicalClock clock = new(wall.Read);

        HLCTimestamp previous = HLCTimestamp.Zero;

        for (uint expectedCounter = 1; expectedCounter <= 200; expectedCounter++)
        {
            HLCTimestamp stamp = clock.SendOrLocalEvent(nodeId: 4);

            Assert.Equal(1_000, stamp.L);
            Assert.Equal(expectedCounter, stamp.C);
            Assert.Equal(4, stamp.N);
            Assert.True(stamp.CompareTo(previous) > 0);
            previous = stamp;
        }
    }

    [Fact]
    public void StillWallTick_EveryStampIsUnique()
    {
        HybridLogicalClock clock = new(() => 5_000);

        HashSet<HLCTimestamp> stamps = [];

        for (int i = 0; i < 300; i++)
            Assert.True(stamps.Add(clock.SendOrLocalEvent(0)));

        Assert.Equal(300, stamps.Count);
    }

    [Fact]
    public void WallTickAdvancing_ResetsTheCounterToZero()
    {
        FixedPhysicalTime wall = new(1_000);
        HybridLogicalClock clock = new(wall.Read);

        HLCTimestamp first = clock.SendOrLocalEvent(0);
        Assert.Equal((1_000, 1u), (first.L, first.C));

        wall.Set(2_500);

        HLCTimestamp second = clock.SendOrLocalEvent(0);
        Assert.Equal((2_500, 0u), (second.L, second.C));
        Assert.True(second.CompareTo(first) > 0);
    }

    [Fact]
    public void WallClockRunningBackwards_ClockStaysMonotonic()
    {
        // A wall clock that steps back must never drag the HLC back with it: physical time
        // only ever raises the clock's logical component.
        FixedPhysicalTime wall = new(10_000);
        HybridLogicalClock clock = new(wall.Read);

        HLCTimestamp before = clock.SendOrLocalEvent(0);
        wall.Set(3_000);
        HLCTimestamp after = clock.SendOrLocalEvent(0);

        Assert.Equal(10_000, after.L);
        Assert.Equal(before.C + 1, after.C);
        Assert.True(after.CompareTo(before) > 0);
    }

    [Fact]
    public void ReceiveEvent_MessageFarInThePast_FollowsTheLocalClock()
    {
        FixedPhysicalTime wall = new(10_000);
        HybridLogicalClock clock = new(wall.Read);
        HLCTimestamp local = clock.SendOrLocalEvent(0);
        HLCTimestamp stale = new(2, 400, 5);

        HLCTimestamp merged = clock.ReceiveEvent(0, stale);

        Assert.Equal(10_000, merged.L);
        Assert.Equal(local.C + 1, merged.C);
        Assert.True(merged.CompareTo(local) > 0);
    }

    [Fact]
    public void ReceiveEvent_MessageFromTheFuture_AdoptsAndHoldsItsLogicalTime()
    {
        FixedPhysicalTime wall = new(1_000);
        HybridLogicalClock clock = new(wall.Read);
        clock.SendOrLocalEvent(0);
        HLCTimestamp future = new(2, 60_000, 3);

        HLCTimestamp merged = clock.ReceiveEvent(0, future);

        Assert.Equal(60_000, merged.L);
        Assert.Equal(4u, merged.C);

        // L running ahead of physical time is what the logical component exists to tolerate:
        // the next local event continues from the adopted L, not from the wall clock.
        HLCTimestamp next = clock.SendOrLocalEvent(0);
        Assert.Equal(60_000, next.L);
        Assert.Equal(5u, next.C);
    }

    [Fact]
    public void ReceiveEvent_MessageMatchingLocalTime_CounterDominatesBoth()
    {
        FixedPhysicalTime wall = new(7_000);
        HybridLogicalClock clock = new(wall.Read);
        clock.SendOrLocalEvent(0);
        HLCTimestamp second = clock.SendOrLocalEvent(0);
        HLCTimestamp message = new(9, second.L, second.C);

        HLCTimestamp merged = clock.ReceiveEvent(0, message);

        Assert.Equal(second.L, merged.L);
        Assert.Equal(second.C + 1, merged.C);
    }

    [Fact]
    public void ReceiveEvent_MessageAndTimestampOverloads_Agree()
    {
        HybridLogicalClock fromMessage = new(() => 1_000);
        HybridLogicalClock fromTimestamp = new(() => 1_000);

        HLCTimestamp viaMessage = fromMessage.ReceiveEvent(3, new HLClockMessage(50_000, 7));
        HLCTimestamp viaTimestamp = fromTimestamp.ReceiveEvent(3, new HLCTimestamp(11, 50_000, 7));

        Assert.Equal(viaTimestamp, viaMessage);
        Assert.Equal((50_000, 8u), (viaMessage.L, viaMessage.C));
    }

    [Fact]
    public void ReceiveEvent_ZeroLogicalTime_IsRefused()
    {
        HybridLogicalClock clock = new(() => 1_000);

        Assert.Throws<RaftException>(() => clock.ReceiveEvent(0, new HLClockMessage(0, 0)));
        Assert.Throws<RaftException>(() => clock.ReceiveEvent(0, HLCTimestamp.Zero));
    }

    [Fact]
    public void PhysicalTimeAtOrBelowZero_IsCorruption()
    {
        // The packed representation reserves non-positive L as the corruption marker, so a
        // physical-tick source that feeds zero or a negative value fails at construction.
        Assert.Throws<RaftException>(() => new HybridLogicalClock(() => 0));
        Assert.Throws<RaftException>(() => new HybridLogicalClock(() => -5));
    }

    [Fact]
    public void PhysicalTimeBeyondThePackedRange_IsCorruption()
    {
        // L is 42 bits; a wildly wrong system clock beyond ~year 2109 cannot be packed.
        Assert.Throws<RaftException>(() => new HybridLogicalClock(() => 1L << 42));
    }

    [Fact]
    public void ReceiveEvent_MessageLogicalTimeBeyondThePackedRange_IsCorruption()
    {
        HybridLogicalClock clock = new(() => 1_000);

        Assert.Throws<RaftException>(() => clock.ReceiveEvent(0, new HLClockMessage(1L << 42, 1)));
    }
}
