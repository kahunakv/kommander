
using Kommander.Diagnostics;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Unit tests for <see cref="PartitionLoadAccumulator"/> — the per-partition EWMA
/// throughput tracker used by the leader balancer for advisory load scoring.
/// </summary>
public sealed class TestPartitionLoadAccumulator
{
    [Fact]
    public void InitialOpsPerSecond_IsZero()
    {
        PartitionLoadAccumulator acc = new();
        Assert.Equal(0.0, acc.CurrentOpsPerSecond());
    }

    [Fact]
    public void RecordOps_RaisesOpsPerSecond()
    {
        // Use a fast-decaying accumulator (large λ) so we can drive the level high
        // without needing to worry about real elapsed time.  We inject a static clock
        // so no actual time passes between Record and read.
        long fakeTick = 0;
        Func<long> clock = () => fakeTick;

        // λ = 10 → time constant 0.1 s; half-life ≈ 69 ms.
        PartitionLoadAccumulator acc = new(decayConstant: 10.0, getTicks: clock);

        Assert.Equal(0.0, acc.CurrentOpsPerSecond());

        acc.RecordOps(100);

        // With zero elapsed time: level = 100, rate = 100 * 10 = 1000 ops/s.
        Assert.True(acc.CurrentOpsPerSecond() > 0.0);
    }

    [Fact]
    public void EwmaDecays_WhenPartitionIsIdle()
    {
        // Use a controlled clock measured in Stopwatch ticks-per-second.
        long ticksPerSecond = global::System.Diagnostics.Stopwatch.Frequency;
        long fakeTick = 0;
        Func<long> clock = () => fakeTick;

        // λ = 10 → half-life ≈ 69 ms (0.069 s).
        PartitionLoadAccumulator acc = new(decayConstant: 10.0, getTicks: clock);

        // Drive 1000 ops at t=0.
        acc.RecordOps(1000);
        double rateAfterBurst = acc.CurrentOpsPerSecond();
        Assert.True(rateAfterBurst > 0.0, "EWMA must rise after recording ops");

        // Advance clock by 1 s (one full time constant worth of decay for λ=10 is 0.1 s,
        // so 1 s is 10 half-lives → rate should drop to < 0.1 % of peak).
        fakeTick += ticksPerSecond;

        double rateAfterIdle = acc.CurrentOpsPerSecond();
        Assert.True(rateAfterIdle < rateAfterBurst,
            "EWMA must decay when no ops are recorded");

        // After 10× the time constant with λ=10 the level is e^(-10*1) ≈ 4.5e-5 of initial.
        Assert.True(rateAfterIdle < rateAfterBurst * 0.01,
            "EWMA must cool significantly after many half-lives idle");
    }

    [Fact]
    public void CurrentLoad_IncludesQueueDepthAdditively()
    {
        long fakeTick = 0;
        PartitionLoadAccumulator acc = new(decayConstant: 10.0, getTicks: () => fakeTick);

        // No ops yet → ops/sec = 0.
        double loadWithQueues = acc.CurrentLoad(wOps: 1.0, wQueue: 0.5, clientQueueDepth: 10, walQueueDepth: 6);

        // wQueue * (10 + 6) = 0.5 * 16 = 8.0; no ops contribution.
        Assert.Equal(8.0, loadWithQueues, precision: 6);
    }

    [Fact]
    public void CurrentLoad_CombinesOpsAndQueueTerms()
    {
        long fakeTick = 0;
        PartitionLoadAccumulator acc = new(decayConstant: 10.0, getTicks: () => fakeTick);

        // 100 ops at t=0; with λ=10 and zero elapsed time: rate = 100 * 10 = 1000 ops/s.
        acc.RecordOps(100);

        double ops = acc.CurrentOpsPerSecond();
        Assert.True(ops > 0);

        double loadNoQueue  = acc.CurrentLoad(1.0, 0.5, 0, 0);
        double loadWithQueue = acc.CurrentLoad(1.0, 0.5, 4, 2); // 0.5 * 6 = 3.0 extra

        Assert.True(loadWithQueue > loadNoQueue,
            "Queue depth must contribute to load score additively");
        Assert.Equal(loadNoQueue + 3.0, loadWithQueue, precision: 6);
    }

    [Fact]
    public void RecordOps_ZeroOrNegativeCount_IsIgnored()
    {
        long fakeTick = 0;
        PartitionLoadAccumulator acc = new(decayConstant: 10.0, getTicks: () => fakeTick);

        acc.RecordOps(0);
        acc.RecordOps(-5);

        Assert.Equal(0.0, acc.CurrentOpsPerSecond());
    }

    [Fact]
    public void EwmaApproachesZero_AfterLongIdle()
    {
        long ticksPerSecond = global::System.Diagnostics.Stopwatch.Frequency;
        long fakeTick = 0;
        PartitionLoadAccumulator acc = new(decayConstant: 10.0, getTicks: () => fakeTick);

        acc.RecordOps(1_000_000);

        // Advance 100 s — effectively infinite idle relative to time constant (0.1 s).
        fakeTick += ticksPerSecond * 100;

        double rate = acc.CurrentOpsPerSecond();
        Assert.True(rate < 1e-10, $"Rate should be negligible after 100 s idle, got {rate}");
    }

    [Fact]
    public void MultipleRecords_AccumulateLevelBeforeDecay()
    {
        long fakeTick = 0;
        PartitionLoadAccumulator acc = new(decayConstant: 10.0, getTicks: () => fakeTick);

        // Three bursts at the same instant (zero elapsed between them).
        acc.RecordOps(100);
        acc.RecordOps(200);
        acc.RecordOps(300);

        // Total level = 600; rate = 600 * 10 = 6000 ops/s at t=0.
        double rate = acc.CurrentOpsPerSecond();
        Assert.Equal(6000.0, rate, precision: 6);
    }
}
