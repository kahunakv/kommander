
using Kommander.Diagnostics;

namespace Kommander.Tests.Diagnostics;

/// <summary>
/// Unit tests for <see cref="PartitionWaitAccumulator"/>.
/// </summary>
public sealed class TestPartitionWaitAccumulator
{
    [Fact]
    public void InitialValue_IsZero()
    {
        PartitionWaitAccumulator acc = new();
        Assert.Equal(0.0, acc.CurrentWaitMs());
    }

    [Fact]
    public void FirstSample_SetsDirect()
    {
        PartitionWaitAccumulator acc = new();
        acc.RecordWaitMs(100.0);
        // First observation initialises the EWMA to the sample value exactly.
        Assert.Equal(100.0, acc.CurrentWaitMs());
    }

    [Fact]
    public void SecondSample_BlendsTowardNewValue()
    {
        PartitionWaitAccumulator acc = new(alpha: 0.5);
        acc.RecordWaitMs(100.0);  // ewma = 100
        acc.RecordWaitMs(200.0);  // ewma = 0.5*200 + 0.5*100 = 150
        Assert.Equal(150.0, acc.CurrentWaitMs(), precision: 6);
    }

    [Fact]
    public void RepeatedSamples_ConvergeToSteadyState()
    {
        PartitionWaitAccumulator acc = new(alpha: 0.3);
        for (int i = 0; i < 50; i++)
            acc.RecordWaitMs(50.0);
        // After many samples at 50ms, EWMA must be very close to 50ms.
        Assert.True(Math.Abs(acc.CurrentWaitMs() - 50.0) < 0.01,
            $"Expected EWMA ≈ 50ms after convergence, got {acc.CurrentWaitMs()}.");
    }

    [Fact]
    public void NegativeSamples_Ignored()
    {
        PartitionWaitAccumulator acc = new();
        acc.RecordWaitMs(40.0);
        acc.RecordWaitMs(-10.0); // must be ignored
        Assert.Equal(40.0, acc.CurrentWaitMs());
    }

    [Fact]
    public void ZeroSample_PullsEwmaDown()
    {
        PartitionWaitAccumulator acc = new(alpha: 0.5);
        acc.RecordWaitMs(100.0);
        acc.RecordWaitMs(0.0); // ewma = 0.5*0 + 0.5*100 = 50
        Assert.Equal(50.0, acc.CurrentWaitMs(), precision: 6);
    }
}
