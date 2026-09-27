using System.Diagnostics.Metrics;
using Kommander.Diagnostics;

namespace Kommander.Benchmark;

/// <summary>
/// Listens to the <c>raft.round.stage_ms</c> histogram (<see cref="RoundStageInstrumentation"/>) and
/// keeps count, sum and a bounded sample per stage for the measurement window only.
///
/// <para>The listener is what makes the histogram active: <see cref="RoundStageInstrumentation.IsActive"/>
/// needs both the switch and a listener. The collector turns the switch on in its constructor and
/// off on dispose, and drops samples outside the window through <see cref="Measuring"/>.</para>
///
/// <para>Measurements arrive on the Kommander threads that record them, so each stage's
/// accumulator takes a lock. The stages record a few values per proposal; the lock is not a
/// measurable part of the round at the proposal rates this benchmark reaches.</para>
/// </summary>
public sealed class StageCollector : IDisposable
{
    private const int MaxSamplesPerStage = 200_000;

    private readonly MeterListener listener = new();

    private readonly Dictionary<string, Accumulator> stages = [];

    public volatile bool Measuring;

    public StageCollector()
    {
        foreach (string name in RoundStageInstrumentation.AllStageNames)
            stages[name] = new();

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == KommanderMetrics.MeterName && instrument.Name == RoundStageInstrumentation.HistogramName)
                l.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<double>(OnMeasurement);
        listener.Start();

        RoundStageInstrumentation.Enabled = true;
    }

    private void OnMeasurement(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        if (!Measuring)
            return;

        foreach (KeyValuePair<string, object?> tag in tags)
        {
            if (tag.Key == RoundStageInstrumentation.StageTag && tag.Value is string name && stages.TryGetValue(name, out Accumulator? accumulator))
            {
                accumulator.Add(value);
                return;
            }
        }
    }

    /// <summary>Per-stage count, mean, p50 and p99 (milliseconds), for every stage with samples.</summary>
    public Dictionary<string, StageStats> Snapshot()
    {
        Dictionary<string, StageStats> result = [];

        foreach ((string name, Accumulator accumulator) in stages)
        {
            StageStats stats = accumulator.Stats();
            if (stats.Count > 0)
                result[name] = stats;
        }

        return result;
    }

    public void Dispose()
    {
        RoundStageInstrumentation.Enabled = false;
        listener.Dispose();
    }

    private sealed class Accumulator
    {
        private readonly object sync = new();
        private readonly List<double> samples = [];
        private long count;
        private double sum;

        public void Add(double value)
        {
            lock (sync)
            {
                count++;
                sum += value;
                if (samples.Count < MaxSamplesPerStage)
                    samples.Add(value);
            }
        }

        public StageStats Stats()
        {
            lock (sync)
            {
                double[] sorted = [.. samples];
                Array.Sort(sorted);
                return new(count, count > 0 ? sum / count : 0, Percentiles.Of(sorted, 0.50), Percentiles.Of(sorted, 0.99));
            }
        }
    }
}

/// <summary>One stage's statistics over the window, in milliseconds.</summary>
public sealed record StageStats(long Count, double MeanMs, double P50Ms, double P99Ms);

/// <summary>Nearest-rank percentile over a sorted array.</summary>
public static class Percentiles
{
    public static double Of(double[] sorted, double q)
    {
        if (sorted.Length == 0)
            return 0;

        int rank = (int)Math.Ceiling(q * sorted.Length) - 1;
        return sorted[Math.Clamp(rank, 0, sorted.Length - 1)];
    }
}
