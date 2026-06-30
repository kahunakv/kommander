using System.Diagnostics;
using System.Diagnostics.Metrics;
using BenchmarkDotNet.Attributes;
using Kommander.Diagnostics;
using Kommander.Scheduling;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Benchmarks the executor metrics tag construction hotspot.
///
/// <para>Before P6: every operation dispatch called <c>kind.ToString()</c> (potential heap alloc)
/// and constructed <c>new TagList { { "partition_id", partitionId }, { "operation_class", ... } }</c>,
/// boxing the <c>int partitionId</c> into <c>object?</c> on every call.</para>
///
/// <para>After P6: <see cref="TagList"/> instances are pre-built once at executor construction
/// time. The hot path holds a single array-index load and two <c>Add/Record</c> calls against
/// the cached struct; no per-call allocation occurs.</para>
///
/// <para>Two variants are measured: warm (a <see cref="MeterListener"/> is subscribed, so the
/// metric path runs in full) and cold (no listener, so the runtime's internal gate exits
/// immediately after the enabled check).</para>
/// </summary>
[Config(typeof(InProcessConfig))]
public class ExecutorMetricsBenchmarks
{
    private const int PartitionId = 1;

    private readonly RaftOperationKind[] _kinds = [
        RaftOperationKind.Control,
        RaftOperationKind.Replication,
        RaftOperationKind.Client,
        RaftOperationKind.Maintenance,
    ];

    // Pre-built (new path): indexed by (int)RaftOperationKind.
    private TagList[] _prebuiltTags = null!;

    // A MeterListener attached during warm runs so instruments actually record.
    private MeterListener? _listener;

    [Params(false, true)]
    public bool WithListener;

    [GlobalSetup]
    public void Setup()
    {
        _prebuiltTags = new TagList[4];
        foreach (RaftOperationKind k in Enum.GetValues<RaftOperationKind>())
            _prebuiltTags[(int)k] = new TagList
            {
                { "partition_id", PartitionId },
                { "operation_class", RaftOperationMapper.GetKindLabel(k) },
            };

        if (WithListener)
        {
            _listener = new MeterListener();
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Kommander")
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.Start();
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _listener?.Dispose();

    // ── old path ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Old approach: build a fresh <see cref="TagList"/> per dispatch, calling
    /// <c>kind.ToString()</c> and boxing <c>partitionId</c> on every call.
    /// </summary>
    [Benchmark(Baseline = true, Description = "old: new TagList + kind.ToString() per dispatch")]
    public void Old_NewTagListPerDispatch()
    {
        foreach (RaftOperationKind kind in _kinds)
        {
#pragma warning disable CA1829 // Not measuring; intentionally testing the old path
            TagList tags = new() { { "partition_id", PartitionId }, { "operation_class", kind.ToString() } };
#pragma warning restore CA1829
            Diagnostics.KommanderMetrics.ExecutorOperationsTotal.Add(1, tags);
            Diagnostics.KommanderMetrics.ExecutorOperationDurationMs.Record(1.0, tags);
        }
    }

    // ── new path ─────────────────────────────────────────────────────────────

    /// <summary>
    /// New approach: index into a pre-built <see cref="TagList"/> array, passing by
    /// <c>in</c> ref to avoid the struct copy.
    /// </summary>
    [Benchmark(Description = "new: pre-built TagList, in-ref, no per-dispatch alloc")]
    public void New_PrebuiltTagList()
    {
        foreach (RaftOperationKind kind in _kinds)
        {
            Diagnostics.KommanderMetrics.ExecutorOperationsTotal.Add(1, in _prebuiltTags[(int)kind]);
            Diagnostics.KommanderMetrics.ExecutorOperationDurationMs.Record(1.0, in _prebuiltTags[(int)kind]);
        }
    }
}
