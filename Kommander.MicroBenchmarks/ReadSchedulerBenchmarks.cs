using BenchmarkDotNet.Attributes;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Benchmarks bytes/read for <see cref="FairReadScheduler"/> enqueues (spec <c>6385886f</c> task
/// M3). The two "new" benchmarks run through the REAL scheduler (workers, per-partition queue,
/// fairness dispatch), so their bytes include amortized queue costs.
///
/// <para>Expected signal per read:</para>
/// <list type="bullet">
///   <item>Pre-change shape (baseline): TCS + capturing closure display + <c>Action</c> +
///         <c>Task&lt;T&gt;</c> — the trio the old <c>EnqueueTask&lt;T&gt;</c> allocated. Built and
///         completed inline (the drain loop no longer accepts an <c>Action</c>), so it EXCLUDES
///         queue/dispatch bytes — the real pre-change cost was at least this number.</item>
///   <item>Legacy overload (<c>Func&lt;T&gt;</c>): one combined work item + its <c>Task&lt;T&gt;</c>
///         + the caller's closure — the closure/Action/TCS trio collapsed into one object.</item>
///   <item>State-carried overload (<c>TState</c> + static delegate): work item + <c>Task&lt;T&gt;</c>
///         only — the target 2-object cost.</item>
/// </list>
/// </summary>
[Config(typeof(InProcessConfig))]
public class ReadSchedulerBenchmarks : IDisposable
{
    private FairReadScheduler _scheduler = null!;
    private long _value;

    [GlobalSetup]
    public void Setup()
    {
        _value = 42;
        _scheduler = new FairReadScheduler(NullLogger<IRaft>.Instance, workerCount: 2);
        _scheduler.Start();
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _scheduler?.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// The old <c>EnqueueTask&lt;T&gt;</c> allocation shape, verbatim: fresh TCS, a closure over
    /// (tcs, operation), and an <c>Action</c> wrapper — completed inline because the current drain
    /// loop no longer runs bare Actions. Allocation-shape baseline only.
    /// </summary>
    [Benchmark(Baseline = true, Description = "pre-change shape: TCS + closure + Action (inline, no queue bytes)")]
    public Task<long> OldShape_TcsClosureAction()
    {
        long value = _value;
        Func<long> operation = () => value;

        TaskCompletionSource<long> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Action work = () =>
        {
            try
            {
                tcs.TrySetResult(operation());
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        };

        work();

        return tcs.Task;
    }

    /// <summary>Legacy overload through the real scheduler: combined item + caller closure.</summary>
    [Benchmark(Description = "legacy overload: EnqueueTask<T>(Func<T>) through the scheduler")]
    public Task<long> Legacy_FuncOverload()
    {
        long value = _value;
        return _scheduler.EnqueueTask(1, () => value);
    }

    /// <summary>State-carried overload through the real scheduler: item + Task only.</summary>
    [Benchmark(Description = "state-carried: EnqueueTask<TState,T> + static lambda through the scheduler")]
    public Task<long> New_StateCarried() =>
        _scheduler.EnqueueTask(1, _value, static v => v);
}
