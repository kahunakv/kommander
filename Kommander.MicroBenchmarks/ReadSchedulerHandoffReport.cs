using global::System.Diagnostics;
using RuntimeInformation = global::System.Runtime.InteropServices.RuntimeInformation;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Hand-off cost report for <see cref="FairReadScheduler"/> with microsecond read bodies.
/// BenchmarkDotNet reports wall time and allocations, but the hand-off cost is CPU burnt on OTHER
/// threads (workers and pool threads that spin before they park), so this harness measures, per
/// operation:
/// <list type="bullet">
///   <item>wall time on the caller;</item>
///   <item>process CPU (<see cref="Process.TotalProcessorTime"/> delta, every thread);</item>
///   <item>thread-pool work items completed (<see cref="ThreadPool.CompletedWorkItemCount"/> delta);</item>
///   <item>bytes allocated on every thread;</item>
///   <item>hand-off latency, enqueue to the start of the body (median and p99).</item>
/// </list>
/// <para>
/// The body spins 1 µs, the cost of a memory read. The scheduler matches Kahuna's backend read
/// instance: 8 workers, <c>concurrentPerPartition: true</c>, plus the ordered default for contrast.
/// </para>
/// <para>Run: <c>dotnet run -c Release -f net10.0 --project Kommander.MicroBenchmarks -- handoff</c></para>
/// </summary>
public static class ReadSchedulerHandoffReport
{
    private const int Operations = 10_000;
    private const int Workers = 8;
    private const int Callers = 8;
    private const int BurstSize = 64;

    private static readonly long TicksPerMicrosecond = Math.Max(1, Stopwatch.Frequency / 1_000_000);

    /// <summary>Per-operation hand-off samples, indexed by operation number.</summary>
    private static long[] _handoffTicks = new long[Operations];

    /// <summary>Spin bound under test; null is the scheduler default. Set from the command line.</summary>
    private static TimeSpan? SpinBound;

    public static void Run(string[] args)
    {
        if (args.Length > 1)
            SpinBound = TimeSpan.FromMicroseconds(double.Parse(args[1], global::System.Globalization.CultureInfo.InvariantCulture));

        Console.WriteLine($"FairReadScheduler hand-off report: {Operations:N0} ops, body 1 us, {Workers} workers, {Environment.ProcessorCount} cores, spin bound {(SpinBound ?? FairReadScheduler.DefaultMaxSpinBeforePark).TotalMicroseconds} us, {RuntimeInformation.FrameworkDescription}");
        Console.WriteLine();
        Console.WriteLine($"{"case",-58} {"wall us/op",10} {"cpu us/op",10} {"pool/op",8} {"B/op",8} {"p50 us",8} {"p99 us",8}");

        foreach (bool concurrent in new[] { true, false })
        {
            string mode = concurrent ? "concurrent" : "ordered";

            Measure($"{mode}: 1 caller, EnqueueTask, awaited", concurrent, OneCallerTask);
            Measure($"{mode}: 1 caller, EnqueueBatchableTask, awaited", concurrent, OneCallerBatchable);
            Measure($"{mode}: {Callers} callers on {Callers} partitions, EnqueueTask", concurrent, ManyCallersTask);
            Measure($"{mode}: burst of {BurstSize} to 1 partition, then await", concurrent, BurstTask);
            Measure($"{mode}: 1 caller, EnqueueTask, 1 ms apart (slow stream)", concurrent, PacedTask, PacedOperations);
            Measure($"{mode}: 1 caller, InlineWhenIdle, awaited", concurrent, OneCallerInline);
            Measure($"{mode}: 1 caller, callback, continuation on worker", concurrent, OneCallerCallback);
            Measure($"{mode}: {Callers} callers on {Callers} partitions, InlineWhenIdle", concurrent, ManyCallersInline);
        }
    }

    private static void Measure(string name, bool concurrent, Func<FairReadScheduler, Task> body) =>
        Measure(name, concurrent, body, Operations);

    private static void Measure(string name, bool concurrent, Func<FairReadScheduler, Task> body, int operations)
    {
        // One warm-up pass on its own scheduler: JIT, worker start, pool injection.
        using (FairReadScheduler warm = NewScheduler(concurrent))
            body(warm).GetAwaiter().GetResult();

        using FairReadScheduler scheduler = NewScheduler(concurrent);

        // Let the workers park so the run starts from the steady idle state.
        Thread.Sleep(50);
        Array.Clear(_handoffTicks);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Process process = Process.GetCurrentProcess();
        process.Refresh();
        TimeSpan cpu0 = process.TotalProcessorTime;
        long pool0 = ThreadPool.CompletedWorkItemCount;
        long alloc0 = GC.GetTotalAllocatedBytes(precise: true);
        long wall0 = Stopwatch.GetTimestamp();

        body(scheduler).GetAwaiter().GetResult();

        long wall1 = Stopwatch.GetTimestamp();
        long alloc1 = GC.GetTotalAllocatedBytes(precise: true);
        long pool1 = ThreadPool.CompletedWorkItemCount;
        process.Refresh();
        TimeSpan cpu1 = process.TotalProcessorTime;

        double wallUs = (wall1 - wall0) * 1_000_000.0 / Stopwatch.Frequency / operations;
        double cpuUs = (cpu1 - cpu0).TotalMicroseconds / operations;
        double poolPerOp = (double)(pool1 - pool0) / operations;
        double bytesPerOp = (double)(alloc1 - alloc0) / operations;

        long[] sorted = _handoffTicks[..operations];
        Array.Sort(sorted);
        double p50 = sorted[sorted.Length / 2] * 1_000_000.0 / Stopwatch.Frequency;
        double p99 = sorted[(int)(sorted.Length * 0.99)] * 1_000_000.0 / Stopwatch.Frequency;

        Console.WriteLine($"{name,-58} {wallUs,10:F2} {cpuUs,10:F2} {poolPerOp,8:F2} {bytesPerOp,8:F0} {p50,8:F2} {p99,8:F2}");
    }

    private static FairReadScheduler NewScheduler(bool concurrent)
    {
        FairReadScheduler scheduler = new(
            NullLogger<IRaft>.Instance,
            workerCount: Workers,
            concurrentPerPartition: concurrent,
            name: "handoff-report",
            maxSpinBeforePark: SpinBound);
        scheduler.Start();
        return scheduler;
    }

    /// <summary>The 1 µs body: records the hand-off latency, then spins for one microsecond.</summary>
    private static int Body((int Index, long EnqueuedAt) state)
    {
        long start = Stopwatch.GetTimestamp();
        _handoffTicks[state.Index] = start - state.EnqueuedAt;

        long end = start + TicksPerMicrosecond;
        while (Stopwatch.GetTimestamp() < end) { }

        return state.Index;
    }

    /// <summary>
    /// A slow stream (the CamusDB JOB shape: one page every few hundred µs). Wall time per op is mostly
    /// the caller's sleep; CPU per op is what each isolated hand-off costs, spin included.
    /// </summary>
    private const int PacedOperations = 1_000;

    private static async Task PacedTask(FairReadScheduler scheduler)
    {
        for (int i = 0; i < PacedOperations; i++)
        {
            await scheduler.EnqueueTask(1, (i, Stopwatch.GetTimestamp()), static s => Body(s));
            Thread.Sleep(1);
        }
    }

    private static async Task OneCallerTask(FairReadScheduler scheduler)
    {
        for (int i = 0; i < Operations; i++)
            await scheduler.EnqueueTask(1, (i, Stopwatch.GetTimestamp()), static s => Body(s));
    }

    private static async Task OneCallerInline(FairReadScheduler scheduler)
    {
        for (int i = 0; i < Operations; i++)
            await scheduler.EnqueueTask(1, (i, Stopwatch.GetTimestamp()), static s => Body(s), ReadExecutionHint.InlineWhenIdle);
    }

    private sealed class SpinExecutor : IReadBatchExecutor<(int Index, long EnqueuedAt), int>
    {
        public int[] ExecuteBatch((int Index, long EnqueuedAt)[] args)
        {
            int[] results = new int[args.Length];
            for (int i = 0; i < args.Length; i++)
                results[i] = Body(args[i]);
            return results;
        }
    }

    private static readonly SpinExecutor Executor = new();

    private static async Task OneCallerBatchable(FairReadScheduler scheduler)
    {
        for (int i = 0; i < Operations; i++)
            await scheduler.EnqueueBatchableTask(1, (i, Stopwatch.GetTimestamp()), Executor);
    }

    private static Task ManyCallersTask(FairReadScheduler scheduler) =>
        ManyCallers(scheduler, ReadExecutionHint.Queue);

    private static Task ManyCallersInline(FairReadScheduler scheduler) =>
        ManyCallers(scheduler, ReadExecutionHint.InlineWhenIdle);

    private static Task ManyCallers(FairReadScheduler scheduler, ReadExecutionHint hint)
    {
        const int perCaller = Operations / Callers;
        Task[] callers = new Task[Callers];

        for (int c = 0; c < Callers; c++)
        {
            int caller = c;
            callers[c] = Task.Run(async () =>
            {
                for (int i = 0; i < perCaller; i++)
                {
                    int index = caller * perCaller + i;
                    await scheduler.EnqueueTask(caller + 1, (index, Stopwatch.GetTimestamp()), static s => Body(s), hint);
                }
            });
        }

        return Task.WhenAll(callers);
    }

    private static async Task BurstTask(FairReadScheduler scheduler)
    {
        Task<int>[] burst = new Task<int>[BurstSize];

        for (int i = 0; i < Operations; i += BurstSize)
        {
            int count = Math.Min(BurstSize, Operations - i);

            for (int j = 0; j < count; j++)
                burst[j] = scheduler.EnqueueTask(1, (i + j, Stopwatch.GetTimestamp()), static s => Body(s));

            for (int j = 0; j < count; j++)
                await burst[j];
        }
    }

    /// <summary>
    /// A reusable awaitable that the completion callback completes WITHOUT queueing the caller's
    /// continuation: the caller resumes on the worker that ran the read. This is the shape of a
    /// consumer whose callback posts to an actor mailbox — no pool hop between the read and the next
    /// step.
    /// </summary>
    private sealed class InlineSignal : global::System.Threading.Tasks.Sources.IValueTaskSource<int>
    {
        private global::System.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<int> _core = new() { RunContinuationsAsynchronously = false };

        public ValueTask<int> Reset()
        {
            _core.Reset();
            return new ValueTask<int>(this, _core.Version);
        }

        public void Complete(int value, Exception? error)
        {
            if (error is not null)
                _core.SetException(error);
            else
                _core.SetResult(value);
        }

        public int GetResult(short token) => _core.GetResult(token);
        public global::System.Threading.Tasks.Sources.ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, global::System.Threading.Tasks.Sources.ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);
    }

    private static async Task OneCallerCallback(FairReadScheduler scheduler)
    {
        InlineSignal signal = new();

        for (int i = 0; i < Operations; i++)
        {
            ValueTask<int> wait = signal.Reset();

            scheduler.EnqueueTask(
                1,
                (signal, i, Stopwatch.GetTimestamp()),
                static s => Body((s.i, s.Item3)),
                static (s, result, error) => s.signal.Complete(result, error));

            await wait;
        }
    }
}
