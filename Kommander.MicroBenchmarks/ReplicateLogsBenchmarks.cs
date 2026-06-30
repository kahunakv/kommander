using BenchmarkDotNet.Attributes;
using Kommander.Data;
using Kommander.Time;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Benchmarks the two materialization hotspots on the public batch-replication path.
///
/// <para><b>Materialization (manager layer).</b> The old path called <c>logs.ToList()</c> inside
/// the <c>ActiveProposal</c> retry loop, re-enumerating generator inputs on every retry and
/// allocating a fresh copy even when the caller already held a <c>List</c> or array. The new path
/// materializes once before the loop with <c>logs as IReadOnlyList&lt;byte[]&gt; ?? logs.ToList()</c>,
/// and the new <see cref="IReadOnlyList{T}"/> overload skips the copy entirely for list/array
/// callers.</para>
///
/// <para><b>Log-record construction (partition layer).</b> The old path used
/// <c>logs.Select(data =&gt; new RaftLog {...}).ToList()</c> — a delegate allocation plus the LINQ
/// pipeline. The new path uses a pre-sized <c>new List&lt;RaftLog&gt;(count)</c> filled with a
/// direct indexed <c>for</c> loop.</para>
/// </summary>
[Config(typeof(InProcessConfig))]
public class ReplicateLogsBenchmarks
{
    [Params(1, 8, 64, 256)]
    public int PayloadCount;

    private byte[][] _array = null!;
    private List<byte[]> _list = null!;
    private string _type = null!;

    [GlobalSetup]
    public void Setup()
    {
        _type = "bench";
        _array = new byte[PayloadCount][];
        for (int i = 0; i < PayloadCount; i++)
            _array[i] = new byte[16];
        _list = [.. _array];
    }

    // ── Materialization patterns ──────────────────────────────────────────

    /// <summary>
    /// Old manager path: <c>logs.ToList()</c> inside the retry loop — always copies, and
    /// re-enumerates generator inputs on every retry.
    /// </summary>
    [Benchmark(Baseline = true, Description = "old: ToList() inside retry loop (array input)")]
    public List<byte[]> Old_ToListInsideLoop_ArrayInput()
    {
        // Simulates one loop iteration: unconditional ToList() even for an array.
        return ((IEnumerable<byte[]>)_array).ToList();
    }

    /// <summary>
    /// New manager path for array/list callers: the cast succeeds, no copy is made.
    /// </summary>
    [Benchmark(Description = "new: as IReadOnlyList (array input, zero copy)")]
    public IReadOnlyList<byte[]> New_CastNoAlloc_ArrayInput()
    {
        IEnumerable<byte[]> logs = _array;
        return logs as IReadOnlyList<byte[]> ?? logs.ToList();
    }

    /// <summary>
    /// Old manager path with a generator input: <c>ToList()</c> materializes on every retry.
    /// Simulates two retries, the typical ActiveProposal case.
    /// </summary>
    [Benchmark(Description = "old: ToList() × 2 retries (generator input)")]
    public (List<byte[]> first, List<byte[]> second) Old_ToListTwice_GeneratorInput()
    {
        IEnumerable<byte[]> generator = Generator(_array);
        List<byte[]> first  = generator.ToList(); // retry 1
        List<byte[]> second = generator.ToList(); // retry 2
        return (first, second);
    }

    /// <summary>
    /// New manager path: generator is materialized once before the loop, retries reuse it.
    /// </summary>
    [Benchmark(Description = "new: materialize once, reuse across retries (generator input)")]
    public IReadOnlyList<byte[]> New_MaterializeOnce_GeneratorInput()
    {
        IEnumerable<byte[]> generator = Generator(_array);
        IReadOnlyList<byte[]> materialized = generator as IReadOnlyList<byte[]> ?? generator.ToList();
        _ = materialized; // retry 1 — reuses materialized
        _ = materialized; // retry 2 — reuses materialized
        return materialized;
    }

    // ── Log-record construction patterns ─────────────────────────────────

    /// <summary>
    /// Old partition path: LINQ <c>Select(...).ToList()</c> — delegate allocation + pipeline.
    /// </summary>
    [Benchmark(Description = "old: LINQ Select(...).ToList() for RaftLog construction")]
    public List<RaftLog> Old_LinqSelectToList()
    {
        string type = _type;
        return _list.Select(data => new RaftLog
        {
            Type = RaftLogType.Proposed,
            LogType = type,
            LogData = data,
            Time = new HLCTimestamp(0, 0, 0)
        }).ToList();
    }

    /// <summary>
    /// New partition path: pre-sized list + direct indexed loop — no delegate, no pipeline.
    /// </summary>
    [Benchmark(Description = "new: pre-sized list + indexed loop for RaftLog construction")]
    public List<RaftLog> New_IndexedLoop()
    {
        IReadOnlyList<byte[]> payloads = _array;
        string type = _type;
        List<RaftLog> result = new(payloads.Count);
        for (int i = 0; i < payloads.Count; i++)
            result.Add(new()
            {
                Type = RaftLogType.Proposed,
                LogType = type,
                LogData = payloads[i],
                Time = new HLCTimestamp(0, 0, 0)
            });
        return result;
    }

    private static IEnumerable<byte[]> Generator(byte[][] source)
    {
        foreach (byte[] item in source)
            yield return item;
    }
}
