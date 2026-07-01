using BenchmarkDotNet.Attributes;
using Kommander.Support.Collections;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Compares <see cref="SmallDictionary{TKey,TValue}"/> against the BCL <see cref="Dictionary{TKey,TValue}"/>
/// for the tiny-map workloads it was built for (e.g. <c>RaftWriteAhead.plan</c>, a 3-entry map keyed by
/// <see cref="RaftLogAction"/>).
///
/// <para><see cref="SmallDictionary{TKey,TValue}"/> is a dense structure-of-arrays with linear-scan lookup:
/// no hashing, no bucket/entry arrays, a packed key array. That trades O(1) hashed lookup for O(n) scan,
/// which only wins while <c>n</c> is small. This suite measures where the crossover actually is on the
/// two axes that matter for the WAL hot path: allocation (construct + fill) and lookup throughput.</para>
///
/// <para>The <see cref="Size"/> sweep spans the real usage (3) up to sizes where the O(n) scan should
/// clearly lose (32), so the report shows both the intended operating point and the crossover. As of the
/// dense-layout optimization the crossover against <see cref="Dictionary{TKey,TValue}"/> sits near 16.</para>
/// </summary>
[Config(typeof(InProcessConfig))]
public class SmallDictionaryBenchmarks
{
    // 3 is the real RaftWriteAhead.plan capacity; the rest probe the O(n)-scan crossover.
    [Params(3, 8, 16, 32)]
    public int Size;

    // Distinct int keys, filled/queried in a fixed order so both maps see identical work.
    private int[] _keys = null!;

    // Pre-populated instances reused by the lookup-only benchmarks (setup cost excluded).
    private SmallDictionary<int, int> _small = null!;
    private Dictionary<int, int> _dict = null!;

    [GlobalSetup]
    public void Setup()
    {
        _keys = new int[Size];
        for (int i = 0; i < Size; i++)
            _keys[i] = i;

        _small = new SmallDictionary<int, int>(Size);
        _dict = new Dictionary<int, int>(Size);
        for (int i = 0; i < Size; i++)
        {
            _small.Add(_keys[i], i);
            _dict.Add(_keys[i], i);
        }
    }

    // ── construct + fill (allocation axis) ───────────────────────────────────

    /// <summary>
    /// Baseline: allocate a <see cref="Dictionary{TKey,TValue}"/> pre-sized to <see cref="Size"/> and
    /// insert every key. Measures the BCL bucket/entry-array allocation plus hashed inserts.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Dictionary: construct + fill")]
    public Dictionary<int, int> Dictionary_ConstructFill()
    {
        Dictionary<int, int> d = new(Size);
        for (int i = 0; i < _keys.Length; i++)
            d.Add(_keys[i], i);
        return d;
    }

    /// <summary>
    /// Allocate a <see cref="SmallDictionary{TKey,TValue}"/> (two packed arrays) and insert every key.
    /// Each <c>Add</c> still does a linear scan to reject duplicates, so fill is O(n²) — but over a
    /// contiguous key array rather than an array of buckets.
    /// </summary>
    [Benchmark(Description = "SmallDictionary: construct + fill")]
    public SmallDictionary<int, int> SmallDictionary_ConstructFill()
    {
        SmallDictionary<int, int> d = new(Size);
        for (int i = 0; i < _keys.Length; i++)
            d.Add(_keys[i], i);
        return d;
    }

    // ── lookup throughput (CPU axis) ─────────────────────────────────────────

    /// <summary>
    /// Hashed lookup of every key in a pre-populated <see cref="Dictionary{TKey,TValue}"/>.
    /// </summary>
    [Benchmark(Description = "Dictionary: TryGetValue all keys")]
    public int Dictionary_Lookup()
    {
        int sum = 0;
        for (int i = 0; i < _keys.Length; i++)
            if (_dict.TryGetValue(_keys[i], out int v))
                sum += v;
        return sum;
    }

    /// <summary>
    /// Linear-scan lookup of every key in a pre-populated <see cref="SmallDictionary{TKey,TValue}"/>.
    /// Each query scans the packed, live key slice; total work across all keys is O(n²).
    /// </summary>
    [Benchmark(Description = "SmallDictionary: TryGetValue all keys")]
    public int SmallDictionary_Lookup()
    {
        int sum = 0;
        for (int i = 0; i < _keys.Length; i++)
            if (_small.TryGetValue(_keys[i], out int v))
                sum += v;
        return sum;
    }
}
