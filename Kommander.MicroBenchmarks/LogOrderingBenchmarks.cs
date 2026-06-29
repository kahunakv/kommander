using BenchmarkDotNet.Attributes;
using Kommander;
using Kommander.Data;
using Kommander.Time;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Compares the previous unconditional <c>logs.OrderBy(l =&gt; l.Id).ToArray()</c> against the
/// allocation-free <see cref="RaftWriteAhead.OrderById"/> fast path on write-path batches.
/// The expected signal:
/// <list type="bullet">
///   <item><description>
///     <b>Sorted input</b> (the common case — callers build batches in id order): the new path
///     returns the original list and allocates <b>0 bytes</b>, while the old path always allocates
///     a new array. The gap widens with batch size.
///   </description></item>
///   <item><description>
///     <b>Unsorted input</b>: the new path falls back to the same stable <c>OrderBy(...).ToArray()</c>,
///     so allocations/time should be on par with the old path (plus one cheap ordered-scan).
///   </description></item>
/// </list>
/// </summary>
[Config(typeof(InProcessConfig))]
public class LogOrderingBenchmarks
{
    [Params(1, 8, 64, 256)]
    public int BatchSize;

    /// <summary>
    /// When <see langword="true"/> the input is already ascending by id (fast path); when
    /// <see langword="false"/> it is strictly descending so <c>OrderById</c> takes the sort fallback.
    /// </summary>
    [Params(true, false)]
    public bool Sorted;

    private List<RaftLog> _logs = null!;

    [GlobalSetup]
    public void Setup() => _logs = BuildLogs(BatchSize, Sorted);

    [Benchmark(Baseline = true, Description = "old: OrderBy(...).ToArray()")]
    public IReadOnlyList<RaftLog> Old_OrderByToArray() => _logs.OrderBy(static l => l.Id).ToArray();

    [Benchmark(Description = "new: OrderById")]
    public IReadOnlyList<RaftLog> New_OrderById() => RaftWriteAhead.OrderById(_logs);

    private static List<RaftLog> BuildLogs(int count, bool sorted)
    {
        List<RaftLog> logs = new(count);

        // Ascending ids for the fast path; strictly descending for the fallback. Descending (rather
        // than a random shuffle) guarantees OrderById's ordered-scan hits a descending step on the
        // first comparison for count > 1, so the fallback is always exercised deterministically.
        for (int i = 0; i < count; i++)
        {
            long id = sorted ? i : count - 1 - i;
            logs.Add(new RaftLog
            {
                Id = id,
                Term = 1,
                Type = RaftLogType.Proposed,
                Time = new HLCTimestamp(0, 0, 0),
                LogType = "bench",
                LogData = []
            });
        }

        return logs;
    }
}
