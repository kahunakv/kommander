using BenchmarkDotNet.Attributes;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL.Data;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Benchmarks the per-partition operation-list allocation inside the scheduler flush loop.
///
/// In the old path, <c>ProcessGroupBatch</c> allocates one <c>new List&lt;WALWriteOperation&gt;</c>
/// per drained partition and one <c>new List&lt;(int, List&lt;RaftLog&gt;)&gt;</c> for the combined
/// log-groups passed to <c>walAdapter.Write</c>. With many partitions coalesced into a single group
/// commit, this is one GC allocation per partition per fsync — steady Gen-0 pressure on the exact
/// path group-commit is designed to smooth.
///
/// The new path pre-allocates both lists once per worker thread at startup and clears+reuses them
/// each iteration, paying zero allocation on the hot flush path regardless of group-batch size.
///
/// Expected signal: the old path allocates bytes proportional to <see cref="PartitionsPerGroupBatch"/>;
/// the new path allocates 0 B per iteration.
/// </summary>
[Config(typeof(InProcessConfig))]
public class WalSchedulerListBenchmarks
{
    /// <summary>Simulates the number of partitions coalesced into one group-commit batch.</summary>
    [Params(1, 8, 64)]
    public int PartitionsPerGroupBatch;

    private const int MaxBatchSize = 256;

    // Pre-allocated per-partition op lists — cleared and reused by the new path.
    private List<WALWriteOperation>[] _opListPool = null!;
    // Pre-allocated logGroups list — cleared and reused by the new path.
    private List<(int, List<RaftLog>)> _logGroupsPool = null!;

    [GlobalSetup]
    public void Setup()
    {
        _opListPool = new List<WALWriteOperation>[PartitionsPerGroupBatch];
        for (int i = 0; i < PartitionsPerGroupBatch; i++)
            _opListPool[i] = new List<WALWriteOperation>(MaxBatchSize);
        _logGroupsPool = new List<(int, List<RaftLog>)>(PartitionsPerGroupBatch);
    }

    /// <summary>
    /// Old path: allocates one new <c>List&lt;WALWriteOperation&gt;</c> per partition and one
    /// new <c>List&lt;(int, List&lt;RaftLog&gt;)&gt;</c> for the combined write plan — exactly what
    /// <c>ProcessGroupBatch</c> used to do on every flush.
    /// </summary>
    [Benchmark(Baseline = true, Description = "old: new list per partition + new logGroups")]
    public List<(int, List<RaftLog>)> Old_AllocatePerBatch()
    {
        List<(int, List<RaftLog>)> logGroups = new(PartitionsPerGroupBatch);
        for (int i = 0; i < PartitionsPerGroupBatch; i++)
        {
            List<WALWriteOperation> pidBatch = new(MaxBatchSize);
            _ = pidBatch; // populated in real code; allocation is what we're measuring
        }
        return logGroups;
    }

    /// <summary>
    /// New path: clears pre-allocated lists instead of allocating — zero heap allocation on
    /// the hot flush path.
    /// </summary>
    [Benchmark(Description = "new: clear pre-allocated lists")]
    public List<(int, List<RaftLog>)> New_ClearPreAllocated()
    {
        _logGroupsPool.Clear();
        for (int i = 0; i < PartitionsPerGroupBatch; i++)
            _opListPool[i].Clear();
        return _logGroupsPool;
    }
}

/// <summary>
/// Benchmarks the minimum-log-id computation performed once per operation in
/// <c>FairWalScheduler.BuildCompletion</c>.
///
/// The old path calls <c>Enumerable.Min</c> on the log list, which allocates a delegate
/// and iterates via the LINQ pipeline. The new path uses a direct manual loop with no
/// allocation.
///
/// Expected signal: the new path shows 0 B allocated vs the old path's per-call delegate
/// allocation from <c>Enumerable.Min</c>, with proportionally faster throughput on short lists.
/// </summary>
[Config(typeof(InProcessConfig))]
public class WalSchedulerMinBenchmarks
{
    [Params(1, 8, 64, 256)]
    public int LogCount;

    private List<RaftLog> _logs = null!;

    [GlobalSetup]
    public void Setup()
    {
        _logs = new List<RaftLog>(LogCount);
        for (int i = 0; i < LogCount; i++)
            _logs.Add(new RaftLog
            {
                Id = LogCount - i, // descending so Min is not the first element
                Term = 1,
                Type = RaftLogType.Proposed,
                Time = new HLCTimestamp(0, 0, 0),
                LogType = "bench",
                LogData = []
            });
    }

    [Benchmark(Baseline = true, Description = "old: Enumerable.Min")]
    public long Old_LinqMin() => _logs.Count > 0 ? _logs.Min(l => l.Id) : -1;

    [Benchmark(Description = "new: manual loop")]
    public long New_ManualMin()
    {
        List<RaftLog> logs = _logs;
        if (logs.Count == 0)
            return -1;
        long min = logs[0].Id;
        for (int i = 1; i < logs.Count; i++)
            if (logs[i].Id < min) min = logs[i].Id;
        return min;
    }
}
