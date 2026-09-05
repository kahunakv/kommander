using BenchmarkDotNet.Attributes;
using Kommander.Data;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Measures the real <see cref="FairWalScheduler.Enqueue"/> entry point — admission, per-partition
/// queueing, the group-batch drain, and the <see cref="InMemoryWAL"/> write — end to end.
/// </summary>
/// <remarks>
/// <para>
/// The existing <c>WalSchedulerListBenchmarks</c> builds and clears lists; it never calls
/// <c>Enqueue</c> and never touches a WAL backend, so it cannot show admission cost, queue accounting,
/// or the write itself. This suite calls the production method.
/// </para>
/// <para>
/// The change under measurement is the admission factory. Admission used to run
/// <c>GetOrAdd(partitionId, _ =&gt; new PartitionState(tickSource))</c>; that lambda reads a field, so
/// the compiler cannot cache it in a static, and a delegate was allocated on every enqueue — including
/// the steady state where the partition already exists. It now uses the state-argument overload with a
/// static lambda.
/// </para>
/// <para>
/// <b>Why manual execution.</b> The scheduler runs in manual mode, where <c>Enqueue</c> drains the
/// batch on the calling thread and delivers the completion before it returns. Allocation on a worker
/// thread is not attributable to the submitting thread, so a threaded configuration would report a
/// floor for this path rather than a total. Manual mode removes the worker threads, which makes the
/// reported bytes the whole cost of a submit-to-durable write. It also removes the thread-handoff
/// latency that otherwise dominates the time and makes it far too noisy to read.
/// </para>
/// <para>
/// <b>What is held constant.</b> The partition is admitted in setup, so every iteration takes the
/// steady-state path. The log list is built once and reused, and its entry keeps one log id, which the
/// backend overwrites — otherwise the partition's storage would grow through the run and the later
/// iterations would measure a different thing from the earlier ones. What each iteration does allocate
/// is what a real caller allocates: the <see cref="WALWriteOperation"/> and, until this change, the
/// admission delegate behind it.
/// </para>
/// </remarks>
[Config(typeof(InProcessConfig))]
public class WalEnqueueBenchmarks : IDisposable
{
    private const int PartitionId = 1;

    private InMemoryWAL _wal = null!;
    private FairWalScheduler _scheduler = null!;
    private List<RaftLog> _logs = null!;
    private ManualResetEventSlim _completed = null!;
    private long _operationId;
    private RaftOperationStatus _lastStatus;

    [GlobalSetup]
    public void Setup()
    {
        _wal = new InMemoryWAL(NullLogger<IRaft>.Instance);
        _scheduler = new FairWalScheduler(_wal, NullLogger<IRaft>.Instance, workerCount: 2);
        _scheduler.Start();

        // Reused rather than allocated per iteration: the wait is the benchmark's own scaffolding, not
        // something a real caller of Enqueue does.
        _completed = new ManualResetEventSlim(false);

        _logs =
        [
            new()
            {
                Id = 1,
                Type = RaftLogType.Proposed,
                Term = 1,
                LogType = "bench",
                LogData = [],
            },
        ];

        // Admit the partition once so the benchmark never pays first admission.
        Enqueue();
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    /// <summary>
    /// One submit-to-durable write on an already-admitted partition — the path every replicated write
    /// takes once a partition is live.
    /// </summary>
    [Benchmark(Description = "enqueue on an already-admitted partition")]
    public RaftOperationStatus Enqueue()
    {
        _completed.Reset();

        WALWriteOperation operation = new(
            onComplete: OnComplete,
            operationId: Interlocked.Increment(ref _operationId),
            type: WALWriteOperationType.LeaderPropose,
            logs: (PartitionId, _logs),
            term: 1,
            logIndex: 1);

        _scheduler.Enqueue(operation);
        _completed.Wait();

        return _lastStatus;
    }

    private void OnComplete(RaftWalCompletion completion)
    {
        _lastStatus = completion.Status;
        _completed.Set();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        // Dispose stops the scheduler itself, so a separate Stop would only repeat the work.
        _scheduler?.Dispose();
        _wal?.Dispose();
        _completed?.Dispose();
    }
}
