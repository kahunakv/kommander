using System.Diagnostics;
using Kommander.Data;

namespace Kommander.Benchmark;

/// <summary>
/// Drives the closed-loop write workload: <c>concurrency</c> workers, each calling the batch overload
/// of <see cref="RaftManager.ReplicateLogs(int, string, IReadOnlyList{byte[]}, bool, long, long, CancellationToken)"/>
/// on the current leader in a loop, with <c>autoCommit: true</c> so a call covers the full
/// propose → replicate → commit path.
///
/// <para><b>Window rule.</b> A call counts when it starts at or after the window start and completes
/// before the stop signal. Calls in flight at either edge are dropped, not pro-rated. With rounds of
/// a few milliseconds and a window of seconds, the loss is far below the run-to-run noise.</para>
///
/// <para><b>No shared hot-path state.</b> Each worker keeps its own latency list and counters; they
/// are merged after the run. A shared histogram would add contention that the round does not
/// have.</para>
/// </summary>
public sealed class WorkloadRunner
{
    public const string LogType = "bench";

    private readonly BenchmarkCluster cluster;

    private readonly BenchmarkOptions options;

    private readonly byte[][] batch;

    private long windowStartTicks = long.MaxValue;

    private volatile bool stopping;

    public WorkloadRunner(BenchmarkCluster cluster, BenchmarkOptions options)
    {
        this.cluster = cluster;
        this.options = options;

        // One payload reused by every entry: allocation of the payload is not part of the round.
        Random random = new(options.Seed);
        byte[] payload = new byte[options.PayloadBytes];
        random.NextBytes(payload);

        batch = new byte[options.BatchSize][];
        for (int i = 0; i < batch.Length; i++)
            batch[i] = payload;
    }

    /// <summary>Per-worker results, merged by <see cref="RunAsync"/>.</summary>
    public sealed class WorkerResult
    {
        public List<long> LatencyTicks { get; } = new(1 << 14);
        public long Calls;
        public long Entries;
        public long Failed;
        public Dictionary<RaftOperationStatus, long> FailedByStatus { get; } = [];
    }

    /// <summary>
    /// Starts the workers, runs the warm-up, calls <paramref name="onWindowStart"/>, runs the window,
    /// calls <paramref name="onWindowEnd"/> and stops the workers. Returns the merged worker results
    /// and the window length. The callbacks run on the caller, between the phases, so the caller can
    /// snapshot CPU, allocation and instrumentation at the window edges.
    /// </summary>
    public async Task<(List<WorkerResult> Workers, TimeSpan Window)> RunAsync(
        TimeSpan warmup,
        TimeSpan duration,
        Action onWindowStart,
        Action onWindowEnd)
    {
        List<WorkerResult> results = [.. Enumerable.Range(0, options.Concurrency).Select(_ => new WorkerResult())];
        Task[] workers = [.. results.Select((r, i) => Task.Run(() => WorkerAsync(i, r)))];

        await Task.Delay(warmup).ConfigureAwait(false);

        onWindowStart();
        long start = Stopwatch.GetTimestamp();
        Volatile.Write(ref windowStartTicks, start);

        await Task.Delay(duration).ConfigureAwait(false);

        stopping = true;
        TimeSpan window = Stopwatch.GetElapsedTime(start);
        onWindowEnd();

        await Task.WhenAll(workers).ConfigureAwait(false);

        return (results, window);
    }

    private async Task WorkerAsync(int index, WorkerResult result)
    {
        int partitionId = 1 + index % options.Partitions;
        RaftManager? leader = null;
        int consecutiveFailures = 0;

        while (!stopping)
        {
            leader ??= await cluster.LeaderFor(partitionId).ConfigureAwait(false);

            if (leader is null)
            {
                await Task.Delay(10).ConfigureAwait(false);
                continue;
            }

            long t0 = Stopwatch.GetTimestamp();
            RaftReplicationResult reply;

            try
            {
                reply = await leader.ReplicateLogs(partitionId, LogType, batch, autoCommit: true).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is RaftException or OperationCanceledException)
            {
                reply = new(false, RaftOperationStatus.Errored, default, -1);
            }

            long t1 = Stopwatch.GetTimestamp();
            bool inWindow = t0 >= Volatile.Read(ref windowStartTicks) && !stopping;

            if (reply.Success)
            {
                consecutiveFailures = 0;

                if (!inWindow)
                    continue;

                result.LatencyTicks.Add(t1 - t0);
                result.Calls++;
                result.Entries += batch.Length;
                continue;
            }

            // A failed call re-resolves the leader: the likely cause is a leader change.
            leader = null;

            if (inWindow)
            {
                result.Failed++;
                result.FailedByStatus[reply.Status] = result.FailedByStatus.GetValueOrDefault(reply.Status) + 1;
            }

            if (++consecutiveFailures > 1000)
                throw new InvalidOperationException($"Worker {index}: 1000 consecutive failed calls, last status {reply.Status}. Aborting the run.");
        }
    }
}
