using System.Diagnostics;
using System.Reflection;
using System.Runtime;
using System.Runtime.InteropServices;
using CommandLine;
using Kommander;
using Kommander.Benchmark;
using Kommander.Data;
using Kommander.Diagnostics;

// Kommander.Benchmark — one arm per process.
// Build → cluster → ready-wait → warm-up → window → report → dispose. Never run it at the same
// time as `dotnet test` or another benchmark: both are timing-sensitive and share the CPU.

ParserResult<BenchmarkOptions> parsed = Parser.Default.ParseArguments<BenchmarkOptions>(args);
if (parsed.Value is not { } options)
    return 2;

if (options.Validate() is { } error)
{
    Console.Error.WriteLine($"[Kommander.Benchmark] {error}");
    return 2;
}

if (options.Fit is not null)
{
    List<FitResult> fits = BenchmarkReport.Fit(options.Fit);
    if (fits.Count == 0)
    {
        Console.Error.WriteLine($"[Kommander.Benchmark] {options.Fit}: no group with two or more batch sizes to fit.");
        return 1;
    }

    foreach (FitResult fit in fits)
    {
        Console.WriteLine(BenchmarkReport.FitSummary(fit));
        if (options.Output is not null)
            await File.AppendAllTextAsync(options.Output, BenchmarkReport.ToJsonLine(fit) + Environment.NewLine);
    }

    return 0;
}

TimeSpan warmup = BenchmarkOptions.ParseDuration(options.Warmup)!.Value;
TimeSpan duration = BenchmarkOptions.ParseDuration(options.Duration)!.Value;

// Enough workers from the start for 128 closed-loop writers plus the nodes' own work: thread-pool
// injection during the warm-up would otherwise show up as a slow first window.
ThreadPool.SetMinThreads(Math.Max(64, options.Concurrency + 32), 64);

using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder
    .AddSimpleConsole(o => o.SingleLine = true)
    .SetMinimumLevel(options.Verbose ? LogLevel.Information : LogLevel.Warning));

Console.Error.WriteLine($"[Kommander.Benchmark] starting {options.Nodes} nodes, transport={options.TransportLabel}, storage={options.Storage} …");

await using BenchmarkCluster cluster = await BenchmarkCluster.StartAsync(options, loggerFactory, TimeSpan.FromSeconds(60));

using StageCollector? stages = options.Stages == false ? null : new StageCollector();

WorkloadRunner runner = new(cluster, options);
Process process = Process.GetCurrentProcess();

TimeSpan cpuStart = default, cpuEnd = default;
long allocStart = 0, allocEnd = 0;
TimeSpan pauseStart = default, pauseEnd = default;
int[] gcStart = new int[3], gcEnd = new int[3];
long acksStart = 0, acksEnd = 0;
Dictionary<string, CountingCommunication.TrafficSnapshot> trafficStart = [], trafficEnd = [];
WalCounters walStart = default, walEnd = default;

(List<WorkloadRunner.WorkerResult> workers, TimeSpan window) = await runner.RunAsync(
    warmup,
    duration,
    onWindowStart: () =>
    {
        WalPhaseInstrumentation.Reset();
        WalPhaseInstrumentation.Enabled = true;
        if (stages is not null)
            stages.Measuring = true;

        (trafficStart, acksStart) = SnapshotTraffic(cluster);
        walStart = SnapshotWal(cluster);
        process.Refresh();
        cpuStart = process.TotalProcessorTime;
        allocStart = GC.GetTotalAllocatedBytes(precise: false);
        pauseStart = GC.GetTotalPauseDuration();
        for (int g = 0; g < 3; g++)
            gcStart[g] = GC.CollectionCount(g);
    },
    onWindowEnd: () =>
    {
        process.Refresh();
        cpuEnd = process.TotalProcessorTime;
        allocEnd = GC.GetTotalAllocatedBytes(precise: false);
        pauseEnd = GC.GetTotalPauseDuration();
        for (int g = 0; g < 3; g++)
            gcEnd[g] = GC.CollectionCount(g);
        (trafficEnd, acksEnd) = SnapshotTraffic(cluster);
        walEnd = SnapshotWal(cluster);

        if (stages is not null)
            stages.Measuring = false;
        WalPhaseInstrumentation.Enabled = false;
    });

// ── Aggregate ────────────────────────────────────────────────────────────────

long calls = workers.Sum(w => w.Calls);
long entries = workers.Sum(w => w.Entries);
long failed = workers.Sum(w => w.Failed);

Dictionary<string, long> failedByStatus = [];
foreach (WorkloadRunner.WorkerResult w in workers)
    foreach ((RaftOperationStatus status, long count) in w.FailedByStatus)
        failedByStatus[status.ToString()] = failedByStatus.GetValueOrDefault(status.ToString()) + count;

double ticksToMs = 1000.0 / Stopwatch.Frequency;
double[] latencies = [.. workers.SelectMany(w => w.LatencyTicks).Select(t => t * ticksToMs)];
Array.Sort(latencies);

LatencyStats round = new(
    latencies.Length > 0 ? latencies.Average() : 0,
    Percentiles.Of(latencies, 0.50),
    Percentiles.Of(latencies, 0.90),
    Percentiles.Of(latencies, 0.99),
    Percentiles.Of(latencies, 0.999),
    latencies.Length > 0 ? latencies[^1] : 0);

double seconds = window.TotalSeconds;
int followers = options.Nodes - 1;

CountingCommunication.TrafficSnapshot traffic = default;
foreach ((string endpoint, CountingCommunication.TrafficSnapshot end) in trafficEnd)
    traffic = Add(traffic, end - trafficStart.GetValueOrDefault(endpoint));

// A single-voter arm has no follower: the per-follower figures are 0, not a division by zero
// (NaN does not serialize to JSON).
bool replicated = calls > 0 && followers > 0;

TrafficStats trafficStats = new(
    replicated ? (double)traffic.Frames / calls / followers : 0,
    traffic.Frames > 0 ? (double)traffic.Entries / traffic.Frames : 0,
    replicated ? (double)traffic.LogBytes / calls / followers : 0,
    calls > 0 ? (double)(acksEnd - acksStart) / calls : 0,
    traffic.Heartbeats / seconds);

double cpuSeconds = (cpuEnd - cpuStart).TotalSeconds;

CostStats cost = new(
    cpuSeconds / seconds,
    entries > 0 ? cpuSeconds * 1e6 / entries : 0,
    entries > 0 ? (double)(allocEnd - allocStart) / entries : 0,
    (pauseEnd - pauseStart).TotalSeconds / seconds,
    gcEnd[0] - gcStart[0],
    gcEnd[1] - gcStart[1],
    gcEnd[2] - gcStart[2]);

WalCounters walDelta = walEnd - walStart;

WalSchedulerStats? walStats = calls == 0 ? null : new(
    (double)walDelta.Writes / calls,
    (double)walDelta.SyncedWrites / calls,
    walDelta.Writes > 0 ? (double)walDelta.Partitions / walDelta.Writes : 0,
    (double)walDelta.MarkerBatchesHeld / calls,
    (double)walDelta.ReturnGathers / calls,
    walDelta.ReturnsAwaited,
    walDelta.ReturnsMissed,
    walDelta.GatherHandoffs);

string version = typeof(RaftManager).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
    ?? typeof(RaftManager).Assembly.GetName().Version?.ToString()
    ?? "unknown";

BenchmarkResult result = new(
    "result",
    options.Label,
    DateTime.UtcNow,
    new HostInfo(
        Environment.ProcessorCount,
        RuntimeInformation.OSDescription,
        RuntimeInformation.ProcessArchitecture.ToString(),
        RuntimeInformation.FrameworkDescription,
        version,
        GCSettings.IsServerGC),
    new RunConfig(
        options.Nodes,
        options.Partitions,
        options.TransportLabel,
        options.Storage,
        options.Storage != "memory" && (options.SyncWrites ?? true),
        options.Storage == "memory" ? null : options.WalDir ?? Path.GetTempPath(),
        options.PayloadBytes,
        options.BatchSize,
        options.Concurrency,
        warmup.TotalSeconds,
        seconds,
        options.Seed,
        options.FanOutBeforeLocalWrite ?? true,
        options.Set.ToList(),
        options.ApplyCostMicros,
        options.ApplyOn,
        options.SimSyncMs),
    entries / seconds,
    calls / seconds,
    entries,
    calls,
    failed,
    failedByStatus,
    round,
    trafficStats,
    cost,
    WalPhaseInstrumentation.Snapshot(),
    stages?.Snapshot(),
    await cluster.LeaderDistribution(options.Partitions),
    walStats);

Console.WriteLine(BenchmarkReport.Summary(result));

string json = BenchmarkReport.ToJsonLine(result);

if (options.Json)
    Console.WriteLine(json);

if (options.Output is not null)
    await File.AppendAllTextAsync(options.Output, json + Environment.NewLine);

// A run that drops most of its calls describes the failure path, not the round.
if (calls == 0 || failed > calls / 10)
{
    Console.Error.WriteLine($"[Kommander.Benchmark] {failed} failed calls against {calls} committed: the numbers above are not a steady-state round.");
    return 1;
}

return 0;

static (Dictionary<string, CountingCommunication.TrafficSnapshot>, long) SnapshotTraffic(BenchmarkCluster cluster)
{
    Dictionary<string, CountingCommunication.TrafficSnapshot> appends = [];
    long acks = 0;

    for (int i = 0; i < cluster.Communications.Count; i++)
    {
        (IReadOnlyDictionary<string, CountingCommunication.TrafficSnapshot> nodeAppends, long nodeAcks) = cluster.Communications[i].Snapshot();
        acks += nodeAcks;

        // Keyed by sender → target so two senders to one target stay apart.
        string sender = cluster.Managers[i].GetLocalEndpoint();
        foreach ((string target, CountingCommunication.TrafficSnapshot counts) in nodeAppends)
            appends[$"{sender}->{target}"] = counts;
    }

    return (appends, acks);
}

// The counters of every node's WAL write scheduler, summed. A scheduler of another type (none
// today) contributes nothing.
static WalCounters SnapshotWal(BenchmarkCluster cluster)
{
    WalCounters total = default;

    foreach (RaftManager manager in cluster.Managers)
    {
        if (manager.WalScheduler is not Kommander.WAL.IO.FairWalScheduler scheduler)
            continue;

        total = new(
            total.Writes + scheduler.TotalBatchesWritten,
            total.SyncedWrites + scheduler.TotalSyncBatchesWritten,
            total.Partitions + scheduler.TotalPartitionsBatched,
            total.MarkerBatchesHeld + scheduler.TotalMarkerBatchesDeferred,
            total.ReturnGathers + scheduler.TotalReturnGathers,
            total.ReturnsAwaited + scheduler.TotalReturnsAwaited,
            total.ReturnsMissed + scheduler.TotalReturnsMissed,
            total.GatherHandoffs + scheduler.TotalGatherHandoffs);
    }

    return total;
}

static CountingCommunication.TrafficSnapshot Add(CountingCommunication.TrafficSnapshot a, CountingCommunication.TrafficSnapshot b) =>
    new(a.Frames + b.Frames, a.Heartbeats + b.Heartbeats, a.Entries + b.Entries, a.LogBytes + b.LogBytes);

/// <summary>Counters of the WAL write schedulers at one edge of the window.</summary>
readonly record struct WalCounters(
    long Writes,
    long SyncedWrites,
    long Partitions,
    long MarkerBatchesHeld,
    long ReturnGathers,
    long ReturnsAwaited,
    long ReturnsMissed,
    long GatherHandoffs)
{
    public static WalCounters operator -(WalCounters a, WalCounters b) => new(
        a.Writes - b.Writes,
        a.SyncedWrites - b.SyncedWrites,
        a.Partitions - b.Partitions,
        a.MarkerBatchesHeld - b.MarkerBatchesHeld,
        a.ReturnGathers - b.ReturnGathers,
        a.ReturnsAwaited - b.ReturnsAwaited,
        a.ReturnsMissed - b.ReturnsMissed,
        a.GatherHandoffs - b.GatherHandoffs);
}
