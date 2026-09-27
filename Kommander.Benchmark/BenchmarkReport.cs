using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kommander.Diagnostics;

namespace Kommander.Benchmark;

/// <summary>The run context: what was measured, on what.</summary>
public sealed record RunConfig(
    int Nodes,
    int Partitions,
    string Transport,
    string Storage,
    bool SyncWrites,
    string? WalDir,
    int PayloadBytes,
    int BatchSize,
    int Concurrency,
    double WarmupSeconds,
    double WindowSeconds,
    int Seed);

/// <summary>The machine and build the run used.</summary>
public sealed record HostInfo(int Cores, string Os, string Arch, string Framework, string KommanderVersion, bool ServerGc);

/// <summary>Per-call round latency over the window, in milliseconds.</summary>
public sealed record LatencyStats(double MeanMs, double P50Ms, double P90Ms, double P99Ms, double P999Ms, double MaxMs);

/// <summary>
/// Replication traffic per proposal. Frames and bytes are per follower (averaged over the
/// followers); acks are totals per proposal. Log bytes leave out framing (see
/// <see cref="CountingCommunication"/>).
/// </summary>
public sealed record TrafficStats(
    double FramesPerProposalPerFollower,
    double EntriesPerFrame,
    double LogBytesPerProposalPerFollower,
    double AcksPerProposal,
    double HeartbeatsPerSecond);

/// <summary>
/// CPU and allocation of the whole process (all nodes) over the window. Process cores is CPU time
/// divided by window time.
/// </summary>
public sealed record CostStats(
    double ProcessCores,
    double CpuMicrosPerEntry,
    double AllocBytesPerEntry,
    double GcPauseFraction,
    int Gen0,
    int Gen1,
    int Gen2);

/// <summary>One benchmark arm's result: one JSON line.</summary>
public sealed record BenchmarkResult(
    string Kind,
    string? Label,
    DateTime Utc,
    HostInfo Host,
    RunConfig Config,
    double EntriesPerSecond,
    double ProposalsPerSecond,
    long CommittedEntries,
    long Calls,
    long Failed,
    Dictionary<string, long> FailedByStatus,
    LatencyStats Round,
    TrafficStats Traffic,
    CostStats Cost,
    InstrumentationSnapshot? WalPhases,
    Dictionary<string, StageStats>? Stages,
    Dictionary<string, int> LeadersPerNode);

/// <summary>A fitted per-proposal cost model: round = a + b·n over the batch-size arms of one group.</summary>
public sealed record FitResult(
    string Kind,
    string Group,
    int Points,
    double FixedMs,
    double PerEntryMicros,
    double RSquared,
    List<FitArm> Arms);

/// <summary>One batch-size arm inside a fit.</summary>
public sealed record FitArm(int BatchSize, double RoundMeanMs, double EntriesPerSecond);

/// <summary>
/// Renders results as a human summary and as JSON lines, and fits a + b·n over a JSON-lines file.
/// </summary>
public static class BenchmarkReport
{
    public static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() },
    };

    public static string ToJsonLine(object value) => JsonSerializer.Serialize(value, Json);

    public static string Summary(BenchmarkResult r)
    {
        StringBuilder sb = new();
        RunConfig c = r.Config;

        sb.AppendLine("Kommander.Benchmark");
        sb.AppendLine(Inv($"  nodes={c.Nodes} partitions={c.Partitions} transport={c.Transport} storage={c.Storage} sync={c.SyncWrites} payload={c.PayloadBytes}B batch={c.BatchSize} concurrency={c.Concurrency}"));
        sb.AppendLine(Inv($"  warmup={c.WarmupSeconds:0.#}s window={c.WindowSeconds:0.#}s host={r.Host.Cores} cores {r.Host.Os} {r.Host.Arch} {r.Host.Framework} kommander={r.Host.KommanderVersion}"));
        sb.AppendLine();
        sb.AppendLine(Inv($"  Throughput : {r.EntriesPerSecond:N0} entries/s   ({r.ProposalsPerSecond:N0} proposals/s)"));
        sb.AppendLine(Inv($"  Round      : mean={r.Round.MeanMs:0.000}ms p50={r.Round.P50Ms:0.000} p90={r.Round.P90Ms:0.000} p99={r.Round.P99Ms:0.000} p99.9={r.Round.P999Ms:0.000} max={r.Round.MaxMs:0.000}"));
        sb.AppendLine(Inv($"  Committed  : {r.CommittedEntries:N0} entries in {r.Calls:N0} calls   Failed: {r.Failed}"));
        sb.AppendLine(Inv($"  Traffic    : {r.Traffic.FramesPerProposalPerFollower:0.00} frames/proposal/follower, {r.Traffic.EntriesPerFrame:0.0} entries/frame, {r.Traffic.LogBytesPerProposalPerFollower:N0} log B/proposal/follower, {r.Traffic.AcksPerProposal:0.00} acks/proposal"));
        sb.AppendLine(Inv($"  Cost       : {r.Cost.ProcessCores:0.00} cores, {r.Cost.CpuMicrosPerEntry:0.0} µs CPU/entry, {r.Cost.AllocBytesPerEntry:N0} B alloc/entry, GC pause {r.Cost.GcPauseFraction:P1} (gen0 {r.Cost.Gen0}, gen1 {r.Cost.Gen1}, gen2 {r.Cost.Gen2})"));

        if (r.Stages is { Count: > 0 } stages)
        {
            sb.AppendLine();
            sb.AppendLine("  Round stages (raft.round.stage_ms; leader chain in order, then its split):");
            foreach (string name in RoundStageInstrumentation.AllStageNames)
            {
                if (stages.TryGetValue(name, out StageStats? s))
                    sb.AppendLine(Inv($"    {name,-24} n={s.Count,9:N0}  mean={s.MeanMs,8:0.000}  p50={s.P50Ms,8:0.000}  p99={s.P99Ms,8:0.000} ms"));
            }

            string[] chain = ["leader.queue", "leader.propose", "leader.wal", "leader.wal_completion", "leader.fanout", "leader.replication", "leader.resume"];
            double chainSum = chain.Sum(n => stages.TryGetValue(n, out StageStats? s) ? s.MeanMs : 0);
            if (stages.TryGetValue("leader.round", out StageStats? round))
                sb.AppendLine(Inv($"    leader chain sum {chainSum:0.000} ms of round {round.MeanMs:0.000} ms (rest: gateway and waiter registration)"));
        }

        return sb.ToString();
    }

    /// <summary>
    /// Reads a JSON-lines file of <see cref="BenchmarkResult"/>s and fits round = a + b·n (least
    /// squares on the per-arm round means) for each group of arms that differ only in batch size.
    /// </summary>
    public static List<FitResult> Fit(string path)
    {
        List<BenchmarkResult> results = [];

        foreach (string line in File.ReadLines(path))
        {
            if (string.IsNullOrWhiteSpace(line) || !line.Contains("\"kind\":\"result\"", StringComparison.Ordinal))
                continue;

            BenchmarkResult? result = JsonSerializer.Deserialize<BenchmarkResult>(line, Json);
            if (result is not null)
                results.Add(result);
        }

        List<FitResult> fits = [];

        foreach (IGrouping<string, BenchmarkResult> group in results.GroupBy(GroupKey).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            // The latest run of each batch size wins, so a re-run arm replaces its earlier line.
            List<BenchmarkResult> arms = [.. group.GroupBy(r => r.Config.BatchSize).Select(g => g.OrderBy(r => r.Utc).Last()).OrderBy(r => r.Config.BatchSize)];
            if (arms.Count < 2)
                continue;

            double[] x = [.. arms.Select(r => (double)r.Config.BatchSize)];
            double[] y = [.. arms.Select(r => r.Round.MeanMs)];
            (double a, double b, double r2) = LeastSquares(x, y);

            fits.Add(new(
                "fit",
                group.Key,
                arms.Count,
                a,
                b * 1000.0,
                r2,
                [.. arms.Select(r => new FitArm(r.Config.BatchSize, r.Round.MeanMs, r.EntriesPerSecond))]));
        }

        return fits;
    }

    public static string FitSummary(FitResult f)
    {
        StringBuilder sb = new();
        sb.AppendLine(Inv($"{f.Group}"));
        sb.AppendLine(Inv($"  round ≈ {f.FixedMs:0.000} ms + {f.PerEntryMicros:0.00} µs × n   (R² {f.RSquared:0.000}, {f.Points} arms)"));
        foreach (FitArm arm in f.Arms)
            sb.AppendLine(Inv($"    n={arm.BatchSize,4}  round={arm.RoundMeanMs,8:0.000} ms  {arm.EntriesPerSecond,10:N0} entries/s"));
        return sb.ToString();
    }

    private static string GroupKey(BenchmarkResult r)
    {
        RunConfig c = r.Config;
        return Inv($"label={r.Label ?? "-"} transport={c.Transport} storage={c.Storage} sync={c.SyncWrites} nodes={c.Nodes} partitions={c.Partitions} payload={c.PayloadBytes}B concurrency={c.Concurrency}");
    }

    private static (double A, double B, double R2) LeastSquares(double[] x, double[] y)
    {
        int n = x.Length;
        double meanX = x.Average();
        double meanY = y.Average();

        double sxx = 0, sxy = 0, syy = 0;
        for (int i = 0; i < n; i++)
        {
            sxx += (x[i] - meanX) * (x[i] - meanX);
            sxy += (x[i] - meanX) * (y[i] - meanY);
            syy += (y[i] - meanY) * (y[i] - meanY);
        }

        double b = sxx > 0 ? sxy / sxx : 0;
        double a = meanY - b * meanX;
        double r2 = syy > 0 ? sxy * sxy / (sxx * syy) : 1;
        return (a, b, r2);
    }

    private static string Inv(FormattableString s) => s.ToString(CultureInfo.InvariantCulture);
}
