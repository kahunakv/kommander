using CommandLine;

namespace Kommander.Benchmark;

/// <summary>
/// The CLI contract of one benchmark run. One process runs one arm: one transport, one storage backend, one batch size,
/// one concurrency. A sweep is a wrapper script that invokes the exe once per arm and then calls
/// <c>--fit</c> over the collected JSON lines.
///
/// <para>The defaults are the arm the round-cost feature asks for: three nodes, one partition, 280 B
/// entries (the measured CamusDB entry size), gRPC with mutual TLS.</para>
/// </summary>
public sealed class BenchmarkOptions
{
    [Option("nodes", Default = 3, HelpText = "Number of Raft nodes in the cluster (>= 2).")]
    public int Nodes { get; set; }

    [Option("partitions", Default = 1, HelpText = "InitialPartitions. The workload targets user partitions 1..N.")]
    public int Partitions { get; set; }

    [Option("transport", Default = "grpc", HelpText = "grpc (real Kestrel/HTTP2 sockets on loopback) | memory (InMemoryCommunication, engine-only control).")]
    public string Transport { get; set; } = "grpc";

    [Option("plaintext", Default = false, HelpText = "gRPC arm only: http:// with node authentication off, instead of mutual TLS.")]
    public bool Plaintext { get; set; }

    [Option("storage", Default = "memory", HelpText = "WAL backend: memory | rocksdb | sqlite.")]
    public string Storage { get; set; } = "memory";

    [Option("wal-dir", Default = null, HelpText = "Parent directory of the per-node WAL directories (use tmpfs or a RAM disk to keep fsync out). Default: the OS temp path.")]
    public string? WalDir { get; set; }

    [Option("sync-writes", Default = true, HelpText = "fsync for rocksdb/sqlite (ignored for memory).")]
    public bool? SyncWrites { get; set; }

    [Option("fan-out-before-local-write", Default = true, HelpText = "RaftConfiguration.FanOutBeforeLocalWrite: send a proposal to the followers while the leader's own write is queued (false = after it is durable).")]
    public bool? FanOutBeforeLocalWrite { get; set; }

    [Option("payload-bytes", Default = 280, HelpText = "Size of each replicated entry.")]
    public int PayloadBytes { get; set; }

    [Option("batch-size", Default = 1, HelpText = "Entries per ReplicateLogs call (the batch overload).")]
    public int BatchSize { get; set; }

    [Option("concurrency", Default = 1, HelpText = "Closed-loop writer tasks. 1 = serial rounds (isolates the fixed cost).")]
    public int Concurrency { get; set; }

    [Option("duration", Default = "20s", HelpText = "Measurement window, e.g. 20s, 1m, 500ms.")]
    public string Duration { get; set; } = "20s";

    [Option("warmup", Default = "5s", HelpText = "Unmeasured warm-up before the window.")]
    public string Warmup { get; set; } = "5s";

    [Option("seed", Default = 12345, HelpText = "RNG seed for the payload bytes.")]
    public int Seed { get; set; }

    [Option("base-port", Default = 52000, HelpText = "First loopback port; node i listens on base-port + i.")]
    public int BasePort { get; set; }

    [Option("stages", Default = true, HelpText = "Collect the raft.round.stage_ms histogram (per-stage round split).")]
    public bool? Stages { get; set; }

    [Option("set", Separator = ',', HelpText = "RaftConfiguration overrides for every node, Name=Value[,Name=Value]: any settable bool, int, long, double or TimeSpan property (e.g. PartitionExecutorPoolSize=2,WriteIOThreads=2).")]
    public IEnumerable<string> Set { get; set; } = [];

    [Option("label", Default = null, HelpText = "Free-form label copied into the JSON (e.g. a build id or git sha).")]
    public string? Label { get; set; }

    [Option("json", Default = false, HelpText = "Write the JSON result line to stdout after the summary.")]
    public bool Json { get; set; }

    [Option("output", Default = null, HelpText = "Append the JSON result line to this file (JSON lines).")]
    public string? Output { get; set; }

    [Option("fit", Default = null, HelpText = "Do not run: read this JSON-lines file and print the fitted a + b·n per arm group.")]
    public string? Fit { get; set; }

    [Option("verbose", Default = false, HelpText = "Kommander logs at Information instead of Warning.")]
    public bool Verbose { get; set; }

    /// <summary>
    /// Validates the options and returns an error message, or <c>null</c> when they are usable.
    /// </summary>
    public string? Validate()
    {
        if (Fit is not null)
            return null;

        if (Nodes < 2)
            return "--nodes must be at least 2";

        if (Partitions < 1)
            return "--partitions must be at least 1";

        if (PayloadBytes < 1)
            return "--payload-bytes must be at least 1";

        if (BatchSize < 1)
            return "--batch-size must be at least 1";

        if (Concurrency < 1)
            return "--concurrency must be at least 1";

        if (Transport is not ("grpc" or "memory"))
            return "--transport must be grpc or memory";

        if (Storage is not ("memory" or "rocksdb" or "sqlite"))
            return "--storage must be memory, rocksdb or sqlite";

        if (ParseDuration(Duration) is null)
            return $"--duration '{Duration}' is not a duration";

        if (ParseDuration(Warmup) is null)
            return $"--warmup '{Warmup}' is not a duration";

        return null;
    }

    /// <summary>Parses "20s", "1m", "500ms" or a bare number of seconds.</summary>
    public static TimeSpan? ParseDuration(string value)
    {
        value = value.Trim();

        if (value.EndsWith("ms", StringComparison.OrdinalIgnoreCase) && double.TryParse(value[..^2], out double ms))
            return TimeSpan.FromMilliseconds(ms);

        if (value.EndsWith('s') && double.TryParse(value[..^1], out double s))
            return TimeSpan.FromSeconds(s);

        if (value.EndsWith('m') && double.TryParse(value[..^1], out double m))
            return TimeSpan.FromMinutes(m);

        if (double.TryParse(value, out double bare))
            return TimeSpan.FromSeconds(bare);

        return null;
    }

    /// <summary>The transport label written to the report: memory, grpc-mtls or grpc-plaintext.</summary>
    public string TransportLabel => Transport == "memory" ? "memory" : Plaintext ? "grpc-plaintext" : "grpc-mtls";
}
