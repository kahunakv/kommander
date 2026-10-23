using Kommander.Diagnostics;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;
using Kommander.Tests.Simulation.Trace;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Records the write-ahead trace of a generated run, for the TLA+ trace validator.
///
/// <para><b>What this test checks, and what it leaves to the validator.</b> It runs a generated plan
/// with every node recording, writes one TLA+ module per node and partition, and asserts that the
/// trace covers the events the model is about: crashes, restores, and resolutions written without
/// an fsync. It does not run the model checker, because the suite must not need Java. The check
/// against the model is one command (see <c>formal/README.md</c>):</para>
///
/// <code>formal/run-tlc.sh --trace &lt;directory this test prints&gt;</code>
///
/// <para><b>Why the plan leaves out checkpoints and storage faults.</b> The validator stops at the
/// first event slice 1 of the model does not cover: a restore seeded from a checkpoint or a
/// compaction floor, a compaction, and a crash on a store that breaks the fsync contract on purpose
/// (a write latency, the lying disk). A plan that draws them validates a short prefix and then
/// nothing. Late broadcasts are on: a Committed row that reaches a follower before its Proposed copy
/// is the shape behind FINDING 7, 8 and 9.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestWalTraceRecording
{
    /// <summary>Directory the traces go to. Defaults beside the test binary.</summary>
    public const string DirectoryVariable = "KOMMANDER_DST_WAL_TRACE_DIR";

    /// <summary>Seed of the traced run. Defaults to a fixed seed.</summary>
    public const string SeedVariable = "KOMMANDER_DST_WAL_TRACE_SEED";

    private readonly ILogger<IRaft> logger;
    private readonly ITestOutputHelper output;

    public TestWalTraceRecording(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
        output = outputHelper;
    }

    [Fact]
    [Trait("Category", "DSTTrace")]
    public async Task ATracedRun_WritesOneModulePerNodeAndPartition_CoveringCrashesAndSyncOffResolutions()
    {
        ulong seed = ulong.TryParse(Environment.GetEnvironmentVariable(SeedVariable), out ulong parsed)
            ? parsed
            : 20261022;

        RandomScenarioOptions options = new()
        {
            ActionCount = 24,
            StepsPerAction = 6,
            ClientWeight = 40,
            LifecycleFaultWeight = 20,
            LateBroadcastWeight = 12,
            MaintenanceWeight = 0,
            StorageFaultWeight = 0,
            TransferFaultWeight = 0,
        };

        SimulationClusterOptions clusterOptions = options.ToClusterOptions(seed) with { RecordWalTrace = true };

        string directory = Environment.GetEnvironmentVariable(DirectoryVariable) is { Length: > 0 } configured
            ? global::System.IO.Path.Combine(configured, seed.ToString())
            : global::System.IO.Path.Combine(AppContext.BaseDirectory, "dst-wal-traces", seed.ToString());

        if (global::System.IO.Directory.Exists(directory))
            global::System.IO.Directory.Delete(directory, recursive: true);

        List<WalTraceRecorder> recorders = [];
        Exception? runFailure = null;

        await using (SimulationCluster cluster = await SimulationCluster.StartAsync(
                         clusterOptions, logger, TestContext.Current.CancellationToken))
        {
            recorders.AddRange(cluster.Nodes.Select(node => node.WalTrace!));

            try
            {
                await new RandomScenarioRunner(cluster, options, new SimulationRandom(seed))
                    .RunAsync(TestContext.Current.CancellationToken);
            }
            catch (Exception error)
            {
                // The trace of a failing run is the most useful one: write it before failing.
                runFailure = error;
            }
        }

        List<string> paths = [];
        foreach (WalTraceRecorder recorder in recorders)
            paths.AddRange(recorder.WriteModules(directory));

        output.WriteLine($"Wrote {paths.Count} trace modules to {directory}");
        output.WriteLine($"Validate with: formal/run-tlc.sh --trace {directory}");

        if (runFailure is not null)
            throw new InvalidOperationException($"The traced run failed (seed {seed}); traces are in {directory}.", runFailure);

        // A test that loads its subject from disk must assert it loaded something.
        Assert.NotEmpty(paths);

        List<WalTraceEvent> all = recorders
            .SelectMany(recorder => recorder.Partitions().SelectMany(recorder.EventsFor))
            .ToList();

        int crashes = all.Count(e => e.Kind == WalTraceEventKind.Crash);
        int restores = all.Count(e => e.Kind == WalTraceEventKind.Restore);
        int syncOffResolutions = all.Count(e => e.Kind == WalTraceEventKind.Resolution && !e.Synced && e.Max >= 0);
        int writes = all.Count(e => e.Kind == WalTraceEventKind.Write);
        int unsupported = all.Count(e => e.Kind == WalTraceEventKind.Unsupported);

        output.WriteLine(
            $"events {all.Count}: writes {writes}, crashes {crashes}, restores {restores}, " +
            $"sync-off resolutions {syncOffResolutions}, unsupported {unsupported}");

        Assert.True(writes > 0, "The trace recorded no write.");
        Assert.True(crashes > 0, $"Seed {seed} drew no crash, so the trace never tests what a crash loses.");
        Assert.True(syncOffResolutions > 0, "No resolution rode sync-off, so the trace never tests the single-fsync path.");
    }
}
