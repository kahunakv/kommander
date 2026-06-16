
using System.Net;
using Grpc.Net.Client;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Communication;

/// <summary>
/// Tests for the gRPC channel-pool concurrency fix.  Exercises pool count,
/// URL-normalization cache coalescing, <see cref="RaftConfiguration.GetEffectiveGrpcChannelsPerNode"/>
/// clamping, and end-to-end wiring from <see cref="RaftConfiguration"/> through
/// <see cref="SharedChannels"/> on a real loopback cluster.
///
/// Structural tests (sections 1–3) create <see cref="GrpcChannel"/> objects but do
/// not open real connections — they exercise the cache and pool-sizing logic only.
/// The integration test (section 4) spins up a 3-node Kestrel cluster.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestGrpcChannelPool
{
    // Enable cleartext HTTP/2 for loopback once per process; required by the integration test.
    static TestGrpcChannelPool()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    // ── 1. GetEffectiveGrpcChannelsPerNode clamping ───────────────────────────

    [Theory]
    [InlineData(0,   1)]
    [InlineData(-1,  1)]
    [InlineData(-99, 1)]
    [InlineData(1,   1)]
    [InlineData(4,   4)]
    [InlineData(32,  32)]
    [InlineData(64,  64)]
    [InlineData(65,  64)]
    [InlineData(200, 64)]
    public void GetEffectiveGrpcChannelsPerNode_ClampsToValidRange(int raw, int expected)
    {
        RaftConfiguration cfg = new() { GrpcChannelsPerNode = raw };
        Assert.Equal(expected, cfg.GetEffectiveGrpcChannelsPerNode());
    }

    // ── 2. Pool count matches configured ChannelsPerNode ─────────────────────
    //
    // Uses unique per-test URLs (.invalid TLD, never resolves) so each test gets
    // a fresh pool uncontaminated by the process-global SharedChannels cache.
    // GrpcChannel.ForAddress is lazy — no connection is opened here.

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(8)]
    public void ChannelPool_CreatesConfiguredCount(int channelsPerNode)
    {
        string url = $"https://pool-count-{channelsPerNode}.k.invalid:9900";
        GrpcChannelPoolOptions opts = new(channelsPerNode, false, null);
        List<GrpcChannel> pool = SharedChannels.GetAllChannels(url, opts);
        Assert.Equal(channelsPerNode, pool.Count);
    }

    [Fact]
    public void ChannelPool_SecondCallSameUrl_ReturnsCachedPool()
    {
        string url = "https://pool-cached.k.invalid:9900";
        GrpcChannelPoolOptions opts = new(3, false, null);
        List<GrpcChannel> first  = SharedChannels.GetAllChannels(url, opts);
        List<GrpcChannel> second = SharedChannels.GetAllChannels(url, opts);
        Assert.Same(first, second);
    }

    // ── 3. URL normalization: raw host:port coalesces with https:// prefix ───

    [Fact]
    public void GetAllChannels_NormalizesUrl_RawAndPrefixedProduceSamePool()
    {
        const string host = "pool-norm.k.invalid:9901";
        GrpcChannelPoolOptions opts = new(2, false, null);
        List<GrpcChannel> fromRaw        = SharedChannels.GetAllChannels(host, opts);
        List<GrpcChannel> fromNormalized = SharedChannels.GetAllChannels("https://" + host, opts);
        Assert.Same(fromRaw, fromNormalized);
    }

    // ── 4. End-to-end: GrpcChannelsPerNode flows from RaftConfiguration ──────
    //
    // Creates a 3-node loopback cluster with GrpcChannelsPerNode = 1.
    // After election and a successful replication, SharedChannels must hold a
    // 1-channel pool for each peer URL (verifying config wiring), and all RPCs
    // must have succeeded without IndexOutOfRange (verifying streaming-count ==
    // channel-count invariant).
    // Ports 8940–8942.

    [Fact]
    public async Task GrpcChannelsPerNode_FlowsEndToEnd_PoolMatchesConfig()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        int[] ports = [8940, 8941, 8942];
        List<RaftNode> allPeers = [.. ports.Select(p => new RaftNode($"localhost:{p}"))];

        List<(RaftManager Manager, WebApplication App)> cluster = [];

        foreach (int port in ports)
        {
            RaftConfiguration config = new()
            {
                NodeId = port,
                Host = "localhost",
                Port = port,
                InitialPartitions = 1,
                GrpcScheme = "http://",
                GrpcChannelsPerNode = 1,
                HeartbeatInterval = TimeSpan.FromMilliseconds(150),
                VotingTimeout = TimeSpan.FromMilliseconds(500),
                StartElectionTimeout = 600,
                EnableQuiescence = false,
                EndElectionTimeout = 900,
                TimerInitialDelay = TimeSpan.FromMilliseconds(500),
                PingInterval = TimeSpan.Zero
            };

            ILogger<IRaft> logger = logFactory.CreateLogger<IRaft>();
            List<RaftNode> peers = [.. allPeers.Where(n => n.Endpoint != $"localhost:{port}")];

            RaftManager manager = new(
                config,
                new StaticDiscovery(peers),
                new InMemoryWAL(logger),
                new GrpcCommunication(),
                new HybridLogicalClock(),
                logger);

            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IRaft>(manager);
            builder.Services.AddSingleton(logger);
            builder.Services.AddGrpc();
            builder.WebHost.ConfigureKestrel(k =>
                k.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2));

            WebApplication app = builder.Build();
            app.MapGrpcRaftRoutes();
            await app.StartAsync(ct);

            cluster.Add((manager, app));
        }

        try
        {
            // Join all nodes to elect a leader and fire inter-node RPCs.
            await Task.WhenAll(cluster.Select(c => c.Manager.JoinCluster(ct)));

            // Wait for a leader so at least one full round of streaming RPCs has fired.
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(15));
            while (!cts.Token.IsCancellationRequested)
            {
                bool anyLeader = false;
                foreach ((RaftManager m, _) in cluster)
                {
                    if (await m.AmILeaderQuick(0))
                    {
                        anyLeader = true;
                        break;
                    }
                }
                if (anyLeader) break;
                await Task.Delay(100, cts.Token);
            }

            // Verify every peer URL's channel pool was created with GrpcChannelsPerNode = 1.
            // The pool count matches config; if streaming count had diverged, the election
            // RPCs above would have thrown IndexOutOfRange.
            foreach (int port in ports)
            {
                string peerUrl = $"http://localhost:{port}";
                List<GrpcChannel> pool = SharedChannels.GetAllChannels(peerUrl);
                Assert.Single(pool);
            }
        }
        finally
        {
            foreach ((RaftManager m, WebApplication app) in cluster)
            {
                m.Dispose();
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
                try { await app.StopAsync(cts.Token); } catch { /* best-effort */ }
                await app.DisposeAsync();
            }
        }
    }

    // ── 5. Cap-exceeded warning emits to stderr exactly once ─────────────────

    [Fact]
    public void GetEffectiveGrpcChannelsPerNode_EmitsStderrWarning_WhenAboveCap()
    {
        // Reset the once-guard so this test can observe the warning regardless
        // of what earlier theory cases may have triggered.
        Interlocked.Exchange(ref RaftConfiguration.grpcChannelsCapWarned, 0);

        RaftConfiguration cfg = new() { GrpcChannelsPerNode = 200 };

        TextWriter originalErr = Console.Error;
        using StringWriter capture = new();
        Console.SetError(capture);
        try
        {
            cfg.GetEffectiveGrpcChannelsPerNode();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        string output = capture.ToString();
        Assert.Contains("[Kommander]", output);
        Assert.Contains("GrpcChannelsPerNode=200", output);
        Assert.Contains("64", output);

        // Second call with the guard now set must NOT emit again.
        using StringWriter capture2 = new();
        Console.SetError(capture2);
        try { cfg.GetEffectiveGrpcChannelsPerNode(); }
        finally { Console.SetError(originalErr); }
        Assert.Equal(string.Empty, capture2.ToString());

        // Restore guard state so subsequent tests are unaffected.
        Interlocked.Exchange(ref RaftConfiguration.grpcChannelsCapWarned, 0);
    }

    // ── 6. End-to-end: GrpcChannelsPerNode = 2 round-trips RPCs ─────────────
    //
    // A second 3-node cluster with GrpcChannelsPerNode = 2 verifies that a pool
    // larger than 1 still elects a leader and replicates without IndexOutOfRange.
    // Ports 8943–8945.

    [Fact]
    public async Task GrpcChannelsPerNode_Two_ElectsLeaderAndReplicates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

        int[] ports = [8943, 8944, 8945];
        List<RaftNode> allPeers = [.. ports.Select(p => new RaftNode($"localhost:{p}"))];

        List<(RaftManager Manager, WebApplication App)> cluster = [];

        foreach (int port in ports)
        {
            RaftConfiguration config = new()
            {
                NodeId = port,
                Host = "localhost",
                Port = port,
                InitialPartitions = 1,
                GrpcScheme = "http://",
                GrpcChannelsPerNode = 2,
                HeartbeatInterval = TimeSpan.FromMilliseconds(150),
                VotingTimeout = TimeSpan.FromMilliseconds(500),
                StartElectionTimeout = 600,
                EnableQuiescence = false,
                EndElectionTimeout = 900,
                TimerInitialDelay = TimeSpan.FromMilliseconds(500),
                PingInterval = TimeSpan.Zero
            };

            ILogger<IRaft> logger = logFactory.CreateLogger<IRaft>();
            List<RaftNode> peers = [.. allPeers.Where(n => n.Endpoint != $"localhost:{port}")];

            RaftManager manager = new(
                config,
                new StaticDiscovery(peers),
                new InMemoryWAL(logger),
                new GrpcCommunication(),
                new HybridLogicalClock(),
                logger);

            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IRaft>(manager);
            builder.Services.AddSingleton(logger);
            builder.Services.AddGrpc();
            builder.WebHost.ConfigureKestrel(k =>
                k.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2));

            WebApplication app = builder.Build();
            app.MapGrpcRaftRoutes();
            await app.StartAsync(ct);

            cluster.Add((manager, app));
        }

        try
        {
            await Task.WhenAll(cluster.Select(c => c.Manager.JoinCluster(ct)));

            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(15));
            while (!cts.Token.IsCancellationRequested)
            {
                bool anyLeader = false;
                foreach ((RaftManager m, _) in cluster)
                {
                    if (await m.AmILeaderQuick(0))
                    {
                        anyLeader = true;
                        break;
                    }
                }
                if (anyLeader) break;
                await Task.Delay(100, cts.Token);
            }

            foreach (int port in ports)
            {
                string peerUrl = $"http://localhost:{port}";
                List<GrpcChannel> pool = SharedChannels.GetAllChannels(peerUrl);
                Assert.Equal(2, pool.Count);
            }
        }
        finally
        {
            foreach ((RaftManager m, WebApplication app) in cluster)
            {
                m.Dispose();
                using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
                try { await app.StopAsync(cts.Token); } catch { /* best-effort */ }
                await app.DisposeAsync();
            }
        }
    }

    // ── 7. GetStreaming URL normalization: raw host:port coalesces with https:// prefix ──

    [Fact]
    public void GetStreaming_NormalizesUrl_RawAndPrefixedProduceSameStreaming()
    {
        const string host = "pool-stream.k.invalid:9903";
        // ChannelsPerNode = 1 guarantees Random.Next(0,1) always returns index 0,
        // so the same GrpcInterSharedStreaming instance is returned on both calls.
        GrpcChannelPoolOptions opts = new(1, false, null);

        GrpcInterSharedStreaming fromRaw        = SharedChannels.GetStreaming(host, null, opts);
        GrpcInterSharedStreaming fromNormalized = SharedChannels.GetStreaming("https://" + host, null, opts);

        Assert.Same(fromRaw, fromNormalized);
    }
}
