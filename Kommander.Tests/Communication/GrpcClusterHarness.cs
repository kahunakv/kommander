
using System.Net;
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
/// Spins up a real Kestrel gRPC server backed by an in-process <see cref="RaftManager"/>
/// using <see cref="GrpcCommunication"/>.  Intended for wire-level integration tests that
/// need to verify actual gRPC RPCs (Leave, GetFollowerLag, Gossip, …) rather than the
/// in-memory transport.
///
/// Call <see cref="StartAsync"/> after construction, and dispose when the test is done.
/// </summary>
public sealed class GrpcTestNode : IAsyncDisposable
{
    private readonly WebApplication _app;

    public RaftManager Manager { get; }
    public string Endpoint { get; }
    public int Port { get; }

    private GrpcTestNode(RaftManager manager, WebApplication app, int port)
    {
        Manager = manager;
        Port = port;
        Endpoint = $"localhost:{port}";
        _app = app;
    }

    public Task StartAsync(CancellationToken ct = default) => _app.StartAsync(ct);

    public async ValueTask DisposeAsync()
    {
        Manager.Dispose();
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
        try { await _app.StopAsync(cts.Token).ConfigureAwait(false); } catch { /* best-effort */ }
        await _app.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Creates and wires a single gRPC test node with SWIM failure detection enabled.
    /// Use this overload for tests that exercise eviction or heal behaviour; the standard
    /// <see cref="Create"/> factory disables SWIM to avoid eviction side-effects in short
    /// wire tests that focus on other RPCs.
    /// </summary>
    public static GrpcTestNode CreateWithSwim(
        int port,
        IReadOnlyList<RaftNode> allPeers,
        ILoggerFactory loggerFactory,
        TimeSpan pingInterval,
        TimeSpan suspicionTimeout,
        TimeSpan deadMemberEvictionGrace,
        int partitions = 1)
    {
        RaftConfiguration config = new()
        {
            NodeId = port,
            Host = "localhost",
            Port = port,
            InitialPartitions = partitions,
            GrpcScheme = "http://",
            HeartbeatInterval = TimeSpan.FromMilliseconds(150),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            StartElectionTimeout = 600,
            EnableQuiescence = false,
            EndElectionTimeout = 900,
            TimerInitialDelay = TimeSpan.FromMilliseconds(500),
            PingInterval = pingInterval,
            PingTimeout = TimeSpan.FromMilliseconds(300),
            IndirectPingFanout = 1,
            SuspicionTimeout = suspicionTimeout,
            DeadMemberEvictionGrace = deadMemberEvictionGrace
        };

        ILogger<IRaft> logger = loggerFactory.CreateLogger<IRaft>();
        List<RaftNode> peers = [.. allPeers.Where(n => n.Endpoint != $"localhost:{port}")];

        RaftManager manager = new(
            config,
            new StaticDiscovery(peers),
            new InMemoryWAL(logger),
            new GrpcCommunication(),
            new HybridLogicalClock(),
            logger
        );

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton<IRaft>(manager);
        builder.Services.AddSingleton(logger);
        builder.Services.AddKommanderGrpc();

        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2);
        });

        WebApplication app = builder.Build();
        app.MapGrpcRaftRoutes();

        return new GrpcTestNode(manager, app, port);
    }

    /// <summary>
    /// Creates and wires a single gRPC test node.
    /// <paramref name="allPeers"/> must contain ALL cluster nodes (including this one);
    /// the harness filters out the local port automatically so discovery only exposes peers.
    /// </summary>
    public static GrpcTestNode Create(
        int port,
        IReadOnlyList<RaftNode> allPeers,
        ILoggerFactory loggerFactory,
        int partitions = 1,
        Action<RaftConfiguration>? configure = null)
    {
        RaftConfiguration config = new()
        {
            NodeId = port,
            Host = "localhost",
            Port = port,
            InitialPartitions = partitions,
            GrpcScheme = "http://",
            HeartbeatInterval = TimeSpan.FromMilliseconds(150),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            StartElectionTimeout = 600,
            EnableQuiescence = false,
            EndElectionTimeout = 900,
            TimerInitialDelay = TimeSpan.FromMilliseconds(500),
            PingInterval = TimeSpan.Zero   // disable SWIM in short-lived harness tests
        };

        configure?.Invoke(config);

        ILogger<IRaft> logger = loggerFactory.CreateLogger<IRaft>();

        // Discovery must exclude the local node; StaticDiscovery is for remote peers only.
        List<RaftNode> peers = [.. allPeers.Where(n => n.Endpoint != $"localhost:{port}")];

        RaftManager manager = new(
            config,
            new StaticDiscovery(peers),
            new InMemoryWAL(logger),
            new GrpcCommunication(),
            new HybridLogicalClock(),
            logger
        );

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton<IRaft>(manager);
        builder.Services.AddSingleton(logger);
        builder.Services.AddKommanderGrpc();

        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2);
        });

        WebApplication app = builder.Build();
        app.MapGrpcRaftRoutes();

        return new GrpcTestNode(manager, app, port);
    }
}

/// <summary>
/// Convenience wrapper that creates, starts, and owns a set of <see cref="GrpcTestNode"/>
/// instances forming a small cluster.
/// </summary>
public sealed class GrpcClusterHarness : IAsyncDisposable
{
    private readonly GrpcTestNode[] _nodes;

    public IReadOnlyList<GrpcTestNode> Nodes => _nodes;

    private GrpcClusterHarness(GrpcTestNode[] nodes) => _nodes = nodes;

    public static async Task<GrpcClusterHarness> CreateAsync(
        IReadOnlyList<int> ports,
        ILoggerFactory loggerFactory,
        int partitions = 1,
        CancellationToken ct = default)
    {
        List<RaftNode> peers = ports.Select(p => new RaftNode($"localhost:{p}")).ToList();

        GrpcTestNode[] nodes = [.. ports.Select(p => GrpcTestNode.Create(p, peers, loggerFactory, partitions))];

        foreach (GrpcTestNode n in nodes)
            await n.StartAsync(ct).ConfigureAwait(false);

        // Join all nodes concurrently to start Raft timers and elect leaders.
        await Task.WhenAll(nodes.Select(n => n.Manager.JoinCluster(ct))).ConfigureAwait(false);

        return new GrpcClusterHarness(nodes);
    }

    /// <summary>
    /// Creates a cluster with SWIM failure detection enabled at the specified intervals.
    /// All nodes share the same SWIM config; individual nodes can be stopped to simulate
    /// a permanent partition and trigger the Dead → eviction pipeline.
    /// </summary>
    public static async Task<GrpcClusterHarness> CreateWithSwimAsync(
        IReadOnlyList<int> ports,
        ILoggerFactory loggerFactory,
        TimeSpan pingInterval,
        TimeSpan suspicionTimeout,
        TimeSpan deadMemberEvictionGrace,
        int partitions = 1,
        CancellationToken ct = default)
    {
        List<RaftNode> peers = ports.Select(p => new RaftNode($"localhost:{p}")).ToList();

        GrpcTestNode[] nodes = [.. ports.Select(p => GrpcTestNode.CreateWithSwim(
            p, peers, loggerFactory, pingInterval, suspicionTimeout, deadMemberEvictionGrace, partitions))];

        foreach (GrpcTestNode n in nodes)
            await n.StartAsync(ct).ConfigureAwait(false);

        await Task.WhenAll(nodes.Select(n => n.Manager.JoinCluster(ct))).ConfigureAwait(false);

        return new GrpcClusterHarness(nodes);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (GrpcTestNode n in _nodes)
            await n.DisposeAsync().ConfigureAwait(false);
    }
}
