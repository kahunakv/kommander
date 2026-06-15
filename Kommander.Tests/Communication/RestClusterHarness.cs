
using System.Net;
using Kommander;
using Kommander.Communication.Rest;
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
/// Spins up a real Kestrel HTTP/1.1 server backed by an in-process <see cref="RaftManager"/>
/// using <see cref="RestCommunication"/>.  Mirrors <see cref="GrpcClusterHarness"/> for the
/// REST transport so live REST wire tests (gossip, leave, …) can verify the full
/// serialization → route handler → ACK round trip.
/// </summary>
public sealed class RestTestNode : IAsyncDisposable
{
    private readonly WebApplication _app;

    public RaftManager Manager { get; }
    public string Endpoint { get; }
    public int Port { get; }

    private RestTestNode(RaftManager manager, WebApplication app, int port)
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
    /// Creates and wires a single REST test node.
    /// <paramref name="allPeers"/> must include ALL cluster nodes (including this one);
    /// the harness filters the local port out of the peer list automatically.
    /// </summary>
    public static RestTestNode Create(int port, IReadOnlyList<RaftNode> allPeers, ILoggerFactory loggerFactory, int partitions = 1)
    {
        RaftConfiguration config = new()
        {
            NodeId = port,
            Host = "localhost",
            Port = port,
            InitialPartitions = partitions,
            HttpScheme = "http://",
            HttpVersion = "1.1",
            HeartbeatInterval = TimeSpan.FromMilliseconds(150),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            StartElectionTimeout = 600,
            EndElectionTimeout = 900,
            TimerInitialDelay = TimeSpan.FromMilliseconds(500),
            TransportSecurity = new() { RequireTls = false },
            PingInterval = TimeSpan.Zero   // disable SWIM in short-lived harness tests
        };

        ILogger<IRaft> logger = loggerFactory.CreateLogger<IRaft>();

        List<RaftNode> peers = [.. allPeers.Where(n => n.Endpoint != $"localhost:{port}")];

        RaftManager manager = new(
            config,
            new StaticDiscovery(peers),
            new InMemoryWAL(logger),
            new RestCommunication(),
            new HybridLogicalClock(),
            logger
        );

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton<IRaft>(manager);
        builder.Services.AddSingleton(logger);

        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http1);
        });

        WebApplication app = builder.Build();
        app.MapRestRaftRoutes();

        return new RestTestNode(manager, app, port);
    }
}

/// <summary>
/// Convenience wrapper that creates, starts, and owns a set of <see cref="RestTestNode"/>
/// instances forming a small cluster.
/// </summary>
public sealed class RestClusterHarness : IAsyncDisposable
{
    private readonly RestTestNode[] _nodes;

    public IReadOnlyList<RestTestNode> Nodes => _nodes;

    private RestClusterHarness(RestTestNode[] nodes) => _nodes = nodes;

    public static async Task<RestClusterHarness> CreateAsync(
        IReadOnlyList<int> ports,
        ILoggerFactory loggerFactory,
        int partitions = 1,
        CancellationToken ct = default)
    {
        List<RaftNode> peers = ports.Select(p => new RaftNode($"localhost:{p}")).ToList();

        RestTestNode[] nodes = [.. ports.Select(p => RestTestNode.Create(p, peers, loggerFactory, partitions))];

        foreach (RestTestNode n in nodes)
            await n.StartAsync(ct).ConfigureAwait(false);

        await Task.WhenAll(nodes.Select(n => n.Manager.JoinCluster(ct))).ConfigureAwait(false);

        return new RestClusterHarness(nodes);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (RestTestNode n in _nodes)
            await n.DisposeAsync().ConfigureAwait(false);
    }
}
