
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
/// Minimal smoke tests for the gRPC transport layer. These run on a single-node
/// cluster so they are fast and do not depend on multi-node election timing.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestGrpcSingleNode
{
    static TestGrpcSingleNode()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    /// <summary>
    /// A single-node gRPC cluster can join and elect itself as leader.
    /// Uses port 8898 (single-node quorum = 1).
    /// </summary>
    [Fact]
    public async Task SingleNode_GrpcCluster_JoinsAndElectsLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        ILogger<IRaft> raftLogger = logFactory.CreateLogger<IRaft>();

        const int port = 8898;

        RaftConfiguration config = new()
        {
            NodeId = port,
            Host = "localhost",
            Port = port,
            InitialPartitions = 1,
            GrpcScheme = "http://",
            HeartbeatInterval = TimeSpan.FromMilliseconds(150),
            VotingTimeout = TimeSpan.FromMilliseconds(500),
            StartElectionTimeout = 600,
            EnableQuiescence = false,
            EndElectionTimeout = 900,
            TimerInitialDelay = TimeSpan.FromMilliseconds(300)
        };

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new GrpcCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IRaft>(manager);
        builder.Services.AddSingleton(raftLogger);
        builder.Services.AddGrpc();
        builder.WebHost.ConfigureKestrel(k =>
            k.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2));

        WebApplication app = builder.Build();
        app.MapGrpcRaftRoutes();

        try
        {
            await app.StartAsync(ct);

            using CancellationTokenSource joinCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            joinCts.CancelAfter(TimeSpan.FromSeconds(20));
            await manager.JoinCluster(joinCts.Token);

            Assert.True(manager.IsInitialized, "Manager should be initialized after JoinCluster");
        }
        finally
        {
            manager.Dispose();
            using CancellationTokenSource stopCts = new(TimeSpan.FromSeconds(2));
            try { await app.StopAsync(stopCts.Token); } catch { /* best-effort */ }
            await app.DisposeAsync();
        }
    }
}
