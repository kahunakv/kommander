
using Grpc.AspNetCore.Server;
using Kommander.Communication.Grpc;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Kommander.Tests.Communication;

/// <summary>
/// The server-side receive limit must follow <see cref="RaftConfiguration.GrpcMaxMessageBytes"/>.
/// The gRPC library default (4 MB) equalled one default backfill batch, so a
/// <c>BatchRequests</c> frame carrying a backfill batch beside live entries was refused with
/// <c>ResourceExhausted</c> on the 1.6.x soak and tore the replication stream down.
/// </summary>
public sealed class TestGrpcMessageSizeLimits
{
    private static RaftManager BuildManager(Action<RaftConfiguration> configure)
    {
        RaftConfiguration config = new() { Host = "localhost", Port = 9000, InitialPartitions = 0 };
        configure(config);

        return new RaftManager(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }

    [Fact]
    public void AddKommanderGrpc_SetsServerReceiveLimitFromTheRegisteredRaftConfiguration()
    {
        const int limit = 24 * 1024 * 1024;

        using RaftManager manager = BuildManager(c => c.GrpcMaxMessageBytes = limit);

        ServiceCollection services = new();
        services.AddLogging();
        services.AddSingleton<IRaft>(manager);
        services.AddKommanderGrpc();

        using ServiceProvider provider = services.BuildServiceProvider();
        GrpcServiceOptions options = provider.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;

        Assert.Equal(limit, options.MaxReceiveMessageSize);
        Assert.NotEmpty(options.CompressionProviders); // the existing registration is kept
    }

    [Fact]
    public void AddKommanderGrpc_DefaultConfiguration_RaisesTheLimitAboveTheLibraryDefault()
    {
        using RaftManager manager = BuildManager(_ => { });

        ServiceCollection services = new();
        services.AddLogging();
        services.AddSingleton<IRaft>(manager);
        services.AddKommanderGrpc();

        using ServiceProvider provider = services.BuildServiceProvider();
        GrpcServiceOptions options = provider.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;

        // 16 MiB: four default backfill batches, or four default dispatcher frames, of headroom.
        Assert.Equal(16 * 1024 * 1024, options.MaxReceiveMessageSize);
    }

    /// <summary>
    /// The Kahuna.Server shape (1.14.7): gRPC registered with a plain <c>AddGrpc()</c>, never with
    /// <see cref="GrpcCommunicationExtensions.AddKommanderGrpc"/>, so the Raft service received at
    /// the 4 MB library default while the sender framed up to 4 MiB of payload plus framing — every
    /// full frame was refused and tore the replication stream down (CamusDB fault soak, 2026-10-30).
    /// Mapping the routes must raise the per-service limit to the configured value, for the Raft
    /// service only.
    /// </summary>
    [Fact]
    public void EnsureRaftReceiveLimit_RaisesThePerServiceLimit_WhenTheHostRegisteredPlainAddGrpc()
    {
        using RaftManager manager = BuildManager(_ => { });

        ServiceCollection services = new();
        services.AddLogging();
        services.AddSingleton<IRaft>(manager);
        services.AddGrpc();

        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Equal(GrpcCommunicationExtensions.GrpcLibraryDefaultMaxReceiveMessageBytes, GrpcCommunicationExtensions.ResolveRaftReceiveLimit(provider));

        int inForce = GrpcCommunicationExtensions.EnsureRaftReceiveLimit(provider);

        Assert.Equal(manager.Configuration.GrpcMaxMessageBytes, inForce);
        Assert.Equal(manager.Configuration.GrpcMaxMessageBytes, GrpcCommunicationExtensions.ResolveRaftReceiveLimit(provider));
        Assert.Equal(
            manager.Configuration.GrpcMaxMessageBytes,
            provider.GetRequiredService<IOptions<GrpcServiceOptions<RaftService>>>().Value.MaxReceiveMessageSize);

        // The host's other services keep the limit the host chose (the library default here).
        Assert.Equal(
            GrpcCommunicationExtensions.GrpcLibraryDefaultMaxReceiveMessageBytes,
            provider.GetRequiredService<IOptions<GrpcServiceOptions>>().Value.MaxReceiveMessageSize);
    }

    /// <summary>A host that registered with <c>AddKommanderGrpc</c> already has the limit: nothing to raise.</summary>
    [Fact]
    public void EnsureRaftReceiveLimit_WithAddKommanderGrpc_LeavesTheLimitAlone()
    {
        const int limit = 24 * 1024 * 1024;
        using RaftManager manager = BuildManager(c => c.GrpcMaxMessageBytes = limit);

        ServiceCollection services = new();
        services.AddLogging();
        services.AddSingleton<IRaft>(manager);
        services.AddKommanderGrpc();

        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Equal(limit, GrpcCommunicationExtensions.EnsureRaftReceiveLimit(provider));
        Assert.Equal(limit, GrpcCommunicationExtensions.ResolveRaftReceiveLimit(provider));
    }

    /// <summary>
    /// A host with no <see cref="IRaft"/> keeps the library default — which is the 4 MB that
    /// equalled one default backfill batch. Pinned so a change to the default is a deliberate one.
    /// </summary>
    [Fact]
    public void AddKommanderGrpc_WithoutARegisteredRaft_KeepsTheLibraryDefault()
    {
        ServiceCollection services = new();
        services.AddLogging();
        services.AddKommanderGrpc();

        using ServiceProvider provider = services.BuildServiceProvider();
        GrpcServiceOptions options = provider.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;

        Assert.Equal(4 * 1024 * 1024, options.MaxReceiveMessageSize);
        Assert.Equal(new RaftConfiguration().MaxBackfillBytesPerRound, options.MaxReceiveMessageSize);
    }
}
