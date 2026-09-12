
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
    /// A host with no <see cref="IRaft"/> keeps the library default — which is the 4 MB that
    /// equalled one default backfill batch. Pinned so the number the ticket asked for is on record.
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
