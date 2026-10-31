using Grpc.AspNetCore.Server;
using Grpc.Net.Compression;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.IO.Compression;

namespace Kommander.Communication.Grpc;

public static class GrpcCommunicationExtensions
{
    /// <summary>
    /// Registers gRPC services for Kommander, including gzip compression providers so peers
    /// can send compressed snapshot chunks when
    /// <see cref="RaftConfiguration.GrpcEnableSnapshotCompression"/> is enabled.
    /// <para>
    /// The server's <c>MaxReceiveMessageSize</c> follows <see cref="RaftConfiguration.GrpcMaxMessageBytes"/>
    /// of the registered <see cref="IRaft"/>: the gRPC library default (4 MB) is exactly one
    /// default backfill batch, so a <c>BatchRequests</c> frame carrying a backfill batch beside
    /// live entries was refused with <c>ResourceExhausted</c> and tore the replication stream down.
    /// A host that registers no <see cref="IRaft"/> keeps the library default.
    /// </para>
    /// </summary>
    public static IServiceCollection AddKommanderGrpc(this IServiceCollection services)
    {
        services.AddGrpc(options =>
        {
            options.CompressionProviders =
            [
                new GzipCompressionProvider(CompressionLevel.Fastest)
            ];
        });

        services
            .AddOptions<GrpcServiceOptions>()
            .Configure<IServiceProvider>((options, provider) =>
            {
                if (provider.GetService<IRaft>() is { } raft)
                    options.MaxReceiveMessageSize = raft.Configuration.GrpcMaxMessageBytes;
            });

        return services;
    }

    /// <summary>
    /// Maps the Raft gRPC service. Before mapping it, the receive limit the host's gRPC options apply
    /// to this service is lined up with <see cref="RaftConfiguration.GrpcMaxMessageBytes"/> (see
    /// <see cref="EnsureRaftReceiveLimit"/>): the per-service options are read by the gRPC call
    /// handler factory when the service is mapped, so this is the last moment a correction takes.
    /// </summary>
    public static void MapGrpcRaftRoutes(this WebApplication app)
    {
        EnsureRaftReceiveLimit(app.Services);
        app.MapGrpcService<RaftService>();
    }

    /// <summary>
    /// The receive limit the gRPC library applies when neither the global nor the per-service
    /// <see cref="GrpcServiceOptions.MaxReceiveMessageSize"/> was set: 4 MB. It equals the default
    /// <see cref="RaftConfiguration.MaxOutboundBatchBytes"/> payload budget, so a full dispatcher
    /// frame (payload plus framing) exceeds it by a few kilobytes.
    /// </summary>
    internal const int GrpcLibraryDefaultMaxReceiveMessageBytes = 4 * 1024 * 1024;

    /// <summary>
    /// The receive limit in force for <see cref="RaftService"/>: the per-service option, else the
    /// global option, else the library default. A limit a host set to <see langword="null"/>
    /// (unlimited) reads as the default here, since the options expose no way to tell the two
    /// apart; that case only makes this value conservative.
    /// </summary>
    internal static int ResolveRaftReceiveLimit(IServiceProvider services)
    {
        GrpcServiceOptions<RaftService>? perService = services.GetService<IOptions<GrpcServiceOptions<RaftService>>>()?.Value;
        GrpcServiceOptions? global = services.GetService<IOptions<GrpcServiceOptions>>()?.Value;

        return perService?.MaxReceiveMessageSize ?? global?.MaxReceiveMessageSize ?? GrpcLibraryDefaultMaxReceiveMessageBytes;
    }

    /// <summary>
    /// Raises the per-service receive limit of <see cref="RaftService"/> to the registered
    /// <see cref="IRaft"/>'s <see cref="RaftConfiguration.GrpcMaxMessageBytes"/> when the host's
    /// gRPC options leave it lower, and returns the limit in force afterwards.
    /// <para>
    /// The configuration and the option are set in different places: Kommander validates
    /// <c>MaxOutboundBatchBytes</c> and <c>MaxBackfillBytesPerRound</c> against the configuration,
    /// while the option is set by <see cref="AddKommanderGrpc"/> — or left at the 4 MB library
    /// default by a host that registered gRPC with a plain <c>AddGrpc()</c>. Kahuna.Server did
    /// exactly that: every full dispatcher frame a leader built (4 MiB of payload plus framing) was
    /// refused by its followers with <c>ResourceExhausted</c>, and the receiver's own error line
    /// reported the configured 16 MiB as the limit. Only the Raft service's options are touched, so
    /// the host's other gRPC services keep whatever it chose for them.
    /// </para>
    /// </summary>
    internal static int EnsureRaftReceiveLimit(IServiceProvider services)
    {
        int inForce = ResolveRaftReceiveLimit(services);

        if (services.GetService<IRaft>() is not { } raft)
            return inForce;

        int configured = raft.Configuration.GrpcMaxMessageBytes;
        if (inForce >= configured)
            return inForce;

        ILogger? logger = services.GetService<ILogger<IRaft>>();

        if (services.GetService<IOptions<GrpcServiceOptions<RaftService>>>()?.Value is not { } perService)
        {
            logger?.LogError(
                "The gRPC receive limit in force for the Raft service is {InForceBytes} bytes, below RaftConfiguration.GrpcMaxMessageBytes ({ConfiguredBytes}), and the per-service options are not registered so it cannot be raised here; frames up to MaxOutboundBatchBytes plus framing will be refused and tear replication streams down. Register gRPC with AddKommanderGrpc()",
                inForce, configured);
            return inForce;
        }

        perService.MaxReceiveMessageSize = configured;

        logger?.LogWarning(
            "The gRPC receive limit in force for the Raft service was {InForceBytes} bytes, below RaftConfiguration.GrpcMaxMessageBytes ({ConfiguredBytes}): the host registered gRPC without AddKommanderGrpc(). Raised to {ConfiguredBytes} for the Raft service only; register with AddKommanderGrpc() so the limit is set where the host configures gRPC",
            inForce, configured, configured);

        return configured;
    }
}
