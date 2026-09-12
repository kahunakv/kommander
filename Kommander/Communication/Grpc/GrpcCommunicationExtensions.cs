using Grpc.AspNetCore.Server;
using Grpc.Net.Compression;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
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

    public static void MapGrpcRaftRoutes(this WebApplication app)
    {
        app.MapGrpcService<RaftService>();
    }
}
