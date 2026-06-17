
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

        return services;
    }

    public static void MapGrpcRaftRoutes(this WebApplication app)
    { 
        app.MapGrpcService<RaftService>();
    }
}