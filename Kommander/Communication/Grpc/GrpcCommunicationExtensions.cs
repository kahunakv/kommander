
namespace Kommander.Communication.Grpc;

public static class GrpcCommunicationExtensions
{
    public static void MapGrpcRaftRoutes(this WebApplication app)
    { 
        app.MapGrpcService<RaftService>();
    }
}