
using Grpc.Net.Client;
using System.Net.Security;
using System.Collections.Concurrent;

namespace Kommander.Communication.Grpc;

public static class SharedChannels
{
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcChannel>>> channels = new();    
    
    public static GrpcChannel GetChannel(string url)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;
        
        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;
        
        return urlChannels[Random.Shared.Next(0, urlChannels.Count)];               
    }
    
    private static Lazy<List<GrpcChannel>> GetSharedChannels(string url)
    {
        return new(() => CreateSharedChannels(url));
    }
    
    private static List<GrpcChannel> CreateSharedChannels(string url)
    {
        SslClientAuthenticationOptions sslOptions = new()
        {
            RemoteCertificateValidationCallback = delegate { return true; }
        };

        SocketsHttpHandler handler = new()
        {
            SslOptions = sslOptions,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
            EnableMultipleHttp2Connections = false
        };
        
        List<GrpcChannel> urlChannels = new(4);
        
        for (int i = 0; i < 4; i++)
        {
            urlChannels.Add(GrpcChannel.ForAddress(url, new() {
                HttpHandler = handler
            }));
        }

        return urlChannels;
    }             
}