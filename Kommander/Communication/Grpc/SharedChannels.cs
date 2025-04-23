
using Grpc.Net.Client;
using System.Net.Security;
using System.Collections.Concurrent;
using Grpc.Core;

namespace Kommander.Communication.Grpc;

public sealed class GrpcInterSharedStreaming
{
    public SemaphoreSlim Semaphore { get; } = new(1, 1);
    
    public AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> Streaming { get; }
    
    public GrpcInterSharedStreaming(AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming)
    {
        Streaming = streaming;
    }
}

public static class SharedChannels
{
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcChannel>>> channels = new();
    
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcInterSharedStreaming>>> streamings = new();
    
    public static GrpcChannel GetChannel(string url)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;
        
        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;
        
        return urlChannels[Random.Shared.Next(0, urlChannels.Count)];               
    }

    public static List<GrpcChannel> GetAllChannels(string url)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;
        
        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(url, GetSharedChannels);

        return urlChannelsLazy.Value;
    }
    
    public static GrpcInterSharedStreaming GetStreaming(string url)
    {
        Lazy<List<GrpcInterSharedStreaming>> lazyStreaming = streamings.GetOrAdd(url, CreateStreaming);
        
        List<GrpcInterSharedStreaming> streamingList = lazyStreaming.Value;
        
        return streamingList[Random.Shared.Next(0, streamingList.Count)];
    }

    private static Lazy<List<GrpcInterSharedStreaming>> CreateStreaming(string url)
    {
        return new(() => CreateAsyncDuplexStreamingCallInternal(url));
    }

    private static List<GrpcInterSharedStreaming> CreateAsyncDuplexStreamingCallInternal(string url)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;
        
        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;
        
        List<GrpcInterSharedStreaming> streamingList = new(4);

        for (int i = 0; i < 4; i++)
        {
            Rafter.RafterClient client = new(urlChannels[i]);

            AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming = client.BatchRequests();

            _ = ListenForEvents(streaming);
            
            streamingList.Add(new(streaming));
        }

        return streamingList;
    }

    private static async Task ListenForEvents(AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming)
    {
        try
        {
            // ReSharper disable once AccessToDisposedClosure
            await foreach (GrpcBatchRequestsResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                //Console.WriteLine(response.RequestId);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("ListenForEvents: {0}: {1}", ex.GetType().Name, ex.Message);
        }
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
            KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
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