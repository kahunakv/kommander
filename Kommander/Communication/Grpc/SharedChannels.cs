
using Grpc.Net.Client;
using System.Net.Security;
using System.Collections.Concurrent;
using Grpc.Core;

namespace Kommander.Communication.Grpc;

public static class SharedChannels
{
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcChannel>>> channels = new();
    
    private static readonly ConcurrentDictionary<string, Lazy<List<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>>>> streamings = new();
    
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
    
    public static AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> GetStreaming(string url)
    {
        Lazy<List<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>>> lazyStreaming = streamings.GetOrAdd(url, CreateStreaming);
        
        List<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>> streamingList = lazyStreaming.Value;
        
        return streamingList[Random.Shared.Next(0, streamingList.Count)];
    }

    private static Lazy<List<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>>> CreateStreaming(string url)
    {
        return new(() => CreateAsyncDuplexStreamingCallInternal(url));
    }

    private static List<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>> CreateAsyncDuplexStreamingCallInternal(string url)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;
        
        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;
        
        List<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>> streamingList = new(4);

        for (int i = 0; i < 4; i++)
        {
            Rafter.RafterClient client = new(urlChannels[i]);

            AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming = client.BatchRequests();

            Task readTask = Task.Run(() => ListenForEvents(streaming));
            
            streamingList.Add(streaming);
        }

        return streamingList;
    }

    private static async Task ListenForEvents(AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming)
    {
        // ReSharper disable once AccessToDisposedClosure
        await foreach (GrpcBatchRequestsResponse response in streaming.ResponseStream.ReadAllAsync())
        {
            //Console.WriteLine(response.RequestId);
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