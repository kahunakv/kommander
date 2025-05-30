
using Grpc.Net.Client;
using System.Net.Security;
using System.Collections.Concurrent;
using Grpc.Core;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Represents a shared gRPC duplex streaming instance that includes synchronization mechanisms
/// and facilitates communication between gRPC clients and servers.
/// </summary>
public sealed class GrpcInterSharedStreaming
{
    public SemaphoreSlim Semaphore { get; } = new(1, 1);
    
    public AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> Streaming { get; }
    
    public GrpcInterSharedStreaming(AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming)
    {
        Streaming = streaming;
    }
}

/// <summary>
/// Provides utilities for managing and retrieving shared gRPC channels and streamings.
/// </summary>
public static class SharedChannels
{
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcChannel>>> channels = new();
    
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcInterSharedStreaming>>> streamings = new();

    /// <summary>
    /// Retrieves a single gRPC channel associated with the specified URL. If there are multiple channels
    /// associated with the URL, one is selected at random. If the channels do not already exist for the URL,
    /// they are created.
    /// </summary>
    /// <param name="url">
    /// The target URL for retrieving or creating the associated gRPC channel. If the URL does not start
    /// with "https://" or "http://", it is automatically prefixed with "https://".
    /// </param>
    /// <returns>
    /// A single <see cref="GrpcChannel"/> instance associated with the specified URL.
    /// </returns>
    public static GrpcChannel GetChannel(string url)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;
        
        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;
        
        return urlChannels[Random.Shared.Next(0, urlChannels.Count)];               
    }

    /// <summary>
    /// Retrieves a list of all gRPC channels associated with the specified URL, creating the shared channels
    /// if they do not already exist.
    /// </summary>
    /// <param name="url">
    /// The target URL for retrieving or creating the associated gRPC channels. If the URL does not start
    /// with "https://" or "http://", it is automatically prefixed with "https://".
    /// </param>
    /// <returns>
    /// A list of <see cref="GrpcChannel"/> instances associated with the specified URL.
    /// </returns>
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

    /// <summary>
    /// Creates a list of asynchronous duplex streaming calls for handling gRPC batch requests,
    /// associating each call with a specific gRPC channel.
    /// </summary>
    /// <param name="url">
    /// The target URL used for creating and retrieving gRPC channels. If the URL does not start
    /// with "https://" or "http://", it is automatically prefixed with "https://".
    /// </param>
    /// <returns>
    /// A list of <see cref="GrpcInterSharedStreaming"/> instances, each containing a duplex
    /// streaming call for gRPC batch requests.
    /// </returns>
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

    /// <summary>
    /// Asynchronously listens for events streamed from the server and handles responses.
    /// </summary>
    /// <param name="streaming">
    /// The gRPC duplex streaming call containing requests and responses of type
    /// <see cref="GrpcBatchRequestsRequest"/> and <see cref="GrpcBatchRequestsResponse"/>.
    /// </param>
    /// <returns>
    /// A <see cref="Task"/> representing the asynchronous operation of reading responses from the streaming call.
    /// </returns>
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