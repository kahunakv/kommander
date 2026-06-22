
using Grpc.Net.Client;
using Grpc.Net.Compression;
using System.Diagnostics;
using System.IO.Compression;
using System.Net.Security;
using System.Collections.Concurrent;
using System.Security.Cryptography;
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
/// Carries the pool-creation options passed to <see cref="SharedChannels"/> so that channel
/// count, multiplexing policy, and security settings travel as one unit.  The channel and
/// streaming pools both derive their counts from <see cref="ChannelsPerNode"/> — keeping
/// them in lockstep prevents <c>urlChannels[i]</c> from ever indexing out of range.
/// </summary>
internal readonly record struct GrpcChannelPoolOptions(
    int ChannelsPerNode,
    bool EnableMultipleHttp2Connections,
    RaftTransportSecurityOptions? SecurityOptions,
    bool EnableSnapshotCompression = false);

/// <summary>
/// Provides utilities for managing and retrieving shared gRPC channels and streamings.
/// </summary>
public static class SharedChannels
{
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcChannel>>> channels = new();

    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcInterSharedStreaming>>> streamings = new();

    // Per-URL round-robin counters for channel and streaming pool selection.
    // Each entry is a single-element int[] so Interlocked.Increment can be used without boxing.
    // Counter wraparound through int.MaxValue → int.MinValue is harmless: the uint cast before
    // the modulo ensures the index is always in [0, count).
    private static readonly ConcurrentDictionary<string, int[]> channelCounters = new();
    private static readonly ConcurrentDictionary<string, int[]> streamingCounters = new();

    // Tracks the scalar pool options (ChannelsPerNode, EnableMultipleHttp2Connections) that were
    // used to create each normalized URL's channel pool.  Debug builds assert that no second caller
    // passes conflicting options for the same URL — Kommander guarantees one transport config per
    // process, so a mismatch indicates a mis-wired call site.  SecurityOptions and streaming
    // metadata are intentionally excluded: they are reference types whose instance identity is
    // unreliable for comparison purposes here.
    private static readonly ConcurrentDictionary<string, (int ChannelsPerNode, bool EnableMultipleHttp2Connections)> registeredPoolConfig = new();

    // Process-wide defaults used by the public (securityOptions-only) overloads.
    // Packed into a single immutable reference so readers always observe a consistent
    // (ChannelsPerNode, EnableMultipleHttp2Connections) pair — two separate volatile
    // fields would admit a torn read between the two stores in Configure().
    // RaftManager calls Configure() during construction (before any peer I/O) so that
    // external consumers such as Kahuna's GrpcServerBatcher inherit the operator's
    // RaftConfiguration values rather than the library-defined (4, false) fallback.
    private sealed record DefaultPoolConfig(int ChannelsPerNode, bool EnableMultipleHttp2Connections);
    private static volatile DefaultPoolConfig defaultPoolConfig = new(4, false);

    /// <summary>
    /// Sets the process-wide default pool size and multiplexing flag used by the public
    /// <see cref="GetChannel"/>, <see cref="GetAllChannels"/>, and <see cref="GetStreaming"/>
    /// overloads.  Must be called before any consumer opens a channel to a peer URL — including
    /// external consumers such as Kahuna's <c>GrpcServerBatcher</c> — so that every caller
    /// agrees on the same pool options and inherits the raised concurrency ceiling.
    /// <para>
    /// <see cref="RaftManager"/> calls this during construction when the configured transport
    /// is gRPC, so the defaults are set before any peer I/O.  Hosts that register additional
    /// external <c>SharedChannels</c> consumers should call this before those consumers start.
    /// </para>
    /// </summary>
    public static void Configure(int channelsPerNode, bool enableMultipleHttp2Connections) =>
        defaultPoolConfig = new(channelsPerNode, enableMultipleHttp2Connections);

    // Checks in debug builds that a caller for this URL uses the same scalar pool options as
    // the pool that was actually built.  Called *after* the pool's Lazy.Value is resolved so
    // the registeredPoolConfig entry was written by the winning factory thread — not by a
    // racing caller — eliminating the TOCTOU that existed when GetOrAdd was used before
    // the pool was created.
    //
    // SecurityOptions and streaming Metadata are intentionally excluded from the check.
    // Both are reference types whose instance identity is unreliable for equality comparison,
    // and Kommander guarantees a single transport configuration per process: every node in a
    // cluster uses the same TLS/secret settings, so a per-URL security mismatch is a
    // deployment misconfiguration not detectable here.  The single-config-per-process
    // invariant is enforced at the Configure()/GetPoolOptions() layer instead.
    [Conditional("DEBUG")]
    private static void AssertConsistentPoolConfig(string normalizedUrl, GrpcChannelPoolOptions opts)
    {
        if (!registeredPoolConfig.TryGetValue(normalizedUrl, out (int ChannelsPerNode, bool EnableMultipleHttp2Connections) stored))
            return;

        Debug.Assert(
            stored.ChannelsPerNode == opts.ChannelsPerNode && stored.EnableMultipleHttp2Connections == opts.EnableMultipleHttp2Connections,
            $"SharedChannels: conflicting pool options for {normalizedUrl}. " +
            $"Pool built with ChannelsPerNode={stored.ChannelsPerNode}, EnableMultipleHttp2Connections={stored.EnableMultipleHttp2Connections}. " +
            $"This caller requested ChannelsPerNode={opts.ChannelsPerNode}, EnableMultipleHttp2Connections={opts.EnableMultipleHttp2Connections}. " +
            "Kommander supports one transport configuration per URL per process.");
    }

    // ── Internal overloads (accept GrpcChannelPoolOptions directly) ───────────
    // Used by GrpcCommunication to flow RaftConfiguration values into the pool.
    // The public overloads below are thin backward-compatible wrappers for external
    // consumers (e.g. Kahuna's GrpcServerBatcher) that do not have access to the
    // internal options type.

    internal static GrpcChannel GetChannel(string url, GrpcChannelPoolOptions opts)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;

        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(
            url,
            k => new Lazy<List<GrpcChannel>>(() =>
            {
                // Record the winning opts inside the factory so registeredPoolConfig always
                // reflects the opts the pool was actually built with, not a racing caller's.
                registeredPoolConfig.TryAdd(k, (opts.ChannelsPerNode, opts.EnableMultipleHttp2Connections));
                return CreateSharedChannels(k, opts);
            }));

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;
        AssertConsistentPoolConfig(url, opts);
        int[] counter = channelCounters.GetOrAdd(url, _ => new int[1]);
        int idx = (int)((uint)Interlocked.Increment(ref counter[0]) % urlChannels.Count);
        return urlChannels[idx];
    }

    internal static List<GrpcChannel> GetAllChannels(string url, GrpcChannelPoolOptions opts)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;

        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(
            url,
            k => new Lazy<List<GrpcChannel>>(() =>
            {
                registeredPoolConfig.TryAdd(k, (opts.ChannelsPerNode, opts.EnableMultipleHttp2Connections));
                return CreateSharedChannels(k, opts);
            }));

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;
        AssertConsistentPoolConfig(url, opts);
        return urlChannels;
    }

    internal static GrpcInterSharedStreaming GetStreaming(string url, Metadata? metadata, GrpcChannelPoolOptions opts)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;

        Lazy<List<GrpcInterSharedStreaming>> lazyStreaming = streamings.GetOrAdd(
            url,
            k => new Lazy<List<GrpcInterSharedStreaming>>(
                () => CreateAsyncDuplexStreamingCallInternal(k, metadata, opts)));

        List<GrpcInterSharedStreaming> streamingList = lazyStreaming.Value;
        AssertConsistentPoolConfig(url, opts);
        int[] counter = streamingCounters.GetOrAdd(url, _ => new int[1]);
        int idx = (int)((uint)Interlocked.Increment(ref counter[0]) % streamingList.Count);
        return streamingList[idx];
    }

    // ── Public backward-compatible overloads ──────────────────────────────────
    // Use the process-wide defaults set by Configure() (called by GrpcCommunication
    // on first use) so external callers such as Kahuna's GrpcServerBatcher inherit
    // the same pool size and multiplexing setting as Kommander.

    public static GrpcChannel GetChannel(string url, RaftTransportSecurityOptions? securityOptions = null)
    {
        DefaultPoolConfig cfg = defaultPoolConfig;
        return GetChannel(url, new GrpcChannelPoolOptions(cfg.ChannelsPerNode, cfg.EnableMultipleHttp2Connections, securityOptions));
    }

    /// <summary>
    /// Retrieves a list of all gRPC channels associated with the specified URL, creating the shared channels
    /// if they do not already exist.
    /// </summary>
    /// <param name="url">
    /// The target URL for retrieving or creating the associated gRPC channels. If the URL does not start
    /// with "https://" or "http://", it is automatically prefixed with "https://".
    /// </param>
    /// <param name="securityOptions">
    /// Transport security options controlling certificate validation and thumbprint pinning.
    /// </param>
    /// <returns>
    /// A list of <see cref="GrpcChannel"/> instances associated with the specified URL.
    /// </returns>
    public static List<GrpcChannel> GetAllChannels(string url, RaftTransportSecurityOptions? securityOptions = null)
    {
        DefaultPoolConfig cfg = defaultPoolConfig;
        return GetAllChannels(url, new GrpcChannelPoolOptions(cfg.ChannelsPerNode, cfg.EnableMultipleHttp2Connections, securityOptions));
    }

    public static GrpcInterSharedStreaming GetStreaming(string url, Metadata? metadata = null, RaftTransportSecurityOptions? securityOptions = null)
    {
        DefaultPoolConfig cfg = defaultPoolConfig;
        return GetStreaming(url, metadata, new GrpcChannelPoolOptions(cfg.ChannelsPerNode, cfg.EnableMultipleHttp2Connections, securityOptions));
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
    private static List<GrpcInterSharedStreaming> CreateAsyncDuplexStreamingCallInternal(
        string url,
        Metadata? metadata,
        GrpcChannelPoolOptions opts)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;

        Lazy<List<GrpcChannel>> urlChannelsLazy = channels.GetOrAdd(
            url,
            k => new Lazy<List<GrpcChannel>>(() =>
            {
                registeredPoolConfig.TryAdd(k, (opts.ChannelsPerNode, opts.EnableMultipleHttp2Connections));
                return CreateSharedChannels(k, opts);
            }));

        List<GrpcChannel> urlChannels = urlChannelsLazy.Value;

        // Streaming pool is sized to match the cached channel pool exactly so urlChannels[i]
        // never indexes out of range — the channel pool was created by whoever first called
        // GetOrAdd for this URL, so we must bind to its actual count, not opts.ChannelsPerNode.
        List<GrpcInterSharedStreaming> streamingList = new(urlChannels.Count);

        CallOptions callOptions = BuildStreamingCallOptions(metadata, opts.EnableSnapshotCompression);

        for (int i = 0; i < urlChannels.Count; i++)
        {
            Rafter.RafterClient client = new(urlChannels[i]);

            AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming =
                client.BatchRequests(callOptions);

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

    private static SslClientAuthenticationOptions BuildSslOptions(RaftTransportSecurityOptions? securityOptions)
    {
        SslClientAuthenticationOptions sslOptions = new();

        if (securityOptions?.AllowInsecureCertificateValidation == true)
        {
            sslOptions.RemoteCertificateValidationCallback = delegate { return true; };
        }
        else if (securityOptions?.TrustedServerCertificateThumbprints is { Count: > 0 } thumbprints)
        {
            sslOptions.RemoteCertificateValidationCallback = (_, certificate, _, _) =>
            {
                if (certificate is null)
                    return false;

                byte[] hash = SHA256.HashData(certificate.GetRawCertData());
                string thumbprint = Convert.ToHexString(hash);
                return thumbprints.Any(t => string.Equals(t, thumbprint, StringComparison.OrdinalIgnoreCase));
            };
        }

        return sslOptions;
    }

    private static List<GrpcChannel> CreateSharedChannels(string url, GrpcChannelPoolOptions opts)
    {
        SslClientAuthenticationOptions sslOptions = BuildSslOptions(opts.SecurityOptions);

        List<GrpcChannel> urlChannels = new(opts.ChannelsPerNode);

        for (int i = 0; i < opts.ChannelsPerNode; i++)
        {
            // Each channel owns its own handler so the pool opens independent connections
            // rather than multiplexing all streams over one shared connection.
            SocketsHttpHandler handler = new()
            {
                SslOptions = sslOptions,
                ConnectTimeout = TimeSpan.FromSeconds(10),
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
                EnableMultipleHttp2Connections = opts.EnableMultipleHttp2Connections
            };

            GrpcChannelOptions channelOptions = new()
            {
                HttpHandler = handler
            };

            if (opts.EnableSnapshotCompression)
            {
                channelOptions.CompressionProviders =
                [
                    new GzipCompressionProvider(CompressionLevel.Fastest)
                ];
            }

            urlChannels.Add(GrpcChannel.ForAddress(url, channelOptions));
        }

        return urlChannels;
    }

    private static CallOptions BuildStreamingCallOptions(Metadata? metadata, bool snapshotCompressionEnabled)
    {
        if (!snapshotCompressionEnabled)
            return metadata is { Count: > 0 } ? new CallOptions(metadata) : default;

        WriteOptions writeOptions = new(WriteFlags.NoCompress);
        return metadata is { Count: > 0 }
            ? new CallOptions(metadata, writeOptions: writeOptions)
            : new CallOptions(writeOptions: writeOptions);
    }
}