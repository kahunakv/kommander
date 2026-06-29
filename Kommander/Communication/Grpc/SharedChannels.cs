
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
/// A queued outbound <c>AppendLogs</c> item waiting to be coalesced into the next batch write.
/// </summary>
/// <param name="BatchItem">
/// The protobuf batch-request item carrying the serialized <c>AppendLogs</c> payload.
/// </param>
/// <param name="PooledRequest">
/// The rented <see cref="GrpcAppendLogsRequest"/> that backs <paramref name="BatchItem"/>.
/// The flusher that drains this entry from the queue is responsible for returning it to
/// <see cref="GrpcCommunicationPool"/> after <c>WriteAsync</c> completes.
/// </param>
internal readonly record struct PendingAppendLogs(
    GrpcBatchRequestsRequestItem BatchItem,
    GrpcAppendLogsRequest PooledRequest);

/// <summary>
/// Represents a shared gRPC duplex streaming instance that includes synchronization mechanisms
/// and facilitates communication between gRPC clients and servers.
/// </summary>
public sealed class GrpcInterSharedStreaming
{
    private readonly Func<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>> factory;
    private readonly object recreateLock = new();
    private volatile AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming;
    private volatile bool faulted;

    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    /// <summary>
    /// The current duplex streaming call.  <see cref="EnsureHealthy"/> swaps this for a fresh
    /// call after the previous one faults, so writers that read it always observe a live (or
    /// just-recreated) stream rather than a permanently dead pool slot.
    /// </summary>
    public AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> Streaming => streaming;

    /// <summary>
    /// Queue of outbound <c>AppendLogs</c> items that arrived while a <c>WriteAsync</c> was
    /// already in flight on this stream.  Only populated when
    /// <see cref="RaftConfiguration.GrpcEnableAppendLogsCoalescing"/> is <see langword="true"/>.
    /// The thread that acquires <see cref="Semaphore"/> drains this queue and flushes all
    /// pending items as a single <c>GrpcBatchRequestsRequest</c>.
    /// </summary>
    internal readonly ConcurrentQueue<PendingAppendLogs> Pending = new();

    internal GrpcInterSharedStreaming(Func<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>> factory)
    {
        this.factory = factory;
        streaming = factory();
        _ = ListenForEvents(streaming);
    }

    /// <summary>
    /// Flags this stream as faulted so the next <see cref="EnsureHealthy"/> recreates it.
    /// A writer whose <c>WriteAsync</c> throws calls this to trigger prompt recovery rather
    /// than waiting for <see cref="ListenForEvents"/> to observe the broken response stream.
    /// Safe to call from any thread.
    /// </summary>
    public void MarkFaulted() => faulted = true;

    /// <summary>
    /// If the stream has faulted, disposes the dead call and lazily recreates a fresh duplex
    /// call (and its response-draining loop) in place.  Called before the pool hands the slot
    /// to a writer, so a transient connection fault no longer kills the slot for the lifetime
    /// of the process.  Idempotent and thread-safe — concurrent callers recreate once.
    /// </summary>
    internal void EnsureHealthy()
    {
        if (!faulted)
            return;

        lock (recreateLock)
        {
            if (!faulted)
                return;

            try
            {
                streaming.Dispose();
            }
            catch
            {
                // The previous call already faulted; disposal is best-effort.
            }

            AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> fresh = factory();
            streaming = fresh;
            faulted = false;
            _ = ListenForEvents(fresh);
        }
    }

    /// <summary>
    /// Drains the response stream for the life of <paramref name="call"/>.  When the loop ends —
    /// whether the peer completed it or a transport fault threw — the stream is flagged faulted
    /// so the pool recreates it on next use instead of leaving a dead slot in rotation.
    /// </summary>
    private async Task ListenForEvents(AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> call)
    {
        try
        {
            // ReSharper disable once AccessToDisposedClosure
            await foreach (GrpcBatchRequestsResponse _ in call.ResponseStream.ReadAllAsync())
            {
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("ListenForEvents: {0}: {1}", ex.GetType().Name, ex.Message);
        }
        finally
        {
            faulted = true;
        }
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

    internal static GrpcInterSharedStreaming GetStreaming(string url, Func<Metadata?>? metadataFactory, GrpcChannelPoolOptions opts)
    {
        if (!url.StartsWith("https://") && !url.StartsWith("http://"))
            url = "https://" + url;

        Lazy<List<GrpcInterSharedStreaming>> lazyStreaming = streamings.GetOrAdd(
            url,
            k => new Lazy<List<GrpcInterSharedStreaming>>(
                () => CreateAsyncDuplexStreamingCallInternal(k, metadataFactory, opts)));

        List<GrpcInterSharedStreaming> streamingList = lazyStreaming.Value;
        AssertConsistentPoolConfig(url, opts);
        int[] counter = streamingCounters.GetOrAdd(url, _ => new int[1]);
        int idx = (int)((uint)Interlocked.Increment(ref counter[0]) % streamingList.Count);
        GrpcInterSharedStreaming selected = streamingList[idx];
        // Self-heal: if this slot's stream faulted (peer reset, keepalive timeout, connection
        // fault), recreate it before handing it out instead of returning a permanently dead stream.
        selected.EnsureHealthy();
        return selected;
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
        // Backward-compatible overload for external consumers that hand in an already-built
        // Metadata instance. Wrap it in a constant factory so the slot-creation path is uniform.
        Func<Metadata?>? metadataFactory = metadata is null ? null : () => metadata;
        return GetStreaming(url, metadataFactory, new GrpcChannelPoolOptions(cfg.ChannelsPerNode, cfg.EnableMultipleHttp2Connections, securityOptions));
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
        Func<Metadata?>? metadataFactory,
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

        for (int i = 0; i < urlChannels.Count; i++)
        {
            Rafter.RafterClient client = new(urlChannels[i]);

            // Each slot keeps a factory bound to its own client/channel so EnsureHealthy can
            // re-open the duplex call in place after a fault, on the same connection.
            //
            // The auth metadata is built *inside* this factory rather than once for the whole
            // pool. gRPC sends stream metadata only at stream establishment, so for shared-secret
            // mode the per-stream nonce/timestamp must be signed when the stream is opened — and
            // re-signed when EnsureHealthy re-opens it. Sharing a single frozen Metadata across
            // every slot (and every re-open) would reuse one nonce, which replay detection on the
            // receiver would reject after the first stream. Invoking metadataFactory per slot
            // gives each stream its own freshly signed headers.
            streamingList.Add(new GrpcInterSharedStreaming(() =>
            {
                CallOptions callOptions = BuildStreamingCallOptions(metadataFactory?.Invoke(), opts.EnableSnapshotCompression);
                return client.BatchRequests(callOptions);
            }));
        }

        return streamingList;
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