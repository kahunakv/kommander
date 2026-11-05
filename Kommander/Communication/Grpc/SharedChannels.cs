
using Grpc.Net.Client;
using Grpc.Net.Compression;
using System.Diagnostics;
using System.IO.Compression;
using System.Net.Security;
using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Kommander.Diagnostics;
using Microsoft.Extensions.Logging;

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
    /// <summary>Spacing of one slot's drain-loop fault lines; the faults between are counted onto the next line.</summary>
    private const long FaultLogIntervalMs = 10_000;

    private readonly Func<AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse>> factory;
    private readonly object recreateLock = new();
    private volatile AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> streaming;
    private volatile bool faulted;

    private long lastFaultLogMs;
    private long suppressedFaults;

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
    /// Flags the current stream as faulted so the next <see cref="EnsureHealthy"/> recreates it.
    /// Safe to call from any thread. Writers inside this assembly use
    /// <see cref="MarkFaultedAfterWriteFailure"/>, which also knows which call failed and why.
    /// </summary>
    public void MarkFaulted() => faulted = true;

    /// <summary>
    /// Flags the slot after a <c>WriteAsync</c> on <paramref name="call"/> threw
    /// <paramref name="exception"/>, so the next hand-out recreates the stream instead of waiting
    /// for <see cref="ListenForEvents"/> to notice — with two exceptions that keep a healthy stream
    /// in place:
    /// <list type="bullet">
    /// <item>a <see cref="StatusCode.ResourceExhausted"/> raised while the call is still open is this
    /// side's own <c>MaxSendMessageSize</c> refusing the frame before it was written; the stream is
    /// intact, and disposing it here is what started the recreate churn described on
    /// <see cref="ListenForEvents"/>;</item>
    /// <item>a failure on a call that <see cref="EnsureHealthy"/> has already replaced says nothing
    /// about its replacement.</item>
    /// </list>
    /// A server-side refusal arrives as a <em>completed</em> call (the peer ended the stream with the
    /// status), so it is classified as dead and recreated like any other fault.
    /// </summary>
    internal void MarkFaultedAfterWriteFailure(
        AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> call,
        Exception exception)
    {
        if (exception is RpcException { StatusCode: StatusCode.ResourceExhausted } && !IsCallComplete(call))
            return;

        FlagFaulted(call);
    }

    /// <summary>
    /// True once gRPC has a final status for <paramref name="call"/>: the peer completed or refused
    /// it, or it was cancelled. <see cref="AsyncDuplexStreamingCall{TRequest,TResponse}.GetStatus"/>
    /// throws <see cref="InvalidOperationException"/> while the call is still in progress, which is
    /// the only signal gRPC exposes for "in progress" without awaiting the response stream.
    /// </summary>
    private static bool IsCallComplete(AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> call)
    {
        try
        {
            call.GetStatus();
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>
    /// Sets the fault flag only when <paramref name="call"/> is still the slot's current call.
    /// Taken under <see cref="recreateLock"/> so the check cannot interleave with the swap in
    /// <see cref="EnsureHealthy"/>: a verdict about a call that was replaced must never land on
    /// its replacement.
    /// </summary>
    private void FlagFaulted(AsyncDuplexStreamingCall<GrpcBatchRequestsRequest, GrpcBatchRequestsResponse> call)
    {
        lock (recreateLock)
        {
            if (ReferenceEquals(streaming, call))
                faulted = true;
        }
    }

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
    ///
    /// <para><b>The flag is set only while <paramref name="call"/> is still the slot's current
    /// call.</b> <see cref="EnsureHealthy"/> disposes the call it replaces, which ends that call's
    /// drain loop with a cancellation some time later — on a loaded thread pool, after the fresh
    /// call was installed and the flag cleared. Setting the flag unconditionally here then marked
    /// the healthy replacement faulted, the next hand-out disposed <em>it</em>, its own loop ended
    /// the same way, and the slot re-opened a stream on every send for as long as traffic flowed:
    /// each frame was written to a brand-new stream and the previous stream was reset under the
    /// frame still in flight on it, so large frames never arrived while small ones did.</para>
    ///
    /// <para>The fault is logged through <see cref="ReportFault"/>, which prints at most one line
    /// per <see cref="FaultLogIntervalMs"/> for this slot: a dead link re-opens the stream on every
    /// send attempt, and each re-open ends here.</para>
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
            // This task is fire-and-forget: an exception that escapes here is never observed and
            // is re-raised on the finalizer thread by TaskExceptionHolder, where a secondary
            // failure aborts the whole process. Under memory exhaustion the log write below is
            // itself an allocation and DID throw OutOfMemoryException out of this catch in the
            // Caraxes run Q crash (SIGABRT from the finalizer thread). Nothing thrown from
            // logging may escape; the finally still flags the stream for recreation.
            //
            // The cancellation of a call EnsureHealthy already replaced is the expected end of its
            // drain loop, not an event: left in, it printed one line per recreated stream.
            bool replacedCallCancelled = ex is RpcException { StatusCode: StatusCode.Cancelled }
                                         && !ReferenceEquals(streaming, call);
            if (!replacedCallCancelled)
            {
                try
                {
                    ReportFault(ex);
                }
                catch
                {
                    // Logging failed (most likely OOM); the fault flag below is the required outcome.
                }
            }
        }
        finally
        {
            FlagFaulted(call);
        }
    }

    /// <summary>
    /// Logs a drain-loop fault at most once per <see cref="FaultLogIntervalMs"/> for this slot and
    /// counts the ones it does not print onto the next printed line. On the 1.13.3 inst3 soak a
    /// stuck channel produced one of these per send attempt beside the dispatcher's own line, for
    /// 53 minutes; the pair made a 1.14 GB node log. Lock-free: concurrent faults race on the
    /// timestamp and the losers count themselves as suppressed.
    /// </summary>
    private void ReportFault(Exception ex)
    {
        long now = Environment.TickCount64;
        long last = Volatile.Read(ref lastFaultLogMs);

        if (last != 0 && now - last < FaultLogIntervalMs)
        {
            Interlocked.Increment(ref suppressedFaults);
            return;
        }

        if (Interlocked.CompareExchange(ref lastFaultLogMs, now, last) != last)
        {
            Interlocked.Increment(ref suppressedFaults);
            return;
        }

        SharedChannels.LogStreamFault(ex, Interlocked.Exchange(ref suppressedFaults, 0), FaultLogIntervalMs / 1000);
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
    bool EnableSnapshotCompression = false,
    int MaxMessageBytes = 0);

/// <summary>
/// One pooled channel to a peer as a soak harness or an operator sees it. <see cref="State"/> is
/// the channel's live connectivity; <see cref="Rebuilds"/> counts the times the pool disposed the
/// slot's channel and opened a new one because it had stopped reconnecting (see
/// <see cref="ChannelConnectivityWatchdog"/>). A peer that answers other nodes while this node
/// reports its channels in <see cref="ConnectivityState.TransientFailure"/> is the one-way failure
/// that the 1.13.3 inst3 soak could only infer from a log flood.
/// </summary>
public readonly record struct PeerChannelHealth(int Slot, ConnectivityState State, long Rebuilds);

/// <summary>
/// Provides utilities for managing and retrieving shared gRPC channels and streamings.
///
/// <para><b>A channel is not for life.</b> Every pooled channel has a watchdog that rebuilds it
/// when its connect loop dies (<see cref="ChannelConnectivityWatchdog"/>): the channel is
/// disposed, a new one takes its slot, and the duplex stream bound to the slot re-opens on it.
/// Callers therefore hold slots and streams, not channels: <see cref="GetChannel(string, RaftTransportSecurityOptions?)"/>
/// resolves the slot's current channel on every call, and <see cref="GetAllChannels(string, RaftTransportSecurityOptions?)"/>
/// is a snapshot whose entries a rebuild may dispose later.</para>
/// </summary>
public static class SharedChannels
{
    /// <summary>
    /// One pooled connection to a peer. The channel is the part that can die: a rebuild swaps a
    /// fresh one in through <see cref="TryReplace"/> and flags the duplex stream bound to the slot
    /// so its next re-open lands on the replacement. Slot identity is stable for the life of the
    /// process, which is what <see cref="GetOrderedStreaming"/> pins to: the ordering guarantee
    /// rests on the stream slot, and the slot survives its channel.
    /// </summary>
    internal sealed class ChannelSlot
    {
        private readonly object swapLock = new();
        private volatile GrpcChannel channel;
        private volatile GrpcInterSharedStreaming? streaming;
        private long rebuilds;

        public ChannelSlot(string url, int index, GrpcChannelPoolOptions options, GrpcChannel channel)
        {
            Url = url;
            Index = index;
            Options = options;
            this.channel = channel;
        }

        public string Url { get; }

        public int Index { get; }

        public GrpcChannelPoolOptions Options { get; }

        /// <summary>The slot's current channel. Read it per use; never cache it across a send.</summary>
        public GrpcChannel Channel => channel;

        public long Rebuilds => Interlocked.Read(ref rebuilds);

        /// <summary>Binds the stream that re-opens its duplex call on this slot's channel, so a rebuild can flag it.</summary>
        public void BindStreaming(GrpcInterSharedStreaming slotStreaming) => streaming = slotStreaming;

        /// <summary>
        /// Installs <paramref name="fresh"/> if <paramref name="stale"/> is still the slot's channel;
        /// returns false, and the caller disposes <paramref name="fresh"/>, when another rebuild won.
        /// The stream is flagged <em>after</em> the swap: a re-open racing with this either already
        /// reads the fresh channel or is flagged again and re-opens once more, and is never left on
        /// the stale one.
        /// </summary>
        public bool TryReplace(GrpcChannel stale, GrpcChannel fresh)
        {
            lock (swapLock)
            {
                if (!ReferenceEquals(channel, stale))
                    return false;

                channel = fresh;
                Interlocked.Increment(ref rebuilds);
            }

            streaming?.MarkFaulted();
            return true;
        }
    }

    /// <summary>
    /// One cached entry per normalized URL: the lazily-built slot list plus its round-robin
    /// cursor. Combining them means the per-message send path performs ONE string-keyed
    /// dictionary lookup instead of two (the counters previously lived in a parallel dictionary
    /// keyed by the same URL). Counter wraparound through int.MaxValue → int.MinValue is
    /// harmless: the uint cast before the modulo keeps the index in [0, count).
    /// </summary>
    private sealed class UrlPool<T>
    {
        public required Lazy<T[]> Slots { get; init; }

        public int Counter;
    }

    private static readonly ConcurrentDictionary<string, UrlPool<ChannelSlot>> channels = new();

    private static readonly ConcurrentDictionary<string, UrlPool<GrpcInterSharedStreaming>> streamings = new();

    // Tracks the scalar pool options (ChannelsPerNode, EnableMultipleHttp2Connections) that were
    // used to create each normalized URL's channel pool.  Debug builds assert that no second caller
    // passes conflicting options for the same URL — Kommander guarantees one transport config per
    // process, so a mismatch indicates a mis-wired call site.  SecurityOptions and streaming
    // metadata are intentionally excluded: they are reference types whose instance identity is
    // unreliable for comparison purposes here.
    private static readonly ConcurrentDictionary<string, (int ChannelsPerNode, bool EnableMultipleHttp2Connections, RaftNodeAuthenticationMode NodeAuthenticationMode)> registeredPoolConfig = new();

    // Process-wide defaults used by the public (securityOptions-only) overloads.
    // Packed into a single immutable reference so readers always observe a consistent
    // (ChannelsPerNode, EnableMultipleHttp2Connections) pair — two separate volatile
    // fields would admit a torn read between the two stores in Configure().
    // RaftManager calls Configure() during construction (before any peer I/O) so that
    // external consumers such as Kahuna's GrpcServerBatcher inherit the operator's
    // RaftConfiguration values rather than the library-defined (4, false) fallback.
    private sealed record DefaultPoolConfig(int ChannelsPerNode, bool EnableMultipleHttp2Connections, int MaxMessageBytes = 0);
    private static volatile DefaultPoolConfig defaultPoolConfig = new(4, false);

    // The pool is process-global and so is its logger: the last manager to configure it wins,
    // which is the one manager a production process has. Without one the pool writes to the
    // console, as the drain loops always did.
    private static volatile ILogger? logger;

    /// <summary>
    /// How long a channel may sit in <see cref="ConnectivityState.TransientFailure"/> with no
    /// transition before its slot is rebuilt. See <see cref="ChannelConnectivityWatchdog"/> for
    /// why three connect budgets is the line between a slow loop and a dead one.
    /// </summary>
    internal static readonly TimeSpan TransientFailureRebuildAfter = ChannelConnectivityWatchdog.DefaultTransientFailureLimit;

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
        Configure(channelsPerNode, enableMultipleHttp2Connections, maxMessageBytes: 0);

    /// <summary>
    /// As <see cref="Configure(int, bool)"/>, also fixing the per-channel
    /// <c>MaxReceiveMessageSize</c>/<c>MaxSendMessageSize</c> the public overloads apply
    /// (<see cref="RaftConfiguration.GrpcMaxMessageBytes"/>); 0 keeps the gRPC library default.
    /// </summary>
    public static void Configure(int channelsPerNode, bool enableMultipleHttp2Connections, int maxMessageBytes) =>
        defaultPoolConfig = new(channelsPerNode, enableMultipleHttp2Connections, maxMessageBytes);

    /// <summary>
    /// As <see cref="Configure(int, bool, int)"/>, also naming the logger the pool reports channel
    /// rebuilds and stream faults to. <see langword="null"/> keeps the console fallback.
    /// </summary>
    public static void Configure(int channelsPerNode, bool enableMultipleHttp2Connections, int maxMessageBytes, ILogger? poolLogger)
    {
        Configure(channelsPerNode, enableMultipleHttp2Connections, maxMessageBytes);
        logger = poolLogger;
    }

    // Checks in debug builds that a caller for this URL uses the same scalar pool options as
    // the pool that was actually built.  Called *after* the pool's Lazy.Value is resolved so
    // the registeredPoolConfig entry was written by the winning factory thread — not by a
    // racing caller — eliminating the TOCTOU that existed when GetOrAdd was used before
    // the pool was created.
    //
    // The rest of SecurityOptions and the streaming Metadata are intentionally excluded from the
    // check.  Both are reference types whose instance identity is unreliable for equality comparison,
    // and Kommander guarantees a single transport configuration per process: every node in a
    // cluster uses the same TLS/secret settings, so a per-URL security mismatch is a
    // deployment misconfiguration not detectable here.  The single-config-per-process
    // invariant is enforced at the Configure()/GetPoolOptions() layer instead.
    //
    // NodeAuthenticationMode is the exception, because it is a value type that compares reliably and
    // because getting it wrong is silent rather than noisy: a pool first built for a Disabled or
    // SharedSecret manager carries no client certificate, so a MutualTls manager that later reuses it
    // for the same URL would connect without presenting one and fail authentication at the peer for
    // reasons that point nowhere near the pool.  In-process test suites that construct managers in
    // different modes are exactly where this happens.
    [Conditional("DEBUG")]
    private static void AssertConsistentPoolConfig(string normalizedUrl, GrpcChannelPoolOptions opts)
    {
        if (!registeredPoolConfig.TryGetValue(normalizedUrl, out (int ChannelsPerNode, bool EnableMultipleHttp2Connections, RaftNodeAuthenticationMode NodeAuthenticationMode) stored))
            return;

        RaftNodeAuthenticationMode requestedAuthMode =
            opts.SecurityOptions?.NodeAuthenticationMode ?? RaftNodeAuthenticationMode.Disabled;

        Debug.Assert(
            stored.ChannelsPerNode == opts.ChannelsPerNode
            && stored.EnableMultipleHttp2Connections == opts.EnableMultipleHttp2Connections
            && stored.NodeAuthenticationMode == requestedAuthMode,
            $"SharedChannels: conflicting pool options for {normalizedUrl}. " +
            $"Pool built with ChannelsPerNode={stored.ChannelsPerNode}, EnableMultipleHttp2Connections={stored.EnableMultipleHttp2Connections}, NodeAuthenticationMode={stored.NodeAuthenticationMode}. " +
            $"This caller requested ChannelsPerNode={opts.ChannelsPerNode}, EnableMultipleHttp2Connections={opts.EnableMultipleHttp2Connections}, NodeAuthenticationMode={requestedAuthMode}. " +
            "Kommander supports one transport configuration per URL per process.");
    }

    /// <summary>A bare <c>host:port</c> is an HTTPS address; a URL with a scheme is kept as given.</summary>
    private static string NormalizeUrl(string url) =>
        url.StartsWith("https://", StringComparison.Ordinal) || url.StartsWith("http://", StringComparison.Ordinal)
            ? url
            : "https://" + url;

    /// <summary>
    /// The channel pool of a normalized URL, created on first use. TryGetValue fast path: on the
    /// warm per-message path the entry always exists, and calling GetOrAdd directly would allocate
    /// the capturing factory closure on every hit.
    /// </summary>
    private static UrlPool<ChannelSlot> GetChannelPool(string url, GrpcChannelPoolOptions opts)
    {
        if (channels.TryGetValue(url, out UrlPool<ChannelSlot>? pool))
            return pool;

        return channels.GetOrAdd(
            url,
            k => new UrlPool<ChannelSlot>
            {
                Slots = new Lazy<ChannelSlot[]>(() =>
                {
                    // Record the winning opts inside the factory so registeredPoolConfig always
                    // reflects the opts the pool was actually built with, not a racing caller's.
                    registeredPoolConfig.TryAdd(k, (opts.ChannelsPerNode, opts.EnableMultipleHttp2Connections, opts.SecurityOptions?.NodeAuthenticationMode ?? RaftNodeAuthenticationMode.Disabled));
                    return CreateSharedChannels(k, opts);
                })
            });
    }

    /// <summary>The streaming pool of a normalized URL, created on first use; same fast path as <see cref="GetChannelPool"/>.</summary>
    private static UrlPool<GrpcInterSharedStreaming> GetStreamingPool(string url, Func<Metadata?>? metadataFactory, GrpcChannelPoolOptions opts)
    {
        if (streamings.TryGetValue(url, out UrlPool<GrpcInterSharedStreaming>? pool))
            return pool;

        return streamings.GetOrAdd(
            url,
            k => new UrlPool<GrpcInterSharedStreaming>
            {
                Slots = new Lazy<GrpcInterSharedStreaming[]>(
                    () => CreateAsyncDuplexStreamingCallInternal(k, metadataFactory, opts))
            });
    }

    // ── Internal overloads (accept GrpcChannelPoolOptions directly) ───────────
    // Used by GrpcCommunication to flow RaftConfiguration values into the pool.
    // The public overloads below are thin backward-compatible wrappers for external
    // consumers (e.g. Kahuna's GrpcServerBatcher) that do not have access to the
    // internal options type.

    internal static GrpcChannel GetChannel(string url, GrpcChannelPoolOptions opts)
    {
        url = NormalizeUrl(url);

        UrlPool<ChannelSlot> pool = GetChannelPool(url, opts);
        ChannelSlot[] slots = pool.Slots.Value;
        AssertConsistentPoolConfig(url, opts);
        int idx = (int)((uint)Interlocked.Increment(ref pool.Counter) % slots.Length);
        return slots[idx].Channel;
    }

    internal static List<GrpcChannel> GetAllChannels(string url, GrpcChannelPoolOptions opts)
    {
        url = NormalizeUrl(url);

        ChannelSlot[] slots = GetChannelPool(url, opts).Slots.Value;
        AssertConsistentPoolConfig(url, opts);

        List<GrpcChannel> snapshot = new(slots.Length);
        foreach (ChannelSlot slot in slots)
            snapshot.Add(slot.Channel);

        return snapshot;
    }

    internal static GrpcInterSharedStreaming GetStreaming(string url, Func<Metadata?>? metadataFactory, GrpcChannelPoolOptions opts)
    {
        url = NormalizeUrl(url);

        UrlPool<GrpcInterSharedStreaming> pool = GetStreamingPool(url, metadataFactory, opts);
        GrpcInterSharedStreaming[] streamingList = pool.Slots.Value;
        AssertConsistentPoolConfig(url, opts);
        int idx = (int)((uint)Interlocked.Increment(ref pool.Counter) % streamingList.Length);
        GrpcInterSharedStreaming selected = streamingList[idx];
        // Self-heal: if this slot's stream faulted (peer reset, keepalive timeout, connection
        // fault), recreate it before handing it out instead of returning a permanently dead stream.
        selected.EnsureHealthy();
        return selected;
    }

    /// <summary>
    /// The one duplex stream to <paramref name="url"/> that carries its replication traffic: always
    /// the pool's first slot (self-healed like <see cref="GetStreaming(string, Func{Metadata?}?, GrpcChannelPoolOptions)"/>),
    /// never the round-robin pick.
    ///
    /// <para><b>Why a fixed slot.</b> The receiver reads each stream in order and hands its items to
    /// the partition executors in that order, but it reads different streams concurrently. Two
    /// consecutive <c>AppendLogs</c> frames sent round-robin on two streams could therefore reach a
    /// follower's executor swapped. The live broadcast is unanchored, so the later batch lands over
    /// a gap and the follower withholds its ack until the gap fills; the withheld ack then comes
    /// back only through the proposal retry, a few proposals per heartbeat. When both followers of
    /// a three-voter partition were caught at once, proposals waited out
    /// <see cref="RaftConfiguration.ProposalTimeout"/> (Kommander.Benchmark, 128 writers, ~50
    /// timeouts per 15 s at 8,000 proposals/s). The sender side loses nothing: the transport
    /// dispatcher already sends one frame at a time per peer.</para>
    ///
    /// <para>Pinning to one slot is also why a stuck channel in that slot stopped a peer's whole
    /// replication while round-robin traffic to it still partly flowed (1.13.3 inst3): the slot's
    /// watchdog rebuild is what bounds that outage now.</para>
    /// </summary>
    internal static GrpcInterSharedStreaming GetOrderedStreaming(string url, Func<Metadata?>? metadataFactory, GrpcChannelPoolOptions opts)
    {
        url = NormalizeUrl(url);

        GrpcInterSharedStreaming[] streamingList = GetStreamingPool(url, metadataFactory, opts).Slots.Value;
        AssertConsistentPoolConfig(url, opts);
        GrpcInterSharedStreaming selected = streamingList[0];
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
        return GetChannel(url, new GrpcChannelPoolOptions(cfg.ChannelsPerNode, cfg.EnableMultipleHttp2Connections, securityOptions, MaxMessageBytes: cfg.MaxMessageBytes));
    }

    /// <summary>
    /// Retrieves a snapshot of the gRPC channels associated with the specified URL, creating the
    /// shared channels if they do not already exist.
    /// </summary>
    /// <param name="url">
    /// The target URL for retrieving or creating the associated gRPC channels. If the URL does not start
    /// with "https://" or "http://", it is automatically prefixed with "https://".
    /// </param>
    /// <param name="securityOptions">
    /// Transport security options controlling certificate validation and thumbprint pinning.
    /// </param>
    /// <returns>
    /// A new list holding the pool's current <see cref="GrpcChannel"/> instances. The pool rebuilds a
    /// channel that stops reconnecting, disposing the one in the snapshot: a caller that binds
    /// long-lived calls to these channels must call again when those calls fault, as Kahuna's
    /// <c>GrpcServerBatcher</c> does when it invalidates its streams.
    /// </returns>
    public static List<GrpcChannel> GetAllChannels(string url, RaftTransportSecurityOptions? securityOptions = null)
    {
        DefaultPoolConfig cfg = defaultPoolConfig;
        return GetAllChannels(url, new GrpcChannelPoolOptions(cfg.ChannelsPerNode, cfg.EnableMultipleHttp2Connections, securityOptions, MaxMessageBytes: cfg.MaxMessageBytes));
    }

    public static GrpcInterSharedStreaming GetStreaming(string url, Metadata? metadata = null, RaftTransportSecurityOptions? securityOptions = null)
    {
        DefaultPoolConfig cfg = defaultPoolConfig;
        // Backward-compatible overload for external consumers that hand in an already-built
        // Metadata instance. Wrap it in a constant factory so the slot-creation path is uniform.
        Func<Metadata?>? metadataFactory = metadata is null ? null : () => metadata;
        return GetStreaming(url, metadataFactory, new GrpcChannelPoolOptions(cfg.ChannelsPerNode, cfg.EnableMultipleHttp2Connections, securityOptions, MaxMessageBytes: cfg.MaxMessageBytes));
    }

    // ── Channel health ────────────────────────────────────────────────────────

    /// <summary>
    /// The live state of every pooled channel to <paramref name="url"/>, in slot order; empty when
    /// no pool exists for it yet. For a soak or an operator telling "the peer is down" from "my
    /// channel to a live peer is down". Does not create the pool.
    /// </summary>
    public static IReadOnlyList<PeerChannelHealth> GetChannelHealth(string url)
    {
        if (!channels.TryGetValue(NormalizeUrl(url), out UrlPool<ChannelSlot>? pool) || !pool.Slots.IsValueCreated)
            return [];

        ChannelSlot[] slots = pool.Slots.Value;
        PeerChannelHealth[] health = new PeerChannelHealth[slots.Length];

        for (int i = 0; i < slots.Length; i++)
            health[i] = new PeerChannelHealth(i, ReadState(slots[i].Channel), slots[i].Rebuilds);

        return health;
    }

    /// <summary>
    /// Per peer URL, how many of its pooled channels are in <see cref="ConnectivityState.TransientFailure"/>
    /// right now; feeds the <c>raft.transport.channels_in_transient_failure</c> gauge.
    /// </summary>
    internal static IEnumerable<(string Url, int InTransientFailure)> CountChannelsInTransientFailure()
    {
        foreach (KeyValuePair<string, UrlPool<ChannelSlot>> entry in channels)
        {
            if (!entry.Value.Slots.IsValueCreated)
                continue;

            int failing = 0;
            foreach (ChannelSlot slot in entry.Value.Slots.Value)
            {
                if (ReadState(slot.Channel) == ConnectivityState.TransientFailure)
                    failing++;
            }

            yield return (entry.Key, failing);
        }
    }

    /// <summary>
    /// The channel's connectivity, or Idle for a handler without the connectivity API (not one the
    /// pool builds, but the public surface promises a value, not an exception).
    /// </summary>
    private static ConnectivityState ReadState(GrpcChannel channel)
    {
        try
        {
            return channel.State;
        }
        catch (InvalidOperationException)
        {
            return ConnectivityState.Idle;
        }
    }

    /// <summary>
    /// Creates a list of asynchronous duplex streaming calls for handling gRPC batch requests,
    /// associating each call with a specific channel slot.
    /// </summary>
    /// <param name="url">
    /// The target URL used for creating and retrieving gRPC channels. If the URL does not start
    /// with "https://" or "http://", it is automatically prefixed with "https://".
    /// </param>
    /// <returns>
    /// A list of <see cref="GrpcInterSharedStreaming"/> instances, each containing a duplex
    /// streaming call for gRPC batch requests.
    /// </returns>
    private static GrpcInterSharedStreaming[] CreateAsyncDuplexStreamingCallInternal(
        string url,
        Func<Metadata?>? metadataFactory,
        GrpcChannelPoolOptions opts)
    {
        url = NormalizeUrl(url);

        ChannelSlot[] slots = GetChannelPool(url, opts).Slots.Value;

        // Streaming pool is sized to match the cached channel pool exactly so slot i of each
        // never indexes out of range — the channel pool was created by whoever first called
        // GetOrAdd for this URL, so we must bind to its actual count, not opts.ChannelsPerNode.
        GrpcInterSharedStreaming[] streamingList = new GrpcInterSharedStreaming[slots.Length];

        for (int i = 0; i < slots.Length; i++)
        {
            ChannelSlot slot = slots[i];

            // Each stream slot keeps a factory bound to its channel slot so EnsureHealthy can
            // re-open the duplex call in place after a fault. The channel is resolved from the
            // slot at every open, never captured: a watchdog rebuild swaps the slot's channel and
            // flags this stream, and the re-open must land on the replacement.
            //
            // The auth metadata is built *inside* this factory rather than once for the whole
            // pool. gRPC sends stream metadata only at stream establishment, so for shared-secret
            // mode the per-stream nonce/timestamp must be signed when the stream is opened — and
            // re-signed when EnsureHealthy re-opens it. Sharing a single frozen Metadata across
            // every slot (and every re-open) would reuse one nonce, which replay detection on the
            // receiver would reject after the first stream. Invoking metadataFactory per slot
            // gives each stream its own freshly signed headers.
            GrpcInterSharedStreaming streaming = new(() =>
            {
                CallOptions callOptions = BuildStreamingCallOptions(metadataFactory?.Invoke(), opts.EnableSnapshotCompression);
                return new Rafter.RafterClient(slot.Channel).BatchRequests(callOptions);
            });

            slot.BindStreaming(streaming);
            streamingList[i] = streaming;
        }

        return streamingList;
    }

    private static SslClientAuthenticationOptions BuildSslOptions(RaftTransportSecurityOptions? securityOptions)
    {
        SslClientAuthenticationOptions sslOptions = new();

        // Presenting our own certificate is independent of how we validate theirs: the branches below
        // configure RemoteCertificateValidationCallback (the server side), so this is set first and
        // composes with whichever of them applies.
        if (securityOptions?.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls)
        {
            X509Certificate2? clientCertificate = securityOptions.GetClientCertificate();

            if (clientCertificate is not null)
                sslOptions.ClientCertificates = [clientCertificate];
        }

        if (securityOptions?.AllowInsecureCertificateValidation == true)
        {
            sslOptions.RemoteCertificateValidationCallback = delegate { return true; };
        }
        else if (securityOptions?.TrustedServerCertificateThumbprints is { Count: > 0 } thumbprints)
        {
            // Thumbprint *and* validity window: pinning alone accepted an expired server certificate,
            // while the server side rejected an expired client certificate.
            sslOptions.RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                RaftClientCertificateValidator.IsServerCertificateTrusted(
                    certificate,
                    thumbprints,
                    TimeProvider.System);
        }

        return sslOptions;
    }

    private static ChannelSlot[] CreateSharedChannels(string url, GrpcChannelPoolOptions opts)
    {
        SslClientAuthenticationOptions sslOptions = BuildSslOptions(opts.SecurityOptions);

        ChannelSlot[] slots = new ChannelSlot[opts.ChannelsPerNode];

        for (int i = 0; i < slots.Length; i++)
        {
            ChannelSlot slot = new(url, i, opts, CreateChannel(url, opts, sslOptions));
            slots[i] = slot;
            _ = WatchAsync(slot);
        }

        return slots;
    }

    /// <summary>
    /// One channel with its own <see cref="SocketsHttpHandler"/>, so the pool opens independent
    /// connections rather than multiplexing all streams over one shared connection. The handler is
    /// owned by the channel (<see cref="GrpcChannelOptions.DisposeHttpClient"/>): a rebuild disposes
    /// the channel, and its sockets must go with it.
    /// </summary>
    private static GrpcChannel CreateChannel(string url, GrpcChannelPoolOptions opts, SslClientAuthenticationOptions sslOptions)
    {
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
            HttpHandler = handler,
            DisposeHttpClient = true
        };

        // Both directions: the receive side so responses honour the operator's limit, the send
        // side so an over-budget frame fails here, on the sender, with the size in the error,
        // instead of being written and refused by the peer as a stream teardown.
        if (opts.MaxMessageBytes > 0)
        {
            channelOptions.MaxReceiveMessageSize = opts.MaxMessageBytes;
            channelOptions.MaxSendMessageSize = opts.MaxMessageBytes;
        }

        if (opts.EnableSnapshotCompression)
        {
            channelOptions.CompressionProviders =
            [
                new GzipCompressionProvider(CompressionLevel.Fastest)
            ];
        }

        return GrpcChannel.ForAddress(url, channelOptions);
    }

    // ── Channel watchdog ──────────────────────────────────────────────────────

    /// <summary>
    /// Follows one slot's channel for the life of the process and rebuilds it when its connect
    /// loop has died: a channel that sits in <see cref="ConnectivityState.TransientFailure"/> for
    /// <see cref="TransientFailureRebuildAfter"/> with no transition (the verdict of
    /// <see cref="ChannelConnectivityWatchdog"/>), or one that was disposed or shut down while still
    /// in the slot. Parks on <see cref="GrpcChannel.WaitForStateChangedAsync"/> otherwise, so a
    /// healthy channel costs nothing. Every path that observes the channel ends in either a wait
    /// or a rebuild, so the loop cannot spin; and a rebuild is only applied to the channel this
    /// iteration judged, so a slot replaced meanwhile is never rebuilt twice for one verdict.
    /// Never throws: the task is unobserved, and a surprise here must not take the process down
    /// from the finalizer thread.
    /// </summary>
    private static async Task WatchAsync(ChannelSlot slot)
    {
        try
        {
            while (true)
            {
                GrpcChannel channel = slot.Channel;
                string reason;

                try
                {
                    ConnectivityState state = channel.State;

                    if (state == ConnectivityState.Shutdown)
                    {
                        reason = "the channel is shut down";
                    }
                    else if (state != ConnectivityState.TransientFailure)
                    {
                        await channel.WaitForStateChangedAsync(state).ConfigureAwait(false);
                        continue;
                    }
                    else if (await ChannelConnectivityWatchdog
                                 .StaysInTransientFailureAsync(new GrpcChannelConnectivity(channel), TransientFailureRebuildAfter)
                                 .ConfigureAwait(false))
                    {
                        reason = $"it sat in TransientFailure for {TransientFailureRebuildAfter.TotalSeconds:F0} s with no connect attempt, so its connect loop is dead";
                    }
                    else
                    {
                        continue;
                    }
                }
                catch (OperationCanceledException)
                {
                    // Disposing a channel cancels its state waiters. The next pass re-reads the
                    // slot: a replaced channel is forgotten, a disposed one still in the slot
                    // surfaces below as ObjectDisposedException.
                    continue;
                }
                catch (ObjectDisposedException)
                {
                    reason = "the channel was disposed while still in its slot";
                }
                catch (InvalidOperationException ex)
                {
                    // The handler has no connectivity API. The pool builds none such, so this is
                    // a wiring change worth one line; there is nothing to watch.
                    Log(LogLevel.Warning, "[SharedChannels] Not watching channel {Slot} to {Url}: {Message}", slot.Index, slot.Url, ex.Message);
                    return;
                }

                if (!ReferenceEquals(slot.Channel, channel))
                    continue;

                Rebuild(slot, channel, reason);
            }
        }
        catch (Exception ex)
        {
            try
            {
                Log(LogLevel.Error, "[SharedChannels] Watchdog of channel {Slot} to {Url} stopped: {Type}: {Message}", slot.Index, slot.Url, ex.GetType().Name, ex.Message);
            }
            catch
            {
                // Nothing thrown from an unobserved task may escape.
            }
        }
    }

    /// <summary>
    /// Rebuilds slot <paramref name="slotIndex"/> of the pool for <paramref name="url"/> now, as the
    /// watchdog would. Test seam: the verdict is unit-tested on its own and this exercises the swap,
    /// the stream re-open and the disposal. Returns false when no such pool or slot exists.
    /// </summary>
    internal static bool RebuildChannel(string url, int slotIndex, string reason)
    {
        if (!channels.TryGetValue(NormalizeUrl(url), out UrlPool<ChannelSlot>? pool) || !pool.Slots.IsValueCreated)
            return false;

        ChannelSlot[] slots = pool.Slots.Value;

        if ((uint)slotIndex >= (uint)slots.Length)
            return false;

        ChannelSlot slot = slots[slotIndex];
        return Rebuild(slot, slot.Channel, reason);
    }

    /// <summary>
    /// Opens a new channel for <paramref name="slot"/>, installs it in place of <paramref name="stale"/>,
    /// and disposes <paramref name="stale"/>: its active calls (the dead duplex stream, and any
    /// unary call that picked it a moment ago) end with a cancellation their callers already handle,
    /// and its sockets close with its handler. The rebuild is applied only if <paramref name="stale"/>
    /// is still the slot's channel.
    /// </summary>
    private static bool Rebuild(ChannelSlot slot, GrpcChannel stale, string reason)
    {
        GrpcChannel fresh = CreateChannel(slot.Url, slot.Options, BuildSslOptions(slot.Options.SecurityOptions));

        if (!slot.TryReplace(stale, fresh))
        {
            fresh.Dispose();
            return false;
        }

        KommanderMetrics.RecordChannelRebuild(slot.Url);

        Log(LogLevel.Warning,
            "[SharedChannels] Rebuilt channel {Slot} of {Count} to {Url} because {Reason} (rebuild #{Rebuilds} of this slot); the stale channel is disposed and the stream slot re-opens on the new one",
            slot.Index, slot.Options.ChannelsPerNode, slot.Url, reason, slot.Rebuilds);

        try
        {
            stale.Dispose();
        }
        catch (Exception ex)
        {
            Log(LogLevel.Debug, "[SharedChannels] Disposing the stale channel {Slot} to {Url}: {Type}: {Message}", slot.Index, slot.Url, ex.GetType().Name, ex.Message);
        }

        return true;
    }

    // ── Logging ───────────────────────────────────────────────────────────────

    /// <summary>
    /// One stream slot's drain-loop fault, with the count of faults the slot did not print since
    /// its last line (see <see cref="GrpcInterSharedStreaming"/>). Warning: the stream re-opens on
    /// its own, but a dead link shows up here first.
    /// </summary>
    internal static void LogStreamFault(Exception ex, long suppressed, long intervalSeconds)
    {
        if (suppressed > 0)
            Log(LogLevel.Warning, "ListenForEvents: {Type}: {Message} ({Suppressed} similar faults in the last {Seconds} s not shown)", ex.GetType().Name, ex.Message, suppressed, intervalSeconds);
        else
            Log(LogLevel.Warning, "ListenForEvents: {Type}: {Message}", ex.GetType().Name, ex.Message);
    }

    private static void Log(LogLevel level, string message, params object?[] args)
    {
        ILogger? target = logger;

        if (target is not null)
        {
            target.Log(level, message, args);
            return;
        }

        // The drain loops always wrote here before the pool had a logger; keep the fallback so a
        // host that never configured one still sees the fault.
        Console.WriteLine(FormatForConsole(message, args));
    }

    /// <summary>Renders a structured template for the console fallback: placeholders in order of appearance.</summary>
    private static string FormatForConsole(string message, object?[] args)
    {
        global::System.Text.StringBuilder sb = new(message.Length + 64);
        int arg = 0;

        for (int i = 0; i < message.Length; i++)
        {
            char c = message[i];

            if (c == '{')
            {
                int close = message.IndexOf('}', i + 1);

                if (close > i && arg < args.Length)
                {
                    sb.Append(args[arg++]);
                    i = close;
                    continue;
                }
            }

            sb.Append(c);
        }

        return sb.ToString();
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
