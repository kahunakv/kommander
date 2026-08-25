
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using Kommander.Data;
using Kommander.Gossip;
using Microsoft.Extensions.Logging;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Allows for communication between Raft nodes using gRPC messages
/// </summary>
public class GrpcCommunication : ICommunication
{
    internal const string GrpcRequestEncodingHeader = "grpc-internal-encoding-request";

    internal const string GzipRequestEncoding = "gzip";

    private readonly GossipRosterJsonCache gossipRosterJsonCache = new();

    private static readonly RequestVotesResponse requestVotesResponse = new();
    
    private static readonly VoteResponse voteResponse = new();
    
    private static readonly AppendLogsResponse appendLogsResponse = new();
    
    private static readonly CompleteAppendLogsResponse completeAppendLogsResponse = new();

    private static readonly BatchRequestsResponse batchRequestsResponse = new();

    // Per-manager cached signing state for the duplex BatchRequests stream.
    //
    // Streaming auth metadata is only consumed when a stream slot is opened (or re-opened by
    // EnsureHealthy), never per message — gRPC sends metadata once at stream establishment. So
    // the hot send path must not recompute HMAC/nonce/Metadata on every call. We cache one
    // StreamingAuth per manager: it holds either a metadata factory (shared-secret mode) that
    // signs fresh headers each time a slot is created, or null (auth disabled). The factory is
    // captured once into the streaming pool's slot factories, so warm-stream sends do zero crypto.
    //
    // Keyed weakly so transient managers (e.g. per-test clusters) do not leak. Security options
    // and LocalEndpoint are immutable after RaftManager construction, so caching the authenticator
    // is safe.
    private static readonly ConditionalWeakTable<RaftManager, StreamingAuth> streamingAuthByManager = new();

    private sealed class StreamingAuth
    {
        public required Func<Metadata?>? Factory { get; init; }
    }

    // Per-manager cached transport state for the per-message send path.
    //
    // GetEffectiveTransportSecurity() allocates a fresh RaftTransportSecurityOptions on every
    // call, and the resulting GrpcChannelPoolOptions is only ever consumed when a channel pool
    // is first created — on the warm path it would be built and discarded once per outbound RPC.
    // Endpoint URLs (scheme + endpoint) are likewise stable per node. Both are derived from
    // configuration that is immutable after RaftManager construction (same contract the
    // streamingAuthByManager cache above relies on), so caching them per manager is safe; the
    // DEBUG-only AssertConsistentPoolConfig in SharedChannels still enforces the
    // one-transport-config-per-URL invariant.
    private static readonly ConditionalWeakTable<RaftManager, TransportCache> transportCacheByManager = new();

    private sealed class TransportCache
    {
        public required GrpcChannelPoolOptions PoolOptions { get; init; }

        public readonly ConcurrentDictionary<string, string> EndpointUrls = new();
    }

    // The generated RafterClient is a stateless, thread-safe wrapper over its channel; caching one
    // per channel avoids re-allocating it on every unary RPC (pings, gossip, and read-index probes
    // fire per peer on timers). Keyed weakly so disposed channels do not pin clients.
    private static readonly ConditionalWeakTable<GrpcChannel, Rafter.RafterClient> clientByChannel = new();

    private static Rafter.RafterClient GetClient(RaftManager manager, RaftNode node) =>
        clientByChannel.GetValue(
            SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)),
            static channel => new Rafter.RafterClient(channel));

    private static TransportCache GetTransportCache(RaftManager manager) =>
        transportCacheByManager.GetValue(manager, static m => new TransportCache
        {
            PoolOptions = new(
                m.Configuration.GetEffectiveGrpcChannelsPerNode(),
                m.Configuration.GrpcEnableMultipleHttp2Connections,
                m.Configuration.GetEffectiveTransportSecurity(),
                m.Configuration.GrpcEnableSnapshotCompression)
        });

    //private static readonly SemaphoreSlim semaphore = new(1, 1);

    /// <summary>
    /// Sends a Handshake message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcHandshakeRequest handshake = new()
        {
            NodeId = request.NodeId,
            Partition = request.Partition,
            MaxLogId = request.MaxLogId,
            Endpoint = request.Endpoint
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Handshake", handshake);
        GrpcHandshakeResponse response = await client
            .HandshakeAsync(handshake, new CallOptions(metadata))
            .ResponseAsync
            .ConfigureAwait(false);
        return new(response.NodeId, response.MaxLogId, response.Endpoint);
    }
    
    /// <summary>
    /// Sends a RequestVotes message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
    {
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(
            GetEndpointUrl(manager, node),
            GetStreamingAuthFactory(manager),
            GetPoolOptions(manager));

        GrpcRequestVotesRequest requestVotes = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            LastLogTerm = request.LastLogTerm,
            TimeNode = request.Time.N,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint,
            PreVote = request.PreVote
        };

        GrpcBatchRequestsRequestItem requestItem = new()
        {
            Type = GrpcBatchRequestsRequestType.RequestVotes,
            RequestVotes = requestVotes
        };

        GrpcBatchRequestsRequest batchRequests = new();
        batchRequests.Requests.Add(requestItem);

        try
        {
            await streaming.Semaphore.WaitAsync();
            
            await streaming.Streaming.RequestStream.WriteAsync(batchRequests);
        } 
        finally
        {
            streaming.Semaphore.Release();
        }

        return requestVotesResponse;
    }

    /// <summary>
    /// Sends a Vote message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
    {
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(
            GetEndpointUrl(manager, node),
            GetStreamingAuthFactory(manager),
            GetPoolOptions(manager));

        GrpcVoteRequest voteRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            LastLogTerm = request.LastLogTerm,
            TimeNode = request.Time.N,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint,
            PreVote = request.PreVote
        };

        GrpcBatchRequestsRequestItem requestItem = new()
        {
            Type = GrpcBatchRequestsRequestType.Vote,
            Vote = voteRequest
        };

        GrpcBatchRequestsRequest batchRequests = new();
        batchRequests.Requests.Add(requestItem);

        try
        {
            await streaming.Semaphore.WaitAsync();
            
            await streaming.Streaming.RequestStream.WriteAsync(batchRequests);
        } 
        finally
        {
            streaming.Semaphore.Release();
        }
        
        return voteResponse;
    }

    /// <summary>
    /// Sends an AppendLogs message to a node via gRPC.
    /// <para>
    /// When <see cref="RaftConfiguration.GrpcEnableAppendLogsCoalescing"/> is
    /// <see langword="true"/>, items that arrive while the stream is busy are queued and
    /// flushed as a single multi-item <c>GrpcBatchRequestsRequest</c> by the thread that
    /// next acquires the stream semaphore (backpressure-driven coalescing, no artificial
    /// delay). When the flag is <see langword="false"/>, the original single-item path is
    /// used unchanged.
    /// </para>
    /// </summary>
    public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(
            GetEndpointUrl(manager, node),
            GetStreamingAuthFactory(manager),
            GetPoolOptions(manager));

        if (manager.Configuration.GrpcEnableAppendLogsCoalescing)
            return await AppendLogsCoalesced(streaming, request, manager.Configuration.GrpcAppendLogsMaxCoalesceBatch);

        GrpcAppendLogsRequest appendLogsRequest = GrpcCommunicationPool.RentAppendLogsRequest();

        try
        {
            FillAppendLogsRequest(appendLogsRequest, request);

            GrpcBatchRequestsRequestItem requestItem = new()
            {
                Type = GrpcBatchRequestsRequestType.AppendLogs,
                AppendLogs = appendLogsRequest
            };

            GrpcBatchRequestsRequest batchRequests = new();
            batchRequests.Requests.Add(requestItem);

            try
            {
                await streaming.Semaphore.WaitAsync();

                await streaming.Streaming.RequestStream.WriteAsync(batchRequests);
            }
            catch
            {
                // A failed write means the duplex stream is broken; flag it so the pool
                // recreates this slot on the next GetStreaming instead of reusing a dead stream.
                streaming.MarkFaulted();
                throw;
            }
            finally
            {
                streaming.Semaphore.Release();
            }
        }
        finally
        {
            GrpcCommunicationPool.Return(appendLogsRequest);
        }

        return appendLogsResponse;
    }

    /// <summary>
    /// Coalescing entry-point: builds the <see cref="PendingAppendLogs"/> item from
    /// <paramref name="request"/> and delegates to <see cref="FlushCoalesced"/>.
    /// </summary>
    private async Task<AppendLogsResponse> AppendLogsCoalesced(
        GrpcInterSharedStreaming streaming,
        AppendLogsRequest request,
        int maxBatch)
    {
        // Bound the pending queue. A non-flusher enqueues and returns immediately, so the
        // dispatcher's await gives NO backpressure on this path: while one flusher sits in a
        // stalled WriteAsync (frozen peer, zero TCP window), every producer keeps adding fully
        // serialized payloads here without limit — the same unbounded-outbound-queue shape that
        // exhausted the Caraxes run Q leader, one config flag away. Dropping is safe by this
        // path's documented contract: the send is fire-and-forget and the heartbeat/backfill
        // retry re-ships unacknowledged entries. Four full flush cycles of headroom keeps the
        // drop rare under healthy bursts.
        const int pendingCapMultiplier = 4;
        if (streaming.Pending.Count >= maxBatch * pendingCapMultiplier)
            return appendLogsResponse;

        GrpcAppendLogsRequest appendLogsRequest = GrpcCommunicationPool.RentAppendLogsRequest();
        FillAppendLogsRequest(appendLogsRequest, request);

        GrpcBatchRequestsRequestItem requestItem = new()
        {
            Type = GrpcBatchRequestsRequestType.AppendLogs,
            AppendLogs = appendLogsRequest
        };

        // Static write delegate with the stream holder as explicit state: the previous
        // `async b => ...` lambda captured `streaming` and allocated a closure per append.
        await FlushCoalesced(
            streaming.Pending,
            streaming.Semaphore,
            streaming,
            static async (s, b) =>
            {
                try
                {
                    await s.Streaming.RequestStream.WriteAsync(b);
                }
                catch
                {
                    // Broken stream: flag for recreation on the next GetStreaming.
                    s.MarkFaulted();
                    throw;
                }
            },
            maxBatch,
            new(requestItem, appendLogsRequest));

        return appendLogsResponse;
    }

    /// <summary>
    /// Core of the backpressure-driven coalescing flush.  Extracted as
    /// <see langword="internal static"/> so tests can inject a fake <paramref name="write"/>
    /// delegate and drive every path without a live gRPC server.
    /// <para>
    /// Each caller enqueues <paramref name="item"/> into <paramref name="pending"/> and then
    /// attempts a non-blocking <c>Wait(0)</c> on <paramref name="semaphore"/>:
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>Flusher</b> (acquired): drains up to <paramref name="maxBatch"/> items per
    ///     write cycle into one <c>GrpcBatchRequestsRequest</c>, calls
    ///     <paramref name="write"/>, and loops while the queue is non-empty — bounding
    ///     individual frames regardless of backlog depth.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Non-flusher</b> (semaphore busy): returns immediately; the active flusher's
    ///     do/while re-loop will pick up the enqueued item.
    ///   </description></item>
    /// </list>
    /// </para>
    /// <para>
    /// <b>Pool ownership:</b> the flusher returns all <see cref="GrpcAppendLogsRequest"/>
    /// objects it drains via <see cref="GrpcCommunicationPool.Return(GrpcAppendLogsRequest)"/>.
    /// Non-flushers must NOT return their own rented object — ownership transfers to the queue.
    /// </para>
    /// <para>
    /// <b>Fire-and-forget semantics:</b> an item that lands in the queue in the narrow window
    /// between the flusher's last drain and its <c>Release()</c> will be picked up by the next
    /// caller that acquires the semaphore.  The Raft retry mechanism handles missing entries.
    /// </para>
    /// <para>
    /// <b>Error attribution:</b> unlike the single-item path — where each send's failure is
    /// observed by its own caller — a failing <paramref name="write"/> here surfaces only to the
    /// flusher's call; the other producers whose items were coalesced into the failed batch have
    /// already returned the synthetic success response.  This is acceptable because the send is
    /// fire-and-forget and the leader re-sends unacknowledged entries via the heartbeat/backfill
    /// path; no caller relies on the <c>AppendLogs</c> return value for delivery confirmation.
    /// </para>
    /// </summary>
    internal static Task FlushCoalesced(
        ConcurrentQueue<PendingAppendLogs> pending,
        SemaphoreSlim semaphore,
        Func<GrpcBatchRequestsRequest, Task> write,
        int maxBatch,
        PendingAppendLogs item) =>
        FlushCoalesced(pending, semaphore, write, static (w, b) => w(b), maxBatch, item);

    /// <summary>
    /// State-carried core of <see cref="FlushCoalesced"/>: the write delegate receives
    /// <paramref name="writeState"/> explicitly so the production caller can pass a static
    /// (non-capturing) delegate — the closure-per-append the plain overload forces on it was a
    /// measured per-send allocation. Semantics are identical to the plain overload.
    /// </summary>
    internal static async Task FlushCoalesced<TState>(
        ConcurrentQueue<PendingAppendLogs> pending,
        SemaphoreSlim semaphore,
        TState writeState,
        Func<TState, GrpcBatchRequestsRequest, Task> write,
        int maxBatch,
        PendingAppendLogs item)
    {
        pending.Enqueue(item);

        // Non-blocking try: if the stream is already busy we are done — the active flusher's
        // re-loop will drain our item.  If we acquire we become the flusher.
        if (!semaphore.Wait(0))
            return;

        // Pre-sized to the batch cap: starting at capacity 0 costs ~9 growth reallocations
        // per flush cycle when the backlog reaches maxBatch (default 256).
        List<GrpcAppendLogsRequest> toReturn = new(maxBatch);
        try
        {
            do
            {
                GrpcBatchRequestsRequest batch = new();

                // Cap per cycle to bound individual frame size.  The do/while re-loops for
                // any remainder, so no items are lost regardless of backlog depth.
                int drained = 0;
                while (drained < maxBatch && pending.TryDequeue(out PendingAppendLogs p))
                {
                    batch.Requests.Add(p.BatchItem);
                    toReturn.Add(p.PooledRequest);
                    drained++;
                }

                if (batch.Requests.Count > 0)
                    await write(writeState, batch);
            }
            while (!pending.IsEmpty);
        }
        finally
        {
            semaphore.Release();
            foreach (GrpcAppendLogsRequest r in toReturn)
                GrpcCommunicationPool.Return(r);
        }
    }

    /// <summary>
    /// Fills a rented <see cref="GrpcAppendLogsRequest"/> from <paramref name="request"/>.
    /// Extracted so both the single-item path and the coalescing path share one copy of
    /// this mapping without duplication.
    /// </summary>
    private static void FillAppendLogsRequest(GrpcAppendLogsRequest target, AppendLogsRequest request)
    {
        target.Partition = request.Partition;
        target.Term = request.Term;
        target.TimeNode = request.Time.N;
        target.TimePhysical = request.Time.L;
        target.TimeCounter = request.Time.C;
        target.Endpoint = request.Endpoint;
        target.PrevLogIndex = request.PrevLogIndex;
        target.PrevLogTerm = request.PrevLogTerm;
        target.Quiesce = request.Quiesce;

        if (request.Logs is { Count: > 0 })
            AddGrpcLogs(target.Logs, request);
    }
    
    /// <summary>
    /// Sends a CompleteAppendLogs message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(
            GetEndpointUrl(manager, node),
            GetStreamingAuthFactory(manager),
            GetPoolOptions(manager));

        GrpcCompleteAppendLogsRequest completeAppendLogsRequest = GrpcCommunicationPool.RentCompleteAppendLogsRequest();

        try
        {
            completeAppendLogsRequest.Partition = request.Partition;
            completeAppendLogsRequest.Term = request.Term;
            completeAppendLogsRequest.TimeNode = request.Time.N;
            completeAppendLogsRequest.TimePhysical = request.Time.L;
            completeAppendLogsRequest.TimeCounter = request.Time.C;
            completeAppendLogsRequest.Endpoint = request.Endpoint;
            completeAppendLogsRequest.Status = (GrpcRaftOperationStatus)request.Status;
            completeAppendLogsRequest.CommitIndex = request.CommitIndex;

            GrpcBatchRequestsRequestItem requestItem = new()
            {
                Type = GrpcBatchRequestsRequestType.CompleteAppendLogs,
                CompleteAppendLogs = completeAppendLogsRequest
            };

            GrpcBatchRequestsRequest batchRequests = new();
            batchRequests.Requests.Add(requestItem);

            try
            {
                await streaming.Semaphore.WaitAsync();

                await streaming.Streaming.RequestStream.WriteAsync(batchRequests);
            }
            finally
            {
                streaming.Semaphore.Release();
            }
        }
        finally
        {
            GrpcCommunicationPool.Return(completeAppendLogsRequest);
        }

        return completeAppendLogsResponse;
    }
    
    /// <summary>
    /// Sends a batch of AppendLogs message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="node"></param>
    /// <param name="requests"></param>
    /// <returns></returns>
    public async Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        if (request.Requests is null)
            return new();

        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(
            GetEndpointUrl(manager, node),
            GetStreamingAuthFactory(manager),
            GetPoolOptions(manager));
        
        // Items are added straight into the outgoing message's own RepeatedField — an intermediate
        // RepeatedField would be copied element-by-element into the message afterwards for nothing.
        GrpcBatchRequestsRequest batchRequests = new();

        foreach (BatchRequestsRequestItem requestItem in request.Requests)
        {
            GrpcBatchRequestsRequestItem? item = TryMapBatchItem(requestItem);

            if (item is null)
            {
                // A typed item with no payload must be dropped HERE, on the sender, where the type
                // and the gap are both known. Shipping it anyway hands the receiver an item it can
                // only fail on — the shape behind the two-year "BatchRequests: NullReferenceException"
                // noise (see TryMapBatchItem).
                manager.Logger.LogError(
                    "BatchRequests: dropped unmappable batch item of type {Type} for {Endpoint} (missing payload or unmapped type)",
                    requestItem.Type, node.Endpoint);
                continue;
            }

            batchRequests.Requests.Add(item);
        }
        
        try
        {
            await streaming.Semaphore.WaitAsync();

            await streaming.Streaming.RequestStream.WriteAsync(batchRequests);
        }
        finally
        {
            streaming.Semaphore.Release();
        }

        // Return the rented wire children only after WriteAsync completed — the serializer may
        // read them until then. On a failed write the objects are simply collected (a pool drop
        // is always safe; a premature return is not).
        foreach (GrpcBatchRequestsRequestItem sentItem in batchRequests.Requests)
        {
            if (sentItem.AppendLogs is not null)
                GrpcCommunicationPool.Return(sentItem.AppendLogs);
            else if (sentItem.CompleteAppendLogs is not null)
                GrpcCommunicationPool.Return(sentItem.CompleteAppendLogs);
        }

        return batchRequestsResponse;
    }

    /// <summary>
    /// Maps one transport-neutral batch item onto its gRPC wire shape, or returns null when the
    /// item cannot be mapped — an unknown type, or a declared type whose payload is missing. The
    /// caller drops a null loudly instead of sending it.
    ///
    /// <para><b>Keyed on the item's Type, mirroring the receiving switch in
    /// <c>RaftService.BatchRequests</c>, so the two ends cannot drift.</b> The previous mapping
    /// keyed on payload-field presence, and its if-chain silently lacked a
    /// <see cref="BatchRequestsRequestItem.TransferLeadershipSuggestion"/> branch: every leadership
    /// balance suggestion went out with <c>Type = TRANSFER_LEADERSHIP_SUGGESTION</c> and a null
    /// payload, the receiver dereferenced the payload and threw a bare
    /// <c>NullReferenceException</c> on the 30-second balancer cadence, and the suggestion itself
    /// was silently lost — the leader balancer never worked across gRPC (Caraxes
    /// "BatchRequests: NullReferenceException" finding, present since at least 1.2.3). A
    /// type-keyed switch turns that class of gap into a compile-visible case list with a loud
    /// default, on the sender, where the bug lives.</para>
    /// </summary>
    internal static GrpcBatchRequestsRequestItem? TryMapBatchItem(BatchRequestsRequestItem requestItem)
    {
        GrpcBatchRequestsRequestItem item = new()
        {
            Type = (GrpcBatchRequestsRequestType)requestItem.Type
        };

        switch (requestItem.Type)
        {
            // A ping is deliberately payload-free: it exists only to keep the stream observably
            // alive, and the receiver ignores it.
            case BatchRequestsRequestType.Ping:
                return item;

            case BatchRequestsRequestType.Handshake when requestItem.Handshake is not null:
                item.Handshake = new()
                {
                    NodeId = requestItem.Handshake.NodeId,
                    Partition = requestItem.Handshake.Partition,
                    MaxLogId = requestItem.Handshake.MaxLogId,
                    Endpoint = requestItem.Handshake.Endpoint
                };
                return item;

            case BatchRequestsRequestType.Vote when requestItem.Vote is not null:
                item.Vote = new()
                {
                    Partition = requestItem.Vote.Partition,
                    Term = requestItem.Vote.Term,
                    MaxLogId = requestItem.Vote.MaxLogId,
                    LastLogTerm = requestItem.Vote.LastLogTerm,
                    TimeNode = requestItem.Vote.Time.N,
                    TimePhysical = requestItem.Vote.Time.L,
                    TimeCounter = requestItem.Vote.Time.C,
                    Endpoint = requestItem.Vote.Endpoint,
                    PreVote = requestItem.Vote.PreVote
                };
                return item;

            case BatchRequestsRequestType.RequestVote when requestItem.RequestVotes is not null:
                item.RequestVotes = new()
                {
                    Partition = requestItem.RequestVotes.Partition,
                    Term = requestItem.RequestVotes.Term,
                    MaxLogId = requestItem.RequestVotes.MaxLogId,
                    LastLogTerm = requestItem.RequestVotes.LastLogTerm,
                    TimeNode = requestItem.RequestVotes.Time.N,
                    TimePhysical = requestItem.RequestVotes.Time.L,
                    TimeCounter = requestItem.RequestVotes.Time.C,
                    Endpoint = requestItem.RequestVotes.Endpoint,
                    PreVote = requestItem.RequestVotes.PreVote
                };
                return item;

            case BatchRequestsRequestType.StepDownNotice when requestItem.StepDownNotice is not null:
                item.StepDownNotice = new()
                {
                    Partition = requestItem.StepDownNotice.Partition,
                    Term = requestItem.StepDownNotice.Term,
                    TimeNode = requestItem.StepDownNotice.Time.N,
                    TimePhysical = requestItem.StepDownNotice.Time.L,
                    TimeCounter = requestItem.StepDownNotice.Time.C,
                    Endpoint = requestItem.StepDownNotice.Endpoint
                };
                return item;

            case BatchRequestsRequestType.TransferLeadership when requestItem.TransferLeadership is not null:
                item.TransferLeadership = new()
                {
                    Partition = requestItem.TransferLeadership.Partition,
                    Term = requestItem.TransferLeadership.Term,
                    TimeNode = requestItem.TransferLeadership.Time.N,
                    TimePhysical = requestItem.TransferLeadership.Time.L,
                    TimeCounter = requestItem.TransferLeadership.Time.C,
                    Endpoint = requestItem.TransferLeadership.Endpoint,
                    TargetEndpoint = requestItem.TransferLeadership.TargetEndpoint
                };
                return item;

            case BatchRequestsRequestType.TransferLeadershipSuggestion when requestItem.TransferLeadershipSuggestion is not null:
                item.TransferLeadershipSuggestion = new()
                {
                    Partition = requestItem.TransferLeadershipSuggestion.Partition,
                    Term = requestItem.TransferLeadershipSuggestion.Term,
                    TimeNode = requestItem.TransferLeadershipSuggestion.Time.N,
                    TimePhysical = requestItem.TransferLeadershipSuggestion.Time.L,
                    TimeCounter = requestItem.TransferLeadershipSuggestion.Time.C,
                    SuggestedBy = requestItem.TransferLeadershipSuggestion.SuggestedBy,
                    TargetEndpoint = requestItem.TransferLeadershipSuggestion.TargetEndpoint
                };
                return item;

            // The two hot payloads rent their wire children from GrpcCommunicationPool instead of
            // allocating: BatchRequests (the only production caller) returns them after the frame
            // write completes, the same after-send rule the dispatcher applies to the managed
            // DTOs. A direct test caller that never returns the child only forgoes reuse — the
            // pool tolerates drops.
            case BatchRequestsRequestType.AppendLogs when requestItem.AppendLogs is not null:
            {
                GrpcAppendLogsRequest appendRequest = GrpcCommunicationPool.RentAppendLogsRequest();
                appendRequest.Partition = requestItem.AppendLogs.Partition;
                appendRequest.Term = requestItem.AppendLogs.Term;
                appendRequest.TimeNode = requestItem.AppendLogs.Time.N;
                appendRequest.TimePhysical = requestItem.AppendLogs.Time.L;
                appendRequest.TimeCounter = requestItem.AppendLogs.Time.C;
                appendRequest.Endpoint = requestItem.AppendLogs.Endpoint;
                appendRequest.PrevLogIndex = requestItem.AppendLogs.PrevLogIndex;
                appendRequest.PrevLogTerm = requestItem.AppendLogs.PrevLogTerm;
                appendRequest.Quiesce = requestItem.AppendLogs.Quiesce;

                if (requestItem.AppendLogs.Logs is { Count: > 0 })
                    AddGrpcLogs(appendRequest.Logs, requestItem.AppendLogs);

                item.AppendLogs = appendRequest;
                return item;
            }

            case BatchRequestsRequestType.CompleteAppendLogs when requestItem.CompleteAppendLogs is not null:
            {
                GrpcCompleteAppendLogsRequest completeRequest = GrpcCommunicationPool.RentCompleteAppendLogsRequest();
                completeRequest.Partition = requestItem.CompleteAppendLogs.Partition;
                completeRequest.Term = requestItem.CompleteAppendLogs.Term;
                completeRequest.TimeNode = requestItem.CompleteAppendLogs.Time.N;
                completeRequest.TimePhysical = requestItem.CompleteAppendLogs.Time.L;
                completeRequest.TimeCounter = requestItem.CompleteAppendLogs.Time.C;
                completeRequest.Endpoint = requestItem.CompleteAppendLogs.Endpoint;
                completeRequest.Status = (GrpcRaftOperationStatus)requestItem.CompleteAppendLogs.Status;
                completeRequest.CommitIndex = requestItem.CompleteAppendLogs.CommitIndex;

                item.CompleteAppendLogs = completeRequest;
                return item;
            }

            default:
                return null;
        }
    }

    /// <summary>
    /// Sends a <see cref="LeaveRequest"/> to <paramref name="node"/> via the <c>Leave</c> gRPC RPC.
    /// If the target is not the P0 leader it returns <see cref="LeaveResponse.LeaderHint"/> so the
    /// caller can retry against the current leader.
    /// </summary>
    public async Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcLeaveRequest grpcRequest = new()
        {
            Endpoint = request.Endpoint,
            NodeId = request.NodeId
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Leave", grpcRequest);
        try
        {
            GrpcLeaveResponse response = await client
                .LeaveAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
                .ResponseAsync
                .ConfigureAwait(false);

            return new LeaveResponse(
                response.Success,
                string.IsNullOrEmpty(response.LeaderHint) ? null : response.LeaderHint,
                response.Terminal,
                response.MembershipVersion);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendLeave to {Endpoint}: {Message}", node.Endpoint, ex.Message);
            return new LeaveResponse(false);
        }
    }

    /// <summary>
    /// Sends a <see cref="SetMemberRoleRequest"/> to <paramref name="node"/> via the
    /// <c>SetMemberRole</c> gRPC RPC. A pre-drain peer answers UNIMPLEMENTED, which surfaces
    /// here as a failed response — the drain then fails loudly instead of silently degrading
    /// into an immediate removal.
    /// </summary>
    public async Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcSetMemberRoleRequest grpcRequest = new()
        {
            Endpoint = request.Endpoint,
            NodeId = request.NodeId,
            TargetRole = (int)request.TargetRole
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/SetMemberRole", grpcRequest);
        try
        {
            GrpcSetMemberRoleResponse response = await client
                .SetMemberRoleAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
                .ResponseAsync
                .ConfigureAwait(false);

            return new SetMemberRoleResponse(
                response.Success,
                string.IsNullOrEmpty(response.LeaderHint) ? null : response.LeaderHint,
                (RaftOperationStatus)response.Status,
                response.MembershipVersion);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendSetMemberRole to {Endpoint}: {Message}", node.Endpoint, ex.Message);
            return new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored);
        }
    }

    /// <summary>
    /// Sends the local membership roster to <paramref name="node"/> for gossip anti-entropy.
    /// The receiver applies it when it is newer and replies with its own roster when it is
    /// strictly ahead, enabling push-pull convergence in one round trip.
    /// </summary>
    public async Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcGossipRequest grpcRequest = new()
        {
            SenderEndpoint = digest.SenderEndpoint,
            MembershipVersion = digest.MembershipVersion,
            RosterJson = digest.Roster is not null
                ? gossipRosterJsonCache.GetUtf8(digest.MembershipVersion, digest.Roster)
                : ByteString.Empty,
            LoadReportJson = digest.LoadReport is not null
                ? ByteString.CopyFromUtf8(global::System.Text.Json.JsonSerializer.Serialize(digest.LoadReport))
                : ByteString.Empty,
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Gossip", grpcRequest);
        try
        {
            GrpcGossipResponse response = await client
                .GossipAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
                .ResponseAsync
                .ConfigureAwait(false);

            Kommander.System.ClusterMembership? roster = null;
            if (!response.RosterJson.IsEmpty)
                roster = global::System.Text.Json.JsonSerializer.Deserialize<Kommander.System.ClusterMembership>(response.RosterJson.Span);

            return new GossipAck(response.MembershipVersion, roster);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendGossip to {Endpoint}: {Message}", node.Endpoint, ex.Message);
            return new GossipAck(0, null);
        }
    }

    /// <summary>
    /// Sends a direct SWIM probe to <paramref name="node"/> via the <c>Ping</c> gRPC RPC.
    /// Returns <c>PingResponse(false, 0)</c> on any transport error so the caller treats
    /// the node as unreachable.
    /// </summary>
    public async Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = GetClient(manager, node);
        GrpcPingRequest grpcRequest = new() { SenderEndpoint = request.SenderEndpoint };
        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Ping", grpcRequest);
        try
        {
            GrpcPingResponse response = await client
                .PingAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
                .ResponseAsync.ConfigureAwait(false);
            return new Gossip.PingResponse(response.Alive, response.Incarnation);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendPing to {Endpoint}: {Message}", node.Endpoint, ex.Message);
            return new Gossip.PingResponse(false, 0);
        }
    }

    /// <summary>
    /// Asks <paramref name="node"/> to relay a direct probe to a third node via the
    /// <c>PingReq</c> gRPC RPC.  Returns <c>PingReqResponse(false)</c> on any transport error.
    /// </summary>
    public async Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = GetClient(manager, node);
        GrpcPingReqRequest grpcRequest = new() { SenderEndpoint = request.SenderEndpoint, TargetEndpoint = request.TargetEndpoint };
        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/PingReq", grpcRequest);
        try
        {
            GrpcPingReqResponse response = await client
                .PingReqAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
                .ResponseAsync.ConfigureAwait(false);
            return new Gossip.PingReqResponse(response.Reached);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendPingReq to {Endpoint}: {Message}", node.Endpoint, ex.Message);
            return new Gossip.PingReqResponse(false);
        }
    }

    /// <summary>
    /// Queries <paramref name="node"/> for the committed log index it has recorded for
    /// <paramref name="followerEndpoint"/> on <paramref name="partitionId"/> via the
    /// <c>GetFollowerLag</c> gRPC RPC.  Returns <see langword="null"/> when the remote
    /// node reports that it has no record for the follower on that partition.
    /// </summary>
    public async Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcGetFollowerLagRequest grpcRequest = new()
        {
            PartitionId = partitionId,
            FollowerEndpoint = followerEndpoint
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/GetFollowerLag", grpcRequest);
        try
        {
            GrpcGetFollowerLagResponse response = await client
                .GetFollowerLagAsync(grpcRequest, new CallOptions(metadata))
                .ResponseAsync
                .ConfigureAwait(false);

            return response.HasValue ? response.Value : null;
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("GetRemoteFollowerLag from {Endpoint} partition {PartitionId}: {Message}",
                node.Endpoint, partitionId, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Fetches a quorum-confirmed read index from <paramref name="node"/> (believed partition
    /// leader) via the <c>GetReadIndex</c> gRPC RPC on behalf of a non-leader running
    /// <c>ConfirmLocalApplicationAsync</c>. Every transport failure maps to a failed response —
    /// the caller must fail closed, so no error may surface as an exception here.
    /// </summary>
    public async Task<GetReadIndexResponse> GetReadIndex(
        RaftManager manager, RaftNode node, GetReadIndexRequest request,
        CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcGetReadIndexRequest grpcRequest = new()
        {
            PartitionId = request.PartitionId
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/GetReadIndex", grpcRequest);
        try
        {
            GrpcGetReadIndexResponse response = await client
                .GetReadIndexAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
                .ResponseAsync
                .ConfigureAwait(false);

            return new GetReadIndexResponse(response.Success, response.ReadIndex);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("GetReadIndex from {Endpoint} partition {PartitionId}: {Message}",
                node.Endpoint, request.PartitionId, ex.Message);
            return new GetReadIndexResponse(false);
        }
    }

    public async Task<SnapshotResponse> SendInstallSnapshot(
        RaftManager manager, RaftNode node, SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcInstallSnapshotRequest grpcRequest = new()
        {
            SessionId = request.SessionId,
            PartitionId = request.PartitionId,
            SnapshotIndex = request.SnapshotIndex,
            FollowerEndpoint = request.FollowerEndpoint,
            ChunkIndex = request.ChunkIndex,
            IsLast = request.IsLast,
            Data = request.Data.Length == 0
                ? ByteString.Empty
                : UnsafeByteOperations.UnsafeWrap(request.Data),
            Kind = (int)request.Kind,
            LeaderTerm = request.LeaderTerm,
            LeaderEndpoint = request.LeaderEndpoint,
            LastIncludedTerm = request.LastIncludedTerm,
            SnapshotChecksum = request.SnapshotChecksum,
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/InstallSnapshot", grpcRequest);
        CallOptions callOptions = BuildInstallSnapshotCallOptions(
            manager.Configuration,
            metadata,
            cancellationToken);
        try
        {
            GrpcInstallSnapshotResponse response = await client
                .InstallSnapshotAsync(grpcRequest, callOptions)
                .ResponseAsync
                .ConfigureAwait(false);

            return new SnapshotResponse(response.Success);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendInstallSnapshot to {Endpoint} partition {PartitionId}: {Message}",
                node.Endpoint, request.PartitionId, ex.Message);
            return new SnapshotResponse(false);
        }
    }

    public async Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
    {
        Rafter.RafterClient client = GetClient(manager, node);

        GrpcJoinRequest grpcRequest = new()
        {
            Endpoint = request.Endpoint,
            NodeId = request.NodeId
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Join", grpcRequest);
        try
        {
            GrpcJoinResponse response = await client
                .JoinAsync(grpcRequest, new CallOptions(metadata))
                .ResponseAsync
                .ConfigureAwait(false);

            return new JoinResponse(response.Success, string.IsNullOrEmpty(response.LeaderHint) ? null : response.LeaderHint, response.MembershipVersion);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendJoin to {Endpoint}: {Message}", node.Endpoint, ex.Message);
            return new JoinResponse(false);
        }
    }

    /// <summary>
    /// Returns the cached per-manager metadata factory for the duplex <c>BatchRequests</c> stream,
    /// or <see langword="null"/> when node authentication is disabled. The returned delegate signs
    /// fresh headers (new nonce/timestamp) on each invocation; the streaming pool invokes it once
    /// per stream slot creation and re-open, so callers on the hot send path pay no per-message
    /// crypto cost. See <see cref="streamingAuthByManager"/> for why this is cached.
    /// </summary>
    private static Func<Metadata?>? GetStreamingAuthFactory(RaftManager manager) =>
        streamingAuthByManager.GetValue(manager, static m => new StreamingAuth
        {
            Factory = CreateStreamingAuthFactory(m.Configuration.GetEffectiveTransportSecurity(), m.LocalEndpoint)
        }).Factory;

    /// <summary>
    /// Builds the metadata factory for the duplex <c>BatchRequests</c> stream from immutable signing
    /// state, or <see langword="null"/> when authentication is disabled. Each invocation of the
    /// returned delegate signs a fresh nonce/timestamp, so per-slot creation and per-reopen yield
    /// distinct credentials — the receiver's replay detection rejects a shared frozen nonce.
    /// Extracted as <see langword="internal static"/> so the freshness invariant can be unit-tested
    /// without constructing a <see cref="RaftManager"/>.
    /// </summary>
    internal static Func<Metadata?>? CreateStreamingAuthFactory(
        RaftTransportSecurityOptions security,
        string localEndpoint)
    {
        if (security.NodeAuthenticationMode != RaftNodeAuthenticationMode.SharedSecret)
            return null;

        // Build the authenticator once and capture it; security options are immutable after
        // construction. The grpc method is always BatchRequests for every streaming send path.
        RaftTransportAuthenticator authenticator = new(security);

        return () =>
        {
            RaftTransportAuthenticationHeaders signed =
                authenticator.Sign("POST", "/Rafter/BatchRequests", localEndpoint);

            return
            [
                new(signed.SignatureHeaderName, signed.Signature),
                new(RaftTransportAuthenticationHeaders.SenderNodeHeaderName, signed.SenderNode),
                new(RaftTransportAuthenticationHeaders.TimestampHeaderName,
                    signed.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture)),
                new(RaftTransportAuthenticationHeaders.NonceHeaderName, signed.Nonce)
            ];
        };
    }

    /// <summary>
    /// Builds the signed metadata for a unary call, binding <paramref name="request"/> into the
    /// signature.
    /// </summary>
    /// <remarks>
    /// The message must be fully populated before this is called — it is digested here, so any field
    /// assigned afterwards travels unsigned and the receiver rejects the call as
    /// <c>InvalidSignature</c>. Every call site therefore builds its request first and passes the
    /// same instance it is about to send.
    /// </remarks>
    private static Metadata BuildAuthMetadata(RaftManager manager, string grpcMethod, IMessage request)
    {
        RaftTransportAuthenticator authenticator = manager.Configuration.GetTransportAuthenticator();
        if (authenticator.Options.NodeAuthenticationMode != RaftNodeAuthenticationMode.SharedSecret)
            return [];

        Span<byte> bodyHash = stackalloc byte[GrpcMessageBodyHash.HashSizeInBytes];
        GrpcMessageBodyHash.Compute(request, bodyHash);

        RaftTransportAuthenticationHeaders signed = authenticator.SignWithBodyHash(
            "POST",
            grpcMethod,
            manager.LocalEndpoint,
            bodyHash);

        return
        [
            new(signed.SignatureHeaderName, signed.Signature),
            new(RaftTransportAuthenticationHeaders.SenderNodeHeaderName, signed.SenderNode),
            new(RaftTransportAuthenticationHeaders.TimestampHeaderName,
                signed.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture)),
            new(RaftTransportAuthenticationHeaders.NonceHeaderName, signed.Nonce)
        ];
    }

    /// <summary>
    /// Builds <see cref="CallOptions"/> for <c>InstallSnapshot</c>. When
    /// <see cref="RaftConfiguration.GrpcEnableSnapshotCompression"/> is set, requests gzip
    /// encoding on the unary request so the registered channel compression provider is used.
    /// </summary>
    internal static CallOptions BuildInstallSnapshotCallOptions(
        RaftConfiguration configuration,
        Metadata metadata,
        CancellationToken cancellationToken)
    {
        if (configuration.GrpcEnableSnapshotCompression)
            metadata.Add(GrpcRequestEncodingHeader, GzipRequestEncoding);

        return new CallOptions(metadata, cancellationToken: cancellationToken);
    }

    private static GrpcChannelPoolOptions GetPoolOptions(RaftManager manager) =>
        GetTransportCache(manager).PoolOptions;

    /// <summary>
    /// Builds the full URL for <paramref name="node"/> by prepending the configured
    /// <see cref="RaftConfiguration.GrpcScheme"/>.  Defaults to <c>https://</c>; set
    /// <c>GrpcScheme = "http://"</c> (plus <c>Http2UnencryptedSupport</c>) for tests.
    /// Memoized per manager+endpoint — the composed string is hashed and prefix-tested by
    /// SharedChannels on every send, so allocating it fresh per message is pure churn.
    /// </summary>
    private static string GetEndpointUrl(RaftManager manager, RaftNode node)
    {
        ConcurrentDictionary<string, string> urls = GetTransportCache(manager).EndpointUrls;

        if (urls.TryGetValue(node.Endpoint, out string? url))
            return url;

        return urls.GetOrAdd(node.Endpoint, manager.Configuration.GrpcScheme + node.Endpoint);
    }

    private static void AddGrpcLogs(RepeatedField<GrpcRaftLog> target, AppendLogsRequest request)
    {
        if (request.GrpcLogCache is not null)
        {
            target.AddRange(request.GrpcLogCache.GetOrCreate(request.Logs!));
            return;
        }

        // Indexed loop, not an iterator: the previous `yield return` fallback allocated a
        // compiler state machine per call on top of the per-entry messages.
        List<RaftLog> requestLogs = request.Logs!;
        for (int i = 0; i < requestLogs.Count; i++)
        {
            RaftLog requestLog = requestLogs[i];
            target.Add(new GrpcRaftLog
            {
                Id = requestLog.Id,
                Term = requestLog.Term,
                Type = (GrpcRaftLogType)requestLog.Type,
                LogType = requestLog.LogType,
                TimeNode = requestLog.Time.N,
                TimePhysical = requestLog.Time.L,
                TimeCounter = requestLog.Time.C,
                Data = UnsafeByteOperations.UnsafeWrap(requestLog.LogData)
            });
        }
    }
}
