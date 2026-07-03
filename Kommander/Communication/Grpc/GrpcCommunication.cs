
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));

        GrpcHandshakeRequest handshake = new()
        {
            NodeId = request.NodeId,
            Partition = request.Partition,
            MaxLogId = request.MaxLogId,
            Endpoint = request.Endpoint
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Handshake");
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
        GrpcAppendLogsRequest appendLogsRequest = GrpcCommunicationPool.RentAppendLogsRequest();
        FillAppendLogsRequest(appendLogsRequest, request);

        GrpcBatchRequestsRequestItem requestItem = new()
        {
            Type = GrpcBatchRequestsRequestType.AppendLogs,
            AppendLogs = appendLogsRequest
        };

        await FlushCoalesced(
            streaming.Pending,
            streaming.Semaphore,
            async b =>
            {
                try
                {
                    await streaming.Streaming.RequestStream.WriteAsync(b);
                }
                catch
                {
                    // Broken stream: flag for recreation on the next GetStreaming.
                    streaming.MarkFaulted();
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
    internal static async Task FlushCoalesced(
        ConcurrentQueue<PendingAppendLogs> pending,
        SemaphoreSlim semaphore,
        Func<GrpcBatchRequestsRequest, Task> write,
        int maxBatch,
        PendingAppendLogs item)
    {
        pending.Enqueue(item);

        // Non-blocking try: if the stream is already busy we are done — the active flusher's
        // re-loop will drain our item.  If we acquire we become the flusher.
        if (!semaphore.Wait(0))
            return;

        List<GrpcAppendLogsRequest> toReturn = [];
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
                    await write(batch);
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
        
        RepeatedField<GrpcBatchRequestsRequestItem> items = new();
            
        foreach (BatchRequestsRequestItem requestItem in request.Requests)
        {
            GrpcBatchRequestsRequestItem item = new()
            {
                Type = (GrpcBatchRequestsRequestType) requestItem.Type
            };
            
            items.Add(item);

            if (requestItem.Handshake is not null)
            {
                GrpcHandshakeRequest handshake = new()
                {
                    NodeId = requestItem.Handshake.NodeId,
                    Partition = requestItem.Handshake.Partition,
                    MaxLogId = requestItem.Handshake.MaxLogId,
                    Endpoint = requestItem.Handshake.Endpoint
                };
                
                item.Handshake = handshake;
                continue;
            }
            
            if (requestItem.Vote is not null)
            {
                GrpcVoteRequest voteRequest = new()
                {
                    Partition = requestItem.Vote.Partition,
                    Term = requestItem.Vote.Term,
                    MaxLogId = requestItem.Vote.MaxLogId,
                    TimeNode = requestItem.Vote.Time.N,
                    TimePhysical = requestItem.Vote.Time.L,
                    TimeCounter = requestItem.Vote.Time.C,
                    Endpoint = requestItem.Vote.Endpoint,
                    PreVote = requestItem.Vote.PreVote
                };
                
                item.Vote = voteRequest;
                continue;
            }
            
            if (requestItem.RequestVotes is not null)
            {
                GrpcRequestVotesRequest requestVotes = new()
                {
                    Partition = requestItem.RequestVotes.Partition,
                    Term = requestItem.RequestVotes.Term,
                    MaxLogId = requestItem.RequestVotes.MaxLogId,
                    TimeNode = requestItem.RequestVotes.Time.N,
                    TimePhysical = requestItem.RequestVotes.Time.L,
                    TimeCounter = requestItem.RequestVotes.Time.C,
                    Endpoint = requestItem.RequestVotes.Endpoint,
                    PreVote = requestItem.RequestVotes.PreVote
                };
                
                item.RequestVotes = requestVotes;
                continue;
            }

            if (requestItem.StepDownNotice is not null)
            {
                GrpcStepDownNoticeRequest stepDownNotice = new()
                {
                    Partition = requestItem.StepDownNotice.Partition,
                    Term = requestItem.StepDownNotice.Term,
                    TimeNode = requestItem.StepDownNotice.Time.N,
                    TimePhysical = requestItem.StepDownNotice.Time.L,
                    TimeCounter = requestItem.StepDownNotice.Time.C,
                    Endpoint = requestItem.StepDownNotice.Endpoint
                };

                item.StepDownNotice = stepDownNotice;
                continue;
            }

            if (requestItem.TransferLeadership is not null)
            {
                GrpcTransferLeadershipRequest transferLeadership = new()
                {
                    Partition = requestItem.TransferLeadership.Partition,
                    Term = requestItem.TransferLeadership.Term,
                    TimeNode = requestItem.TransferLeadership.Time.N,
                    TimePhysical = requestItem.TransferLeadership.Time.L,
                    TimeCounter = requestItem.TransferLeadership.Time.C,
                    Endpoint = requestItem.TransferLeadership.Endpoint,
                    TargetEndpoint = requestItem.TransferLeadership.TargetEndpoint
                };

                item.TransferLeadership = transferLeadership;
                continue;
            }

            if (requestItem.AppendLogs is not null)
            {
                GrpcAppendLogsRequest appendRequest = new()
                {
                    Partition = requestItem.AppendLogs.Partition,
                    Term = requestItem.AppendLogs.Term,
                    TimeNode = requestItem.AppendLogs.Time.N,
                    TimePhysical = requestItem.AppendLogs.Time.L,
                    TimeCounter = requestItem.AppendLogs.Time.C,
                    Endpoint = requestItem.AppendLogs.Endpoint,
                    PrevLogIndex = requestItem.AppendLogs.PrevLogIndex,
                    PrevLogTerm = requestItem.AppendLogs.PrevLogTerm,
                    Quiesce = requestItem.AppendLogs.Quiesce
                };

                if (requestItem.AppendLogs.Logs is { Count: > 0 })
                    AddGrpcLogs(appendRequest.Logs, requestItem.AppendLogs);
                
                item.AppendLogs = appendRequest;
                continue;
            }
            
            if (requestItem.CompleteAppendLogs is not null)
            {
                GrpcCompleteAppendLogsRequest completeRequest = new()
                {
                    Partition = requestItem.CompleteAppendLogs.Partition,
                    Term = requestItem.CompleteAppendLogs.Term,
                    TimeNode = requestItem.CompleteAppendLogs.Time.N,
                    TimePhysical = requestItem.CompleteAppendLogs.Time.L,
                    TimeCounter = requestItem.CompleteAppendLogs.Time.C,
                    Endpoint = requestItem.CompleteAppendLogs.Endpoint,
                    Status = (GrpcRaftOperationStatus) requestItem.CompleteAppendLogs.Status,
                    CommitIndex = requestItem.CompleteAppendLogs.CommitIndex
                };
                
                item.CompleteAppendLogs = completeRequest;
            }
        }
        
        GrpcBatchRequestsRequest batchRequests = new();
        
        batchRequests.Requests.AddRange(items);
        
        try
        {
            await streaming.Semaphore.WaitAsync();
            
            await streaming.Streaming.RequestStream.WriteAsync(batchRequests);
        } 
        finally
        {
            streaming.Semaphore.Release();
        }
        
        return batchRequestsResponse;
    }
    
    /// <summary>
    /// Sends a <see cref="LeaveRequest"/> to <paramref name="node"/> via the <c>Leave</c> gRPC RPC.
    /// If the target is not the P0 leader it returns <see cref="LeaveResponse.LeaderHint"/> so the
    /// caller can retry against the current leader.
    /// </summary>
    public async Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));

        GrpcLeaveRequest grpcRequest = new()
        {
            Endpoint = request.Endpoint,
            NodeId = request.NodeId
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Leave");
        try
        {
            GrpcLeaveResponse response = await client
                .LeaveAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
                .ResponseAsync
                .ConfigureAwait(false);

            return new LeaveResponse(response.Success, string.IsNullOrEmpty(response.LeaderHint) ? null : response.LeaderHint, response.Terminal);
        }
        catch (Exception ex)
        {
            manager.Logger.LogWarning("SendLeave to {Endpoint}: {Message}", node.Endpoint, ex.Message);
            return new LeaveResponse(false);
        }
    }

    /// <summary>
    /// Sends the local membership roster to <paramref name="node"/> for gossip anti-entropy.
    /// The receiver applies it when it is newer and replies with its own roster when it is
    /// strictly ahead, enabling push-pull convergence in one round trip.
    /// </summary>
    public async Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));

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

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Gossip");
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));
        GrpcPingRequest grpcRequest = new() { SenderEndpoint = request.SenderEndpoint };
        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Ping");
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));
        GrpcPingReqRequest grpcRequest = new() { SenderEndpoint = request.SenderEndpoint, TargetEndpoint = request.TargetEndpoint };
        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/PingReq");
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));

        GrpcGetFollowerLagRequest grpcRequest = new()
        {
            PartitionId = partitionId,
            FollowerEndpoint = followerEndpoint
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/GetFollowerLag");
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

    public async Task<SnapshotResponse> SendInstallSnapshot(
        RaftManager manager, RaftNode node, SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));

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
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/InstallSnapshot");
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetPoolOptions(manager)));

        GrpcJoinRequest grpcRequest = new()
        {
            Endpoint = request.Endpoint,
            NodeId = request.NodeId
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/Join");
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

    private static Metadata BuildAuthMetadata(RaftManager manager, string grpcMethod)
    {
        RaftTransportSecurityOptions security = manager.Configuration.GetEffectiveTransportSecurity();
        if (security.NodeAuthenticationMode != RaftNodeAuthenticationMode.SharedSecret)
            return [];

        RaftTransportAuthenticator authenticator = new(security);
        RaftTransportAuthenticationHeaders signed = authenticator.Sign("POST", grpcMethod, manager.LocalEndpoint);

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

    private static RaftTransportSecurityOptions GetSecurityOptions(RaftManager manager) =>
        manager.Configuration.GetEffectiveTransportSecurity();

    private static GrpcChannelPoolOptions GetPoolOptions(RaftManager manager) =>
        new(manager.Configuration.GetEffectiveGrpcChannelsPerNode(),
            manager.Configuration.GrpcEnableMultipleHttp2Connections,
            GetSecurityOptions(manager),
            manager.Configuration.GrpcEnableSnapshotCompression);

    /// <summary>
    /// Builds the full URL for <paramref name="node"/> by prepending the configured
    /// <see cref="RaftConfiguration.GrpcScheme"/>.  Defaults to <c>https://</c>; set
    /// <c>GrpcScheme = "http://"</c> (plus <c>Http2UnencryptedSupport</c>) for tests.
    /// </summary>
    private static string GetEndpointUrl(RaftManager manager, RaftNode node) =>
        manager.Configuration.GrpcScheme + node.Endpoint;

    private static void AddGrpcLogs(RepeatedField<GrpcRaftLog> target, AppendLogsRequest request)
    {
        if (request.GrpcLogCache is not null)
            target.AddRange(request.GrpcLogCache.GetOrCreate(request.Logs!));
        else
            target.AddRange(GetLogs(request.Logs!));
    }

    private static IEnumerable<GrpcRaftLog> GetLogs(List<RaftLog> requestLogs)
    {
        foreach (RaftLog requestLog in requestLogs)
        {
            yield return new()
            {
                Id = requestLog.Id,
                Term = requestLog.Term,
                Type = (GrpcRaftLogType)requestLog.Type,
                LogType = requestLog.LogType,
                TimeNode = requestLog.Time.N,
                TimePhysical = requestLog.Time.L,
                TimeCounter = requestLog.Time.C,
                Data = UnsafeByteOperations.UnsafeWrap(requestLog.LogData)
            };
        }
    } 
}
