
using System.Globalization;
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
    private static readonly RequestVotesResponse requestVotesResponse = new();
    
    private static readonly VoteResponse voteResponse = new();
    
    private static readonly AppendLogsResponse appendLogsResponse = new();
    
    private static readonly CompleteAppendLogsResponse completeAppendLogsResponse = new();

    private static readonly BatchRequestsResponse batchRequestsResponse = new();
    
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));

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
            BuildAuthMetadata(manager, "/Rafter/BatchRequests"),
            GetSecurityOptions(manager));

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
            BuildAuthMetadata(manager, "/Rafter/BatchRequests"),
            GetSecurityOptions(manager));

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
    /// Sends an AppendLogs message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(
            GetEndpointUrl(manager, node),
            BuildAuthMetadata(manager, "/Rafter/BatchRequests"),
            GetSecurityOptions(manager));

        GrpcAppendLogsRequest appendLogsRequest = GrpcCommunicationPool.RentAppendLogsRequest();

        try
        {            
            appendLogsRequest.Partition = request.Partition;
            appendLogsRequest.Term = request.Term;
            appendLogsRequest.TimeNode = request.Time.N;
            appendLogsRequest.TimePhysical = request.Time.L;
            appendLogsRequest.TimeCounter = request.Time.C;
            appendLogsRequest.Endpoint = request.Endpoint;
            appendLogsRequest.PrevLogIndex = request.PrevLogIndex;
            appendLogsRequest.PrevLogTerm = request.PrevLogTerm;

            if (request.Logs is not null)
                appendLogsRequest.Logs.AddRange(GetLogs(request.Logs ?? []));

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
            BuildAuthMetadata(manager, "/Rafter/BatchRequests"),
            GetSecurityOptions(manager));

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
            BuildAuthMetadata(manager, "/Rafter/BatchRequests"),
            GetSecurityOptions(manager));
        
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
                    PrevLogTerm = requestItem.AppendLogs.PrevLogTerm
                };

                if (requestItem.AppendLogs.Logs is not null)
                    appendRequest.Logs.AddRange(GetLogs(requestItem.AppendLogs.Logs));
                
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));

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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));

        GrpcGossipRequest grpcRequest = new()
        {
            SenderEndpoint = digest.SenderEndpoint,
            MembershipVersion = digest.MembershipVersion,
            RosterJson = digest.Roster is not null
                ? ByteString.CopyFromUtf8(global::System.Text.Json.JsonSerializer.Serialize(digest.Roster))
                : ByteString.Empty
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));

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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));

        GrpcInstallSnapshotRequest grpcRequest = new()
        {
            SessionId = request.SessionId,
            PartitionId = request.PartitionId,
            SnapshotIndex = request.SnapshotIndex,
            FollowerEndpoint = request.FollowerEndpoint,
            ChunkIndex = request.ChunkIndex,
            IsLast = request.IsLast,
            Data = Google.Protobuf.ByteString.CopyFrom(request.Data),
        };

        Metadata metadata = BuildAuthMetadata(manager, "/Rafter/InstallSnapshot");
        try
        {
            GrpcInstallSnapshotResponse response = await client
                .InstallSnapshotAsync(grpcRequest, new CallOptions(metadata, cancellationToken: cancellationToken))
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
        Rafter.RafterClient client = new(SharedChannels.GetChannel(GetEndpointUrl(manager, node), GetSecurityOptions(manager)));

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

    private static RaftTransportSecurityOptions GetSecurityOptions(RaftManager manager) =>
        manager.Configuration.GetEffectiveTransportSecurity();

    /// <summary>
    /// Builds the full URL for <paramref name="node"/> by prepending the configured
    /// <see cref="RaftConfiguration.GrpcScheme"/>.  Defaults to <c>https://</c>; set
    /// <c>GrpcScheme = "http://"</c> (plus <c>Http2UnencryptedSupport</c>) for tests.
    /// </summary>
    private static string GetEndpointUrl(RaftManager manager, RaftNode node) =>
        manager.Configuration.GrpcScheme + node.Endpoint;

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
