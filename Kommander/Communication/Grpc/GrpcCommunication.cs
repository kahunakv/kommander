
using System.Collections.Concurrent;
using System.Net.Security;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Net.Client;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Allows for communication between Raft nodes using gRPC messages
/// </summary>
public class GrpcCommunication : ICommunication
{
    private const int MaxChannels = 5;
    
    private readonly ConcurrentDictionary<string, List<GrpcChannel>> channels = new();
    
    private int channelIndex;
    
    private static readonly HandshakeResponse handshakeResponse = new();
    
    private static readonly RequestVotesResponse requestVotesResponse = new();
    
    private static readonly VoteResponse voteResponse = new();
    
    private static readonly AppendLogsResponse appendLogsResponse = new();
    
    private static readonly AppendLogsBatchResponse appendLogsBatchResponse = new();
    
    private static readonly CompleteAppendLogsResponse completeAppendLogsResponse = new();
    
    private static readonly CompleteAppendLogsBatchResponse completeAppendLogsBatchResponse = new();
    
    private static readonly SocketsHttpHandler httpHandler = GetHandler();
    
    private static SocketsHttpHandler GetHandler()
    {
        SslClientAuthenticationOptions sslOptions = new()
        {
            // @todo proper certificate validation
            RemoteCertificateValidationCallback = delegate { return true; }
        };
        
        SocketsHttpHandler handler = new()
        {
            SslOptions = sslOptions,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(60),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
            EnableMultipleHttp2Connections = true
        };

        return handler;
    }

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
        GrpcChannel channel = GetChannel(node.Endpoint);

        Rafter.RafterClient client = new(channel);

        GrpcHandshakeRequest requestVotes = new()
        {
            Partition = request.Partition,
            MaxLogId = request.MaxLogId,
            Endpoint = request.Endpoint
        };
        
        await client.HandshakeAsync(requestVotes).ConfigureAwait(false);
        
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got RequestVotes reply from {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);

        return handshakeResponse;
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
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Sent RequestVotes message to {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);

        GrpcChannel channel = GetChannel(node.Endpoint);

        Rafter.RafterClient client = new(channel);

        GrpcRequestVotesRequest requestVotes = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
        };
        
        await client.RequestVotesAsync(requestVotes).ConfigureAwait(false);
        
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got RequestVotes reply from {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);

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
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Send Vote to {Node} message on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);
        
        GrpcChannel channel = GetChannel(node.Endpoint);
        
        Rafter.RafterClient client = new(channel);

        GrpcVoteRequest voteRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
        };
        
        await client.VoteAsync(voteRequest).ConfigureAwait(false);
        
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got Vote reply from {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);
        
        return voteResponse;
    }

    /// <summary>
    /// Sends an AppendLogs message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        GrpcChannel channel = GetChannel(node.Endpoint);
        
        Rafter.RafterClient client = new(channel);

        GrpcAppendLogsRequest grpcRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
        };

        if (request.Logs is not null)
            grpcRequest.Logs.AddRange(GetLogs(request.Logs ?? []));
        
        await client.AppendLogsAsync(grpcRequest).ConfigureAwait(false);
        
        return appendLogsResponse;
    }
    
    /// <summary>
    /// Sends a batch of AppendLogs message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="node"></param>
    /// <param name="requests"></param>
    /// <returns></returns>
    public async Task<AppendLogsBatchResponse> AppendLogsBatch(RaftManager manager, RaftNode node, AppendLogsBatchRequest request)
    {
        GrpcChannel channel = GetChannel(node.Endpoint);
        
        Rafter.RafterClient client = new(channel);
        
        GrpcAppendLogsBatchRequest batchRequest = new();

        if (request.AppendLogs is not null)
        {
            RepeatedField<GrpcAppendLogsRequest> grpcRequests = new();
            
            foreach (AppendLogsRequest appendLogsRequest in request.AppendLogs)
            {
                GrpcAppendLogsRequest grpcRequest = new()
                {
                    Partition = appendLogsRequest.Partition,
                    Term = appendLogsRequest.Term,
                    TimePhysical = appendLogsRequest.Time.L,
                    TimeCounter = appendLogsRequest.Time.C,
                    Endpoint = appendLogsRequest.Endpoint
                };
                
                if (appendLogsRequest.Logs is not null)
                    grpcRequest.Logs.AddRange(GetLogs(appendLogsRequest.Logs));

                grpcRequests.Add(grpcRequest);
            }
            
            batchRequest.AppendLogs.AddRange(grpcRequests);
        }
        
        await client.AppendLogsBatchAsync(batchRequest).ConfigureAwait(false);
        
        return appendLogsBatchResponse;
    }
    
    /// <summary>
    /// Sends a CompleteAppendLogs message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        GrpcChannel channel = GetChannel(node.Endpoint);
        
        Rafter.RafterClient client = new(channel);

        GrpcCompleteAppendLogsRequest grpcRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint,
            Status = (GrpcRaftOperationStatus) request.Status,
            CommitIndex = request.CommitIndex
        };
        
        await client.CompleteAppendLogsAsync(grpcRequest).ConfigureAwait(false);
        
        return completeAppendLogsResponse;
    }
    
    /// <summary>
    /// Sends a batch of AppendLogs message to a node via gRPC
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="node"></param>
    /// <param name="requests"></param>
    /// <returns></returns>
    public async Task<CompleteAppendLogsBatchResponse> CompleteAppendLogsBatch(RaftManager manager, RaftNode node, CompleteAppendLogsBatchRequest request)
    {
        GrpcChannel channel = GetChannel(node.Endpoint);
        
        Rafter.RafterClient client = new(channel);
        
        GrpcCompleteAppendLogsBatchRequest batchRequest = new();

        if (request.CompleteLogs is not null)
        {
            RepeatedField<GrpcCompleteAppendLogsRequest> grpcRequests = new();
            
            foreach (CompleteAppendLogsRequest appendLogsRequest in request.CompleteLogs)
            {
                GrpcCompleteAppendLogsRequest grpcRequest = new()
                {
                    Partition = appendLogsRequest.Partition,
                    Term = appendLogsRequest.Term,
                    TimePhysical = appendLogsRequest.Time.L,
                    TimeCounter = appendLogsRequest.Time.C,
                    Endpoint = appendLogsRequest.Endpoint,
                    Status = (GrpcRaftOperationStatus) appendLogsRequest.Status,
                    CommitIndex = appendLogsRequest.CommitIndex
                };

                grpcRequests.Add(grpcRequest);
            }
            
            batchRequest.CompleteLogs.AddRange(grpcRequests);
        }
        
        await client.CompleteAppendLogsBatchAsync(batchRequest).ConfigureAwait(false);
        
        return completeAppendLogsBatchResponse;
    }

    private static IEnumerable<GrpcRaftLog> GetLogs(List<RaftLog> requestLogs)
    {
        foreach (RaftLog? requestLog in requestLogs)
        {
            yield return new()
            {
                Id = requestLog.Id,
                Term = requestLog.Term,
                Type = (GrpcRaftLogType)requestLog.Type,
                LogType = requestLog.LogType,
                TimePhysical = requestLog.Time.L,
                TimeCounter = requestLog.Time.C,
                Data = UnsafeByteOperations.UnsafeWrap(requestLog.LogData)
            };
        }
    }
    
    private GrpcChannel GetChannel(string endpoint)
    {
        if (!channels.TryGetValue(endpoint, out List<GrpcChannel>? grpcChannels))
        {
            grpcChannels = new(MaxChannels);
            
            for (int i = 0; i < MaxChannels; i++)
                grpcChannels.Add(GrpcChannel.ForAddress($"https://{endpoint}", new() { HttpHandler = httpHandler }));
            
            channels.TryAdd(endpoint, grpcChannels);
        }

        int nextIndex = Interlocked.Increment(ref channelIndex);
        if (nextIndex < 0)
            nextIndex = -nextIndex;
        
        return grpcChannels[nextIndex % MaxChannels];
    }
}