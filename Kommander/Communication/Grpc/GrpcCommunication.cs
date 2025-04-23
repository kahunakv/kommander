
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Allows for communication between Raft nodes using gRPC messages
/// </summary>
public class GrpcCommunication : ICommunication
{    
    private static readonly HandshakeResponse handshakeResponse = new();
    
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
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(node.Endpoint);

        GrpcHandshakeRequest handshake = new()
        {
            Partition = request.Partition,
            MaxLogId = request.MaxLogId,
            Endpoint = request.Endpoint
        };

        GrpcBatchRequestsRequestItem requestItem = new()
        {
            Type = GrpcBatchRequestsRequestType.Handshake,
            Handshake = handshake
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
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(node.Endpoint);

        GrpcRequestVotesRequest requestVotes = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
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
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(node.Endpoint);

        GrpcVoteRequest voteRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
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
    /// <param name="partition"></param>
    /// <param name="node"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(node.Endpoint);

        GrpcAppendLogsRequest appendLogsRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
        };

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
        
        return appendLogsResponse;
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
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(node.Endpoint);

        GrpcCompleteAppendLogsRequest completeAppendLogsRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint,
            Status = (GrpcRaftOperationStatus) request.Status,
            CommitIndex = request.CommitIndex
        };
        
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
        
        GrpcInterSharedStreaming streaming = SharedChannels.GetStreaming(node.Endpoint);
        
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
                    TimePhysical = requestItem.Vote.Time.L,
                    TimeCounter = requestItem.Vote.Time.C,
                    Endpoint = requestItem.Vote.Endpoint
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
                    TimePhysical = requestItem.RequestVotes.Time.L,
                    TimeCounter = requestItem.RequestVotes.Time.C,
                    Endpoint = requestItem.RequestVotes.Endpoint
                };
                
                item.RequestVotes = requestVotes;
                continue;
            }

            if (requestItem.AppendLogs is not null)
            {
                GrpcAppendLogsRequest appendRequest = new()
                {
                    Partition = requestItem.AppendLogs.Partition,
                    Term = requestItem.AppendLogs.Term,
                    TimePhysical = requestItem.AppendLogs.Time.L,
                    TimeCounter = requestItem.AppendLogs.Time.C,
                    Endpoint = requestItem.AppendLogs.Endpoint
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
                TimePhysical = requestLog.Time.L,
                TimeCounter = requestLog.Time.C,
                Data = UnsafeByteOperations.UnsafeWrap(requestLog.LogData)
            };
        }
    } 
}