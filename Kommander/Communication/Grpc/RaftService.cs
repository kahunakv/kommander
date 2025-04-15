
using System.Runtime.InteropServices;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

public sealed class RaftService : Rafter.RafterBase
{        
    private  static readonly Task<GrpcVoteResponse> voteResponse = Task.FromResult(new GrpcVoteResponse());

    private static readonly Task<GrpcRequestVotesResponse> requestVoteResponse = Task.FromResult(new GrpcRequestVotesResponse());
    
    private static readonly Task<GrpcCompleteAppendLogsResponse> completeAppendLogsResponse = Task.FromResult(new GrpcCompleteAppendLogsResponse());
    
    private static readonly Task<GrpcAppendLogsResponse> appendLogsResponse = Task.FromResult(new GrpcAppendLogsResponse());
    
    private static readonly Task<GrpcAppendLogsBatchResponse> appendLogsBatchResponse = Task.FromResult(new GrpcAppendLogsBatchResponse());
    
    private static readonly Task<GrpcCompleteAppendLogsBatchResponse> completeAppendLogsBatchResponse = Task.FromResult(new GrpcCompleteAppendLogsBatchResponse());
    
    private readonly IRaft raft;

    private readonly ILogger<IRaft> logger;

    public RaftService(IRaft raft, ILogger<IRaft> logger)
    {
        this.raft = raft;
        this.logger = logger;
    }
    
    public override async Task<GrpcHandshakeResponse> Handshake(GrpcHandshakeRequest request, ServerCallContext context)
    {
        //logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got Vote message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);
                
        await raft.Handshake(new(
            request.Partition,
            request.MaxLogId,
            request.Endpoint
        ));
        
        return new();
    }
    
    public override Task<GrpcVoteResponse> Vote(GrpcVoteRequest request, ServerCallContext context)
    {
        //logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got Vote message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);

        raft.Vote(new(
            request.Partition,
            request.Term,
            request.MaxLogId,
            new(request.TimePhysical, request.TimeCounter),
            request.Endpoint
        ));
        
        return voteResponse;
    }
    
    public override Task<GrpcRequestVotesResponse> RequestVotes(GrpcRequestVotesRequest request, ServerCallContext context)
    {
        //logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got RequestVotes message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);

        raft.RequestVote(new(
            request.Partition,
            request.Term,
            request.MaxLogId,
            new(request.TimePhysical, request.TimeCounter),
            request.Endpoint
        ));
        
        return requestVoteResponse;
    }
    
    public override Task<GrpcAppendLogsResponse> AppendLogs(GrpcAppendLogsRequest request, ServerCallContext context)
    {
        //if (request.Logs.Count > 0)
        //    logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got AppendLogs message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);
        
        raft.AppendLogs(new(
            request.Partition, 
            request.Term, 
            new(request.TimePhysical, request.TimeCounter), 
            request.Endpoint, 
            GetLogs(request.Logs)
        ));
        
        return appendLogsResponse;
    }
    
    public override Task<GrpcAppendLogsBatchResponse> AppendLogsBatch(GrpcAppendLogsBatchRequest request, ServerCallContext context)
    {
        //if (request.Logs.Count > 0)
        //    logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got AppendLogs message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);

        foreach (GrpcAppendLogsRequest? req in request.AppendLogs)
        {
            raft.AppendLogs(new(
                req.Partition,
                req.Term,
                new(req.TimePhysical, req.TimeCounter),
                req.Endpoint,
                GetLogs(req.Logs)
            ));
        }

        return appendLogsBatchResponse;
    }
    
    public override Task<GrpcCompleteAppendLogsResponse> CompleteAppendLogs(GrpcCompleteAppendLogsRequest request, ServerCallContext context)
    {
        //if (request.Logs.Count > 0)
        //    logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got AppendLogs message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);
        
        raft.CompleteAppendLogs(new(
            request.Partition, 
            request.Term, 
            new(request.TimePhysical, request.TimeCounter), 
            request.Endpoint,
            (RaftOperationStatus)request.Status,
            request.CommitIndex
        ));
        
        return completeAppendLogsResponse;
    }
    
    public override Task<GrpcCompleteAppendLogsBatchResponse> CompleteAppendLogsBatch(GrpcCompleteAppendLogsBatchRequest request, ServerCallContext context)
    {
        //if (request.Logs.Count > 0)
        //    logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got AppendLogs message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);

        foreach (GrpcCompleteAppendLogsRequest? req in request.CompleteLogs)
        {
            raft.CompleteAppendLogs(new(
                req.Partition,
                req.Term,
                new(req.TimePhysical, req.TimeCounter),
                req.Endpoint,
                (RaftOperationStatus)req.Status,
                req.CommitIndex
            ));
        }

        return completeAppendLogsBatchResponse;
    }

    private static List<RaftLog> GetLogs(RepeatedField<GrpcRaftLog> requestLogs)
    {
        List<RaftLog> logs = new(requestLogs.Count);

        foreach (GrpcRaftLog? requestLog in requestLogs)
        {
            RaftLog raftLog = new()
            {
                Id = requestLog.Id,
                Term = requestLog.Term,
                Type = (RaftLogType)requestLog.Type,
                Time = new(requestLog.TimePhysical, requestLog.TimeCounter),
                LogType = requestLog.LogType
            };
            
            if (MemoryMarshal.TryGetArray(requestLog.Data.Memory, out ArraySegment<byte> segment))
                raftLog.LogData = segment.Array;
            else
                raftLog.LogData = requestLog.Data.ToByteArray();
            
            logs.Add(raftLog);
        }

        return logs;
    }
    
    public override async Task<GrpcBatchRequestsResponse> BatchRequests(GrpcBatchRequestsRequest request, ServerCallContext context)
    {
        //if (request.Logs.Count > 0)
        //    logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got AppendLogs message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);

        foreach (GrpcBatchRequestsRequestItem? requestItem in request.Requests)
        {
            switch (requestItem)
            {
                case { Type: GrpcBatchRequestsRequestType.Handshake, Handshake: { } handshakeRequest }:
                                        
                    await raft.Handshake(new(
                        handshakeRequest.Partition,
                        handshakeRequest.MaxLogId,
                        handshakeRequest.Endpoint
                    ));
                    break;
                
                case { Type: GrpcBatchRequestsRequestType.Vote, Vote: { } voteRequest }:
                    raft.Vote(new(
                        voteRequest.Partition,
                        voteRequest.Term,
                        voteRequest.MaxLogId,
                        new(voteRequest.TimePhysical, voteRequest.TimeCounter),
                        voteRequest.Endpoint
                    ));
                    break;
                
                case { Type: GrpcBatchRequestsRequestType.RequestVotes, RequestVotes: { } requestVoteRequest }:
                    raft.RequestVote(new(
                        requestVoteRequest.Partition,
                        requestVoteRequest.Term,
                        requestVoteRequest.MaxLogId,
                        new(requestVoteRequest.TimePhysical, requestVoteRequest.TimeCounter),
                        requestVoteRequest.Endpoint
                    ));
                    break;
                
                case { Type: GrpcBatchRequestsRequestType.AppendLogs, AppendLogs: { } appendLogsRequest }:
                    raft.AppendLogs(new(
                        appendLogsRequest.Partition, 
                        appendLogsRequest.Term, 
                        new(appendLogsRequest.TimePhysical, appendLogsRequest.TimeCounter), 
                        appendLogsRequest.Endpoint, 
                        GetLogs(appendLogsRequest.Logs)
                    ));
                    break;
                
                case { Type: GrpcBatchRequestsRequestType.CompleteAppendLogs, CompleteAppendLogs: { } completeAppendLogsRequest }:
                    raft.CompleteAppendLogs(new(
                        completeAppendLogsRequest.Partition, 
                        completeAppendLogsRequest.Term, 
                        new(completeAppendLogsRequest.TimePhysical, completeAppendLogsRequest.TimeCounter), 
                        completeAppendLogsRequest.Endpoint,
                        (RaftOperationStatus)completeAppendLogsRequest.Status,
                        completeAppendLogsRequest.CommitIndex
                    ));
                    break;
                
                default:
                    throw new NotImplementedException($"Unknown grpc request type {requestItem.GetType()}");
            }
        }
        
        return new();
    }
}