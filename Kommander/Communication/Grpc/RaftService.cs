
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
    
    public override async Task BatchRequests(
        IAsyncStreamReader<GrpcBatchRequestsRequest> requestStream,
        IServerStreamWriter<GrpcBatchRequestsResponse> responseStream, 
        ServerCallContext context
    )
    {
        if (raft is null)
            throw new InvalidOperationException("Raft is null");
        
        GrpcBatchRequestsResponse response = new() { RequestId = 0 };
        
        await foreach (GrpcBatchRequestsRequest message in requestStream.ReadAllAsync())
        {
            //GrpcBatchClientRequestsRequest message = requestStream.Current;
            
            //Console.WriteLine($"Client sent: {message.Type}");

            foreach (GrpcBatchRequestsRequestItem request in message.Requests)
            {
                try
                {
                    switch (request.Type)
                    {
                        case GrpcBatchRequestsRequestType.Handshake:
                        {
                            GrpcHandshakeRequest handshake = request.Handshake!;

                            await raft.Handshake(new(
                                handshake.Partition,
                                handshake.MaxLogId,
                                handshake.Endpoint
                            ));
                            break;
                        }

                        case GrpcBatchRequestsRequestType.RequestVotes:
                        {
                            GrpcRequestVotesRequest requestVotes = request.RequestVotes!;
                            
                            if (requestVotes is null)
                                throw new InvalidOperationException("requestVotes is null");

                            raft.RequestVote(new(
                                requestVotes.Partition,
                                requestVotes.Term,
                                requestVotes.MaxLogId,
                                new(requestVotes.TimePhysical, requestVotes.TimeCounter),
                                requestVotes.Endpoint
                            ));
                            break;
                        }

                        case GrpcBatchRequestsRequestType.Vote:
                        {
                            GrpcVoteRequest voteRequest = request.Vote!;

                            raft.Vote(new(
                                voteRequest.Partition,
                                voteRequest.Term,
                                voteRequest.MaxLogId,
                                new(voteRequest.TimePhysical, voteRequest.TimeCounter),
                                voteRequest.Endpoint
                            ));
                            break;
                        }

                        case GrpcBatchRequestsRequestType.AppendLogs:
                        {
                            GrpcAppendLogsRequest appendLogsRequest = request.AppendLogs!;

                            raft.AppendLogs(new(
                                appendLogsRequest.Partition,
                                appendLogsRequest.Term,
                                new(appendLogsRequest.TimePhysical, appendLogsRequest.TimeCounter),
                                appendLogsRequest.Endpoint,
                                GetLogs(appendLogsRequest.Logs)
                            ));
                            break;
                        }

                        case GrpcBatchRequestsRequestType.CompleteAppendLogs:
                        {
                            GrpcCompleteAppendLogsRequest completeAppendLogsRequest = request.CompleteAppendLogs!;

                            raft.CompleteAppendLogs(new(
                                completeAppendLogsRequest.Partition,
                                completeAppendLogsRequest.Term,
                                new(completeAppendLogsRequest.TimePhysical, completeAppendLogsRequest.TimeCounter),
                                completeAppendLogsRequest.Endpoint,
                                (RaftOperationStatus)completeAppendLogsRequest.Status,
                                completeAppendLogsRequest.CommitIndex
                            ));

                            break;
                        }

                        case GrpcBatchRequestsRequestType.Ping:
                            break;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError("Exception {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
                }

                await responseStream.WriteAsync(response);
            }
        }
    }
}