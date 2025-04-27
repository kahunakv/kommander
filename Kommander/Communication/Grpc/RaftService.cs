
using System.Runtime.InteropServices;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

/// <summary>
/// The RaftService class is a gRPC service implementation responsible for handling Raft protocol-related operations.
/// </summary>
/// <remarks>
/// This class provides methods for handling various Raft-related requests, such as voting, requesting votes, appending logs,
/// and performing handshake operations. It interacts with an <see cref="IRaft"/> instance for executing the protocol logic
/// and uses a logger for debugging purposes.
/// </remarks>
/// <example>
/// This service can be mapped to a gRPC server using the provided GrpcCommunicationExtensions class.
/// </example>
/// <threadsafety>
/// This class is thread-safe for all operations.
/// </threadsafety>
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

    /// <summary>
    /// Handles a handshake request by delegating to the IRaft implementation, using the details provided in the request.
    /// </summary>
    /// <param name="request">The handshake request containing the partition ID, maximum log ID, and endpoint details.</param>
    /// <param name="context">The server call context associated with the gRPC request.</param>
    /// <returns>A task representing the asynchronous operation, with a result of type GrpcHandshakeResponse.</returns>
    public override async Task<GrpcHandshakeResponse> Handshake(GrpcHandshakeRequest request, ServerCallContext context)
    {
        //logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got Vote message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);
                
        await raft.Handshake(new(
            request.NodeId,
            request.Partition,
            request.MaxLogId,
            request.Endpoint
        ));
        
        return new();
    }

    /// <summary>
    /// Processes a vote request by delegating the operation to the IRaft implementation, using the provided request details.
    /// </summary>
    /// <param name="request">The gRPC vote request containing details such as partition ID, term, maximum log ID, timestamp, and endpoint.</param>
    /// <param name="context">The server call context associated with the gRPC request.</param>
    /// <returns>A task representing the asynchronous operation, with a result of type GrpcVoteResponse.</returns>
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

    /// <summary>
    /// Handles a request to vote as part of the Raft consensus algorithm, delegating to the IRaft implementation with the provided details.
    /// </summary>
    /// <param name="request">The vote request containing the partition ID, term, maximum log ID, timestamp details, and endpoint information.</param>
    /// <param name="context">The server call context associated with the gRPC request.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response of type GrpcRequestVotesResponse.</returns>
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

    /// <summary>
    /// Processes an AppendLogs request by delegating the logs and associated metadata to the IRaft implementation.
    /// </summary>
    /// <param name="request">The AppendLogs request containing partition information, term, timestamp, endpoint, and log entries.</param>
    /// <param name="context">The server call context associated with the gRPC request.</param>
    /// <returns>A task representing the asynchronous operation, with a result of type GrpcAppendLogsResponse.</returns>
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

        try
        {
            GrpcBatchRequestsResponse response = new() { RequestId = 0 };

            await foreach (GrpcBatchRequestsRequest message in requestStream.ReadAllAsync())
            {
                //GrpcBatchClientRequestsRequest message = requestStream.Current;                

                foreach (GrpcBatchRequestsRequestItem request in message.Requests)
                {
                    //if (request.Type != GrpcBatchRequestsRequestType.AppendLogs && request.Type != GrpcBatchRequestsRequestType.CompleteAppendLogs)
                    //    Console.WriteLine($"Client sent: {request.Type}");
                    
                    try
                    {
                        switch (request.Type)
                        {
                            case GrpcBatchRequestsRequestType.Handshake:
                            {
                                GrpcHandshakeRequest handshake = request.Handshake!;

                                await raft.Handshake(new(
                                    handshake.NodeId,
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
                            default:
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("BatchRequests: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
                    }

                    await responseStream.WriteAsync(response);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError("BatchRequests: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }
}