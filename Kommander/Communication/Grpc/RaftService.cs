
using Google.Protobuf.Collections;
using Grpc.Core;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

public class RaftService : Rafter.RafterBase
{
    private readonly IRaft raft;

    private readonly ILogger<IRaft> logger;

    public RaftService(IRaft raft, ILogger<IRaft> logger)
    {
        this.raft = raft;
        this.logger = logger;
    }
    
    public override async Task<GrpcVoteResponse> Vote(GrpcVoteRequest request, ServerCallContext context)
    {
        //logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got Vote message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);
        
        await raft.Vote(new(request.Partition, request.Term, request.Endpoint));
        
        return new();
    }
    
    public override async Task<GrpcRequestVotesResponse> RequestVotes(GrpcRequestVotesRequest request, ServerCallContext context)
    {
        //logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got RequestVotes message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);
        
        await raft.RequestVote(new(request.Partition, request.Term, request.MaxLogId, request.Endpoint));
        
        return new();
    }
    
    public override async Task<GrpcAppendLogsResponse> AppendLogs(GrpcAppendLogsRequest request, ServerCallContext context)
    {
        //if (request.Logs.Count > 0)
        //    logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got AppendLogs message from {Endpoint} on Term={Term}", raft.GetLocalEndpoint(), request.Partition, request.Endpoint, request.Term);
        
        (RaftOperationStatus status, long commitIndexId) = await raft.AppendLogs(new(request.Partition, request.Term, request.Endpoint, GetLogs(request.Logs)));
        return new() { Status = (GrpcRaftOperationStatus)status, CommitedIndex = commitIndexId };
    }

    private static List<RaftLog> GetLogs(RepeatedField<GrpcRaftLog> requestLogs)
    {
        List<RaftLog> logs = new(requestLogs.Count);

        foreach (GrpcRaftLog? requestLog in requestLogs)
        {
            logs.Add(new()
            {
                Id = requestLog.Id,
                Term = requestLog.Term,
                Type = requestLog.Type == GrpRaftLogType.Regular ? RaftLogType.Regular : RaftLogType.Checkpoint,
                LogType = requestLog.LogType,
                LogData = requestLog.Data.ToByteArray()
            });
        }

        return logs;
    }
}