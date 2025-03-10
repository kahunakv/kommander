
using Kommander.Communication;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Nixie;

namespace Kommander;

/// <summary>
/// Represents a partition in a Raft cluster.
/// </summary>
public sealed class RaftPartition
{
    private static readonly RaftRequest RaftStateRequest = new(RaftRequestType.GetNodeState);

    private readonly IActorRefStruct<RaftStateActor, RaftRequest, RaftResponse> raftActor;

    private readonly RaftManager raftManager;

    internal string Leader { get; set; } = "";

    internal int PartitionId { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raftManager"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    /// <param name="partitionId"></param>
    public RaftPartition(
        ActorSystem actorSystem, 
        RaftManager raftManager, 
        IWAL walAdapter, 
        ICommunication communication, 
        int partitionId,
        ILogger<IRaft> logger
    )
    {
        this.raftManager = raftManager;
        
        PartitionId = partitionId;

        raftActor = actorSystem.SpawnStruct<RaftStateActor, RaftRequest, RaftResponse>(
            "bra-" + partitionId, 
            raftManager, 
            this,
            walAdapter,
            communication,
            logger
        );
    }

    /// <summary>
    /// Enqueues a request a vote from the partition.
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request)
    {
        raftActor.Send(new(RaftRequestType.RequestVote, request.Term, request.MaxLogId, request.Time, request.Endpoint));
    }

    /// <summary>
    /// Enqueues a vote to become leader in a partition.
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request)
    {
        raftActor.Send(new(RaftRequestType.ReceiveVote, request.Term, request.MaxLogId, request.Time, request.Endpoint));
    }

    /// <summary>
    /// Append logs to the partition returning the commited index.
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void AppendLogs(AppendLogsRequest request)
    {
        raftActor.Ask(new(
            RaftRequestType.AppendLogs,
            request.Term,
            0,
            request.Time,
            request.Endpoint,
            RaftOperationStatus.Success,
            request.Logs
        ));
    }
    
    /// <summary>
    /// Complete the append logs request
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request)
    {
        raftActor.Ask(new(
            RaftRequestType.CompleteAppendLogs,
            request.Term,
            request.CommitIndex,
            request.Time,
            request.Endpoint,
            request.Status,
            null
        ));
    }

    /// <summary>
    /// Replicate a single log to the partition.
    /// </summary>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateLogs(string type, byte[] data)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (Leader != raftManager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        List<RaftLog> logsToReplicate = [new() { Type = RaftLogType.Proposed, LogType = type, LogData = data }];
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate)).ConfigureAwait(false); 
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);
        
        return (false, response.Status, HLCTimestamp.Zero);
    }
    
    /// <summary>
    /// Replicate logs to the partition.
    /// </summary>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateLogs(string type, IEnumerable<byte[]> logs)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (Leader != raftManager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        List<RaftLog> logsToReplicate = logs.Select(data => new RaftLog { Type = RaftLogType.Proposed, LogType = type, LogData = data }).ToList();
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate)).ConfigureAwait(false);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);
        
        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Replicate a checkpoint to the partition.
    /// </summary>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)>  ReplicateCheckpoint()
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (Leader != raftManager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateCheckpoint)).ConfigureAwait(false);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);
        
        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Obtain the state of the partition.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<RaftNodeState> GetState()
    {
        if (!string.IsNullOrEmpty(Leader) && Leader == raftManager.LocalEndpoint)
            return RaftNodeState.Leader;

        RaftResponse response = await raftActor.Ask(RaftStateRequest, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        
        if (response.Type != RaftResponseType.NodeState)
            throw new RaftException("Unknown response (2)");

        return response.NodeState;
    }
    
    /// <summary>
    /// Obtain the ticket state by the specified ticketId
    /// </summary>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async Task<(RaftTicketState state, long commitId)> GetTicketState(HLCTimestamp ticketId)
    {
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.GetTicketState, ticketId), TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        
        if (response.Type != RaftResponseType.TicketState)
            throw new RaftException("Unknown response (2)");

        return (response.TicketState, response.CommitIndex);
    }
}