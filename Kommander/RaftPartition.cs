
using Kommander.Communication;
using Kommander.Data;
using Kommander.WAL;
using Nixie;

namespace Kommander;

/// <summary>
/// Represents a partition in a Raft cluster.
/// </summary>
public sealed class RaftPartition
{
    private static readonly RaftRequest raftStateRequest = new(RaftRequestType.GetState);

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
    /// Request a vote from the partition.
    /// </summary>
    /// <param name="request"></param>
    public async Task RequestVote(RequestVotesRequest request)
    {
        try
        {
            await raftActor.Ask(
                new(RaftRequestType.RequestVote, request.Term, request.MaxLogId, request.Time, request.Endpoint),
                TimeSpan.FromSeconds(5)
            ).ConfigureAwait(false);
        }
        catch (AskTimeoutException)
        {
            raftManager.Logger.LogWarning(
                "[{LocalEndpoint}/{ParitionId}] Timeout RequestVote {Term} {MaxLogId} {Endpoint}",
                raftManager.LocalEndpoint,
                PartitionId,
                request.Term, 
                request.MaxLogId, 
                request.Endpoint
            );
        }
    }

    /// <summary>
    /// Vote to become leader in a partition.
    /// </summary>
    /// <param name="request"></param>
    public async Task Vote(VoteRequest request)
    {
        try
        {
            await raftActor.Ask(
                new(RaftRequestType.ReceiveVote, request.Term, 0, request.Time, request.Endpoint), 
                TimeSpan.FromSeconds(5)
            ).ConfigureAwait(false);
        }
        catch (AskTimeoutException)
        {
            raftManager.Logger.LogWarning(
                "[{LocalEndpoint}/{ParitionId}] Timeout Vote {Term} {Endpoint}",
                raftManager.LocalEndpoint,
                PartitionId,
                request.Term, 
                request.Endpoint
            );
        }
    }

    /// <summary>
    /// Append logs to the partition returning the commited index.
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<(RaftOperationStatus, long)> AppendLogs(AppendLogsRequest request)
    {
        // Make sure HLC clocks are synced
        if (request.Logs is not null && request.Logs.Count > 0)
        {
            foreach (RaftLog log in request.Logs)
                await raftManager.HybridLogicalClock.ReceiveEvent(log.Time).ConfigureAwait(false);
        }

        RaftResponse response = await raftActor.Ask(new(RaftRequestType.AppendLogs, request.Term, 0, request.Time, request.Endpoint, request.Logs)).ConfigureAwait(false);
        return (response.Status, response.CurrentIndex);
    }

    /// <summary>
    /// Replicate a single log to the partition.
    /// </summary>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> ReplicateLogs(string type, byte[] data)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, -1);
        
        if (Leader != raftManager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, -1);
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, [new() { LogType = type, LogData = data }]), TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.CurrentIndex);
        
        return (false, response.Status, response.CurrentIndex);
    }
    
    /// <summary>
    /// Replicate logs to the partition.
    /// </summary>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> ReplicateLogs(string type, IEnumerable<byte[]> logs)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, -1);
        
        if (Leader != raftManager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, -1);

        List<RaftLog> logsToReplicate = logs.Select(data => new RaftLog { LogType = type, LogData = data }).ToList();
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate), TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.CurrentIndex);
        
        return (false, response.Status, response.CurrentIndex);
    }

    /// <summary>
    /// Replicate a checkpoint to the partition.
    /// </summary>
    public async Task<(bool success, long commitLogId)>  ReplicateCheckpoint()
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, -1);
        
        if (Leader != raftManager.LocalEndpoint)
            return (false, -1);
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateCheckpoint), TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        return (true, response.CurrentIndex);
    }

    /// <summary>
    /// Obtain the state of the partition.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<NodeState> GetState()
    {
        if (!string.IsNullOrEmpty(Leader) && Leader == raftManager.LocalEndpoint)
            return NodeState.Leader;

        RaftResponse response = await raftActor.Ask(raftStateRequest, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        
        if (response.Type == RaftResponseType.None)
            throw new RaftException("Unknown response (2)");

        return response.State;
    }
}