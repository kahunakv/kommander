
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
    public void RequestVote(RequestVotesRequest request)
    {
        raftActor.Send(new(RaftRequestType.RequestVote, request.Term, request.MaxLogId, request.Endpoint));
    }

    /// <summary>
    /// Vote to become leader in a partition.
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request)
    {
        raftActor.Send(new(RaftRequestType.ReceiveVote, request.Term, 0, request.Endpoint));
    }

    /// <summary>
    /// Append logs to the partition returning the commited index.
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<long> AppendLogs(AppendLogsRequest request)
    {
        // Make sure HLC clocks are synced
        if (request.Logs is not null && request.Logs.Count > 0)
        {
            foreach (RaftLog log in request.Logs)
                await raftManager.HybridLogicalClock.ReceiveEvent(log.Time);
        }

        RaftResponse response = await raftActor.Ask(new(RaftRequestType.AppendLogs, request.Term, 0, request.Endpoint, request.Logs));
        return response.CurrentIndex;
    }

    /// <summary>
    /// Replicate logs to the partition.
    /// </summary>
    /// <param name="message"></param>
    public async Task ReplicateLogs(byte[] message)
    {
        if (string.IsNullOrEmpty(Leader))
            throw new RaftException("Leader is not set.");
        
        if (Leader != raftManager.LocalEndpoint)
            throw new RaftException("Leader is not set.");
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, [new() { Log = message }]));
        return;
    }
    
    /// <summary>
    /// Replicate logs to the partition.
    /// </summary>
    /// <param name="message"></param>
    public async Task ReplicateLogs(IEnumerable<byte[]> logs)
    {
        if (string.IsNullOrEmpty(Leader))
            throw new RaftException("Leader is not set.");
        
        if (Leader != raftManager.LocalEndpoint)
            throw new RaftException("Leader is not set.");

        List<RaftLog> logsToReplicate = logs.Select(x => new RaftLog { Log = x }).ToList();
        
        RaftResponse response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate));
        return;
    }

    /// <summary>
    /// Replicate a checkpoint to the partition.
    /// </summary>
    public void ReplicateCheckpoint()
    {
        if (string.IsNullOrEmpty(Leader))
            throw new RaftException("Leader is not set.");
        
        if (Leader != raftManager.LocalEndpoint)
            throw new RaftException("Leader is not set.");
        
        raftActor.Send(new(RaftRequestType.ReplicateCheckpoint));
    }

    /// <summary>
    /// Obtain the state of the partition.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<NodeState> GetState()
    {
        // Console.WriteLine("GetState {0} {1}", Leader, raftManager.LocalEndpoint);
        
        if (!string.IsNullOrEmpty(Leader) && Leader == raftManager.LocalEndpoint)
            return NodeState.Leader;

        RaftResponse response = await raftActor.Ask(raftStateRequest, TimeSpan.FromSeconds(5));
        
        // Console.WriteLine("GetState {0} {1} {2}", Leader, raftManager.LocalEndpoint, response.Type);

        if (response.Type == RaftResponseType.None)
            throw new RaftException("Unknown response (2)");

        return response.State;
    }
}