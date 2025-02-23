
using System.Diagnostics;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.WAL;
using Nixie;

namespace Kommander;

/// <summary>
/// Manages the Raft cluster.
/// </summary>
public sealed class RaftManager : IRaft
{
    internal readonly string LocalEndpoint;

    private readonly ActorSystem actorSystem;

    private readonly RaftConfiguration configuration;
    
    private readonly IWAL walAdapter;

    private readonly ICommunication communication;
    
    private readonly ClusterHandler clusterHandler;

    private readonly RaftPartition?[] partitions;

    private readonly ILogger<IRaft> logger;

    internal List<RaftNode> Nodes { get; set; } = [];

    public IWAL WalAdapter => walAdapter;
    
    public ICommunication Communication => communication;
    
    public RaftConfiguration Configuration => configuration;

    internal event Action? OnRestoreStarted;

    internal event Action? OnRestoreFinished;

    internal event Func<string, Task<bool>>? OnReplicationReceived;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="configuration"></param>
    /// <param name="discovery"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    public RaftManager(
        ActorSystem actorSystem, 
        RaftConfiguration configuration, 
        IDiscovery discovery, 
        IWAL walAdapter, 
        ICommunication communication,
        ILogger<IRaft> logger
    )
    {
        this.actorSystem = actorSystem;
        this.configuration = configuration;
        this.walAdapter = walAdapter;
        this.communication = communication;
        this.logger = logger;
        
        LocalEndpoint = string.Concat(configuration.Host, ":", configuration.Port);
        
        partitions = new RaftPartition[configuration.MaxPartitions];

        clusterHandler = new(this, discovery);
    }
    
    /// <summary>
    /// Joins the cluster
    /// </summary>
    public async Task JoinCluster()
    {
        if (partitions[0] is null)
        {
            for (int i = 0; i < configuration.MaxPartitions; i++)
                partitions[i] = new(actorSystem, this, walAdapter, communication, i, logger);
        }

        await clusterHandler.JoinCluster(configuration);
    }

    /// <summary>
    /// Updates the internal state of the nodes
    /// </summary>
    public async Task UpdateNodes()
    {
        if (partitions[0] is null)
            return;

        await clusterHandler.UpdateNodes();
    }

    /// <summary>
    /// Returns the raft partition for the given partition number
    /// </summary>
    /// <param name="partition"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    private RaftPartition GetPartition(int partition)
    {
        if (partitions[partition] is null)
            throw new RaftException("It has not yet joined the cluster.");

        if (partition < 0 || partition >= partitions.Length)
            throw new RaftException("Invalid partition.");

        return partitions[partition]!;
    }

    /// <summary>
    /// Passes the request to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.RequestVote(request);
    }

    /// <summary>
    /// Passes the request to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.Vote(request);
    }

    /// <summary>
    /// Append logs in the appropriate partition
    /// Returns the index of the last log
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public async Task<long> AppendLogs(AppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        return await partition.AppendLogs(request);
    }

    /// <summary>
    /// Replicate logs to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="message"></param>
    public void ReplicateLogs(int partitionId, string message)
    {
        RaftPartition partition = GetPartition(partitionId);
        partition.ReplicateLogs(message);
    }

    /// <summary>
    /// Replicates a checkpoint to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    public void ReplicateCheckpoint(int partitionId)
    {
        RaftPartition partition = GetPartition(partitionId);
        partition.ReplicateCheckpoint();
    }

    /// <summary>
    /// Calls the restore started event
    /// </summary>
    internal void InvokeRestoreStarted()
    {
        OnRestoreStarted?.Invoke();
    }
    
    /// <summary>
    /// Calls the restore finished event
    /// </summary>
    internal void InvokeRestoreFinished()
    {
        OnRestoreFinished?.Invoke();
    }

    /// <summary>
    /// Calls the replication received event
    /// </summary>
    /// <param name="obj"></param>
    internal async Task InvokeReplicationReceived(string? obj)
    {
        if (obj is null)
            return;

        if (OnReplicationReceived != null)
        {
            Func<string, Task<bool>> callback = OnReplicationReceived;
            await callback(obj);
        }
    }

    /// <summary>
    /// Returns the local endpoint
    /// </summary>
    /// <returns></returns>
    public string GetLocalEndpoint()
    {
        return LocalEndpoint;
    }
    
    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    public async ValueTask<bool> AmILeaderQuick(int partitionId)
    {
        if (partitions[0] is null)
            return false;

        RaftPartition partition = GetPartition(partitionId);

        if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
            return true;

        try
        {
            NodeState response = await partition.GetState();

            if (response == NodeState.Leader)
                return true;

            return false;
        }
        catch (AskTimeoutException e)
        {
            Console.WriteLine("AmILeaderQuick: {0}", e.Message);
        }

        return false;
    }

    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<bool> AmILeader(int partitionId)
    {
        if (partitions[0] is null)
            return false;

        RaftPartition partition = GetPartition(partitionId);
        Stopwatch stopwatch = Stopwatch.StartNew();

        while (stopwatch.ElapsedMilliseconds < 60000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
                return true;

            try
            {
                NodeState response = await partition.GetState();

                return response == NodeState.Leader;
            }
            catch (AskTimeoutException e)
            {
                Console.WriteLine("AmILeader: {0}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }

    /// <summary>
    /// Waits for the leader to be elected in the given partition
    /// If the leader is already elected, it returns the leader
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<string> WaitForLeader(int partitionId)
    {
        RaftPartition partition = GetPartition(partitionId);
        Stopwatch stopwatch = Stopwatch.StartNew();

        while (stopwatch.ElapsedMilliseconds < 60000)
        {
            if (!string.IsNullOrEmpty(partition.Leader))
                return partition.Leader;

            try
            {
                NodeState response = await partition.GetState();

                if (response == NodeState.Leader)
                    return LocalEndpoint;

                if (string.IsNullOrEmpty(partition.Leader))
                {
                    await Task.Delay(500 + Random.Shared.Next(-50, 50));
                    continue;
                }

                return partition.Leader;
            }
            catch (AskTimeoutException e)
            {
                Console.WriteLine("WaitForLeader: {0}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }
    
    /// <summary>
    /// Returns the number of the partition for the given partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPartitionKey(string partitionKey)
    {
        return HashUtils.ConsistentHash(partitionKey, configuration.MaxPartitions);
    }

    internal static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}