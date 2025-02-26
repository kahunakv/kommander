
using System.Diagnostics;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Nixie;
// ReSharper disable ConvertToAutoProperty
// ReSharper disable ConvertToAutoPropertyWhenPossible

namespace Kommander;

/// <summary>
/// Manages the Raft cluster.
/// </summary>
public sealed class RaftManager : IRaft
{
    internal readonly string LocalEndpoint;
    
    internal readonly ILogger<IRaft> Logger;

    private readonly ActorSystem actorSystem;

    private readonly RaftConfiguration configuration;
    
    private readonly IWAL walAdapter;

    private readonly ICommunication communication;

    private readonly HybridLogicalClock hybridLogicalClock;
    
    private readonly ClusterHandler clusterHandler;

    private readonly RaftPartition?[] partitions;

    internal List<RaftNode> Nodes { get; set; } = [];

    /// <summary>
    /// Whether the node has joined the Raft cluster
    /// </summary>
    public bool Joined => clusterHandler.Joined;

    /// <summary>
    /// Current WAL adapter
    /// </summary>
    public IWAL WalAdapter => walAdapter;
    
    /// <summary>
    /// Current Communication adapter
    /// </summary>
    public ICommunication Communication => communication;
    
    /// <summary>
    /// Current Raft configuration
    /// </summary>
    public RaftConfiguration Configuration => configuration;

    /// <summary>
    /// Hybrid Logical Clock
    /// </summary>
    public HybridLogicalClock HybridLogicalClock => hybridLogicalClock;

    /// <summary>
    /// Event when the restore process starts
    /// </summary>
    public event Action? OnRestoreStarted;        

    /// <summary>
    /// Event when the restore process finishes
    /// </summary>
    public event Action? OnRestoreFinished;

    /// <summary>
    /// Event when a replication log is received
    /// </summary>
    public event Func<byte[], Task<bool>>? OnReplicationReceived;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="configuration"></param>
    /// <param name="discovery"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    /// <param name="hybridLogicalClock"></param>
    /// <param name="logger"></param>
    public RaftManager(ActorSystem actorSystem,
        RaftConfiguration configuration,
        IDiscovery discovery,
        IWAL walAdapter,
        ICommunication communication,
        HybridLogicalClock hybridLogicalClock,
        ILogger<IRaft> logger)
    {
        this.actorSystem = actorSystem;
        this.configuration = configuration;
        this.walAdapter = walAdapter;
        this.communication = communication;
        this.hybridLogicalClock = hybridLogicalClock;
        
        Logger = logger;
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
                partitions[i] = new(actorSystem, this, walAdapter, communication, i, Logger);
        }

        await clusterHandler.JoinCluster(configuration);
    }
    
    /// <summary>
    /// Leaves the cluster
    /// </summary>
    public async Task LeaveCluster()
    {
        await clusterHandler.LeaveCluster(configuration);
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
    public async Task<(RaftOperationStatus, long)> AppendLogs(AppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        return await partition.AppendLogs(request);
    }

    /// <summary>
    /// Replicate logs to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    public async Task<(bool success, long commitLogId)> ReplicateLogs(int partitionId, byte[] log)
    {
        RaftPartition partition = GetPartition(partitionId);
        return await partition.ReplicateLogs(log);
    }
    
    /// <summary>
    /// Replicate logs to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="logs"></param>
    public async Task<(bool success, long commitLogId)> ReplicateLogs(int partitionId, IEnumerable<byte[]> logs)
    {
        RaftPartition partition = GetPartition(partitionId);
        return await partition.ReplicateLogs(logs);
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
    /// <param name="log"></param>
    internal async Task InvokeReplicationReceived(byte[]? log)
    {
        if (log is null)
            return;

        if (OnReplicationReceived != null)
        {
            Func<byte[], Task<bool>> callback = OnReplicationReceived;
            await callback(log);
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
            Logger.LogError("AmILeaderQuick: {Message}", e.Message);
        }

        return false;
    }

    /// <summary>
    /// Checks if the local node is the leader in the given partition 
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="OperationCanceledException"></exception>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken)
    {
        if (partitions[0] is null)
            return false;

        Stopwatch stopwatch = Stopwatch.StartNew();
        RaftPartition partition = GetPartition(partitionId);

        while (stopwatch.ElapsedMilliseconds < 60000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
                return true;
            
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            try
            {
                NodeState response = await partition.GetState();

                return response == NodeState.Leader;
            }
            catch (AskTimeoutException e)
            {
                Logger.LogError("AmILeader: {Message}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }

    /// <summary>
    /// Waits for the leader to be elected in the given partition
    /// If the leader is already elected, it returns the leader 
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken)
    {
        RaftPartition partition = GetPartition(partitionId);
        Stopwatch stopwatch = Stopwatch.StartNew();

        while (stopwatch.ElapsedMilliseconds < 60000)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();
            
            try
            {
                NodeState response = await partition.GetState();

                if (response == NodeState.Leader)
                    return LocalEndpoint;

                if (string.IsNullOrEmpty(partition.Leader))
                {
                    await Task.Delay(150 + Random.Shared.Next(-50, 50), cancellationToken);
                    continue;
                }

                return partition.Leader;
            }
            catch (AskTimeoutException e)
            {
                Logger.LogError("WaitForLeader: {Message}", e.Message);
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
        return (int)HashUtils.ConsistentHash(partitionKey, configuration.MaxPartitions);
    }
}