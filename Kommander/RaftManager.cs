
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
    /// Event when a replication log is now acknowledged by the application
    /// </summary>
    public event Action<RaftLog>? OnReplicationError;
    
    /// <summary>
    /// Event when a replication log is restored
    /// </summary>
    public event Func<string, byte[], Task<bool>>? OnReplicationRestored;

    /// <summary>
    /// Event when a replication log is received
    /// </summary>
    public event Func<string, byte[], Task<bool>>? OnReplicationReceived;
    
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

        await clusterHandler.JoinCluster(configuration).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Leaves the cluster
    /// </summary>
    public async Task LeaveCluster()
    {
        await clusterHandler.LeaveCluster(configuration).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates the internal state of the nodes
    /// </summary>
    public async Task UpdateNodes()
    {
        if (partitions[0] is null)
            return;

        await clusterHandler.UpdateNodes().ConfigureAwait(false);
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
    public async Task RequestVote(RequestVotesRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        await partition.RequestVote(request).ConfigureAwait(false);
    }

    /// <summary>
    /// Passes the request to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public async Task Vote(VoteRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        await partition.Vote(request).ConfigureAwait(false);
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
        return await partition.AppendLogs(request).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicate a single log to the follower nodes in the specified partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> ReplicateLogs(int partitionId, string type, byte[] data)
    {
        RaftPartition partition = GetPartition(partitionId);
        return await partition.ReplicateLogs(type, data).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Replicate logs to the follower nodes in the specified partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs)
    {
        RaftPartition partition = GetPartition(partitionId);
        return await partition.ReplicateLogs(type, logs).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a checkpoint to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    public async Task<(bool success, long commitLogId)> ReplicateCheckpoint(int partitionId)
    {
        RaftPartition partition = GetPartition(partitionId);
        return await partition.ReplicateCheckpoint().ConfigureAwait(false);
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
    /// Calls when a replication error occurs
    /// </summary>
    /// <param name="log"></param>
    internal void InvokeReplicationError(RaftLog log)
    {
        OnReplicationError?.Invoke(log);
    }

    /// <summary>
    /// Calls the replication received event
    /// </summary>
    /// <param name="type"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeReplicationReceived(string? type, byte[]? log)
    {
        if (type is null)
            return false;
        
        if (log is null)
            return false;

        if (OnReplicationReceived != null)
        {
            Func<string, byte[], Task<bool>> callback = OnReplicationReceived;
            bool success = await callback(type, log).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
    }
    
    /// <summary>
    /// Calls the replication restored event
    /// </summary>
    /// <param name="type"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeReplicationRestored(string? type, byte[]? log)
    {
        if (type is null)
            return false;
        
        if (log is null)
            return false;

        if (OnReplicationRestored != null)
        {
            Func<string, byte[], Task<bool>> callback = OnReplicationRestored;
            bool success = await callback(type, log).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
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
            NodeState response = await partition.GetState().ConfigureAwait(false);

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

        while (stopwatch.ElapsedMilliseconds < 30000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
                return true;
            
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            try
            {
                NodeState response = await partition.GetState().ConfigureAwait(false);

                return response == NodeState.Leader;
            }
            catch (AskTimeoutException e)
            {
                Logger.LogError("AmILeader: {Message}", e.Message);
            }

            await Task.Yield();
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

        while (stopwatch.ElapsedMilliseconds < 30000)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();
            
            try
            {
                NodeState response = await partition.GetState().ConfigureAwait(false);

                if (response == NodeState.Leader)
                    return LocalEndpoint;

                if (string.IsNullOrEmpty(partition.Leader))
                {
                    await Task.Delay(150 + Random.Shared.Next(-50, 50), cancellationToken).ConfigureAwait(false);
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