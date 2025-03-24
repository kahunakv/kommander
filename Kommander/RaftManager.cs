
using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Diagnostics;
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
    
    internal readonly string LocalNodeId;
    
    internal readonly ILogger<IRaft> Logger;

    private readonly ActorSystem actorSystem;

    private readonly RaftConfiguration configuration;
    
    private readonly IWAL walAdapter;
    
    private readonly IDiscovery discovery;

    private readonly ICommunication communication;

    private readonly HybridLogicalClock hybridLogicalClock;
    
    private readonly ClusterHandler clusterHandler;

    private readonly RaftPartition?[] partitions;

    private readonly IActorRef<RaftLeaderSupervisor, RaftLeaderSupervisorRequest> leaderSupervisorActor;

    /// <summary>
    /// Allows to retrieve the list of known nodes within the Raft cluster
    /// </summary>
    internal List<RaftNode> Nodes { get; set; } = [];
    
    /// <summary>
    /// Allows to retreive the list of partitions
    /// </summary>
    internal RaftPartition?[] Partitions => partitions;

    /// <summary>
    /// Whether the node has joined the Raft cluster
    /// </summary>
    public bool Joined => clusterHandler.Joined;
    
    /// <summary>
    /// Current Actor System
    /// </summary>
    public ActorSystem ActorSystem => actorSystem;

    /// <summary>
    /// Current WAL adapter
    /// </summary>
    public IWAL WalAdapter => walAdapter;
    
    /// <summary>
    /// Current Communication adapter
    /// </summary>
    public ICommunication Communication => communication;
    
    /// <summary>
    /// Current Discovery adapter
    /// </summary>
    public IDiscovery Discovery => discovery;
    
    /// <summary>
    /// 
    /// </summary>
    public ClusterHandler ClusterHandler => clusterHandler;
    
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
    public event Func<RaftLog, Task<bool>>? OnReplicationRestored;

    /// <summary>
    /// Event when a replication log is received
    /// </summary>
    public event Func<RaftLog, Task<bool>>? OnReplicationReceived;
    
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
    public RaftManager(
        ActorSystem actorSystem,
        RaftConfiguration configuration,
        IDiscovery discovery,
        IWAL walAdapter,
        ICommunication communication,
        HybridLogicalClock hybridLogicalClock,
        ILogger<IRaft> logger
    )
    {
        this.actorSystem = actorSystem;
        this.configuration = configuration;
        this.walAdapter = walAdapter;
        this.discovery = discovery;
        this.communication = communication;
        this.hybridLogicalClock = hybridLogicalClock;
        
        Logger = logger;
        
        LocalNodeId = string.IsNullOrEmpty(this.configuration.NodeId) ? Environment.MachineName : this.configuration.NodeId;
        LocalEndpoint = string.Concat(configuration.Host, ":", configuration.Port);
        
        partitions = new RaftPartition[configuration.MaxPartitions];

        clusterHandler = new(this, discovery);
        
        leaderSupervisorActor = actorSystem.Spawn<RaftLeaderSupervisor, RaftLeaderSupervisorRequest>("raft-leader-supervisor", this, Logger);
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
    /// Passes the Handshake to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void Handshake(HandshakeRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.Handshake(request);
    }

    /// <summary>
    /// Passes the RequestVote to the appropriate partition
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
    public void AppendLogs(AppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.AppendLogs(request);
    }
    
    /// <summary>
    /// Completes an append logs operation in the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.CompleteAppendLogs(request);
    }

    /// <summary>
    /// Replicate a single log to the follower nodes in the specified partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <returns></returns>
    public async Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true)
    {
        RaftPartition partition = GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateLogs(type, data, autoCommit).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);
        
        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Replicate logs to the follower nodes in the specified partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    public async Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true)
    {
        RaftPartition partition = GetPartition(partitionId);
        
        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateLogs(type, logs, autoCommit).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);
        
        if (!success)
            return new(success, status, ticketId, -1);
        
        return await WaitForQuorum(partition, ticketId, autoCommit).ConfigureAwait(false);
    }

    /// <summary>
    /// Commit logs and notify followers in the partition 
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="proposalIndex"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId)
    {
        RaftPartition partition = GetPartition(partitionId);
        
        return await partition.CommitLogs(ticketId).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Rollback logs and notify followers in the partition 
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="proposalIndex"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId)
    {
        RaftPartition partition = GetPartition(partitionId);
        
        return await partition.RollbackLogs(ticketId).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a checkpoint to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    public async Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId)
    {
        RaftPartition partition = GetPartition(partitionId);
        
        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateCheckpoint().ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);
        
        if (!success)
            return new(success, status, ticketId, -1);
        
        return await WaitForQuorum(partition, ticketId, true).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Waits for the replication proposal to be completed in the given partition
    /// </summary>
    /// <param name="partition"></param>
    /// <param name="ticketId"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    private async Task<RaftReplicationResult> WaitForQuorum(RaftPartition partition, HLCTimestamp ticketId, bool autoCommit)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        while (stopwatch.GetElapsedMilliseconds() < 30000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader != LocalEndpoint)
                return new(false, RaftOperationStatus.NodeIsNotLeader, ticketId, -1);
            
            //if (cancellationToken.IsCancellationRequested)
            //    throw new OperationCanceledException();

            try
            {
                (RaftTicketState state, long commitId) = await partition.GetTicketState(ticketId, autoCommit).ConfigureAwait(false);
                
                switch (state)
                {
                    case RaftTicketState.NotFound:
                        return new(false, RaftOperationStatus.Errored, ticketId, -1);
                    
                    case RaftTicketState.Committed:
                        return new(true, RaftOperationStatus.Success, ticketId, commitId);
                    
                    case RaftTicketState.Proposed:
                    default:
                        break;
                }
            }
            catch (AskTimeoutException e)
            {
                Logger.LogError("ReplicateLogs: {Message}", e.Message);
            }

            await Task.Yield();
        }
        
        return new(false, RaftOperationStatus.Errored, ticketId, -1);
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
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeReplicationReceived(RaftLog log)
    {
        if (OnReplicationReceived != null)
        {
            Func<RaftLog, Task<bool>> callback = OnReplicationReceived;
            bool success = await callback(log).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
    }
    
    /// <summary>
    /// Calls the replication restored event
    /// </summary>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeReplicationRestored(RaftLog log)
    {
        if (OnReplicationRestored != null)
        {
            Func<RaftLog, Task<bool>> callback = OnReplicationRestored;
            bool success = await callback(log).ConfigureAwait(false);
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
    /// Returns the local node id
    /// </summary>
    /// <returns></returns>
    public string GetLocalNodeId()
    {
        return LocalNodeId;
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
            RaftNodeState response = await partition.GetState().ConfigureAwait(false);

            if (response == RaftNodeState.Leader)
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

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        RaftPartition partition = GetPartition(partitionId);

        while (stopwatch.GetElapsedMilliseconds() < 30000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
                return true;
            
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            try
            {
                RaftNodeState response = await partition.GetState().ConfigureAwait(false);

                return response == RaftNodeState.Leader;
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
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 30000)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();
            
            try
            {
                RaftNodeState response = await partition.GetState().ConfigureAwait(false);

                if (response == RaftNodeState.Leader)
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