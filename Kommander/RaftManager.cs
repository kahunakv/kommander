using Nixie;
using System.Collections.Concurrent;

using Kommander.Communication;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;

using ThreadPool = Kommander.WAL.IO.ThreadPool;
// ReSharper disable ConvertToAutoPropertyWithPrivateSetter

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable ConvertToAutoProperty
// ReSharper disable ConvertToAutoPropertyWhenPossible

namespace Kommander;

/// <summary>
/// The RaftManager class is responsible for managing the Raft distributed consensus algorithm.
/// It coordinates cluster nodes, handles log replication, voting processes, and partition management
/// associated with a Raft-based architecture.
/// </summary>
public sealed class RaftManager : IRaft, IDisposable
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

    private readonly RaftBatcher raftBatcher;

    private RaftPartition? systemPartition;

    private readonly Dictionary<int, RaftPartition> partitions = [];

    private readonly ThreadPool readThreadPool;

    private readonly ThreadPool writeThreadPool;

    private readonly IActorRef<RaftSystemActor, RaftSystemRequest> systemActor;

    private readonly IActorRef<RaftLeaderSupervisor, RaftLeaderSupervisorRequest> leaderSupervisorActor;

    private readonly ConcurrentDictionary<string, HLCTimestamp> lastActivity = new();
    
    private readonly ConcurrentDictionary<string, HLCTimestamp> lastHearthBeat = new();
    
    private readonly ConcurrentDictionary<string, Lazy<IActorRefAggregate<RaftResponderActor, RaftResponderRequest>>> responderActors = [];

    /// <summary>
    /// Allows to retrieve the list of known nodes within the Raft cluster
    /// </summary>
    internal List<RaftNode> Nodes { get; set; } = [];

    /// <summary>
    /// Returns the system partition
    /// </summary>
    internal RaftPartition? SystemPartition => systemPartition;

    /// <summary>
    /// Returns the user partitions
    /// </summary>
    internal Dictionary<int, RaftPartition> Partitions => partitions;

    /// <summary>
    /// Whether the node is fully initialized or not
    /// </summary>
    public bool IsInitialized { get; private set; }

    /// <summary>
    /// Read I/O thread pool
    /// </summary>
    public ThreadPool ReadThreadPool => readThreadPool;

    /// <summary>
    /// Write I/O thread pool
    /// </summary>
    public ThreadPool WriteThreadPool => writeThreadPool;

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
    /// 
    /// </summary>
    internal RaftBatcher RaftBatcher => raftBatcher;

    /// <summary>
    /// Event when the restore process starts
    /// </summary>
    public event Action<int>? OnRestoreStarted;

    /// <summary>
    /// Event when the restore process finishes from a user partition
    /// </summary>
    public event Action<int>? OnRestoreFinished;

    /// <summary>
    /// Event when the restore process finishes from a system partition
    /// </summary>
    public event Action<int>? OnSystemRestoreFinished;

    /// <summary>
    /// Event when a replication log is now acknowledged by the application
    /// </summary>
    public event Action<int, RaftLog>? OnReplicationError;

    /// <summary>
    /// Event when a replication log is restored from a user partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnLogRestored;

    /// <summary>
    /// Event when a replication log is restored from a system partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnSystemLogRestored;

    /// <summary>
    /// Event when a replication log is received from a user partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived;

    /// <summary>
    /// Event when a replication log is received from a system partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnSystemReplicationReceived;

    /// <summary>
    /// Event called when a leader is elected on certain partition
    /// </summary>
    public event Func<int, string, Task<bool>>? OnLeaderChanged;

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

        clusterHandler = new(this, discovery);

        systemActor = actorSystem.Spawn<RaftSystemActor, RaftSystemRequest>("raft-system", this, Logger);
        leaderSupervisorActor = actorSystem.Spawn<RaftLeaderSupervisor, RaftLeaderSupervisorRequest>("raft-leader-supervisor", this, Logger);

        readThreadPool = new(logger, configuration.ReadIOThreads);
        writeThreadPool = new(logger, configuration.WriteIOThreads);

        OnSystemLogRestored += SystemLogRestored;
        OnSystemReplicationReceived += SystemReplicationReceived;
        OnSystemRestoreFinished += SystemRestoreFinished;
        OnLeaderChanged += SystemLeaderChanged;

        raftBatcher = new(this);
    }

    private Task<bool> SystemLeaderChanged(int partitionId, string node)
    {
        /*if (!IsInitialized && partitions.Count >= 1)
        {
            bool isInitialized = true;

            foreach (KeyValuePair<int, RaftPartition> partition in partitions)
            {
                if (string.IsNullOrEmpty(partition.Value.Leader))
                {
                    isInitialized = false;
                    break;
                }
            }
            
            IsInitialized = isInitialized;
        }*/

        if (partitionId != RaftSystemConfig.SystemPartition)
            return Task.FromResult(true);

        systemActor.Send(new(RaftSystemRequestType.LeaderChanged, node));
        return Task.FromResult(true);
    }

    private Task<bool> SystemLogRestored(int partitionId, RaftLog log)
    {
        if (log.LogType != RaftSystemConfig.RaftLogType || log.LogData is null)
        {
            Logger.LogError("Invalid log type: {LogType} in system partition", log.LogType);
            
            return Task.FromResult(true);
        }

        systemActor.Send(new(RaftSystemRequestType.ConfigRestored, log.LogData));

        return Task.FromResult(true);
    }

    private Task<bool> SystemReplicationReceived(int partitionId, RaftLog log)
    {
        if (log.LogType != RaftSystemConfig.RaftLogType || log.LogData is null)
        {
            Logger.LogError("Invalid log type: {LogType} in system partition", log.LogType);
            
            return Task.FromResult(true);
        }

        systemActor.Send(new(RaftSystemRequestType.ConfigReplicated, log.LogData));

        return Task.FromResult(true);
    }

    private void SystemRestoreFinished(int partitionId)
    {
        systemActor.Send(new(RaftSystemRequestType.RestoreCompleted));
    }

    /// <summary>
    /// Joins the cluster
    /// </summary>
    public async Task JoinCluster()
    {
        // Registers itself at the discovery service
        await clusterHandler.JoinCluster(configuration).ConfigureAwait(false);

        if (systemPartition is null)
        {
            readThreadPool.Start();
            writeThreadPool.Start();

            // Add system partition
            systemPartition = new(
                actorSystem,
                this,
                walAdapter,
                communication,
                RaftSystemConfig.SystemPartition,
                0,
                0,
                Logger
            );
        }

        while (!IsInitialized)
            await Task.Delay(1000).ConfigureAwait(false);
    }

    /// <summary>
    /// Start the user partitions
    /// </summary>
    /// <param name="ranges"></param>
    internal void StartUserPartitions(List<RaftPartitionRange> ranges)
    {
        foreach (RaftPartitionRange range in ranges)
        {
            if (partitions.TryGetValue(range.PartitionId, out RaftPartition? partition))
            {
                partition.StartRange = range.StartRange;
                partition.EndRange = range.EndRange;
            }
            else
            {
                partitions.Add(
                    range.PartitionId,
                    new(
                        actorSystem,
                        this,
                        walAdapter,
                        communication,
                        range.PartitionId,
                        range.StartRange,
                        range.EndRange,
                        Logger
                    )
                );
            }
        }
        
        IsInitialized = true;
    }

    /// <summary>
    /// Leaves the cluster
    /// </summary>
    /// <param name="disposeActorSystem"></param>
    public async Task LeaveCluster(bool disposeActorSystem = false)
    {
        await clusterHandler.LeaveCluster(configuration).ConfigureAwait(false);

        readThreadPool.Stop();
        writeThreadPool.Stop();

        if (disposeActorSystem)
            Dispose();
    }

    /// <summary>
    /// Updates the internal state of the nodes
    /// </summary>
    public async Task UpdateNodes()
    {
        if (systemPartition is null && partitions.Count == 0)
            return;

        await clusterHandler.UpdateNodes().ConfigureAwait(false);
    }

    /// <summary>
    /// Obtains the last activity known of a specific node on any partitions
    /// </summary>
    /// <param name="nodeId"></param>
    /// <returns></returns>
    internal HLCTimestamp GetLastNodeActivity(string nodeId)
    {
        return lastActivity.TryGetValue(nodeId, out HLCTimestamp lastTimestamp) ? lastTimestamp : HLCTimestamp.Zero;
    }

    /// <summary>
    /// Updates the last activity known of a specific node on any partitions
    /// </summary>
    /// <param name="nodeId"></param>
    /// <param name="lastTimestamp"></param>
    internal void UpdateLastNodeActivity(string nodeId, HLCTimestamp lastTimestamp)
    {                
        if (lastActivity.TryGetValue(nodeId, out HLCTimestamp currentTimestamp))
        {
            if (lastTimestamp > currentTimestamp)
                lastActivity[nodeId] = lastTimestamp;
        }
        else        
            lastActivity.TryAdd(nodeId, lastTimestamp);
    }
    
    /// <summary>
    /// Obtains the last heathbeat sent to a specific node on any partitions
    /// </summary>
    /// <param name="nodeId"></param>
    /// <returns></returns>
    internal HLCTimestamp GetLastNodeHearthbeat(string nodeId)
    {
        return lastHearthBeat.TryGetValue(nodeId, out HLCTimestamp lastTimestamp) ? lastTimestamp : HLCTimestamp.Zero;
    }
    
    /// <summary>
    /// Updates the last heathbeat sent to a node
    /// </summary>
    /// <param name="nodeId"></param>
    /// <param name="lastTimestamp"></param>
    internal void UpdateLastHeartbeat(string nodeId, HLCTimestamp lastTimestamp)
    {
        if (lastHearthBeat.TryGetValue(nodeId, out HLCTimestamp currentTimestamp))
        {
            if (lastTimestamp > currentTimestamp)
                lastHearthBeat[nodeId] = lastTimestamp;
        }
        else        
            lastHearthBeat.TryAdd(nodeId, lastTimestamp);
    }

    /// <summary>
    /// Returns a list of nodes in the cluster
    /// </summary>
    public IList<RaftNode> GetNodes()
    {
        return Nodes;
    }

    /// <summary>
    /// Returns the raft partition for the given partition number
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    private RaftPartition GetPartition(int partitionId)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
        {
            if (systemPartition is null)
                throw new RaftException("System partition not initialized.");

            return systemPartition;
        }

        //if (partitionId < 0 || partitionId > partitions.Count)
        //    throw new RaftException("Invalid partition: " + partitionId);

        if (!partitions.TryGetValue(partitionId, out RaftPartition? partition))
            throw new RaftException("Invalid partition: " + partitionId);

        return partition;
    }

    /// <summary>
    /// Passes the Handshake to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public async Task Handshake(HandshakeRequest request)
    {                
        while (request.Partition != RaftSystemConfig.SystemPartition && !IsInitialized)
            await Task.Delay(100);
        
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
    /// Replicate a single log to the follower nodes in the system partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal async Task<RaftReplicationResult> ReplicateSystemLogs(string type, byte[] data, bool autoCommit = true, CancellationToken cancellationToken = default)
    {
        if (systemPartition is null)
            throw new RaftException("System partition not initialized.");

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await systemPartition.ReplicateLogs(type, data, autoCommit).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(systemPartition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicate a single log to the follower nodes in the specified partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, CancellationToken cancellationToken = default)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be used from userland");

        RaftPartition partition = GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateLogs(type, data, autoCommit).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicate logs to the follower nodes in the specified partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, CancellationToken cancellationToken = default)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be used from userland");

        RaftPartition partition = GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            // ReSharper disable once PossibleMultipleEnumeration
            (success, status, ticketId) = await partition.ReplicateLogs(type, logs.ToList(), autoCommit).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
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
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default)
    {
        RaftPartition partition = GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateCheckpoint().ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits for the replication proposal to be completed in the given partition
    /// </summary>
    /// <param name="partition"></param>
    /// <param name="ticketId"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<RaftReplicationResult> WaitForQuorum(RaftPartition partition, HLCTimestamp ticketId, bool autoCommit, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader != LocalEndpoint)
                return new(false, RaftOperationStatus.NodeIsNotLeader, ticketId, -1);

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                (RaftTicketState state, long commitId) = await partition.GetTicketState(ticketId, autoCommit).ConfigureAwait(false);

                switch (state)
                {
                    case RaftTicketState.NotFound:
                        return new(false, RaftOperationStatus.ReplicationFailed, ticketId, -1);

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

            await Task.Delay(1, cancellationToken);
        }

        return new(false, RaftOperationStatus.ProposalTimeout, ticketId, -1);
    }

    /// <summary>
    /// Calls the restore started event
    /// </summary>
    /// <param name="partitionId"></param>
    internal void InvokeRestoreStarted(int partitionId)
    {
        if (OnRestoreStarted != null)
        {
            Action<int>? callback = OnRestoreStarted;
            callback?.Invoke(partitionId);
        }
    }

    /// <summary>
    /// Calls the restore finished event
    /// </summary>
    /// <param name="partitionId"></param>
    internal void InvokeRestoreFinished(int partitionId)
    {
        if (OnRestoreFinished != null)
        {
            Action<int>? callback = OnRestoreFinished;
            callback?.Invoke(partitionId);
        }
    }

    /// <summary>
    /// Calls the restore finished event
    /// </summary>
    /// <param name="partitionId"></param>
    internal void InvokeSystemRestoreFinished(int partitionId)
    {
        if (OnSystemRestoreFinished != null)
        {
            Action<int>? callback = OnSystemRestoreFinished;
            callback?.Invoke(partitionId);
        }
    }

    /// <summary>
    /// Calls when a replication error occurs
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    internal void InvokeReplicationError(int partitionId, RaftLog log)
    {
        if (OnReplicationError != null)
        {
            Action<int, RaftLog>? callback = OnReplicationError;
            callback?.Invoke(partitionId, log);
        }
    }

    /// <summary>
    /// Calls the replication received event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log)
    {
        if (OnReplicationReceived != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnReplicationReceived;

            bool success = await callback(partitionId, log);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the replication received event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log)
    {
        if (OnSystemReplicationReceived != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnSystemReplicationReceived;

            bool success = await callback(partitionId, log);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the replication restored event on system partitions
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeSystemLogRestored(int partitionId, RaftLog log)
    {
        if (OnSystemLogRestored != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnSystemLogRestored;

            bool success = await callback(partitionId, log).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the replication restored event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeLogRestored(int partitionId, RaftLog log)
    {
        if (OnLogRestored != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnLogRestored;

            bool success = await callback(partitionId, log).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the leader changed event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="node"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeLeaderChanged(int partitionId, string node)
    {
        if (OnLeaderChanged != null)
        {
            Func<int, string, Task<bool>> callback = OnLeaderChanged;

            bool success = await callback(partitionId, node).ConfigureAwait(false);
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
        if (!IsInitialized)
            return false;

        RaftPartition partition = GetPartition(partitionId);

        if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
            return true;

        try
        {
            RaftNodeState response = await partition.GetState().ConfigureAwait(false);

            return response == RaftNodeState.Leader;
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
        if (!IsInitialized)
            return false;

        RaftPartition partition = GetPartition(partitionId);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
                return true;

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                RaftNodeState response = await partition.GetState().ConfigureAwait(false);

                return response == RaftNodeState.Leader;
            }
            catch (AskTimeoutException e)
            {
                Logger.LogError("AmILeader: {Message}", e.Message);
            }

            await Task.Delay(1, cancellationToken);
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

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

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
    /// Queues a request to split a partition. Splitting is an asynchronous
    /// operation initiated by the leader of the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <exception cref="RaftException"></exception>
    public async Task SplitPartition(int partitionId)
    {
        if (!IsInitialized)
            throw new RaftException("System is not initialized");

        if (partitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be split");

        if (!await AmILeader(partitionId, CancellationToken.None).ConfigureAwait(false))
            throw new RaftException("Split cannot be initiated by follower");

        systemActor.Send(new(RaftSystemRequestType.SplitPartition, partitionId));
    }

    /// <summary>
    /// Returns the number of the partition for the given partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPartitionKey(string partitionKey)
    {
        int rangeId = (int)HashUtils.InversePrefixedStaticHash(partitionKey, '/');
        if (rangeId < 0)
            rangeId = -rangeId;

        foreach (KeyValuePair<int, RaftPartition> partition in partitions)
        {
            if (partition.Value.StartRange <= rangeId && partition.Value.EndRange >= rangeId)
                return partition.Key;
        }

        throw new RaftException("Couldn't find partition range for: " + partitionKey + " " + rangeId);
    }
    
    /// <summary>
    /// Returns the number of the partition for the given partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPrefixPartitionKey(string prefixPartitionKey)
    {
        int rangeId = (int)HashUtils.SimpleHash(prefixPartitionKey);
        if (rangeId < 0)
            rangeId = -rangeId;

        foreach (KeyValuePair<int, RaftPartition> partition in partitions)
        {
            if (partition.Value.StartRange <= rangeId && partition.Value.EndRange >= rangeId)
                return partition.Key;
        }
        
        throw new RaftException("Couldn't find partition range for: " + prefixPartitionKey + " " + rangeId);
    }
    
    internal void EnqueueResponse(string endpoint, RaftResponderRequest request)
    {
        Lazy<IActorRefAggregate<RaftResponderActor, RaftResponderRequest>> responderActorLazy = responderActors.GetOrAdd(endpoint, GetOrCreateResponderActor);
        IActorRefAggregate<RaftResponderActor, RaftResponderRequest> responderActor = responderActorLazy.Value;        
        responderActor.Send(request);
    }

    private Lazy<IActorRefAggregate<RaftResponderActor, RaftResponderRequest>> GetOrCreateResponderActor(string endpoint)
    {
        return new(() => CreateResponderActor(endpoint));
    }

    public IActorRefAggregate<RaftResponderActor, RaftResponderRequest> CreateResponderActor(string endpoint)
    {
        return actorSystem.SpawnAggregate<RaftResponderActor, RaftResponderRequest>(
            string.Concat("raft-responder-", endpoint),
            this,
            new RaftNode(endpoint),
            communication,
            Logger
        );
    }

    public void Dispose()
    {
        actorSystem.Dispose();
        hybridLogicalClock.Dispose();
        systemPartition?.Dispose();
        walAdapter.Dispose();
    }
}