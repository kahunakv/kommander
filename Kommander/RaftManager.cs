
using System.Collections.Concurrent;

using Kommander.Communication;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
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
public sealed class RaftManager : IRaft, Scheduling.IRaftTimerHost, IDisposable
{
    private static readonly TimeSpan ProposalRetryDelay = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan ProposalStatusPollDelay = TimeSpan.FromMilliseconds(10);

    internal readonly string LocalEndpoint;

    internal readonly string LocalNodeName;

    internal readonly int LocalNodeId;

    internal readonly ILogger<IRaft> Logger;

    private readonly RaftConfiguration configuration;

    private readonly IWAL walAdapter;

    private readonly IDiscovery discovery;

    private readonly ICommunication communication;

    private readonly HybridLogicalClock hybridLogicalClock;

    private readonly ClusterHandler clusterHandler;

    //private readonly RaftBatcher raftBatcher;

    private RaftPartition? systemPartition;

    private readonly ConcurrentDictionary<int, RaftPartition> partitions = new();

    private readonly FairReadScheduler readScheduler;

    private readonly FairWalScheduler walScheduler;

    private readonly RaftSystemCoordinator systemCoordinator;

    private readonly RaftTimerService timerService;

    private int _disposed;

    private readonly ConcurrentDictionary<string, HLCTimestamp> lastActivity = new();
    
    private readonly ConcurrentDictionary<string, HLCTimestamp> lastHearthBeat = new();
    
    private readonly Communication.RaftTransportDispatcher transportDispatcher;

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
    internal ConcurrentDictionary<int, RaftPartition> Partitions => partitions;

    internal RaftSystemCoordinator SystemCoordinator => systemCoordinator;

    /// <summary>
    /// Whether the node is fully initialized or not
    /// </summary>
    public bool IsInitialized { get; private set; }

    /// <summary>
    /// Fair read scheduler. Dispatches partition-tagged synchronous WAL reads
    /// to dedicated worker threads with fair, bounded per-partition queues.
    /// </summary>
    public IRaftReadScheduler ReadScheduler => readScheduler;

    /// <summary>
    /// WAL write scheduler. Submits partition-tagged WAL commands to the
    /// <see cref="FairWalScheduler"/> and delivers completions via
    /// <see cref="WAL.Data.RaftWalCompletion"/> callbacks.
    /// </summary>
    public IRaftWalScheduler WalScheduler => walScheduler;

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
    //internal RaftBatcher RaftBatcher => raftBatcher;

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
    /// <param name="configuration"></param>
    /// <param name="discovery"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    /// <param name="hybridLogicalClock"></param>
    /// <param name="logger"></param>
    public RaftManager(
        RaftConfiguration configuration,
        IDiscovery discovery,
        IWAL walAdapter,
        ICommunication communication,
        HybridLogicalClock hybridLogicalClock,
        ILogger<IRaft> logger
    )
    {
        this.configuration = configuration;
        this.walAdapter = walAdapter;
        this.discovery = discovery;
        this.communication = communication;
        this.hybridLogicalClock = hybridLogicalClock;

        Logger = logger;

        LocalEndpoint = string.Concat(configuration.Host, ":", configuration.Port);
        LocalNodeName = string.IsNullOrEmpty(this.configuration.NodeName) ? Environment.MachineName : this.configuration.NodeName;
        LocalNodeId = this.configuration.NodeId > 0 ? this.configuration.NodeId : HashUtils.SmallSimpleHash(LocalNodeName);

        clusterHandler = new(this, discovery);

        systemCoordinator = new RaftSystemCoordinator(this, Logger);
        timerService = new RaftTimerService(this, Logger, configuration);
        timerService.Start();

        transportDispatcher = new Communication.RaftTransportDispatcher(this, communication, Logger);

        readScheduler = new(logger, configuration.ReadIOThreads);
        walScheduler = new(walAdapter, logger, configuration.WriteIOThreads);

        OnSystemLogRestored += SystemLogRestored;
        OnSystemReplicationReceived += SystemReplicationReceived;
        OnSystemRestoreFinished += SystemRestoreFinished;
        OnLeaderChanged += SystemLeaderChanged;

        //raftBatcher = new(this);
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

        systemCoordinator.Send(new(RaftSystemRequestType.LeaderChanged, node));
        return Task.FromResult(true);
    }

    private Task<bool> SystemLogRestored(int partitionId, RaftLog log)
    {
        if (log.LogType != RaftSystemConfig.RaftLogType || log.LogData is null)
        {
            Logger.LogError("Invalid log type: {LogType} in system partition", log.LogType);
            
            return Task.FromResult(true);
        }

        systemCoordinator.Send(new(RaftSystemRequestType.ConfigRestored, log.LogData));

        return Task.FromResult(true);
    }

    private Task<bool> SystemReplicationReceived(int partitionId, RaftLog log)
    {
        if (log.LogType != RaftSystemConfig.RaftLogType || log.LogData is null)
        {
            Logger.LogError("Invalid log type: {LogType} in system partition", log.LogType);
            
            return Task.FromResult(true);
        }

        systemCoordinator.Send(new(RaftSystemRequestType.ConfigReplicated, log.LogData));

        return Task.FromResult(true);
    }

    private void SystemRestoreFinished(int partitionId)
    {
        systemCoordinator.Send(new(RaftSystemRequestType.RestoreCompleted));
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
            readScheduler.Start();
            walScheduler.Start();

            // Add system partition
            systemPartition = new(
                this,
                walAdapter,
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
                // Volatile writes — visible to any thread already holding a reference.
                partition.StartRange = range.StartRange;
                partition.EndRange = range.EndRange;
            }
            else
            {
                partitions.TryAdd(
                    range.PartitionId,
                    new(
                        this,
                        walAdapter,
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
    /// <param name="dispose">If true, also disposes the manager</param>
    public async Task LeaveCluster(bool dispose = false)
    {
        await clusterHandler.LeaveCluster(configuration).ConfigureAwait(false);

        // Stop in the correct order: timer first (no new work injected), drain
        // partition queues, stop shared I/O schedulers while partition executors are
        // still alive so WAL completions can be posted back, drain those completions,
        // then stop executor threads. RaftTimerService.Dispose() is idempotent and
        // safe to call again from RaftManager.Dispose() if that path is taken.
        timerService.Dispose();

        await DrainPartitions(CancellationToken.None).ConfigureAwait(false);

        readScheduler.Stop();
        walScheduler.Stop();

        await DrainPartitions(CancellationToken.None).ConfigureAwait(false);

        foreach (RaftPartition partition in partitions.Values)
            partition.Stop();

        systemPartition?.Stop();

        // Complete dispatcher channels now that no executor thread is producing more
        // outbound messages; workers drain the remaining buffered items then exit.
        transportDispatcher.Stop();

        // Stop system coordinator channel — no more system events will be produced.
        systemCoordinator.Stop();

        if (dispose)
            Dispose();
    }

    private async Task DrainPartitions(CancellationToken cancellationToken)
    {
        List<Task> drainTasks = new(partitions.Count + 1);

        foreach (RaftPartition partition in partitions.Values)
            drainTasks.Add(partition.DrainAsync(cancellationToken));

        if (systemPartition is not null)
            drainTasks.Add(systemPartition.DrainAsync(cancellationToken));

        if (drainTasks.Count > 0)
            await Task.WhenAll(drainTasks).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates the internal state of the nodes
    /// </summary>
    public async Task UpdateNodes()
    {
        if (systemPartition is null && partitions.IsEmpty)
            return;

        await clusterHandler.UpdateNodes().ConfigureAwait(false);
    }

    // ── IRaftTimerHost ─────────────────────────────────────────────────────

    RaftPartition? Scheduling.IRaftTimerHost.SystemPartition => systemPartition;

    IEnumerable<RaftPartition> Scheduling.IRaftTimerHost.GetUserPartitions() => partitions.Values;

    Task Scheduling.IRaftTimerHost.UpdateNodes() => UpdateNodes();

    // ──────────────────────────────────────────────────────────────────────

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
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

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
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates logs across a Raft partition.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition where logs are to be replicated.</param>
    /// <param name="type">The type of the operation being performed.</param>
    /// <param name="logs">A collection of logs to be replicated.</param>
    /// <param name="autoCommit">Indicates whether the logs should be automatically committed after proposal replication.</param>
    /// <param name="cancellationToken">A token to signal the cancellation of the operation.</param>
    /// <returns>A task representing the asynchronous operation, with a result of type <see cref="RaftReplicationResult"/>.</returns>
    public async Task<RaftReplicationResult> ReplicateLogs(
        int partitionId,
        string type,
        IEnumerable<byte[]> logs,
        bool autoCommit = true,
        CancellationToken cancellationToken = default
    )
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
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

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
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

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
                (RaftProposalTicketState state, long commitId) = await partition.GetTicketState(ticketId, autoCommit).ConfigureAwait(false);

                switch (state)
                {
                    case RaftProposalTicketState.NotFound:
                        return new(false, RaftOperationStatus.ReplicationFailed, ticketId, -1);

                    case RaftProposalTicketState.Committed:
                        return new(true, RaftOperationStatus.Success, ticketId, commitId);

                    case RaftProposalTicketState.Proposed:
                    default:
                        break;
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogError("ReplicateLogs: {Message}", e.Message);
            }

            await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
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
    public int GetLocalNodeId()
    {
        return LocalNodeId;
    }

    /// <summary>
    /// Returns the local node id
    /// </summary>
    /// <returns></returns>
    public string GetLocalNodeName()
    {
        return LocalNodeName;
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
        catch (Exception e) when (e is not OperationCanceledException)
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
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogError("AmILeader: {Message}", e.Message);
            }

            await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
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
                    await Task.Delay(100 + Random.Shared.Next(-50, 50), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return partition.Leader;
            }
            catch (Exception e) when (e is not OperationCanceledException)
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

        systemCoordinator.Send(new(RaftSystemRequestType.SplitPartition, partitionId));
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
    
    internal void EnqueueResponse(string endpoint, RaftResponderRequest request) =>
        transportDispatcher.Enqueue(endpoint, request);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // 1. Stop and dispose the timer so no new work is injected and both
        //    Timer instances are released without waiting for GC.
        timerService.Dispose();

        // 2. Drain partition queues before stopping shared schedulers. Then stop the
        //    I/O schedulers while executors are still alive, so accepted WAL work can
        //    post completions back into the owning executor. Drain once more to process
        //    those completion messages before executor threads are joined.
        DrainPartitions(CancellationToken.None).GetAwaiter().GetResult();

        readScheduler.Stop();
        walScheduler.Stop();

        DrainPartitions(CancellationToken.None).GetAwaiter().GetResult();

        foreach (RaftPartition partition in partitions.Values)
            partition.Dispose();

        systemPartition?.Dispose();

        // 3. Dispose the transport dispatcher now that all partition executors have
        //    stopped; workers drain buffered responses then are hard-aborted.
        transportDispatcher.Dispose();

        // Dispose the system coordinator after the dispatcher (no more system events).
        systemCoordinator.Dispose();

        // 4. Dispose I/O schedulers after they have already been stopped above.
        readScheduler.Dispose();
        walScheduler.Dispose();

        // 4. Dispose remaining shared resources.
        hybridLogicalClock.Dispose();
        walAdapter.Dispose();
    }
}
