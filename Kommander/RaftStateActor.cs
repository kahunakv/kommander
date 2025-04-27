
using System.Diagnostics;
using Nixie;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Logging;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;

namespace Kommander;

/// <summary>
/// The actor functions as a state machine that allows switching between different
/// states (follower, candidate, leader) and conducting elections without concurrency conflicts.
/// </summary>
public sealed class RaftStateActor : IActorAggregate<RaftRequest, RaftResponse>
{
    /// <summary>
    /// Reference to the actor context
    /// </summary>
    private readonly IActorAggregateContext<RaftStateActor, RaftRequest, RaftResponse> context;
    
    /// <summary>
    /// Reference to the raft manager
    /// </summary>
    private readonly RaftManager manager;

    /// <summary>
    /// Reference to the raft partition
    /// </summary>
    private readonly RaftPartition partition;

    /// <summary>
    /// Reference to the communication layer
    /// </summary>
    private readonly ICommunication communication;

    /// <summary>
    /// Reference to the WAL actor
    /// </summary>
    private readonly RaftWriteAhead walActor;

    /// <summary>
    /// Track votes per term
    /// </summary>
    private readonly Dictionary<long, HashSet<string>> votes = [];

    /// <summary>
    /// Track the last commit index per node
    /// </summary>
    private readonly Dictionary<string, long> lastCommitIndexes = [];
    
    /// <summary>
    /// Track the initial commit index per node
    /// </summary>
    private readonly Dictionary<string, long> startCommitIndexes = [];
    
    /// <summary>
    /// Current leader per term
    /// </summary>
    private readonly Dictionary<long, string> expectedLeaders = [];
    
    /// <summary>
    /// 
    /// </summary>
    private readonly Dictionary<HLCTimestamp, RaftProposalQuorum> activeProposals = [];
    
    /// <summary>
    /// Enqueued actions in the actor are divided and aggregated into priorities
    /// </summary>
    private readonly Dictionary<RaftStatePriority, Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>> plan = new();

    /// <summary>
    /// Reference to the logger
    /// </summary>
    private readonly ILogger<IRaft> logger;
    
    /// <summary>
    /// Keep track of slow message processing
    /// </summary>
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();

    /// <summary>
    /// Current node state in the partition
    /// </summary>
    private RaftNodeState nodeState = RaftNodeState.Follower;

    /// <summary>
    /// Current term in the partition
    /// </summary>
    private long currentTerm;

    /// <summary>
    /// Last time the leader sent a heartbeat
    /// </summary>
    private HLCTimestamp lastHeartbeat = HLCTimestamp.Zero;
    
    /// <summary>
    /// Last time we voted
    /// </summary>
    private HLCTimestamp lastVotation = HLCTimestamp.Zero;

    /// <summary>
    /// Time when the voting process started
    /// </summary>
    private HLCTimestamp votingStartedAt = HLCTimestamp.Zero;

    /// <summary>
    /// Timeout to start a new election
    /// </summary>
    private TimeSpan electionTimeout;

    /// <summary>
    /// Whether the WAL is restored or not
    /// </summary>
    private bool restored;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="manager"></param>
    /// <param name="partition"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    public RaftStateActor(
        IActorAggregateContext<RaftStateActor, RaftRequest, RaftResponse> context, 
        RaftManager manager, 
        RaftPartition partition,
        IWAL walAdapter,
        ICommunication communication,
        ILogger<IRaft> logger
    )
    {
        this.context = context;
        this.manager = manager;
        this.partition = partition;
        this.communication = communication;
        this.logger = logger;
        
        electionTimeout = TimeSpan.FromMilliseconds(Random.Shared.Next(
            manager.Configuration.StartElectionTimeout, 
            manager.Configuration.EndElectionTimeout
        ));

        walActor = new(manager, partition, walAdapter);
    }

    /// <summary>
    /// Entry point for the actor
    /// </summary>
    /// <param name="messages"></param>
    /// <returns></returns>
    public async Task Receive(List<ActorMessageReply<RaftRequest, RaftResponse>> messages)
    {
        try
        {
            await RestoreWal().ConfigureAwait(false);
            
            if (messages.Count == 1)
            {
                await ExecuteActions(messages);
                return;
            }
            
            //Console.WriteLine("Batch={0}", messages.Count);
            
            foreach (ActorMessageReply<RaftRequest, RaftResponse> message in messages)
            {
                RaftRequest request = message.Request;

                switch (request.Type)
                {
                    case RaftRequestType.CheckLeader:
                        AddToPrioritySingle(plan, RaftStatePriority.High, message); 
                        break;

                    case RaftRequestType.ReceiveVote:
                    case RaftRequestType.RequestVote:
                    case RaftRequestType.ReceiveHandshake:
                        AddToPriority(plan, RaftStatePriority.High, message);
                        break;

                    case RaftRequestType.GetNodeState:
                    case RaftRequestType.GetTicketState:
                        AddToPriority(plan, RaftStatePriority.Low, message);
                        break;

                    case RaftRequestType.AppendLogs:
                    case RaftRequestType.CompleteAppendLogs:
                    case RaftRequestType.ReplicateLogs:
                    case RaftRequestType.ReplicateCheckpoint:
                    case RaftRequestType.CommitLogs:
                    case RaftRequestType.RollbackLogs:
                        AddToPriority(plan, RaftStatePriority.Mid, message);
                        break;

                    default:
                        throw new NotImplementedException();
                }
            }

            if (plan.TryGetValue(RaftStatePriority.High, out Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>? highMessages))
            {
                foreach (KeyValuePair<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>> tkv in highMessages)
                {
                    if (tkv.Value.Count > 0)
                        await ExecuteActions(tkv.Value);
                }
            }

            if (plan.TryGetValue(RaftStatePriority.Mid, out Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>? midMessages))
            {
                foreach (KeyValuePair<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>> tkv in midMessages)
                {
                    if (tkv.Value.Count == 0)
                        continue;
                    
                    if (tkv is { Key: RaftRequestType.ReplicateLogs, Value.Count: >= 2 })
                        await ReplicateLogsBatch(tkv.Value);
                    else
                        await ExecuteActions(tkv.Value);                    
                }
            }

            if (plan.TryGetValue(RaftStatePriority.Low, out Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>? lowMessages))
            {
                foreach (KeyValuePair<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>> tkv in lowMessages)
                {
                    if (tkv.Value.Count == 0)
                        continue;
                    
                    await ExecuteActions(tkv.Value);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] {Name} {Message} {StackTrace}",
                manager.LocalEndpoint,
                partition.PartitionId,
                nodeState,
                ex.GetType().Name,
                ex.Message,
                ex.StackTrace
            );
        }
        finally
        {
            foreach (KeyValuePair<RaftStatePriority, Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>> pkv in plan)
            {
                foreach (KeyValuePair<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>> tkv in pkv.Value)
                    tkv.Value.Clear();
            }
        }
    }

    private async Task ExecuteActions(List<ActorMessageReply<RaftRequest, RaftResponse>> messages)
    {
        int slowRaftStateMachineLog = manager.Configuration.SlowRaftStateMachineLog;
        
        foreach (ActorMessageReply<RaftRequest, RaftResponse> message in messages)
        {
            stopwatch.Restart();
            
            RaftRequest request = message.Request;
            
            try
            {
                switch (request.Type)
                {
                    case RaftRequestType.CheckLeader:
                        await CheckPartitionLeadership().ConfigureAwait(false);
                        message.Promise.TrySetResult(RaftResponseStatic.NoneResponse);
                        break;

                    case RaftRequestType.GetNodeState:
                    {
                        message.Promise.TrySetResult(new(RaftResponseType.NodeState, nodeState));
                        break;
                    }

                    case RaftRequestType.GetTicketState:
                    {
                        (RaftTicketState ticketState, long commitIndex) = CheckTicketCompletion(request.Timestamp, request.AutoCommit);
                        message.Promise.TrySetResult(new(RaftResponseType.TicketState, ticketState, commitIndex));
                        break;
                    }

                    case RaftRequestType.AppendLogs:
                        await AppendLogs(request.Endpoint ?? "", request.Term, request.Timestamp, request.Logs).ConfigureAwait(false);
                        message.Promise.TrySetResult(RaftResponseStatic.NoneResponse);
                        break;

                    case RaftRequestType.CompleteAppendLogs:
                        await CompleteAppendLogs(request.Endpoint ?? "", request.Timestamp, request.Status, request.CommitIndex).ConfigureAwait(false);
                        message.Promise.TrySetResult(RaftResponseStatic.NoneResponse);
                        break;

                    case RaftRequestType.ReplicateLogs:
                    {
                        (RaftOperationStatus status, HLCTimestamp ticketId) = await ReplicateLogs(request.Logs, request.AutoCommit).ConfigureAwait(false);
                        message.Promise.TrySetResult(new(RaftResponseType.None, status, ticketId));
                        break;
                    }

                    case RaftRequestType.ReplicateCheckpoint:
                    {
                        (RaftOperationStatus status, HLCTimestamp ticketId) = await ReplicateCheckpoint().ConfigureAwait(false);
                        message.Promise.TrySetResult(new(RaftResponseType.None, status, ticketId));
                        break;
                    }

                    case RaftRequestType.CommitLogs:
                    {
                        (RaftOperationStatus status, long commitIndex) = await CommitLogs(request.Timestamp).ConfigureAwait(false);
                        message.Promise.TrySetResult(new(RaftResponseType.None, status, commitIndex));
                        break;
                    }

                    case RaftRequestType.RollbackLogs:
                    {
                        (RaftOperationStatus status, long commitIndex) = await RollbackLogs(request.Timestamp).ConfigureAwait(false);
                        message.Promise.TrySetResult(new(RaftResponseType.None, status, commitIndex));
                        break;
                    }

                    case RaftRequestType.RequestVote:
                        await Vote(new(request.Endpoint ?? ""), request.Term, request.CommitIndex, request.Timestamp).ConfigureAwait(false);
                        message.Promise.TrySetResult(RaftResponseStatic.NoneResponse);
                        break;

                    case RaftRequestType.ReceiveVote:
                        await ReceivedVote(request.Endpoint ?? "", request.Term, request.CommitIndex).ConfigureAwait(false);
                        message.Promise.TrySetResult(RaftResponseStatic.NoneResponse);
                        break;

                    case RaftRequestType.ReceiveHandshake:
                        ReceiveHandshake((int)request.Term, request.Endpoint ?? "", request.CommitIndex);
                        message.Promise.TrySetResult(RaftResponseStatic.NoneResponse);
                        break;

                    default:
                        logger.LogError(
                            "[{LocalEndpoint}/{PartitionId}/{State}] Invalid message type: {Type}",
                            manager.LocalEndpoint, 
                            partition.PartitionId, 
                            nodeState, 
                            request.Type
                        );
                        
                        message.Promise.TrySetResult(RaftResponseStatic.NoneResponse);
                        break;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(
                    "[{LocalEndpoint}/{PartitionId}/{State}] {Name} {Message} {StackTrace}",
                    manager.LocalEndpoint, 
                    partition.PartitionId, 
                    nodeState, 
                    ex.GetType().Name, 
                    ex.Message,
                    ex.StackTrace
                );
                
                message.Promise.TrySetException(ex);
            }
            finally
            {
                if (stopwatch.ElapsedMilliseconds > slowRaftStateMachineLog)
                    logger.LogWarning(
                        "[{LocalEndpoint}/{PartitionId}/{State}] Slow message processing: {Type} Elapsed={Elapsed}ms",
                        manager.LocalEndpoint, 
                        partition.PartitionId, 
                        nodeState, 
                        request.Type,
                        stopwatch.ElapsedMilliseconds
                    );

                //await File.AppendAllTextAsync($"/tmp/{partition.PartitionId}.txt", $"{stopwatch.ElapsedMilliseconds} {message.Type}\n");
            }
        }
    }
    
    private static void AddToPriority(
        Dictionary<RaftStatePriority, Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>> plan, 
        RaftStatePriority priority,
        ActorMessageReply<RaftRequest, RaftResponse> message
    )
    {
        RaftRequest request = message.Request;
        
        if (plan.TryGetValue(priority, out Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>? messagesByPriority))
        {
            if (messagesByPriority.TryGetValue(request.Type, out List<ActorMessageReply<RaftRequest, RaftResponse>>? messagesByType))
                messagesByType.Add(message);
            else
                messagesByPriority.Add(request.Type, [message]);
        }
        else
        {
            messagesByPriority = [];
            messagesByPriority.Add(request.Type, [message]);
            plan.Add(priority, messagesByPriority);
        }
    }

    private static void AddToPrioritySingle(
        Dictionary<RaftStatePriority, Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>> plan, 
        RaftStatePriority priority,
        ActorMessageReply<RaftRequest, RaftResponse> message
    )
    {
        RaftRequest request = message.Request;
        
        if (plan.TryGetValue(priority, out Dictionary<RaftRequestType, List<ActorMessageReply<RaftRequest, RaftResponse>>>? messagesByPriority))
        {
            if (!messagesByPriority.ContainsKey(request.Type))
                messagesByPriority.Add(request.Type, [message]);
            else
                message.Promise.TrySetResult(new(RaftResponseType.None));
        }
        else
        {
            messagesByPriority = [];
            messagesByPriority.Add(request.Type, [message]);
            plan.Add(priority, messagesByPriority);
        }
    }

    /// <summary>
    /// un the entire content of the Write-Ahead Log on the partition to recover the initial state of the node.
    /// This should only be done once during node startup.
    /// </summary>
    private async ValueTask RestoreWal()
    {
        if (restored)
            return;

        restored = true;
        lastHeartbeat = manager.HybridLogicalClock.TrySendOrLocalEvent();

        long currentIndex = await walActor.Recover().ConfigureAwait(false);
        
        logger.LogInfoWalRestored(manager.LocalEndpoint, partition.PartitionId, nodeState, currentIndex, stopwatch.ElapsedMilliseconds);
        
        currentTerm = await walActor.GetCurrentTerm().ConfigureAwait(false);

        await SendHandshake().ConfigureAwait(false);
    }

    /// <summary>
    /// Periodically, it checks the leadership status of the partition and, based on timeouts,
    /// decides whether to start a new election process.
    /// </summary>
    private async Task CheckPartitionLeadership()
    {
        HLCTimestamp currentTime = manager.HybridLogicalClock.TrySendOrLocalEvent();

        switch (nodeState)
        {
            // if node is leader just send hearthbeats every Configuration.HeartbeatInterval
            case RaftNodeState.Leader:
            {
                if (currentTime != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) >= manager.Configuration.HeartbeatInterval))
                    SendHearthbeat(false);
            
                return;
            }
            
            // Wait Configuration.VotingTimeout seconds after the voting process starts to check if a quorum is available
            case RaftNodeState.Candidate when votingStartedAt != HLCTimestamp.Zero && (currentTime - votingStartedAt) < manager.Configuration.VotingTimeout:
                return;
            
            case RaftNodeState.Candidate:
                
                logger.LogInfoVotingConcluded(manager.LocalEndpoint, partition.PartitionId, nodeState, (currentTime - votingStartedAt).TotalMilliseconds);
            
                nodeState = RaftNodeState.Follower; 
                partition.Leader = "";
                lastHeartbeat = currentTime;
                electionTimeout += TimeSpan.FromMilliseconds(Random.Shared.Next(manager.Configuration.StartElectionTimeoutIncrement, manager.Configuration.EndElectionTimeoutIncrement));
                expectedLeaders.Clear();
                lastCommitIndexes.Clear();
                activeProposals.Clear();
                
                await manager.InvokeLeaderChanged(partition.PartitionId, "");
                return;
            
            // if node is follower and leader is not sending hearthbeats, start an election
            case RaftNodeState.Follower when (lastHeartbeat != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) < electionTimeout)):
                return;
            
            case RaftNodeState.Follower:
                
                // Don't start a new election if we recently voted
                if ((lastVotation != HLCTimestamp.Zero && ((currentTime - lastVotation) < (electionTimeout * 2))))
                    return;

                // Other partitions may have received pings about the partition leader
                // however, due to delays in processing messages in the state machine,
                // those messages might not have been processed yet.
                // By taking those pings into account, an unnecessary re-election can be avoided.
                
                string expectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
                if (!string.IsNullOrEmpty(expectedLeader))
                {
                    HLCTimestamp lastKnownHeartbeat = manager.GetLastNodeActivity(expectedLeader);

                    if (lastKnownHeartbeat != HLCTimestamp.Zero && ((currentTime - lastKnownHeartbeat) < electionTimeout))
                    {
                        lastHeartbeat = lastKnownHeartbeat;
                        return;
                    }
                }
                
                // make sure we are up to date with the logs
                
                if (await AmIOutdated().ConfigureAwait(false))
                {
                    electionTimeout += TimeSpan.FromMilliseconds(Random.Shared.Next(manager.Configuration.StartElectionTimeoutIncrement, manager.Configuration.EndElectionTimeoutIncrement));
                    
                    logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] We're outdated, cannot become leader...", manager.LocalEndpoint, partition.PartitionId, nodeState);
                    break;
                }
                
                partition.Leader = "";
                expectedLeaders.Clear();
                nodeState = RaftNodeState.Candidate;
                votingStartedAt = currentTime;
                
                await manager.InvokeLeaderChanged(partition.PartitionId, "");
        
                currentTerm++;
        
                IncreaseVotes(manager.LocalEndpoint, currentTerm);

                logger.LogWarnVotedToBecomeLeader(manager.LocalEndpoint, partition.PartitionId, nodeState, (currentTime - lastHeartbeat).TotalMilliseconds, currentTerm);

                await RequestVotes(currentTime).ConfigureAwait(false);
                break;
            
            default:
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Unknown node state. Term={CurrentTerm}", manager.LocalEndpoint, partition.PartitionId, nodeState, currentTerm);
                break;
        }
    }

    /// <summary>
    /// Compares the current log id with the log id of the other nodes in the partition to determine if the node is outdated.
    /// </summary>
    /// <returns></returns>
    private async Task<bool> AmIOutdated()
    {
        // if we don't have info about other nodes, we can't be outdated?
        if (startCommitIndexes.Count == 0)
            return false;

        long maxIndex = -1;
        
        foreach (KeyValuePair<string, long> startCommitIndex in startCommitIndexes)
        {
            if (startCommitIndex.Value >= maxIndex)
                maxIndex = startCommitIndex.Value;
        }
        
        long localMaxId = await walActor.GetMaxLog().ConfigureAwait(false);
        
        return localMaxId < maxIndex;
    }

    /// <summary>
    /// Requests votes to obtain leadership when a node becomes a candidate, reaching out to other known nodes in the cluster.
    /// </summary>
    /// <param name="timestamp"></param>
    /// <exception cref="RaftException"></exception>
    private async Task RequestVotes(HLCTimestamp timestamp)
    {
        List<RaftNode> nodes = manager.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to vote", manager.LocalEndpoint, partition.PartitionId, nodeState);
            return;
        }
        
        long currentMaxLog = await walActor.GetMaxLog().ConfigureAwait(false);
        
        RequestVotesRequest request = new(partition.PartitionId, currentTerm, currentMaxLog, timestamp, manager.LocalEndpoint);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogInfoAskedForVotes(manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, currentTerm);
            
            manager.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.RequestVotes, node, request));
        }
    }

    /// <summary>
    /// Sends a heartbeat message to follower nodes to indicate that the leader node in the partition is still alive.
    /// </summary>
    /// <param name="force"></param>
    /// <exception cref="RaftException"></exception>
    private void SendHearthbeat(bool force)
    {
        List<RaftNode> nodes = manager.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send hearthbeat", manager.LocalEndpoint, partition.PartitionId, nodeState);
            return;
        }

        lastHeartbeat = manager.HybridLogicalClock.TrySendOrLocalEvent();

        if (nodeState != RaftNodeState.Leader && nodeState != RaftNodeState.Candidate)
            return;

        //int number = 0;
        
        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            if (partition.PartitionId != RaftSystemConfig.SystemPartition && !force)
            {
                HLCTimestamp lastHearthBeatToNode = manager.GetLastNodeHearthbeat(node.Endpoint);

                if (lastHearthBeatToNode != HLCTimestamp.Zero && ((lastHeartbeat - lastHearthBeatToNode) <= manager.Configuration.RecentHeartbeat))
                    continue;
            }

            manager.UpdateLastHeartbeat(node.Endpoint, lastHeartbeat);
            
            //logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Sending heartbeat to {Node} #{Number}", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, ++number);
            
            AppendLogToNode(node, lastHeartbeat, null);
        }
    }
    
    /// <summary>
    /// After the partition startup a handshake is sent to the other nodes to
    /// verify if we have the most recent logs and the node id is unique
    /// </summary>
    /// <param name="remoteNodeId"></param>
    /// <param name="endpoint"></param>
    /// <param name="remoteMaxLogId"></param>
    private void ReceiveHandshake(int remoteNodeId, string endpoint, long remoteMaxLogId)
    {
        if (manager.LocalNodeId == remoteNodeId)
        {
            logger.LogCritical("[{LocalEndpoint}/{PartitionId}/{State}] Same node id was found in the cluster {NodeId} {RemoteNodeId}", manager.LocalEndpoint, partition.PartitionId, nodeState, manager.LocalNodeId, remoteNodeId);
            
            Environment.Exit(1);
            return;
        }
        
        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received handshake from {Endpoint}/{RemoteNodeId}. WAL log at {Index}.", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, remoteNodeId, remoteMaxLogId);
        
        startCommitIndexes[endpoint] = remoteMaxLogId;
    }

    /// <summary>
    /// Sends a handshake to every node available in the cluster to verify if we have the most recent logs.
    /// </summary>
    /// <exception cref="RaftException"></exception>
    private async Task SendHandshake()
    {
        List<RaftNode> nodes = manager.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send handshake", manager.LocalEndpoint, partition.PartitionId, nodeState);
            return;
        }
        
        long localMaxId = await walActor.GetMaxLog().ConfigureAwait(false);
        
        HandshakeRequest request = new(manager.LocalNodeId, partition.PartitionId, localMaxId, manager.LocalEndpoint);
        
        int number = 0;
        
        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Sending handshake to {Node} #{Number}", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, ++number);
            
            manager.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Handshake, node, request));
        }
    }

    /// <summary>
    /// When another node requests our vote, we verify that the term is valid and the commitIndex is
    /// higher than ours to ensure we don't elect outdated nodes as leaders. 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId"></param>
    /// <param name="timestamp"></param>
    private async Task Vote(RaftNode node, long voteTerm, long remoteMaxLogId, HLCTimestamp timestamp)
    {
        if (votes.ContainsKey(voteTerm))
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but already voted in that Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (nodeState != RaftNodeState.Follower && voteTerm == currentTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we're candidate or leader on the same Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (currentTerm > voteTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on previous term from {Endpoint} Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        string expectedLeader = expectedLeaders.GetValueOrDefault(voteTerm, "");
        
        if (!string.IsNullOrEmpty(expectedLeader))
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we already voted for {ExpectedLeader}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, expectedLeader);
            return;
        }
        
        long localMaxId = await walActor.GetMaxLog().ConfigureAwait(false);
        
        if (localMaxId > remoteMaxLogId)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on outdated log from {Endpoint} RemoteMaxId={RemoteId} LocalMaxId={MaxId}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, remoteMaxLogId, localMaxId);
            
            // If we know that we have a commitIndex ahead of other nodes in this partition,
            // we increase the term to force being chosen as leaders.
            currentTerm++;  
            return;
        }
        
        lastHeartbeat = manager.HybridLogicalClock.ReceiveEvent(timestamp);
        lastVotation = lastHeartbeat;
        
        expectedLeaders.Add(voteTerm, node.Endpoint);

        logger.LogInfoSendingVote(manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, voteTerm);

        VoteRequest request = new(partition.PartitionId, voteTerm, localMaxId, timestamp, manager.LocalEndpoint);
        
        manager.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, request));
    }

    /// <summary>
    /// When a node receives a vote from another node, it verifies that the term is valid and that the node
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId"></param>
    private async Task ReceivedVote(string endpoint, long voteTerm, long remoteMaxLogId)
    {
        if (nodeState == RaftNodeState.Follower)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Node} but we didn't ask for it Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }

        if (voteTerm < currentTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} on previous term Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }
        
        if (nodeState == RaftNodeState.Leader)
        {
            lastCommitIndexes[endpoint] = remoteMaxLogId;
            startCommitIndexes[endpoint] = remoteMaxLogId;
            
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Node} but already declared as leader Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }
        
        long maxLogResponse = await walActor.GetMaxLog().ConfigureAwait(false);

        if (maxLogResponse < remoteMaxLogId)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} but remote node is on a higher RemoteCommitId={CommitId} Local={LocalCommitId}. Ignoring...", 
                manager.LocalEndpoint, 
                partition.PartitionId, 
                nodeState, 
                endpoint, 
                remoteMaxLogId, 
                maxLogResponse
            );
            return;
        }

        int numberVotes = IncreaseVotes(endpoint, voteTerm);
        int quorum = Math.Max(2, (int)Math.Floor((manager.Nodes.Count + 1) / 2f));
        
        lastCommitIndexes[endpoint] = remoteMaxLogId;
        startCommitIndexes[endpoint] = remoteMaxLogId;
        
        logger.LogInformation(
            "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} Term={Term} Votes={Votes} Quorum={Quorum}/{Total} RemoteCommitId={CommitId} Local={LocalCommitId}", 
            manager.LocalEndpoint, 
            partition.PartitionId, 
            nodeState, 
            endpoint, 
            voteTerm, 
            numberVotes, 
            quorum, 
            manager.Nodes.Count + 1, 
            remoteMaxLogId, 
            maxLogResponse
        );

        if (numberVotes < quorum)
            return;
        
        nodeState = RaftNodeState.Leader;
        partition.Leader = manager.LocalEndpoint;

        lastHeartbeat = manager.HybridLogicalClock.TrySendOrLocalEvent();

        logger.LogInformation(
            "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} and proclamed leader in {Elapsed}ms Term={Term} Votes={Votes} Quorum={Quorum}/{Total} RemoteCommitId={CommitId} Local={LocalCommitId}", 
            manager.LocalEndpoint, 
            partition.PartitionId, 
            nodeState, 
            endpoint, 
            (lastHeartbeat - votingStartedAt).TotalMilliseconds, 
            voteTerm, 
            numberVotes, 
            quorum, 
            manager.Nodes.Count + 1,
            remoteMaxLogId, 
            maxLogResponse
        );

        await manager.InvokeLeaderChanged(partition.PartitionId, manager.LocalEndpoint);

        SendHearthbeat(true);
    }

    /// <summary>
    /// Appends logs to the Write-Ahead Log and updates the state of the node based on the leader's term.
    /// This method usually runs on follower nodes.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="leaderTerm"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    private async Task AppendLogs(string endpoint, long leaderTerm, HLCTimestamp timestamp, List<RaftLog>? logs)
    {
        if (currentTerm > leaderTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from a leader {Endpoint} with old ReceivedTerm={Term} CurrentTerm={CurrentTerm}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, leaderTerm, currentTerm);
            
            manager.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs, 
                new(endpoint), 
                new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, timestamp, manager.LocalEndpoint, RaftOperationStatus.LeaderInOldTerm, -1)
            ));
            
            return;
        }
        
        // Validate if we voted in the current term and we expect a different leader
        string expectedLeader = expectedLeaders.GetValueOrDefault(leaderTerm, "");

        if (endpoint == expectedLeader || string.IsNullOrEmpty(expectedLeader))
        {
            if (partition.Leader != endpoint)
            {
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Leader is now {Endpoint} LeaderTerm={Term}", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, leaderTerm);

                partition.Leader = endpoint;
                nodeState = RaftNodeState.Follower;
                currentTerm = leaderTerm;
                lastCommitIndexes.Clear();
                activeProposals.Clear();
                expectedLeaders.TryAdd(leaderTerm, endpoint);
                
                await manager.InvokeLeaderChanged(partition.PartitionId, endpoint);
            }
        }
        else
        {
            if (endpoint != expectedLeader)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from another leader {Endpoint} (current leader {CurrentLeader}) Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, expectedLeader, leaderTerm);
                
                manager.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, timestamp, manager.LocalEndpoint, RaftOperationStatus.LogsFromAnotherLeader, -1)
                ));
                return;
            }
        }
        
        lastHeartbeat = manager.HybridLogicalClock.ReceiveEvent(timestamp);
        
        manager.UpdateLastNodeActivity(expectedLeader, lastHeartbeat);

        if (logs is not null && logs.Count > 0)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugReceivedLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, leaderTerm, timestamp, string.Join(',', logs.Select(x => x.Id.ToString())));

            (RaftOperationStatus Status, long Index) response = await walActor.ProposeOrCommit(logs).ConfigureAwait(false);
            
            if (response.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't append logs from leader {Endpoint} with Term={Term} Status={Status} Logs={Logs}", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, leaderTerm, response.Status, logs.Count);
                
                manager.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, timestamp, manager.LocalEndpoint, response.Status, -1)
                ));
                return;
            }
            
            foreach (HLCTimestamp logTimestamp in logs.Select(x => x.Time).Distinct())
            {
                manager.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, logTimestamp, manager.LocalEndpoint, RaftOperationStatus.Success, response.Index)
                ));    
            }
            
            return;
        }
        
        manager.EnqueueResponse(endpoint, new(
            RaftResponderRequestType.CompleteAppendLogs, 
            new(endpoint), 
            new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, lastHeartbeat, manager.LocalEndpoint, RaftOperationStatus.Success, -1)
        ));
    }

    /// <summary>
    /// Replicates logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    private async Task<(RaftOperationStatus, HLCTimestamp ticketId)> ReplicateLogs(List<RaftLog>? logs, bool autoCommit)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, HLCTimestamp.Zero);

        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        /*foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
        {
            if (!proposal.Value.HasQuorum())
                return (RaftOperationStatus.ActiveProposal, HLCTimestamp.Zero);
        }*/
        
        List<RaftNode> nodes = manager.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No quorum available to propose logs", manager.LocalEndpoint, partition.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }
        
        HLCTimestamp currentTime = manager.HybridLogicalClock.SendOrLocalEvent();

        foreach (RaftLog log in logs)
        {
            log.Type = RaftLogType.Proposed;
            log.Time = currentTime;
        }

        // Append proposal logs to the Write-Ahead Log
        (RaftOperationStatus Status, long) proposeResponse = await walActor.Propose(currentTerm, logs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", manager.LocalEndpoint, partition.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = new(logs, autoCommit, currentTime);
        
        // Mark itself as completed
        proposalQuorum.MarkNodeCompleted(manager.LocalEndpoint);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            AppendLogToNode(node, currentTime, logs);
        }

        activeProposals.TryAdd(currentTime, proposalQuorum);
        
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, currentTime, string.Join(',', logs.Select(x => x.Id.ToString())));

        return (RaftOperationStatus.Success, currentTime);
    }
    
    /// <summary>
    /// Puts together a plan to replicate logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    private async Task ReplicateLogsBatch(List<ActorMessageReply<RaftRequest, RaftResponse>> messages)
    {
        Dictionary<bool, List<RaftLog>> logsPlan = new();
        
        foreach (ActorMessageReply<RaftRequest, RaftResponse> message in messages)
        {
            RaftRequest request = message.Request;

            if (logsPlan.TryGetValue(request.AutoCommit, out List<RaftLog>? logs))
            {
                if (request.Logs is not null && request.Logs.Count > 0)
                    logs.AddRange(request.Logs);
            }
            else
            {
                if (request.Logs is not null && request.Logs.Count > 0)
                {
                    logs = [];
                    logs.AddRange(request.Logs);
                    logsPlan.Add(request.AutoCommit, logs);
                }
            }
        }

        foreach (KeyValuePair<bool, List<RaftLog>> kv in logsPlan)
        {            
            (RaftOperationStatus status, HLCTimestamp ticketId) = await ReplicateLogs(kv.Value, kv.Key);            
            
            foreach (ActorMessageReply<RaftRequest, RaftResponse> message in messages)
            {
                if (message.Request.AutoCommit == kv.Key)
                    message.Promise.TrySetResult(new(RaftResponseType.None, status, ticketId));            
            }            
        }
    }

    /// <summary>
    /// Replicates the checkpoint to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <returns></returns>
    private async Task<(RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateCheckpoint()
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
        {
            if (!proposal.Value.HasQuorum())
                return (RaftOperationStatus.ActiveProposal, HLCTimestamp.Zero);
        }
        
        List<RaftNode> nodes = manager.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No quorum available to propose logs", manager.LocalEndpoint, partition.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }
        
        // We need a proper HLC sequence to determine a consistent order of the logs
        HLCTimestamp currentTime = manager.HybridLogicalClock.SendOrLocalEvent();
        
        List<RaftLog> checkpointLogs = [new()
        {
            Id = 0,
            Term = currentTerm,
            Type = RaftLogType.ProposedCheckpoint,
            Time = currentTime,
            LogType = "",
            LogData = []
        }];
        
        // Append proposal logs to the Write-Ahead Log
        (RaftOperationStatus Status, long) proposeResponse = await walActor.Propose(currentTerm, checkpointLogs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", manager.LocalEndpoint, partition.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = new(checkpointLogs, true, currentTime);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            AppendLogToNode(node, currentTime, checkpointLogs);
        }

        activeProposals.TryAdd(currentTime, proposalQuorum);
        
        logger.LogInfoProposedCheckpointLogs(
            manager.LocalEndpoint, 
            partition.PartitionId, 
            nodeState, 
            currentTime, 
            checkpointLogs.Count
        );

        return (RaftOperationStatus.Success, currentTime);
    }

    /// <summary>
    /// Marks proposals as committed
    /// </summary>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    private async Task<(RaftOperationStatus, long commitIndex)> CommitLogs(HLCTimestamp ticketId)
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, 0);
        
        if (!activeProposals.TryGetValue(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);

        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} without quorum...", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId);
            
            return (RaftOperationStatus.Errored, 0);
        }
        
        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} in state {State}...", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId, proposal.State);
            
            return (RaftOperationStatus.Errored, 0);
        }

        (RaftOperationStatus Status, long Index) commitResponse = await walActor.Commit(proposal.Logs).ConfigureAwait(false);
        
        if (commitResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit proposal {Timestamp}", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId);

            return (commitResponse.Status, 0);
        }
        
        proposal.SetState(RaftProposalState.Committed);

        HLCTimestamp currentTime = manager.HybridLogicalClock.TrySendOrLocalEvent();

        foreach (string node in proposal.Nodes)
        {
            if (node == manager.LocalEndpoint)
                continue;
            
            AppendLogToNode(new(node), ticketId, proposal.Logs);
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugCommittedLogs(
                manager.LocalEndpoint, 
                partition.PartitionId, 
                nodeState, ticketId, 
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString())),
                (currentTime - proposal.StartTimestamp).TotalMilliseconds                
            );
        
        //activeProposals.Remove(ticketId);
        
        return (RaftOperationStatus.Success, commitResponse.Index);
    }
    
    /// <summary>
    /// Marks proposals as rolled back
    /// </summary>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    private async Task<(RaftOperationStatus, long commitIndex)> RollbackLogs(HLCTimestamp ticketId)
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, 0);
        
        if (!activeProposals.TryGetValue(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);
        
        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} without quorum...", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId);
            
            return (RaftOperationStatus.Errored, 0);
        }
        
        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} in state {State}...", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId, proposal.State);
            
            return (RaftOperationStatus.Errored, 0);
        }
        
        (RaftOperationStatus Status, long Index) commitResponse = await walActor.Rollback(proposal.Logs).ConfigureAwait(false);
        
        if (commitResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't rollback proposal {Timestamp}", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId);

            return (commitResponse.Status, 0);
        }
        
        proposal.SetState(RaftProposalState.RolledBack);

        foreach (string node in proposal.Nodes)
        {
            if (node == manager.LocalEndpoint)
                continue;
            
            AppendLogToNode(new(node), ticketId, proposal.Logs); //.ConfigureAwait(false);
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugRolledbackLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId, string.Join(',', proposal.Logs.Select(x => x.Id.ToString())));
        
        //activeProposals.Remove(ticketId);
        
        return (RaftOperationStatus.Success, commitResponse.Index);
    }

    /// <summary>
    /// Increases the number of votes for a given term.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="term"></param>
    /// <returns></returns>
    private int IncreaseVotes(string endpoint, long term)
    {
        if (votes.TryGetValue(term, out HashSet<string>? votesPerEndpoint))
            votesPerEndpoint.Add(endpoint);
        else
            votes[term] = [endpoint];

        return votes[term].Count;
    }
    
    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    private void AppendLogToNode(RaftNode node, HLCTimestamp timestamp, List<RaftLog>? logs)
    {
        AppendLogsRequest request;

        if (logs is null || logs.Count == 0)
            request = new(partition.PartitionId, currentTerm, timestamp, manager.LocalEndpoint);
        else
        {
            /*long lastCommitIndex = lastCommitIndexes.GetValueOrDefault(node.Endpoint, 0);

            lastCommitIndex -= 3;
            if (lastCommitIndex < 0)
                lastCommitIndex = 0;

            RaftWALResponse getRangeResponse = await walActor.Ask(new(RaftWALActionType.GetRange, currentTerm, lastCommitIndex)).ConfigureAwait(false);
            if (getRangeResponse.Logs is null)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Failed to get logs range {Timestamp} From={From}", manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp, lastCommitIndex);

                return;
            }*/

            request = new(partition.PartitionId, currentTerm, timestamp, manager.LocalEndpoint, logs);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}/{PartitionId}/{State}] Enqueued entries for {Endpoint} {Timestamp} From={From} Logs={Logs}", 
                    manager.LocalEndpoint, 
                    partition.PartitionId, 
                    nodeState, 
                    node.Endpoint, 
                    timestamp, 
                    0, 
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );
        }

        /*if (request.Logs is null || request.Logs.Count == 0)
        {
            manager.ResponseBatcherActor.Send(new(RaftResponderRequestType.AppendLogs, node, request));
            return;
        }*/

        manager.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.AppendLogs, node, request));
    }

    /// <summary>
    /// Called when a node completes an append log operation
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="timestamp"></param>
    /// <param name="status"></param>
    /// <param name="committedIndex"></param>
    private async ValueTask CompleteAppendLogs(string endpoint, HLCTimestamp timestamp, RaftOperationStatus status, long committedIndex)
    {
        HLCTimestamp currentTime = manager.HybridLogicalClock.TrySendOrLocalEvent();
        
        if (committedIndex > 0)
        {
            if (lastCommitIndexes.TryGetValue(endpoint, out long currentIndex))
            {
                if (committedIndex > currentIndex)
                    lastCommitIndexes[endpoint] = committedIndex;
            }
            else
                lastCommitIndexes[endpoint] = committedIndex;

            if (startCommitIndexes.TryGetValue(endpoint, out currentIndex))
            {
                if (committedIndex > currentIndex)
                    startCommitIndexes[endpoint] = committedIndex;
            } 
            else
                startCommitIndexes[endpoint] = committedIndex;

            logger.LogInfoSuccessfullyCompletedLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, timestamp, committedIndex, (currentTime - timestamp).TotalMilliseconds);
        }

        if (status != RaftOperationStatus.Success)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Got {Status} from {Endpoint} Timestamp={Timestamp} CommittedIndex={CommittedIndex}",
                manager.LocalEndpoint,
                partition.PartitionId,
                nodeState,
                status,
                endpoint,
                timestamp,
                committedIndex
            );

            return;
        }

        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return;
        
        if (proposal.State != RaftProposalState.Incomplete)
            return;

        proposal.MarkNodeCompleted(endpoint);

        if (!proposal.HasQuorum())
        {
            logger.LogInfoProposalPartiallyCompletedAt(manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);
            return;
        }
        
        logger.LogInfoProposalCompletedAt(manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);

        proposal.SetState(RaftProposalState.Completed);

        if (!proposal.AutoCommit)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Proposal {Timestamp} doesn't have auto-commit", manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp);
            return;
        }

        currentTime = manager.HybridLogicalClock.TrySendOrLocalEvent();

        (RaftOperationStatus Status, long) commitResponse = await walActor.Commit(proposal.Logs).ConfigureAwait(false);
        
        if (commitResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit proposal {Timestamp}", manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp);

            return;
        }
        
        proposal.SetState(RaftProposalState.Committed);

        foreach (string node in proposal.Nodes)
        {
            if (node == manager.LocalEndpoint)
                continue;
            
            AppendLogToNode(new(node), timestamp, proposal.Logs);
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugCommittedLogs(
                manager.LocalEndpoint, 
                partition.PartitionId, 
                nodeState, 
                timestamp, 
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString())), 
                (currentTime - proposal.StartTimestamp).TotalMilliseconds
            );
    }
    
    /// <summary>
    /// Checks whether a ticket has been completed or not.
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    private (RaftTicketState state, long commitIndex) CheckTicketCompletion(HLCTimestamp timestamp, bool autoCommit)
    {
        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return (RaftTicketState.NotFound, -1);

        if (proposal.HasQuorum())
        {
            if (autoCommit)
            {
                logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Removed proposal {Timestamp}", manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp);
                
                //activeProposals.Remove(timestamp);
            }

            return (RaftTicketState.Committed, proposal.LastLogIndex);
        }

        return (RaftTicketState.Proposed, -1);
    }
}