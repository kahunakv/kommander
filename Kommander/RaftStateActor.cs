
using System.Diagnostics;
using Nixie;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Time;
using Kommander.WAL;

namespace Kommander;

/// <summary>
/// The actor functions as a state machine that allows switching between different
/// states (follower, candidate, leader) and conducting elections without concurrency conflicts.
/// </summary>
public sealed class RaftStateActor : IActorStruct<RaftRequest, RaftResponse>
{
    /// <summary>
    /// Reference to the actor context
    /// </summary>
    private readonly IActorContextStruct<RaftStateActor, RaftRequest, RaftResponse> context;
    
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
    private readonly IActorRefStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> walActor;

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
        IActorContextStruct<RaftStateActor, RaftRequest, RaftResponse> context, 
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
        
        walActor = context.ActorSystem.SpawnStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse>(
            "raft-wal-" + partition.PartitionId, 
            manager, 
            partition,
            walAdapter
        );
    }

    /// <summary>
    /// Entry point for the actor
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<RaftResponse> Receive(RaftRequest message)
    {
        stopwatch.Restart();
        
        //Console.WriteLine("[{0}/{1}/{2}] Processing:{3}", manager.LocalEndpoint, partition.PartitionId, state, message.Type);
        //await File.AppendAllTextAsync($"/tmp/{partition.PartitionId}.txt", $"{message.Type}\n");
        
        try
        {
            await RestoreWal().ConfigureAwait(false);

            switch (message.Type)
            {
                case RaftRequestType.CheckLeader:
                    await CheckPartitionLeadership().ConfigureAwait(false);
                    break;

                case RaftRequestType.GetNodeState:
                    return new(RaftResponseType.NodeState, nodeState);

                case RaftRequestType.GetTicketState:
                {
                    (RaftTicketState ticketState, long commitIndex) = CheckTicketCompletion(message.Timestamp, message.AutoCommit);
                    return new(RaftResponseType.TicketState, ticketState, commitIndex);
                }

                case RaftRequestType.AppendLogs:
                    await AppendLogs(message.Endpoint ?? "", message.Term, message.Timestamp, message.Logs).ConfigureAwait(false);
                    break;

                case RaftRequestType.CompleteAppendLogs:
                    await CompleteAppendLogs(message.Endpoint ?? "", message.Timestamp, message.Status, message.CommitIndex).ConfigureAwait(false);
                    break;
                
                case RaftRequestType.ReplicateLogs:
                {
                    (RaftOperationStatus status, HLCTimestamp ticketId) = await ReplicateLogs(message.Logs, message.AutoCommit).ConfigureAwait(false);
                    return new(RaftResponseType.None, status, ticketId);
                }

                case RaftRequestType.ReplicateCheckpoint:
                {
                    (RaftOperationStatus status, HLCTimestamp ticketId) = await ReplicateCheckpoint().ConfigureAwait(false);
                    return new(RaftResponseType.None, status, ticketId);
                }
                
                case RaftRequestType.CommitLogs:
                {
                    (RaftOperationStatus status, long commitIndex) = await CommitLogs(message.Timestamp).ConfigureAwait(false);
                    return new(RaftResponseType.None, status, commitIndex);
                }
                
                case RaftRequestType.RollbackLogs:
                {
                    (RaftOperationStatus status, long commitIndex) = await RollbackLogs(message.Timestamp).ConfigureAwait(false);
                    return new(RaftResponseType.None, status, commitIndex);
                }

                case RaftRequestType.RequestVote:
                    await Vote(new(message.Endpoint ?? ""), message.Term, message.CommitIndex, message.Timestamp).ConfigureAwait(false);
                    break;

                case RaftRequestType.ReceiveVote:
                    await ReceivedVote(message.Endpoint ?? "", message.Term, message.CommitIndex).ConfigureAwait(false);
                    break;
                
                case RaftRequestType.ReceiveHandshake:
                    ReceiveHandshake(message.Endpoint ?? "", message.CommitIndex);
                    break;

                default:
                    logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Invalid message type: {Type}", manager.LocalEndpoint, partition.PartitionId, nodeState, message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] {Name} {Message} {StackTrace}", manager.LocalEndpoint, partition.PartitionId, nodeState, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        finally
        {
            if (stopwatch.ElapsedMilliseconds > manager.Configuration.SlowRaftStateMachineLog)
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Slow message processing: {Type} Elapsed={Elapsed}ms", manager.LocalEndpoint, partition.PartitionId, nodeState,  message.Type, stopwatch.ElapsedMilliseconds);
            
            //await File.AppendAllTextAsync($"/tmp/{partition.PartitionId}.txt", $"{stopwatch.ElapsedMilliseconds} {message.Type}\n");
        }

        return new(RaftResponseType.None);
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
        lastHeartbeat = await manager.HybridLogicalClock.TrySendOrLocalEvent().ConfigureAwait(false);

        RaftWALResponse currentCommitIndexResponse = await walActor.Ask(new(RaftWALActionType.Recover)).ConfigureAwait(false);
        
        logger.LogInfoWalRestored(manager.LocalEndpoint, partition.PartitionId, nodeState, currentCommitIndexResponse.Index, stopwatch.ElapsedMilliseconds);
        
        RaftWALResponse currentTermResponse = await walActor.Ask(new(RaftWALActionType.GetCurrentTerm)).ConfigureAwait(false);
        currentTerm = currentTermResponse.Index;

        await SendHandshake().ConfigureAwait(false);
    }

    /// <summary>
    /// Periodically, it checks the leadership status of the partition and, based on timeouts,
    /// decides whether to start a new election process.
    /// </summary>
    private async Task CheckPartitionLeadership()
    {
        HLCTimestamp currentTime = await manager.HybridLogicalClock.TrySendOrLocalEvent().ConfigureAwait(false);

        switch (nodeState)
        {
            // if node is leader just send hearthbeats every Configuration.HeartbeatInterval
            case RaftNodeState.Leader:
            {
                if (currentTime != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) >= manager.Configuration.HeartbeatInterval))
                    await SendHearthbeat().ConfigureAwait(false);
            
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
                        return;
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
        // if we don't have info about other nodes, we can't be outdated
        if (startCommitIndexes.Count == 0)
            return false;

        long maxIndex = -1;
        
        foreach (KeyValuePair<string, long> startCommitIndex in startCommitIndexes)
        {
            if (startCommitIndex.Value >= maxIndex)
                maxIndex = startCommitIndex.Value;
        }
        
        RaftWALResponse localMaxId = await walActor.Ask(new(RaftWALActionType.GetMaxLog)).ConfigureAwait(false);

        return localMaxId.Index < maxIndex;
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
        
        RaftWALResponse currentMaxLog = await walActor.Ask(new(RaftWALActionType.GetMaxLog)).ConfigureAwait(false);
        
        RequestVotesRequest request = new(partition.PartitionId, currentTerm, currentMaxLog.Index, timestamp, manager.LocalEndpoint);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogInfoAskedForVotes(manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, currentTerm);
            
            partition.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.RequestVotes, node, request));
        }
    }

    /// <summary>
    /// Sends a heartbeat message to follower nodes to indicate that the leader node in the partition is still alive.
    /// </summary>
    private async Task SendHearthbeat()
    {
        List<RaftNode> nodes = manager.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send hearthbeat", manager.LocalEndpoint, partition.PartitionId, nodeState);
            return;
        }

        lastHeartbeat = await manager.HybridLogicalClock.TrySendOrLocalEvent().ConfigureAwait(false);

        if (nodeState != RaftNodeState.Leader && nodeState != RaftNodeState.Candidate)
            return;

        //int number = 0;
        
        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            //logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Sending heartbeat to {Node} #{Number}", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, ++number);
            
            await AppendLogToNode(node, lastHeartbeat, true).ConfigureAwait(false);
        }
    }
    
    /// <summary>
    /// After the partition startups a handshake we send a handshake to the other nodes to
    /// verify if we have the most recent logs. 
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="remoteMaxLogId"></param>
    private void ReceiveHandshake(string endpoint, long remoteMaxLogId)
    {
        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received handshake from {Endpoint}. WAL log at {Index}.", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, remoteMaxLogId);
        
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
        
        RaftWALResponse localMaxId = await walActor.Ask(new(RaftWALActionType.GetMaxLog)).ConfigureAwait(false);
        
        HandshakeRequest request = new(partition.PartitionId, localMaxId.Index, manager.LocalEndpoint);
        
        int number = 0;
        
        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Sending handshake to {Node} #{Number}", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, ++number);
            
            partition.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Handshake, node, request));
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

        string? expectedLeader = expectedLeaders.GetValueOrDefault(voteTerm, "");
        
        if (!string.IsNullOrEmpty(expectedLeader))
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we already voted for {ExpectedLeader}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, expectedLeader);
            return;
        }
        
        RaftWALResponse localMaxId = await walActor.Ask(new(RaftWALActionType.GetMaxLog)).ConfigureAwait(false);;
        if (localMaxId.Index > remoteMaxLogId)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on outdated log from {Endpoint} RemoteMaxId={RemoteId} LocalMaxId={MaxId}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, remoteMaxLogId, localMaxId.Index);
            
            // If we know that we have a commitIndex ahead of other nodes in this partition,
            // we increase the term to force being chosen as leaders.
            currentTerm++;  
            return;
        }
        
        lastHeartbeat = await manager.HybridLogicalClock.ReceiveEvent(timestamp).ConfigureAwait(false);
        lastVotation = lastHeartbeat;
        
        expectedLeaders.Add(voteTerm, node.Endpoint);

        logger.LogInfoSendingVote(manager.LocalEndpoint, partition.PartitionId, nodeState, node.Endpoint, voteTerm);

        VoteRequest request = new(partition.PartitionId, voteTerm, localMaxId.Index, timestamp, manager.LocalEndpoint);
        
        partition.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, request));
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
        
        RaftWALResponse maxLogResponse = await walActor.Ask(new(RaftWALActionType.GetMaxLog)).ConfigureAwait(false);

        if (maxLogResponse.Index < remoteMaxLogId)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} but remote node is on a higher RemoteCommitId={CommitId} Local={LocalCommitId}. Ignoring...", 
                manager.LocalEndpoint, 
                partition.PartitionId, 
                nodeState, 
                endpoint, 
                remoteMaxLogId, 
                maxLogResponse.Index
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
            maxLogResponse.Index
        );

        if (numberVotes < quorum)
            return;
        
        nodeState = RaftNodeState.Leader;
        partition.Leader = manager.LocalEndpoint;

        lastHeartbeat = await manager.HybridLogicalClock.TrySendOrLocalEvent().ConfigureAwait(false);

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
            maxLogResponse.Index
        );

        await SendHearthbeat().ConfigureAwait(false);
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
            
            partition.EnqueueResponse(endpoint, new(
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
            }
        }
        else
        {
            if (endpoint != expectedLeader)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from another leader {Endpoint} (current leader {CurrentLeader}) Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, expectedLeader, leaderTerm);
                
                partition.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, timestamp, manager.LocalEndpoint, RaftOperationStatus.LogsFromAnotherLeader, -1)
                ));
                return;
            }
        }
        
        lastHeartbeat = await manager.HybridLogicalClock.ReceiveEvent(timestamp).ConfigureAwait(false);
        
        manager.UpdateLastNodeActivity(expectedLeader, lastHeartbeat);

        if (logs is not null && logs.Count > 0)
        {
            logger.LogDebugSendingVote(manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, leaderTerm, logs.Count);

            RaftWALResponse response = await walActor.Ask(new(RaftWALActionType.ProposeOrCommit, leaderTerm, timestamp, logs)).ConfigureAwait(false);
            
            if (response.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't append logs from leader {Endpoint} with Term={Term} Logs={Logs}", manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, leaderTerm, logs.Count);
                
                partition.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, timestamp, manager.LocalEndpoint, response.Status, -1)
                ));
                return;
            }
            
            partition.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs, 
                new(endpoint), 
                new CompleteAppendLogsRequest(partition.PartitionId, leaderTerm, timestamp, manager.LocalEndpoint, RaftOperationStatus.Success, response.Index)
            ));
            
            return;
        }
        
        partition.EnqueueResponse(endpoint, new(
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
        
        HLCTimestamp currentTime = await manager.HybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);

        foreach (RaftLog log in logs)
        {
            log.Type = RaftLogType.Proposed;
            log.Time = currentTime;
        }

        // Append proposal logs to the Write-Ahead Log
        RaftWALResponse proposeResponse = await walActor.Ask(new(RaftWALActionType.Propose, currentTerm, currentTime, logs)).ConfigureAwait(false);
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", manager.LocalEndpoint, partition.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = new(logs, autoCommit);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedCompletion(node.Endpoint);
            
            await AppendLogToNode(node, currentTime, false);
        }

        activeProposals.TryAdd(currentTime, proposalQuorum);
        
        logger.LogInfoProposedLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, currentTime, logs.Count);

        return (RaftOperationStatus.Success, currentTime);
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
        
        // We need a proper HLC sequence to determine the order of the logs
        HLCTimestamp currentTime = await manager.HybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);
        
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
        RaftWALResponse proposeResponse = await walActor.Ask(new(RaftWALActionType.Propose, currentTerm, currentTime, checkpointLogs)).ConfigureAwait(false);
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", manager.LocalEndpoint, partition.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = new(checkpointLogs, true);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedCompletion(node.Endpoint);
            
            await AppendLogToNode(node, currentTime, false).ConfigureAwait(false);
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
            return (RaftOperationStatus.Errored, 0);
        
        RaftWALResponse commitResponse = await walActor.Ask(new(RaftWALActionType.Commit, currentTerm, ticketId, proposal.Logs)).ConfigureAwait(false);
        if (commitResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit logs {Timestamp}", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId);

            return (commitResponse.Status, 0);
        }

        HLCTimestamp currentTime = await manager.HybridLogicalClock.ReceiveEvent(ticketId);
        
        foreach (string node in proposal.Nodes)
            await AppendLogToNode(new(node), currentTime, false);
        
        logger.LogInfoCommittedLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId, proposal.Logs.Count);
        
        activeProposals.Remove(ticketId);
        
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
            return (RaftOperationStatus.Errored, 0);
        
        RaftWALResponse commitResponse = await walActor.Ask(new(RaftWALActionType.Rollback, currentTerm, ticketId, proposal.Logs)).ConfigureAwait(false);
        if (commitResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't rollback logs {Timestamp}", manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId);

            return (commitResponse.Status, 0);
        }

        HLCTimestamp currentTime = await manager.HybridLogicalClock.ReceiveEvent(ticketId);
        
        foreach (string node in proposal.Nodes)
            await AppendLogToNode(new(node), currentTime, false);
        
        logger.LogInfoRolledbackLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, ticketId, proposal.Logs.Count);
        
        activeProposals.Remove(ticketId);
        
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
            votes[term] = [endpoint];;

        return votes[term].Count;
    }
    
    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="isHearthbeat"></param>
    /// <returns></returns>
    private async Task AppendLogToNode(RaftNode node, HLCTimestamp timestamp, bool isHearthbeat)
    {
        AppendLogsRequest request;

        if (isHearthbeat)
            request = new(partition.PartitionId, currentTerm, timestamp, manager.LocalEndpoint, null);
        else
        {
            long lastCommitIndex = lastCommitIndexes.GetValueOrDefault(node.Endpoint, 0);
            
            lastCommitIndex -= 3;
            if (lastCommitIndex < 0)
                lastCommitIndex = 0;

            RaftWALResponse getRangeResponse = await walActor.Ask(new(RaftWALActionType.GetRange, currentTerm, lastCommitIndex)).ConfigureAwait(false);
            if (getRangeResponse.Logs is null)
                return;

            request = new(partition.PartitionId, currentTerm, timestamp, manager.LocalEndpoint, getRangeResponse.Logs);
        }

        /*if (request.Logs is null || request.Logs.Count == 0)
        {
            manager.ResponseBatcherActor.Send(new(RaftResponderRequestType.AppendLogs, node, request));
            return;
        }*/
        
        partition.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.AppendLogs, node, request));
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
        if (committedIndex > 0)
        {
            lastCommitIndexes[endpoint] = committedIndex;
            startCommitIndexes[endpoint] = committedIndex;

            logger.LogInfoSuccessfullySentLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, endpoint, committedIndex);
        }

        if (status != RaftOperationStatus.Success)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Got {Status} from {Endpoint} Timestamp={Timestamp}",
                manager.LocalEndpoint,
                partition.PartitionId,
                nodeState,
                status,
                endpoint,
                timestamp
            );

            return;
        }

        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return;
        
        proposal.MarkCompleted(endpoint);

        if (!proposal.HasQuorum())
            return;
        
        logger.LogInfoProposalCompletedAt(manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp);

        if (!proposal.AutoCommit)
            return;
        
        RaftWALResponse commitResponse = await walActor.Ask(new(RaftWALActionType.Commit, currentTerm, timestamp, proposal.Logs)).ConfigureAwait(false);
        if (commitResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit logs {Timestamp}", manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp);

            return;
        }

        HLCTimestamp currentTime = await manager.HybridLogicalClock.ReceiveEvent(timestamp);
        
        foreach (string node in proposal.Nodes)
            await AppendLogToNode(new(node), currentTime, false);
        
        logger.LogInfoCommittedLogs(manager.LocalEndpoint, partition.PartitionId, nodeState, timestamp, proposal.Logs.Count);
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
                activeProposals.Remove(timestamp);
            
            return (RaftTicketState.Committed, proposal.LastLogIndex);
        }

        return (RaftTicketState.Proposed, -1);
    }
}