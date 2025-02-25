
using System.Diagnostics;
using Nixie;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

namespace Kommander;

/// <summary>
/// The actor functions as a state machine that allows switching between different
/// states (follower, candidate, leader) and conducting elections without concurrency conflicts.
/// </summary>
public sealed class RaftStateActor : IActorStruct<RaftRequest, RaftResponse>
{
    private static readonly RaftRequest checkLeaderRequest = new(RaftRequestType.CheckLeader);

    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    private readonly ICommunication communication;

    private readonly IActorRefStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> walActor;

    private readonly Dictionary<long, int> votes = [];

    private readonly Dictionary<string, long> lastCommitIndexes = [];

    private readonly ILogger<IRaft> logger;

    private NodeState state = NodeState.Follower;

    private long currentTerm;

    private long currentIndex;

    private HLCTimestamp lastHeartbeat = HLCTimestamp.Zero;

    private HLCTimestamp votingStartedAt = HLCTimestamp.Zero;

    private TimeSpan electionTimeout = TimeSpan.FromMilliseconds(Random.Shared.Next(2000, 7000));

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
        this.manager = manager;
        this.partition = partition;
        this.communication = communication;
        this.logger = logger;
        
        walActor = context.ActorSystem.SpawnStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse>(
            "bra-wal-" + partition.PartitionId, 
            manager, 
            partition,
            walAdapter
        );

        context.ActorSystem.StartPeriodicTimerStruct(
            context.Self,
            "check-election",
            checkLeaderRequest,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(150)
        );
    }

    /// <summary>
    /// Entry point for the actor
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<RaftResponse> Receive(RaftRequest message)
    {
        try
        {
            await RestoreWal();

            switch (message.Type)
            {
                case RaftRequestType.CheckLeader:
                    await CheckClusterLeadership();
                    break;

                case RaftRequestType.GetState:
                    return new(RaftResponseType.State, state);

                case RaftRequestType.AppendLogs:
                    (RaftOperationStatus status, long commitLogIndex) = await AppendLogs(message.Endpoint ?? "", message.Term, message.Logs);
                    return new(RaftResponseType.None, status, commitLogIndex);

                case RaftRequestType.RequestVote:
                    await Vote(new(message.Endpoint ?? ""), message.Term, message.MaxLogId);
                    break;

                case RaftRequestType.ReceiveVote:
                    await ReceivedVote(message.Endpoint ?? "", message.Term);
                    break;

                case RaftRequestType.ReplicateLogs:
                    RaftOperationStatus response = await ReplicateLogs(message.Logs);
                    return new(RaftResponseType.None, response, 0);

                case RaftRequestType.ReplicateCheckpoint:
                    await ReplicateCheckpoint();
                    break;
                
                default:
                    logger.LogError("Invalid message type: {Type}", message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogError("{Name} {Message} {StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
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
        lastHeartbeat = await manager.HybridLogicalClock.SendOrLocalEvent();
        Stopwatch stopWatch = Stopwatch.StartNew();

        RaftWALResponse currentCommitIndexResponse = await walActor.Ask(new(RaftWALActionType.Recover));
        currentIndex = currentCommitIndexResponse.NextId;
        
        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] WAL restored at #{NextId} in {ElapsedMs}ms", manager.LocalEndpoint, partition.PartitionId, state, currentCommitIndexResponse.NextId, stopWatch.ElapsedMilliseconds);
        
        RaftWALResponse currentTermResponse = await walActor.Ask(new(RaftWALActionType.GetCurrentTerm));
        currentTerm = currentTermResponse.NextId;
    }

    /// <summary>
    /// Periodically, it checks the leadership status of the partition and, based on timeouts,
    /// decides whether to start a new election process.
    /// </summary>
    private async Task CheckClusterLeadership()
    {
        HLCTimestamp currentTime = await manager.HybridLogicalClock.SendOrLocalEvent();

        switch (state)
        {
            // if node is leader just send hearthbeats every 1.5 seconds
            case NodeState.Leader:
            {
                if (currentTime != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) > TimeSpan.FromMilliseconds(1500)))
                    await SendHearthbeat();
            
                return;
            }
            
            // Wait 2.5 seconds after the voting process starts to check if a quorum is available
            case NodeState.Candidate when votingStartedAt != HLCTimestamp.Zero && (currentTime - votingStartedAt) < TimeSpan.FromMilliseconds(2500):
                return;
            
            case NodeState.Candidate:
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Voting concluded after {Elapsed}ms. No quorum available", manager.LocalEndpoint, partition.PartitionId, state, (currentTime - votingStartedAt));
            
                state = NodeState.Follower; 
                partition.Leader = "";
                lastHeartbeat = currentTime;
                electionTimeout = TimeSpan.FromMilliseconds(Random.Shared.Next(2000, 7000));
                return;
            
            // if node is follower and leader is not sending hearthbeats, start election
            case NodeState.Follower when lastHeartbeat != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) < electionTimeout):
                return;
            
            case NodeState.Follower:
                partition.Leader = "";
                state = NodeState.Candidate;
                votingStartedAt = currentTime;
        
                currentTerm++;
        
                SetVotes(currentTerm, 1);

                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Voted to become leader after {LastHeartbeat}ms. Term={CurrentTerm}", manager.LocalEndpoint, partition.PartitionId, state, currentTime - lastHeartbeat, currentTerm);

                await RequestVotes();
                break;
        }
    }

    /// <summary>
    /// Requests votes to obtain leadership when a node becomes a candidate, reaching out to other known nodes in the cluster.
    /// </summary>
    /// <exception cref="RaftException"></exception>
    private async Task RequestVotes()
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to vote", manager.LocalEndpoint, partition.PartitionId, state);
            return;
        }
        
        RaftWALResponse currentMaxLog = await walActor.Ask(new(RaftWALActionType.GetMaxLog));

        RequestVotesRequest request = new(partition.PartitionId, currentTerm, currentMaxLog.NextId, manager.LocalEndpoint);

        foreach (RaftNode node in manager.Nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Asked {Endpoint} for votes on Term={CurrentTerm}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, currentTerm);
            
            await communication.RequestVotes(manager, partition, node, request);
        }
    }

    /// <summary>
    /// It sends a heartbeat message to the follower nodes to indicate that the leader node in the partition is still alive.
    /// </summary>
    private async Task SendHearthbeat()
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send hearthbeat", manager.LocalEndpoint, partition.PartitionId, state);
            return;
        }

        lastHeartbeat = await manager.HybridLogicalClock.SendOrLocalEvent();

        await Ping();
    }

    /// <summary>
    /// When another node requests our vote, it verifies that the term is valid and that the commitIndex is
    /// higher than ours to ensure we don't elect outdated nodes as leaders.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId"></param>
    private async Task Vote(RaftNode node, long voteTerm, long remoteMaxLogId)
    {
        if (votes.ContainsKey(voteTerm))
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but already voted in that Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);
            return;
        }

        if (state != NodeState.Follower && voteTerm == currentTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote from {Endpoint} but we're candidate or leader on the same Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);
            return;
        }

        if (currentTerm > voteTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on previous term from {Endpoint} Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);
            return;
        }
        
        RaftWALResponse localMaxId = await walActor.Ask(new(RaftWALActionType.GetMaxLog));
        if (localMaxId.NextId > remoteMaxLogId)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on outdated log from {Endpoint} RemoteMaxId={RemoteId} LocalMaxId={MaxId}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, remoteMaxLogId, localMaxId.NextId);
            
            // If we know that we have a commitIndex ahead of other nodes in this partition,
            // we increase the term to force being chosen as leaders.
            currentTerm++;  
            return;
        }

        lastHeartbeat = await manager.HybridLogicalClock.SendOrLocalEvent();

        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Requested vote from {Endpoint} Term={Term}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);

        VoteRequest request = new(partition.PartitionId, voteTerm, manager.LocalEndpoint);
        
        await communication.Vote(manager, partition, node, request);
    }

    /// <summary>
    /// When a node receives a vote from another node, it verifies that the term is valid and that the node
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="voteTerm"></param>
    private async Task ReceivedVote(string endpoint, long voteTerm)
    {
        if (state == NodeState.Leader)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote but already declared as leader from {Node} Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm);
            return;
        }

        if (state == NodeState.Follower)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote but we didn't ask for it from {Node} Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm);
            return;
        }

        if (voteTerm < currentTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote on previous term from {Endpoint} Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm);
            return;
        }

        int numberVotes = IncreaseVotes(voteTerm);
        int quorum = Math.Min(2, (int)Math.Floor((manager.Nodes.Count + 1) / 2f));

        if (numberVotes < quorum)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} Term={Term} Votes={Votes} Quorum={Quorum}/{Total}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm, numberVotes, quorum, manager.Nodes.Count + 1);
            return;
        }

        state = NodeState.Leader;
        partition.Leader = manager.LocalEndpoint;

        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} and proclamed leader Term={Term} Votes={Votes} Quorum={Quorum}/{Total}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm, numberVotes, quorum, manager.Nodes.Count + 1);

        await SendHearthbeat();
    }

    /// <summary>
    /// Appends logs to the Write-Ahead Log and updates the state of the node based on the leader's term.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="leaderTerm"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    private async Task<(RaftOperationStatus, long)> AppendLogs(string endpoint, long leaderTerm, List<RaftLog>? logs)
    {
        long commitedIndex = -1;
        
        if (currentTerm > leaderTerm)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from a leader {Endpoint} with old Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, leaderTerm);
            return (RaftOperationStatus.LeaderInOldTerm, commitedIndex);
        }

        if (leaderTerm >= currentTerm)
        {
            if (logs is not null)
            {
                RaftWALResponse updateResponse = await walActor.Ask(new(RaftWALActionType.Update, currentTerm, logs));
                commitedIndex = updateResponse.NextId;
            }

            state = NodeState.Follower;
            lastHeartbeat = await manager.HybridLogicalClock.SendOrLocalEvent();
            currentTerm = leaderTerm;

            if (partition.Leader != endpoint)
            {
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Leader is now {Endpoint} LeaderTerm={Term}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, leaderTerm);
                partition.Leader = endpoint;
            }
        }

        return (RaftOperationStatus.Success, commitedIndex);
    }

    /// <summary>
    /// Replicates logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    private async Task<RaftOperationStatus> ReplicateLogs(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return RaftOperationStatus.Success;

        if (state != NodeState.Leader)
            return RaftOperationStatus.NodeIsNotLeader;

        HLCTimestamp currentTime = await manager.HybridLogicalClock.SendOrLocalEvent();
        
        foreach (RaftLog log in logs)
            log.Time = currentTime;
        
        // Append logs to the Write-Ahead Log
        RaftWALResponse appendResponse = await walActor.Ask(new(RaftWALActionType.Append, currentTerm, logs));
        currentIndex = appendResponse.NextId;
        
        // Replicate logs to other nodes in the cluster
        await AppendLogsToNodes(false);

        return RaftOperationStatus.Success;
    }

    /// <summary>
    /// Replicates the checkpoint to other nodes in the cluster when the node is the leader.
    /// </summary>
    private async Task ReplicateCheckpoint()
    {
        if (state != NodeState.Leader)
            return;

        await walActor.Ask(new(RaftWALActionType.AppendCheckpoint, currentTerm));
    }

    /// <summary>
    /// Sets the number of votes for a given term.
    /// </summary>
    /// <param name="term"></param>
    /// <param name="number"></param>
    /// <returns></returns>
    private int SetVotes(long term, int number)
    {
        votes[term] = number;
        return number;
    }

    /// <summary>
    /// Increases the number of votes for a given term.
    /// </summary>
    /// <param name="term"></param>
    /// <returns></returns>
    private int IncreaseVotes(long term)
    {
        if (votes.TryGetValue(term, out int numberVotes))
            votes[term] = numberVotes + 1;
        else
            votes.Add(term, 1);

        return votes[term];
    }
    
    /// <summary>
    /// Pings the other nodes in the cluster to replicate logs when the node is the leader.
    /// </summary>
    /// <param name="term"></param>
    private async ValueTask Ping()
    {
        if (state != NodeState.Leader)
            return;
        
        await AppendLogsToNodes(true);
    }
    
    /// <summary>
    /// Appends logs to the Write-Ahead Log and replicates them to other nodes in the cluster when the node is the leader.
    /// </summary>
    private async Task AppendLogsToNodes(bool isPing)
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to replicate logs", manager.LocalEndpoint, partition.PartitionId, state);
            return;
        }

        List<RaftNode> nodes = manager.Nodes;

        List<Task> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(AppendLogToNode(node, isPing));

        await Task.WhenAll(tasks);
    }
    
    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="isPing"></param>
    private async Task AppendLogToNode(RaftNode node, bool isPing)
    {
        AppendLogsRequest request;
        
        if (isPing)
            request = new(partition.PartitionId, currentTerm, manager.LocalEndpoint, null);
        else
        {
            if (!lastCommitIndexes.TryGetValue(node.Endpoint, out long lastCommitIndex))
                lastCommitIndex = currentIndex - 8;

            RaftWALResponse getRangeResponse =  await walActor.Ask(new(RaftWALActionType.GetRange, currentTerm, lastCommitIndex));
            if (getRangeResponse.Logs is null)
                return;
            
            request = new(partition.PartitionId, currentTerm, manager.LocalEndpoint, getRangeResponse.Logs);
        }
        
        AppendLogsResponse response = await communication.AppendLogToNode(manager, partition, node, request);
        if (response.CommitedIndex > 0)
        {
            lastCommitIndexes[node.Endpoint] = response.CommitedIndex;
            
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Appended logs to {Endpoint} CommitedIndex={Index}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, response.CommitedIndex);
        }
    }
}