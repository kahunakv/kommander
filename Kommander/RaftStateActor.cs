
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
    
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();

    private NodeState state = NodeState.Follower;

    private long currentTerm;

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
        stopwatch.Restart();
        
        //await File.AppendAllTextAsync($"/tmp/{partition.PartitionId}.txt", $"{message.Type}\n");
        
        try
        {
            await RestoreWal().ConfigureAwait(false);

            switch (message.Type)
            {
                case RaftRequestType.CheckLeader:
                    await CheckClusterLeadership().ConfigureAwait(false);
                    break;

                case RaftRequestType.GetState:
                    return new(RaftResponseType.State, state);

                case RaftRequestType.AppendLogs:
                {
                    (RaftOperationStatus status, long commitLogIndex) = await AppendLogs(message.Endpoint ?? "", message.Term, message.Timestamp, message.Logs).ConfigureAwait(false);
                    return new(RaftResponseType.None, status, commitLogIndex);
                }

                case RaftRequestType.RequestVote:
                    await Vote(new(message.Endpoint ?? ""), message.Term, message.MaxLogId, message.Timestamp).ConfigureAwait(false);
                    break;

                case RaftRequestType.ReceiveVote:
                    await ReceivedVote(message.Endpoint ?? "", message.Term).ConfigureAwait(false);
                    break;

                case RaftRequestType.ReplicateLogs:
                {
                    (RaftOperationStatus status, long commitId) = await ReplicateLogs(message.Logs).ConfigureAwait(false);
                    return new(RaftResponseType.None, status, commitId);
                }

                case RaftRequestType.ReplicateCheckpoint:
                {
                    (RaftOperationStatus status, long commitId) = await ReplicateCheckpoint().ConfigureAwait(false);
                    return new(RaftResponseType.None, status, commitId);
                }

                default:
                    logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Invalid message type: {Type}", manager.LocalEndpoint, partition.PartitionId, state, message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] {Name} {Message} {StackTrace}", manager.LocalEndpoint, partition.PartitionId, state, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        finally
        {
            if (stopwatch.ElapsedMilliseconds > 2500)
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Slow message processing: {Type} Elapsed={Elapsed}ms", manager.LocalEndpoint, partition.PartitionId, state,  message.Type, stopwatch.ElapsedMilliseconds);
            
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
        lastHeartbeat = await manager.HybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);
        Stopwatch stopWatch = Stopwatch.StartNew();

        RaftWALResponse currentCommitIndexResponse = await walActor.Ask(new(RaftWALActionType.Recover)).ConfigureAwait(false);
        
        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] WAL restored at #{NextId} in {ElapsedMs}ms", manager.LocalEndpoint, partition.PartitionId, state, currentCommitIndexResponse.Index, stopWatch.ElapsedMilliseconds);
        
        RaftWALResponse currentTermResponse = await walActor.Ask(new(RaftWALActionType.GetCurrentTerm)).ConfigureAwait(false);
        currentTerm = currentTermResponse.Index;
    }

    /// <summary>
    /// Periodically, it checks the leadership status of the partition and, based on timeouts,
    /// decides whether to start a new election process.
    /// </summary>
    private async Task CheckClusterLeadership()
    {
        HLCTimestamp currentTime = await manager.HybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);

        switch (state)
        {
            // if node is leader just send hearthbeats every 1.5 seconds
            case NodeState.Leader:
            {
                if (currentTime != HLCTimestamp.Zero && ((currentTime - lastHeartbeat) > TimeSpan.FromMilliseconds(1500)))
                    await SendHearthbeat().ConfigureAwait(false);
            
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
                electionTimeout += TimeSpan.FromMilliseconds(Random.Shared.Next(200, 1000));
                lastCommitIndexes.Clear();
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

                await RequestVotes(currentTime).ConfigureAwait(false);
                break;
        }
    }

    /// <summary>
    /// Requests votes to obtain leadership when a node becomes a candidate, reaching out to other known nodes in the cluster.
    /// </summary>
    /// <param name="timestamp"></param>
    /// <exception cref="RaftException"></exception>
    private async Task RequestVotes(HLCTimestamp timestamp)
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to vote", manager.LocalEndpoint, partition.PartitionId, state);
            return;
        }
        
        RaftWALResponse currentMaxLog = await walActor.Ask(new(RaftWALActionType.GetMaxLog)).ConfigureAwait(false);;

        RequestVotesRequest request = new(partition.PartitionId, currentTerm, currentMaxLog.Index, timestamp, manager.LocalEndpoint);

        foreach (RaftNode node in manager.Nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Asked {Endpoint} for votes on Term={CurrentTerm}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, currentTerm);
            
            _ = communication.RequestVotes(manager, partition, node, request);
        }
    }

    /// <summary>
    /// It sends a heartbeat message to the follower nodes to indicate that the leader node in the partition is still alive.
    /// </summary>
    private async Task<bool> SendHearthbeat()
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send hearthbeat", manager.LocalEndpoint, partition.PartitionId, state);
            return false;
        }

        lastHeartbeat = await manager.HybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);;

        if (state != NodeState.Leader)
            return false;
        
        return await SendHearthbeatToNodes(lastHeartbeat).ConfigureAwait(false);;
    }

    /// <summary>
    /// When another node requests our vote, it verifies that the term is valid and that the commitIndex is
    /// higher than ours to ensure we don't elect outdated nodes as leaders.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId"></param>
    private async Task Vote(RaftNode node, long voteTerm, long remoteMaxLogId, HLCTimestamp timestamp)
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
        
        RaftWALResponse localMaxId = await walActor.Ask(new(RaftWALActionType.GetMaxLog)).ConfigureAwait(false);;
        if (localMaxId.Index > remoteMaxLogId)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received request to vote on outdated log from {Endpoint} RemoteMaxId={RemoteId} LocalMaxId={MaxId}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, remoteMaxLogId, localMaxId.Index);
            
            // If we know that we have a commitIndex ahead of other nodes in this partition,
            // we increase the term to force being chosen as leaders.
            currentTerm++;  
            return;
        }

        lastHeartbeat = await manager.HybridLogicalClock.ReceiveEvent(timestamp).ConfigureAwait(false);

        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Requested vote from {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);

        VoteRequest request = new(partition.PartitionId, voteTerm, timestamp, manager.LocalEndpoint);
        
        _ = communication.Vote(manager, partition, node, request);
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
        int quorum = Math.Max(2, (int)Math.Floor((manager.Nodes.Count + 1) / 2f));

        if (numberVotes < quorum)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} Term={Term} Votes={Votes} Quorum={Quorum}/{Total}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm, numberVotes, quorum, manager.Nodes.Count + 1);
            return;
        }

        state = NodeState.Leader;
        partition.Leader = manager.LocalEndpoint;

        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} and proclamed leader Term={Term} Votes={Votes} Quorum={Quorum}/{Total}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm, numberVotes, quorum, manager.Nodes.Count + 1);

        await SendHearthbeat().ConfigureAwait(false);
    }

    /// <summary>
    /// Appends logs to the Write-Ahead Log and updates the state of the node based on the leader's term.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="leaderTerm"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    private async Task<(RaftOperationStatus, long)> AppendLogs(string endpoint, long leaderTerm, HLCTimestamp timestamp, List<RaftLog>? logs)
    {
        if (currentTerm > leaderTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from a leader {Endpoint} with old Term={Term}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, leaderTerm);
            
            return (RaftOperationStatus.LeaderInOldTerm, -1);
        }

        if (leaderTerm >= currentTerm)
        {
            state = NodeState.Follower;
            lastHeartbeat = await manager.HybridLogicalClock.ReceiveEvent(timestamp).ConfigureAwait(false);
            currentTerm = leaderTerm;
            lastCommitIndexes.Clear();

            if (partition.Leader != endpoint)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Leader is now {Endpoint} LeaderTerm={Term}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, leaderTerm);
                
                partition.Leader = endpoint;
            }
        }
        
        if (logs is not null && logs.Count > 0)
        {
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from leader {Endpoint} with Term={Term} Logs={Logs}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, leaderTerm, logs.Count);

            RaftWALResponse response = await walActor.Ask(new(RaftWALActionType.ProposeOrCommit, leaderTerm, timestamp, logs)).ConfigureAwait(false);
                
            if (response.Index < 0)
                return (RaftOperationStatus.LeaderInOutdatedTerm, -1);
                
            return (RaftOperationStatus.Success, response.Index);
        }

        return (RaftOperationStatus.Success, -1);
    }

    /// <summary>
    /// Replicates logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <returns></returns>
    private async Task<(RaftOperationStatus, long commitId)> ReplicateLogs(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, -1);

        if (state != NodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, -1);

        HLCTimestamp currentTime = await manager.HybridLogicalClock.SendOrLocalEvent().ConfigureAwait(false);
        
        foreach (RaftLog log in logs)
            log.Time = currentTime;
        
        // Append proposal logs to the Write-Ahead Log
        RaftWALResponse proposeResponse = await walActor.Ask(new(RaftWALActionType.Propose, currentTerm, currentTime, logs)).ConfigureAwait(false);
        
        // Replicate logs to other nodes in the cluster
        if (!await ProposeLogsToNodes(currentTime).ConfigureAwait(false))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No quorum available to commit log {CommitIndex}", manager.LocalEndpoint, partition.PartitionId, state, proposeResponse.Index);
            
            return (RaftOperationStatus.Errored, proposeResponse.Index);
        }

        // Commit logs to the Write-Ahead Log
        RaftWALResponse commitResponse = await walActor.Ask(new(RaftWALActionType.Commit, currentTerm, currentTime, logs)).ConfigureAwait(false);
           
        if (!await CommitLogsToNodes(currentTime).ConfigureAwait(false))
            return (RaftOperationStatus.Errored, commitResponse.Index);
        
        return (RaftOperationStatus.Success, commitResponse.Index);
    }

    /// <summary>
    /// Replicates the checkpoint to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <returns></returns>
    private async Task<(RaftOperationStatus, long commitId)> ReplicateCheckpoint()
    {
        if (state != NodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, -1);
        
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

        RaftWALResponse proposeResponse = await walActor.Ask(new(RaftWALActionType.Propose, currentTerm, currentTime, checkpointLogs)).ConfigureAwait(false);
       
        // Replicate the checkpoint to other nodes in the cluster
        if (!await ProposeLogsToNodes(currentTime).ConfigureAwait(false))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No quorum available to commit checkpoint log {CommitIndex}", manager.LocalEndpoint, partition.PartitionId, state, proposeResponse.Index);
            
            return (RaftOperationStatus.Errored, proposeResponse.Index);
        }

        // Commit logs to the Write-Ahead Log
        RaftWALResponse commitResponse = await walActor.Ask(new(RaftWALActionType.Commit, currentTerm, currentTime, checkpointLogs)).ConfigureAwait(false);
           
        if (!await CommitLogsToNodes(currentTime).ConfigureAwait(false))
            return (RaftOperationStatus.Errored, commitResponse.Index);
        
        return (RaftOperationStatus.Success, commitResponse.Index);
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
    /// Proposes logs to the Write-Ahead Log and replicates them to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="timestamp"></param> 
    /// <returns></returns>
    private async Task<bool> ProposeLogsToNodes(HLCTimestamp timestamp)
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to replicate logs", manager.LocalEndpoint, partition.PartitionId, state);
            return false;
        }

        List<RaftNode> nodes = manager.Nodes;

        List<Task<RaftOperationStatus>> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(AppendLogToNode(node, timestamp, false));

        RaftOperationStatus[] responses = await Task.WhenAll(tasks).ConfigureAwait(false);

        int count = 1 + responses.Count(x => x == RaftOperationStatus.Success);
        int quorum = Math.Max(2, (int)Math.Floor((manager.Nodes.Count + 1) / 2f));
        
        return count >= quorum;
    }
    
    /// <summary>
    /// Appends logs to the Write-Ahead Log and replicates them to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="timestamp"></param> 
    /// <returns></returns>
    private async Task<bool> CommitLogsToNodes(HLCTimestamp timestamp)
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to replicate logs", manager.LocalEndpoint, partition.PartitionId, state);
            return false;
        }

        List<RaftNode> nodes = manager.Nodes;

        List<Task<RaftOperationStatus>> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(AppendLogToNode(node, timestamp, false));

        await Task.WhenAny(tasks).ConfigureAwait(false);

        return true;
    }
    
    /// <summary>
    /// Appends logs to the Write-Ahead Log and replicates them to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="timestamp"></param> 
    /// <param name="isHeathbeat"></param>
    /// <returns></returns>
    private async Task<bool> SendHearthbeatToNodes(HLCTimestamp timestamp)
    {
        if (manager.Nodes.Count == 0)
        {
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to replicate logs", manager.LocalEndpoint, partition.PartitionId, state);
            return false;
        }

        List<RaftNode> nodes = manager.Nodes;

        List<Task<RaftOperationStatus>> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(AppendLogToNode(node, timestamp, true));

        await Task.WhenAny(tasks).ConfigureAwait(false);

        return true;
    }
    
    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="isHeathbeat"></param>
    private async Task<RaftOperationStatus> AppendLogToNode(RaftNode node, HLCTimestamp timestamp, bool isHeathbeat)
    {
        AppendLogsRequest request;
        
        if (isHeathbeat)
            request = new(partition.PartitionId, currentTerm, timestamp, manager.LocalEndpoint, null);
        else
        {
            long lastCommitIndex = lastCommitIndexes.GetValueOrDefault(node.Endpoint, 0);

            RaftWALResponse getRangeResponse =  await walActor.Ask(new(RaftWALActionType.GetRange, currentTerm, lastCommitIndex)).ConfigureAwait(false);
            if (getRangeResponse.Logs is null)
                return RaftOperationStatus.Errored;
            
            request = new(partition.PartitionId, currentTerm, timestamp, manager.LocalEndpoint, getRangeResponse.Logs);
        }
        
        AppendLogsResponse response = await communication.AppendLogToNode(manager, partition, node, request).ConfigureAwait(false);
        if (response.CommitedIndex > 0)
        {
            lastCommitIndexes[node.Endpoint] = response.CommitedIndex;
            
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Sent logs to {Endpoint} CommitedIndex={Index}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, response.CommitedIndex);
        }

        return response.Status;
    }
}