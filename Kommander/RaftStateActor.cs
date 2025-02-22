
using System.Diagnostics;
using Nixie;
using Kommander.Communication;
using Kommander.Data;
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

    private NodeState state = NodeState.Follower;

    private long currentTerm;

    private long lastHeartbeat = -1;

    private long votingStartedAt = -1;

    private int electionTimeout = Random.Shared.Next(1000, 3000);

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
        ICommunication communication
    )
    {
        this.manager = manager;
        this.partition = partition;
        this.communication = communication;
        
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
            TimeSpan.FromMilliseconds(100)
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
                    AppendLogs(message.Endpoint ?? "", message.Term, message.Log);
                    break;

                case RaftRequestType.RequestVote:
                    await Vote(new(message.Endpoint ?? ""), message.Term, message.MaxLogId);
                    break;

                case RaftRequestType.ReceiveVote:
                    await ReceivedVote(message.Endpoint ?? "", message.Term);
                    break;

                case RaftRequestType.ReplicateLogs:
                    await ReplicateLogs(message.Log);
                    break;

                case RaftRequestType.ReplicateCheckpoint:
                    ReplicateCheckpoint();
                    break;
                
                default:
                    Console.WriteLine("Invalid message type: {0}", message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("{0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
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
        lastHeartbeat = GetCurrentTime();
        Stopwatch stopWatch = Stopwatch.StartNew();

        RaftWALResponse currentCommitIndexResponse = await walActor.Ask(new(RaftWALActionType.Recover));
        Console.WriteLine("[{0}/{1}/{2}] WAL restored at #{3} in {4}ms", manager.LocalEndpoint, partition.PartitionId, state, currentCommitIndexResponse.NextId, stopWatch.ElapsedMilliseconds);
        
        RaftWALResponse currentTermResponse = await walActor.Ask(new(RaftWALActionType.GetCurrentTerm));
        currentTerm = (long)currentTermResponse.NextId;
    }

    /// <summary>
    /// Periodically, it checks the leadership status of the partition and, based on timeouts,
    /// decides whether to start a new election process.
    /// </summary>
    private async Task CheckClusterLeadership()
    {
        long currentTime = GetCurrentTime();

        if (state == NodeState.Leader)
        {
            if (lastHeartbeat > 0 && ((currentTime - lastHeartbeat) > 1500))
                await SendHearthbeat();
            
            return;
        }

        if (state == NodeState.Candidate)
        {
            if (votingStartedAt > 0 && (currentTime - votingStartedAt) < 1500)
                return;
            
            Console.WriteLine("[{0}/{1}/{2}] Voting concluded after {3}ms. No quorum available", manager.LocalEndpoint, partition.PartitionId, state, (currentTime - votingStartedAt));
            
            state = NodeState.Follower;
            lastHeartbeat = currentTime;
            electionTimeout = Random.Shared.Next(1000, 3000);
            return;
        }

        if (state == NodeState.Follower)
        {
            if (lastHeartbeat > 0 && ((currentTime - lastHeartbeat) < electionTimeout))
                return;
        }

        partition.Leader = "";
        state = NodeState.Candidate;
        votingStartedAt = GetCurrentTime();
        
        currentTerm++;
        
        SetVotes(currentTerm, 1);

        Console.WriteLine("[{0}/{1}/{2}] Voted to become leader after {3}ms. Term={4}", manager.LocalEndpoint, partition.PartitionId, state, currentTime - lastHeartbeat, currentTerm);

        await RequestVotes();
    }

    /// <summary>
    /// Requests votes to obtain leadership when a node becomes a candidate, reaching out to other known nodes in the cluster.
    /// </summary>
    /// <exception cref="RaftException"></exception>
    private async Task RequestVotes()
    {
        if (manager.Nodes.Count == 0)
        {
            Console.WriteLine("[{0}/{1}/{2}] No other nodes availables to vote", manager.LocalEndpoint, partition.PartitionId, state);
            return;
        }
        
        RaftWALResponse currentMaxLog = await walActor.Ask(new(RaftWALActionType.GetMaxLog));

        RequestVotesRequest request = new(partition.PartitionId, currentTerm, currentMaxLog.NextId, manager.LocalEndpoint);

        foreach (RaftNode node in manager.Nodes)
        {
            if (node.Endpoint == manager.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            Console.WriteLine("[{0}/{1}/{2}] Asked {3} for votes on Term={4}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, currentTerm);
            
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
            Console.WriteLine("[{0}/{1}/{2}] No other nodes availables to send hearthbeat", manager.LocalEndpoint, partition.PartitionId, state);
            return;
        }

        lastHeartbeat = GetCurrentTime();

        await Ping(currentTerm);
    }

    /// <summary>
    /// When another node requests our vote, it verifies that the term is valid and that the commitIndex is
    /// higher than ours to ensure we don't elect outdated nodes as leaders.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId"></param>
    private async Task Vote(RaftNode node, long voteTerm, ulong remoteMaxLogId)
    {
        if (votes.ContainsKey(voteTerm))
        {
            Console.WriteLine("[{0}/{1}/{2}] Received request to vote from {3} but already voted in that Term={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);
            return;
        }

        if (state != NodeState.Follower && voteTerm == currentTerm)
        {
            Console.WriteLine("[{0}/{1}/{2}] Received request to vote from {3} but we're candidate or leader on the same Term={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);
            return;
        }

        if (currentTerm > voteTerm)
        {
            Console.WriteLine("[{0}/{1}/{2}] Received request to vote on previous term from {3} Term={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);
            return;
        }
        
        RaftWALResponse localMaxId = await walActor.Ask(new(RaftWALActionType.GetMaxLog));
        if (localMaxId.NextId > remoteMaxLogId)
        {
            Console.WriteLine("[{0}/{1}/{2}] Received request to vote on outdated log from {3} RemoteMaxId={4} LocalMaxId={5}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, remoteMaxLogId, localMaxId.NextId);
            
            // If we know that we have a commitIndex ahead of other nodes in this partition,
            // we increase the term to force being chosen as leaders.
            currentTerm++;  
            return;
        }

        lastHeartbeat = GetCurrentTime();

        Console.WriteLine("[{0}/{1}/{2}] Requested vote from {3} Term={4}", manager.LocalEndpoint, partition.PartitionId, state, node.Endpoint, voteTerm);

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
            Console.WriteLine("[{0}/{1}/{2}] Received vote but already declared as leader from {3} Term={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm);
            return;
        }

        if (state == NodeState.Follower)
        {
            Console.WriteLine("[{0}/{1}/{2}] Received vote but we didn't ask for it from {3} Term={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm);
            return;
        }

        if (voteTerm < currentTerm)
        {
            Console.WriteLine("[{0}/{1}/{2}] Received vote on previous term from {3} Term={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm);
            return;
        }

        int numberVotes = IncreaseVotes(voteTerm);
        int quorum = Math.Min(2, (manager.Nodes.Count / 2) + 1);

        if (numberVotes < quorum)
        {
            Console.WriteLine("[{0}/{1}/{2}] Received vote from {3} Term={4} Votes={5} Quorum={6}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm, numberVotes, quorum);
            return;
        }

        state = NodeState.Leader;
        partition.Leader = manager.LocalEndpoint;

        Console.WriteLine("[{0}/{1}/{2}] Received vote from {3} and proclamed leader Term={4} Votes={5} Quorum={6}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, voteTerm, numberVotes, quorum);

        await SendHearthbeat();
    }

    /// <summary>
    /// Appends logs to the Write-Ahead Log and updates the state of the node based on the leader's term.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="leaderTerm"></param>
    /// <param name="logs"></param>
    private void AppendLogs(string endpoint, long leaderTerm, List<RaftLog>? logs)
    {
        if (currentTerm > leaderTerm)
        {
            Console.WriteLine("[{0}/{1}/{2}] Received logs from a leader {3} with old Term={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, state, endpoint, leaderTerm);
            return;
        }

        if (leaderTerm >= currentTerm)
        {
            if (logs is not null)
                walActor.Send(new(RaftWALActionType.Update, currentTerm, logs));

            state = NodeState.Follower;
            lastHeartbeat = GetCurrentTime();
            currentTerm = leaderTerm;

            if (partition.Leader != endpoint)
            {
                Console.WriteLine("[{0}/{1}/{2}] Leader is now {3} LeaderTerm={4}", manager.LocalEndpoint, partition.PartitionId, state, endpoint, leaderTerm);
                partition.Leader = endpoint;
            }
        }
    }

    /// <summary>
    /// Replicates logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    private async Task ReplicateLogs(List<RaftLog>? logs)
    {
        if (logs is null)
            return;

        if (state != NodeState.Leader)
            return;

        RaftWALResponse response = await walActor.Ask(new(RaftWALActionType.Append, currentTerm, logs));
        if (response.Logs is null)
            return;
        
        AppendLogsRequest request = new(partition.PartitionId, currentTerm, manager.LocalEndpoint, response.Logs);
        await AppendLogsToNodes(request);
    }

    /// <summary>
    /// Replicates the checkpoint to other nodes in the cluster when the node is the leader.
    /// </summary>
    private void ReplicateCheckpoint()
    {
        if (state != NodeState.Leader)
            return;

        walActor.Send(new(RaftWALActionType.AppendCheckpoint, currentTerm));
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
    private async ValueTask Ping(long term)
    {
        if (state != NodeState.Leader)
            return;
        
        AppendLogsRequest request = new(partition.PartitionId, term, manager.LocalEndpoint);

        await AppendLogsToNodes(request);
    }
    
    /// <summary>
    /// Appends logs to the Write-Ahead Log and replicates them to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="request"></param>
    private async Task AppendLogsToNodes(AppendLogsRequest request)
    {
        if (manager.Nodes.Count == 0)
        {
            Console.WriteLine("[{0}/{1}/{2}] No other nodes availables to replicate logs", manager.LocalEndpoint, partition.PartitionId, state);
            return;
        }

        List<RaftNode> nodes = manager.Nodes;

        List<Task> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(AppendLogToNode(node, request));

        await Task.WhenAll(tasks);
    }
    
    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="request"></param>
    private async Task AppendLogToNode(RaftNode node, AppendLogsRequest request)
    {
        await communication.AppendLogToNode(manager, partition, node, request);
    }

    /// <summary>
    /// Obtains the current time in milliseconds.
    /// </summary>
    /// <returns></returns>
    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}