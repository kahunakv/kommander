
using Nixie;
using System.Text.Json;
using Flurl.Http;
using Kommander.Communication;
using Kommander.Data;
using Kommander.WAL;

namespace Kommander;

public sealed class RaftStateActor : IActorStruct<RaftRequest, RaftResponse>
{
    private static readonly RaftRequest checkLeaderRequest = new(RaftRequestType.CheckLeader);
    
    private readonly int ElectionTimeout = Random.Shared.Next(1750, 2550);

    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    private readonly ICommunication communication;

    private readonly IActorRefStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> walActor;

    private readonly Dictionary<long, int> votes = [];

    private NodeState state = NodeState.Follower;

    private long currentTerm;

    private long lastHeartbeat = -1;

    private long voteStartedAt = -1;

    private bool restored;

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
            TimeSpan.FromSeconds(Random.Shared.Next(8, 12)),
            TimeSpan.FromMilliseconds(Random.Shared.Next(50, 120))
        );
    }

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
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("{0} {1} {2}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }

        return new(RaftResponseType.None);
    }

    private async ValueTask RestoreWal()
    {
        if (restored)
            return;

        restored = true;

        await walActor.Ask(new(RaftWALActionType.Recover));
    }

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
            if ((currentTime - voteStartedAt) < 1500)
                return;
        }

        if (state == NodeState.Follower)
        {
            if (lastHeartbeat > 0 && ((currentTime - lastHeartbeat) < ElectionTimeout))
                return;
        }

        if (state == NodeState.Candidate)
            Console.WriteLine("[{0}/{1}] Voting concluded after {2}ms. No quorum available", manager.LocalEndpoint, partition.PartitionId, (currentTime - voteStartedAt));

        partition.Leader = "";
        state = NodeState.Candidate;
        voteStartedAt = GetCurrentTime();

        while (votes.ContainsKey(++currentTerm))
            continue;

        SetVotes(currentTerm, 1);

        Console.WriteLine("[{0}/{1}] Voted to become leader. Term={2}", manager.LocalEndpoint, partition.PartitionId, currentTerm);

        await RequestVotes();
    }

    private async Task RequestVotes()
    {
        if (manager.Nodes.Count == 0)
        {
            Console.WriteLine("[{0}/{1}] No other nodes availables to vote", manager.LocalEndpoint, partition.PartitionId);
            return;
        }
        
        RaftWALResponse currentMaxLog = await walActor.Ask(new(RaftWALActionType.GetMaxLog));

        RequestVotesRequest request = new(partition.PartitionId, currentTerm, currentMaxLog.NextId, manager.LocalEndpoint);

        //string payload = JsonSerializer.Serialize(request); // , RaftJsonContext.Default.RequestVotesRequest

        foreach (RaftNode node in manager.Nodes)
        {
            Console.WriteLine("[{0}/{1}] Asked {2} for votes on Term={3}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, currentTerm);

            /*try
            {
                await ("http://" + node.Endpoint)
                        .WithOAuthBearerToken("xxx")
                        .AppendPathSegments("v1/raft/request-vote")
                        .WithHeader("Accept", "application/json")
                        .WithHeader("Content-Type", "application/json")
                        .WithTimeout(5)
                        .WithSettings(o => o.HttpVersion = "2.0")
                        .PostStringAsync(payload)
                        .ReceiveJson<RequestVotesResponse>();
            }
            catch (Exception e)
            {
                Console.WriteLine("[{0}/{1}] {2}", manager.LocalEndpoint, partition.PartitionId, e.Message);
            }*/
            
            await communication.RequestVotes(manager, partition, node, request);
        }
    }

    private async Task SendHearthbeat()
    {
        if (manager.Nodes.Count == 0)
        {
            Console.WriteLine("[{0}/{1}] No other nodes availables to send hearthbeat", manager.LocalEndpoint, partition.PartitionId);
            return;
        }

        lastHeartbeat = GetCurrentTime();

        await Ping(currentTerm);
    }

    private async Task Vote(RaftNode node, long voteTerm, ulong remoteMaxLogId)
    {
        if (votes.ContainsKey(voteTerm))
        {
            Console.WriteLine("[{0}/{1}] Received request to vote from {2} but already voted in that Term={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, voteTerm);
            return;
        }

        if (state != NodeState.Follower && voteTerm == currentTerm)
        {
            Console.WriteLine("[{0}/{1}] Received request to vote from {2} but we're candidate or leader on the same Term={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, voteTerm);
            return;
        }

        if (currentTerm > voteTerm)
        {
            Console.WriteLine("[{0}/{1}] Received request to vote on previous term from {2} Term={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, voteTerm);
            return;
        }
        
        RaftWALResponse localMaxId = await walActor.Ask(new(RaftWALActionType.GetMaxLog));
        if (localMaxId.NextId > remoteMaxLogId)
        {
            Console.WriteLine("[{0}/{1}] Received request to vote on outdated log {2} RemoteMaxId={3} LocalMaxId={4}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, localMaxId, remoteMaxLogId);
            return;
        }

        lastHeartbeat = GetCurrentTime();

        Console.WriteLine("[{0}/{1}] Requested vote from {2} Term={3}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, voteTerm);

        VoteRequest request = new(partition.PartitionId, voteTerm, manager.LocalEndpoint);

        /*string payload = JsonSerializer.Serialize(request); // RaftJsonContext.Default.RequestVotesRequest

        try
        {
            await ("http://" + endpoint)
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/raft/vote")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<VoteResponse>();
        }
        catch (Exception e)
        {
            Console.WriteLine("[{0}/{1}] {2}", manager.LocalEndpoint, partition.PartitionId, e.Message);
        }*/
        
        await communication.Vote(manager, partition, node, request);
    }

    private async Task ReceivedVote(string endpoint, long voteTerm)
    {
        if (state == NodeState.Leader)
        {
            Console.WriteLine("[{0}/{1}] Received vote but already declared as leader from {2} Term={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        if (state == NodeState.Follower)
        {
            Console.WriteLine("[{0}/{1}] Received vote but we didn't ask for it from {2} Term={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        if (voteTerm < currentTerm)
        {
            Console.WriteLine("[{0}/{1}] Received vote on previous term from {2} Term={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        int numberVotes = IncreaseVotes(voteTerm);
        int quorum = Math.Min(2, (manager.Nodes.Count / 2) + 1);

        if (numberVotes < quorum)
        {
            Console.WriteLine("[{0}/{1}] Received vote from {2} Term={3} Votes={4} Quorum={5}", manager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm, numberVotes, quorum);
            return;
        }

        state = NodeState.Leader;
        partition.Leader = manager.LocalEndpoint;

        Console.WriteLine("[{0}/{1}] Received vote from {2} and proclamed leader Term={3} Votes={4} Quorum={5}", manager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm, numberVotes, quorum);

        await SendHearthbeat();
    }

    private void AppendLogs(string endpoint, long leaderTerm, List<RaftLog>? logs)
    {
        if (currentTerm > leaderTerm)
        {
            Console.WriteLine("[{0}/{1}] Received logs from a leader {2} with old Term={3}. Ignoring...", manager.LocalEndpoint, partition.PartitionId, endpoint, leaderTerm);
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
                Console.WriteLine("[{0}/{1}] Leader is now {2} LeaderTerm={3}", manager.LocalEndpoint, partition.PartitionId, endpoint, leaderTerm);
                partition.Leader = endpoint;
            }
        }
    }

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

    private void ReplicateCheckpoint()
    {
        if (state != NodeState.Leader)
            return;

        walActor.Send(new(RaftWALActionType.AppendCheckpoint, currentTerm));
    }

    private int SetVotes(long term, int number)
    {
        votes[term] = number;
        return number;
    }

    private int IncreaseVotes(long term)
    {
        if (votes.TryGetValue(term, out int numberVotes))
            votes[term] = numberVotes + 1;
        else
            votes.Add(term, 1);

        return votes[term];
    }
    
    private async ValueTask Ping(long term)
    {
        if (state != NodeState.Leader)
            return;
        
        AppendLogsRequest request = new(partition.PartitionId, term, manager.LocalEndpoint);

        await AppendLogsToNodes(request);
    }
    
    private async Task AppendLogsToNodes(AppendLogsRequest request)
    {
        if (manager.Nodes.Count == 0)
        {
            Console.WriteLine("[{0}/{1}] No other nodes availables to replicate logs", manager.LocalEndpoint, partition.PartitionId);
            return;
        }

        List<RaftNode> nodes = manager.Nodes;

        List<Task> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(AppendLogToNode(node, request));

        await Task.WhenAll(tasks);
    }
    
    private async Task AppendLogToNode(RaftNode node, AppendLogsRequest request)
    {
        await communication.AppendLogToNode(manager, partition, node, request);
    }

    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}