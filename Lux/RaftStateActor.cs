
using Nixie;
using System.Text.Json;
using Flurl.Http;
using Lux.Data;

namespace Lux;

public sealed class RaftStateActor : IActorStruct<RaftRequest, RaftResponse>
{
    private static readonly RaftRequest checkLeaderRequest = new(RaftRequestType.CheckLeader);
    
    private readonly int ElectionTimeout = Random.Shared.Next(1750, 2550);

    private readonly RaftManager manager;

    private readonly RaftPartition partition;

    private readonly IActorRefStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse> walActor;

    private readonly Dictionary<long, int> votes = [];

    private NodeState state = NodeState.Follower;

    private long term;

    private long lastHeartbeat = -1;

    private long voteStartedAt = -1;

    private bool restored;

    public RaftStateActor(IActorContextStruct<RaftStateActor, RaftRequest, RaftResponse> context, RaftManager manager, RaftPartition partition)
    {
        Console.WriteLine("created actor");
        
        this.manager = manager;
        this.partition = partition;
        
        walActor = manager.ActorSystem.SpawnStruct<RaftWriteAheadActor, RaftWALRequest, RaftWALResponse>("bra-wal-" + partition.PartitionId, manager, partition);

        manager.ActorSystem.StartPeriodicTimerStruct(
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
                    await Vote(message.Endpoint ?? "", message.Term);
                    break;

                case RaftRequestType.ReceiveVote:
                    ReceivedVote(message.Endpoint ?? "", message.Term);
                    break;

                case RaftRequestType.ReplicateLogs:
                    ReplicateLogs(message.Log);
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
                SendHearthbeat();
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
            Console.WriteLine("[{0}/{1}] Voting concluded after {2}ms. No quorum available", RaftManager.LocalEndpoint, partition.PartitionId, (currentTime - voteStartedAt));

        partition.Leader = "";
        state = NodeState.Candidate;
        voteStartedAt = GetCurrentTime();

        while (votes.ContainsKey(++term))
            continue;

        SetVotes(term, 1);

        Console.WriteLine("[{0}/{1}] Voted to become leader. Term={2}", RaftManager.LocalEndpoint, partition.PartitionId, term);

        await RequestVotes();
    }

    private async Task RequestVotes()
    {
        if (manager.Nodes.Count == 0)
        {
            Console.WriteLine("[{0}/{1}] No other nodes availables to vote", RaftManager.LocalEndpoint, partition.PartitionId);
            return;
        }

        RequestVotesRequest request = new(partition.PartitionId, term, RaftManager.LocalEndpoint);

        string payload = JsonSerializer.Serialize(request); // , RaftJsonContext.Default.RequestVotesRequest

        foreach (RaftNode node in manager.Nodes)
        {
            Console.WriteLine("[{0}/{1}] Asked {2} for votes on Term={3}", RaftManager.LocalEndpoint, partition.PartitionId, node.Ip, term);

            try
            {
                await ("http://" + node.Ip)
                                .WithOAuthBearerToken("xxx")
                                .AppendPathSegments("v1/raft/request-vote")
                                .WithTimeout(5)
                                .WithSettings(o => o.HttpVersion = "2.0")
                                .PostStringAsync(payload)
                                .ReceiveJson<RequestVotesResponse>();
            }
            catch (Exception e)
            {
                Console.WriteLine("[{0}/{1}] {2}", RaftManager.LocalEndpoint, partition.PartitionId, e.Message);
            }
        }
    }

    private void SendHearthbeat()
    {
        if (manager.Nodes.Count == 0)
        {
            Console.WriteLine("[{0}/{1}] No other nodes availables to send hearthbeat", RaftManager.LocalEndpoint, partition.PartitionId);
            return;
        }

        lastHeartbeat = GetCurrentTime();

        walActor.Send(new(RaftWALActionType.Ping, term));
    }

    private async Task Vote(string endpoint, long voteTerm)
    {
        if (votes.ContainsKey(voteTerm))
        {
            Console.WriteLine("[{0}/{1}] Received request to vote from {2} but already voted in that Term={3}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        if (state != NodeState.Follower && voteTerm == term)
        {
            Console.WriteLine("[{0}/{1}] Received request to vote from {2} but we're candidate or leader on the same Term={3}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        if (term > voteTerm)
        {
            Console.WriteLine("[{0}/{1}] Received request to vote on previous term from {2} Term={3}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        lastHeartbeat = GetCurrentTime();

        Console.WriteLine("[{0}/{1}] Requested vote from {2} Term={3}", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);

        RequestVotesRequest request = new(partition.PartitionId, voteTerm, RaftManager.LocalEndpoint);

        string payload = JsonSerializer.Serialize(request); // RaftJsonContext.Default.RequestVotesRequest

        try
        {
            await ("http://" + endpoint)
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/raft/vote")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<RequestVotesResponse>();
        }
        catch (Exception e)
        {
            Console.WriteLine("[{0}/{1}] {2}", RaftManager.LocalEndpoint, partition.PartitionId, e.Message);
        }
    }

    private void ReceivedVote(string endpoint, long voteTerm)
    {
        if (state == NodeState.Leader)
        {
            //logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Received vote but already declared as leader from {Endpoint} Term={VoteTerm}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        if (state == NodeState.Follower)
        {
            //logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Received vote but we didn't ask for it from {Endpoint} Term={VoteTerm}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        if (voteTerm < term)
        {
            //logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Received vote on previous term from {Endpoint} Term={VoteTerm}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm);
            return;
        }

        int numberVotes = IncreaseVotes(voteTerm);

        int quorum = Math.Max(2, (manager.Nodes.Count / 2) + 1);

        if (numberVotes < quorum)
        {
            Console.WriteLine("[{0}/{1}] Received vote from {2} Term={3} Votes={4} Quorum={5}", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm, numberVotes, quorum);
            return;
        }

        state = NodeState.Leader;
        partition.Leader = RaftManager.LocalEndpoint;

        Console.WriteLine("[{0}/{1}] Received vote from {2} and proclamed leader Term={3} Votes={4} Quorum={5}", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, voteTerm, numberVotes, quorum);

        SendHearthbeat();
    }

    private void AppendLogs(string endpoint, long leaderTerm, List<RaftLog>? logs)
    {
        if (term > leaderTerm)
        {
            //logger.LogWarning("[{LocalEndpoint}/{PartitionId}] Received logs from a leader {Endopoint} with old Term={LeaderTerm}. Ignoring...", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, leaderTerm);
            return;
        }

        if (leaderTerm >= term)
        {
            if (logs is not null)
                walActor.Send(new(RaftWALActionType.Update, term, logs));

            state = NodeState.Follower;
            lastHeartbeat = GetCurrentTime();
            term = leaderTerm;

            if (partition.Leader != endpoint)
            {
                //logger.LogInformation("[{LocalEndpoint}/{PartitionId}] Leader is now {Endpoint} LeaderTerm={LeaderTerm}", RaftManager.LocalEndpoint, partition.PartitionId, endpoint, leaderTerm);
                partition.Leader = endpoint;
            }
        }
    }

    private void ReplicateLogs(List<RaftLog>? logs)
    {
        if (logs is null)
            return;

        if (state != NodeState.Leader)
            return;

        walActor.Send(new(RaftWALActionType.Append, term, logs));
    }

    private void ReplicateCheckpoint()
    {
        if (state != NodeState.Leader)
            return;

        walActor.Send(new(RaftWALActionType.AppendCheckpoint, term));
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

    private static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}