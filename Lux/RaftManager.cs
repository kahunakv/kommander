
using System.Diagnostics;
using Lux.Data;
using Nixie;

namespace Lux;

public class RaftManager
{
    internal static readonly string LocalEndpoint = GetLocalEndpointFromEnv();

    private static string? localEndpoint;

    private readonly ActorSystem actorSystem;

    private readonly RaftPartition[] partitions = new RaftPartition[MaxPartitions];

    private const int MaxPartitions = 8;

    //internal ILogger<IRaft> Logger { get; }

    internal List<RaftNode> Nodes { get; set; } = [];

    internal ClusterHandler Cluster { get; }

    public ActorSystem ActorSystem => actorSystem;

    public event Action? OnRestoreStarted;

    public event Action? OnRestoreFinished;

    public event Func<string, Task<bool>>? OnReplicationReceived;
    
    public RaftManager(ActorSystem actorSystem)
    {
        this.actorSystem = actorSystem;

        Cluster = new(this);
    }
    
    public async Task JoinCluster()
    {
        if (partitions[0] is null)
        {
            for (int i = 0; i < MaxPartitions; i++)
                partitions[i] = new(actorSystem, this, i);
        }

        await Cluster.JoinCluster();
    }

    public async Task UpdateNodes()
    {
        if (partitions[0] is null)
            return;

        await Cluster.UpdateNodes();
    }

    private RaftPartition GetPartition(int partition)
    {
        if (partitions[0] is null)
            throw new RaftException("It has not yet joined the cluster.");

        if (partition < 0 || partition >= partitions.Length)
            throw new RaftException("Invalid partition.");

        return partitions[partition];
    }

    public void RequestVote(RequestVotesRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.RequestVote(request);
    }

    public void Vote(VoteRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.Vote(request);
    }

    public void AppendLogs(AppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        partition.AppendLogs(request);
    }

    public void ReplicateLogs(int partitionId, string message)
    {
        RaftPartition partition = GetPartition(partitionId);
        partition.ReplicateLogs(message);
    }

    public void ReplicateCheckpoint(int partitionId)
    {
        RaftPartition partition = GetPartition(partitionId);
        partition.ReplicateCheckpoint();
    }

    public void InvokeRestoreStarted()
    {
        OnRestoreStarted?.Invoke();
    }

    public void InvokeRestoreFinished()
    {
        OnRestoreFinished?.Invoke();
    }

    public async Task InvokeReplicationReceived(string? obj)
    {
        if (obj is null)
            return;

        if (OnReplicationReceived != null)
        {
            Func<string, Task<bool>> callback = OnReplicationReceived;
            await callback(obj);
        }
    }

    public string GetLocalEndpoint()
    {
        return LocalEndpoint;
    }

    public async ValueTask<bool> AmILeaderQuick(int partitionId)
    {
        if (partitions[0] is null)
            return false;

        RaftPartition partition = GetPartition(partitionId);

        if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
            return true;

        try
        {
            NodeState response = await partition.GetState();

            if (response == NodeState.Leader)
                return true;

            return false;
        }
        catch (AskTimeoutException e)
        {
            Console.WriteLine("AmILeaderQuick: {Error}", e.Message);
        }

        return false;
    }

    public async ValueTask<bool> AmILeader(int partitionId)
    {
        if (partitions[0] is null)
            return false;

        RaftPartition partition = GetPartition(partitionId);
        Stopwatch stopwatch = Stopwatch.StartNew();

        while (stopwatch.ElapsedMilliseconds < 60000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
                return true;

            try
            {
                NodeState response = await partition.GetState();

                if (response == NodeState.Leader)
                    return true;

                return false;
            }
            catch (AskTimeoutException e)
            {
                Console.WriteLine("AmILeader: {Error}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }

    public async ValueTask<string> WaitForLeader(int partitionId)
    {
        RaftPartition partition = GetPartition(partitionId);
        Stopwatch stopwatch = Stopwatch.StartNew();

        while (stopwatch.ElapsedMilliseconds < 60000)
        {
            if (!string.IsNullOrEmpty(partition.Leader))
                return partition.Leader;

            try
            {
                NodeState response = await partition.GetState();

                if (response == NodeState.Leader)
                    return LocalEndpoint;

                if (string.IsNullOrEmpty(partition.Leader))
                {
                    await Task.Delay(500 + System.Random.Shared.Next(-50, 50));
                    continue;
                }

                return partition.Leader;
            }
            catch (AskTimeoutException e)
            {
                Console.WriteLine("WaitForLeader: {Error}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }
    
    public int GetPartitionKey(string partitionKey)
    {
        return HashUtils.ConsistentHash(partitionKey, MaxPartitions);
    }

    private static string GetLocalEndpointFromEnv()
    {
        if (!string.IsNullOrEmpty(localEndpoint))
            return localEndpoint;

        localEndpoint = string.Concat(
            Environment.GetEnvironmentVariable("BACKEND_POD_IP") ?? "127.0.0.1",
            ":",
            Environment.GetEnvironmentVariable("BACKEND_POD_PORT") ?? "8004"
        );

        return localEndpoint;
    }

    internal static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}