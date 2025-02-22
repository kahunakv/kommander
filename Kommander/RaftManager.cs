
using System.Diagnostics;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.WAL;
using Nixie;

namespace Kommander;

/// <summary>
/// Manages the Raft cluster.
/// </summary>
public sealed class RaftManager
{
    internal readonly string LocalEndpoint;

    private readonly ActorSystem actorSystem;

    private readonly RaftConfiguration configuration;
    
    private readonly IWAL walAdapter;

    private readonly ICommunication communication;
    
    private readonly ClusterHandler clusterHandler;

    private readonly RaftPartition?[] partitions;

    internal List<RaftNode> Nodes { get; set; } = [];

    public IWAL WalAdapter => walAdapter;
    
    public ICommunication Communication => communication;
    
    public RaftConfiguration Configuration => configuration;

    public event Action? OnRestoreStarted;

    public event Action? OnRestoreFinished;

    public event Func<string, Task<bool>>? OnReplicationReceived;
    
    public RaftManager(ActorSystem actorSystem, RaftConfiguration configuration, IDiscovery discovery, IWAL walAdapter, ICommunication communication)
    {
        this.actorSystem = actorSystem;
        this.configuration = configuration;
        this.walAdapter = walAdapter;
        this.communication = communication;
        
        LocalEndpoint = string.Concat(configuration.Host, ":", configuration.Port);
        
        partitions = new RaftPartition[configuration.MaxPartitions];

        clusterHandler = new(this, discovery);
    }
    
    public async Task JoinCluster()
    {
        if (partitions[0] is null)
        {
            for (int i = 0; i < configuration.MaxPartitions; i++)
                partitions[i] = new(actorSystem, this, walAdapter, communication, i);
        }

        await clusterHandler.JoinCluster(configuration);
    }

    public async Task UpdateNodes()
    {
        if (partitions[0] is null)
            return;

        await clusterHandler.UpdateNodes();
    }

    private RaftPartition GetPartition(int partition)
    {
        if (partitions[partition] is null)
            throw new RaftException("It has not yet joined the cluster.");

        if (partition < 0 || partition >= partitions.Length)
            throw new RaftException("Invalid partition.");

        return partitions[partition]!;
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

    public async Task<long> AppendLogs(AppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);
        return await partition.AppendLogs(request);
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
            Console.WriteLine("AmILeaderQuick: {0}", e.Message);
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
                Console.WriteLine("AmILeader: {0}", e.Message);
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
                    await Task.Delay(500 + Random.Shared.Next(-50, 50));
                    continue;
                }

                return partition.Leader;
            }
            catch (AskTimeoutException e)
            {
                Console.WriteLine("WaitForLeader: {0}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }
    
    public int GetPartitionKey(string partitionKey)
    {
        return HashUtils.ConsistentHash(partitionKey, configuration.MaxPartitions);
    }

    internal static long GetCurrentTime()
    {
        return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds();
    }
}