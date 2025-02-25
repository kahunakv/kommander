
using Kommander.Data;
using Kommander.WAL;
using Kommander.Communication;
using Kommander.Time;

namespace Kommander;

public interface IRaft
{
    public IWAL WalAdapter { get; }
    
    public ICommunication Communication { get; }
    
    public RaftConfiguration Configuration { get; }

    public HybridLogicalClock HybridLogicalClock { get; }

    /// <summary>
    /// Event when the restore process starts
    /// </summary>
    public event Action? OnRestoreStarted;        

    /// <summary>
    /// Event when the restore process finishes
    /// </summary>
    public event Action? OnRestoreFinished;

    /// <summary>
    /// Event when a replication log is received
    /// </summary>
    public event Func<byte[], Task<bool>>? OnReplicationReceived;
    
    /// <summary>
    /// Joins the Raft cluster
    /// </summary>
    /// <returns></returns>
    public Task JoinCluster();

    /// <summary>
    /// Updates the active Raft cluster nodes
    /// </summary>
    /// <returns></returns>
    public Task UpdateNodes();

    /// <summary>
    /// Requests a votes from other nodes in the cluster
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request);

    /// <summary>
    /// Communicate a vote from a node in the cluster
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request);

    /// <summary>
    /// Append logs from the leader in a partition
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public Task<long> AppendLogs(AppendLogsRequest request);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    public Task ReplicateLogs(int partitionId, byte[] log);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="logs"></param>
    public Task ReplicateLogs(int partitionId, IEnumerable<byte[]> logs);

    /// <summary>
    /// Replicate a checkpoint to the followers in the partition    
    /// </summary>
    /// <param name="partitionId"></param>
    public void ReplicateCheckpoint(int partitionId);

    /// <summary>
    /// Obtains the local endpoint
    /// </summary>
    /// <returns></returns>
    public string GetLocalEndpoint();

    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    public ValueTask<bool> AmILeaderQuick(int partitionId);

    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    public ValueTask<bool> AmILeader(int partitionId);

    /// <summary>
    /// Waits for the local node to check/become the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    public ValueTask<string> WaitForLeader(int partitionId);
}