
using Kommander.Data;
using Kommander.WAL;
using Kommander.Communication;
using Kommander.Time;

namespace Kommander;

public interface IRaft
{
    /// <summary>
    /// Whether the node has joined the Raft cluster
    /// </summary>
    public bool Joined { get; }
    
    /// <summary>
    /// Current WAL adapter
    /// </summary>
    public IWAL WalAdapter { get; }
    
    /// <summary>
    /// Current Communication adapter
    /// </summary>
    public ICommunication Communication { get; }
    
    /// <summary>
    /// Current Raft configuration
    /// </summary>
    public RaftConfiguration Configuration { get; }

    /// <summary>
    /// Hybrid Logical Clock
    /// </summary>
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
    /// Leaves the Raft cluster
    /// </summary>
    /// <returns></returns>
    public Task LeaveCluster();

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
    public Task<(RaftOperationStatus, long)> AppendLogs(AppendLogsRequest request);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    public Task<(bool success, long commitLogId)> ReplicateLogs(int partitionId, byte[] log);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="logs"></param>
    public Task<(bool success, long commitLogId)> ReplicateLogs(int partitionId, IEnumerable<byte[]> logs);

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

    /// <summary>
    /// Returns the correct partition id according to the partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public long GetPartitionKey(string partitionKey);
}