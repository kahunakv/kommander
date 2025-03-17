
using Kommander.Data;
using Kommander.WAL;
using Kommander.Communication;
using Kommander.Discovery;
using Kommander.Time;
using Nixie;

namespace Kommander;

public interface IRaft
{
    /// <summary>
    /// Whether the node has joined the Raft cluster
    /// </summary>
    public bool Joined { get; }
    
    /// <summary>
    /// Current actor system
    /// </summary>
    public ActorSystem ActorSystem { get; }
    
    /// <summary>
    /// Current WAL adapter
    /// </summary>
    public IWAL WalAdapter { get; }
    
    /// <summary>
    /// Current Communication adapter
    /// </summary>
    public ICommunication Communication { get; }
    
    /// <summary>
    /// Current Discovery adapter
    /// </summary>
    public IDiscovery Discovery { get; }
    
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
    /// Event when a replication error occurs
    /// </summary>
    public event Action<RaftLog>? OnReplicationError;
    
    /// <summary>
    /// Event when a replication log is restored
    /// </summary>
    public event Func<RaftLog, Task<bool>>? OnReplicationRestored;

    /// <summary>
    /// Event when a replication log is received
    /// </summary>
    public event Func<RaftLog, Task<bool>>? OnReplicationReceived;
    
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
    /// Passes the Handshake to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void Handshake(HandshakeRequest request);

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
    public void AppendLogs(AppendLogsRequest request);
    
    /// <summary>
    /// Report a complete logs operation to the leader
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true);

    /// <summary>
    /// Replicate a checkpoint to the followers in the partition    
    /// </summary>
    /// <param name="partitionId"></param>
    public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId);
    
    /// <summary>
    /// Commit logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId);

    /// <summary>
    /// Obtains the local endpoint
    /// </summary>
    /// <returns></returns>
    public string GetLocalEndpoint();
    
    /// <summary>
    /// Obtains the local node id
    /// </summary>
    /// <returns></returns>
    public string GetLocalNodeId();

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
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken);

    /// <summary>
    /// Waits for the local node to check/become the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the correct partition id according to the partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPartitionKey(string partitionKey);
}