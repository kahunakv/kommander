
using Nixie;

using Kommander.Data;
using Kommander.WAL;
using Kommander.Communication;
using Kommander.Discovery;
using Kommander.Time;
using IOThreadPool = Kommander.WAL.IO.ThreadPool;

namespace Kommander;

/// <summary>
/// Represents a Raft interface for distributed consensus and coordination.
/// </summary>
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
    /// Read I/O thread pool
    /// </summary>
    public IOThreadPool ReadThreadPool { get; }
    
    /// <summary>
    /// Write I/O thread pool
    /// </summary>
    public IOThreadPool WriteThreadPool { get; }
    
    /// <summary>
    /// Whether the Raft partitions are initialized or not
    /// </summary>
    public bool IsInitialized { get; }

    /// <summary>
    /// Event when the restore process starts
    /// </summary>
    public event Action<int>? OnRestoreStarted;        

    /// <summary>
    /// Event when the restore process finishes
    /// </summary>
    public event Action<int>? OnRestoreFinished;
    
    /// <summary>
    /// Event when a replication error occurs
    /// </summary>
    public event Action<int, RaftLog>? OnReplicationError;
    
    /// <summary>
    /// Event when a replication log is restored
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnLogRestored;

    /// <summary>
    /// Event when a replication log is received
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived;
    
    /// <summary>
    /// Event called when a leader is elected on certain partition
    /// </summary>
    public event Func<int, string, Task<bool>>? OnLeaderChanged;
    
    /// <summary>
    /// Joins the Raft cluster
    /// </summary>
    /// <returns></returns>
    public Task JoinCluster();

    /// <summary>
    /// Leaves the Raft cluster
    /// </summary>
    /// <param name="disposeActorSystem"></param>
    /// <returns></returns>
    public Task LeaveCluster(bool disposeActorSystem = false);

    /// <summary>
    /// Updates the active Raft cluster nodes
    /// </summary>
    /// <returns></returns>
    public Task UpdateNodes();

    /// <summary>
    /// Returns all the visible nodes by the local node
    /// </summary>
    /// <returns></returns>
    public IList<RaftNode> GetNodes();

    /// <summary>
    /// Passes the Handshake to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public Task Handshake(HandshakeRequest request);

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
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replicate logs to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replicate a checkpoint to the followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Commit logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId);

    /// <summary>
    /// Rollback logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId);

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

    /// <summary>
    /// Returns the correct partition id according to a prefix key
    /// </summary>
    /// <param name="prefixPartitionKey"></param>
    /// <returns></returns>
    public int GetPrefixPartitionKey(string prefixPartitionKey);
}