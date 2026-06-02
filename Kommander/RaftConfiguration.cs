
namespace Kommander;

/// <summary>
/// Raft configuration
/// </summary>
public class RaftConfiguration
{
    /// <summary>
    /// Unique name for the node in the cluster.
    /// </summary>
    public string? NodeName { get; set; }
    
    /// <summary>
    /// Unique identifier for the node in the cluster.
    /// </summary>
    public int NodeId { get; set; }
    
    /// <summary>
    /// Host to identify the node within the cluster
    /// </summary>
    public string? Host { get; set; }
    
    /// <summary>
    /// Port to bind for incoming connections
    /// </summary>
    public int Port { get; set; }

    /// <summary>
    /// Number of initial partitions to create
    /// </summary>
    public int InitialPartitions { get; set; } = 1;

    /// <summary>
    /// Communication scheme when sending HTTP requests
    /// </summary>
    public string? HttpScheme { get; set; } = "https://";

    /// <summary>
    /// Authorization bearer token for HTTP requests
    /// </summary>
    public string? HttpAuthBearerToken { get; set; } = "";
    
    /// <summary>
    /// Timeout for HTTP requests in seconds
    /// </summary>
    public int HttpTimeout { get; set; } = 5;
    
    /// <summary>
    /// HTTP version to use for requests
    /// </summary>
    public string? HttpVersion { get; set; } = "2.0";
    
    /// <summary>
    /// Interval to send pings to other nodes from the leader
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    
    /// <summary>
    /// If any partition sends a heartbeat to a node within this range, it will be considered as a recent heartbeat
    /// </summary>
    public TimeSpan RecentHeartbeat { get; set; } = TimeSpan.FromMilliseconds(100);
    
    /// <summary>
    /// Wait time for the leader to receive votes from other nodes
    /// </summary>
    public TimeSpan VotingTimeout { get; set; } = TimeSpan.FromMilliseconds(1500);
    
    /// <summary>
    /// Interval to perform leader election actions
    /// </summary>
    public TimeSpan CheckLeaderInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Initial delay before periodic Raft timers start firing.
    /// </summary>
    public TimeSpan TimerInitialDelay { get; set; } = TimeSpan.FromMilliseconds(2500);
    
    /// <summary>
    /// Interval to report liveness to the node registry
    /// </summary>
    public TimeSpan UpdateNodesInterval { get; set; } = TimeSpan.FromMilliseconds(5000);

    /// <summary>
    /// If followers hadn't received a heartbeat from the leader in this time, they will start an election
    /// </summary>
    public int StartElectionTimeout { get; set; } = 2000;
    
    /// <summary>
    /// If followers hadn't received a heartbeat from the leader in this time, they will start an election
    /// </summary>
    public int EndElectionTimeout { get; set; } = 4000;
    
    /// <summary>
    /// Increment election timeout by this value every time the node couldn't find quorum
    /// </summary>
    public int StartElectionTimeoutIncrement { get; set; } = 100;
    
    /// <summary>
    /// Increment election timeout by this value every time the node couldn't find quorum
    /// </summary>
    public int EndElectionTimeoutIncrement { get; set; } = 200;
    
    /// <summary>
    /// If the per-partition raft state machine takes more than this value to process a message it will show a log
    /// Slow processing of messages might indicate a performance issue
    /// </summary>
    public int SlowRaftStateMachineLog { get; set; } = 50;
    
    /// <summary>
    /// If the per-partition raft state machine takes more than this value to process a message it will show a log
    /// Slow processing of messages might indicate a performance issue
    /// </summary>
    public int SlowRaftWALMachineLog { get; set; } = 25;
    
    /// <summary>
    /// Number of background threads used for I/O read operations
    /// </summary>
    public int ReadIOThreads { get; set; } = 8;
    
    /// <summary>
    /// Number of background threads used for I/O write operations
    /// </summary>
    public int WriteIOThreads { get; set; } = 4;

    /// <summary>
    /// Committed operations between automatic WAL compaction triggers per partition.
    /// Set to 0 or a negative value to disable automatic compaction.
    /// </summary>
    public int CompactEveryOperations { get; set; } = 10000;
    
    /// <summary>
    /// Maximum number of log entries removed per <c>CompactLogsOlderThan</c> call during compaction.
    /// Values below 1 are treated as 1 at use time.
    /// </summary>
    public int CompactNumberEntries { get; set; } = 100;

    /// <summary>
    /// Upper bound on entries removed per triggered compaction pass before yielding.
    /// Must be greater than or equal to <see cref="CompactNumberEntries"/>.
    /// </summary>
    public int MaxEntriesPerCompaction { get; set; } = 5000;

    /// <summary>
    /// Returns <see cref="CompactNumberEntries"/> clamped to at least 1 when misconfigured.
    /// </summary>
    public int GetEffectiveCompactNumberEntries() =>
        CompactNumberEntries >= 1 ? CompactNumberEntries : 1;

    /// <summary>
    /// Returns <see cref="MaxEntriesPerCompaction"/> clamped to at least
    /// <see cref="GetEffectiveCompactNumberEntries"/> when misconfigured.
    /// </summary>
    public int GetEffectiveMaxEntriesPerCompaction()
    {
        int batchSize = GetEffectiveCompactNumberEntries();
        return MaxEntriesPerCompaction >= batchSize
            ? MaxEntriesPerCompaction
            : batchSize;
    }
}
