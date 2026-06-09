
namespace Kommander;

/// <summary>
/// Raft configuration
/// </summary>
public class RaftConfiguration
{
    private RaftTransportSecurityOptions transportSecurity = new();

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
    /// Transport security and node authentication settings for network transports.
    /// </summary>
    public RaftTransportSecurityOptions TransportSecurity
    {
        get => transportSecurity;
        set => transportSecurity = value ?? new();
    }

    /// <summary>
    /// Legacy bearer token for HTTP requests. Prefer <see cref="TransportSecurity"/>
    /// and <see cref="RaftTransportSecurityOptions.SharedSecret"/> for node
    /// authentication configuration.
    /// </summary>
    [Obsolete("Use TransportSecurity.SharedSecret or other TransportSecurity settings instead.")]
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
    /// When set, election timeouts are derived from <c>new Random(ElectionTimeoutSeed ^ partitionId)</c>
    /// instead of <see cref="Random.Shared"/>, making leader election fully reproducible for a given
    /// seed value. Each partition gets a distinct but deterministic random sequence so nodes converge
    /// on a single leader without deadlocking. Leave <c>null</c> (the default) for normal randomised
    /// behaviour in production.
    /// </summary>
    public int? ElectionTimeoutSeed { get; set; }

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

    // ── Backpressure and admission control ────────────────────────────────────

    /// <summary>
    /// Maximum number of client proposals that may be queued in a partition executor
    /// at one time (pending + in-drain). When this limit is reached, new proposals are
    /// rejected with <see cref="Data.RaftOperationStatus.ProposalQueueFull"/> so client
    /// traffic cannot consume unbounded memory or starve control-plane operations.
    /// Set to 0 or a negative value to disable the per-partition limit (not recommended
    /// in production; leaves the queue unbounded).
    /// </summary>
    public int MaxQueuedClientProposalsPerPartition { get; set; } = 2048;

    /// <summary>
    /// Per-partition pending write queue depth limit for the WAL scheduler.
    /// When a partition's queue reaches this depth the WAL scheduler throws
    /// <see cref="WAL.IO.BackpressureExceededException"/> so WAL write pressure
    /// propagates back to the Raft state machine before log memory grows unbounded.
    /// </summary>
    public int MaxWalQueueDepthPerPartition { get; set; } = 4096;

    /// <summary>
    /// Global pending write queue depth limit across all partitions in the WAL scheduler.
    /// Bounds the total aggregate WAL memory independent of partition count.
    /// Set to 0 to disable the global cap (only per-partition limits apply).
    /// </summary>
    public int MaxGlobalWalQueueDepth { get; set; }

    /// <summary>
    /// Maximum number of WAL write operations batched into a single storage flush.
    /// Larger batches reduce per-call overhead at the cost of higher individual latency.
    /// </summary>
    public int MaxWalBatchSize { get; set; } = 256;

    // ── Partition executor drain quanta ───────────────────────────────────────

    /// <summary>
    /// Maximum number of control-plane operations (heartbeats, votes, handshakes)
    /// drained per executor wake cycle. Higher values give control traffic more
    /// throughput advantage over lower-priority classes.
    /// </summary>
    public int MaxDrainQuantumControl { get; set; } = (int)Scheduling.RaftOperationPriority.Control;

    /// <summary>
    /// Maximum number of replication operations (AppendLogs, CompleteAppendLogs, WAL
    /// completions) drained per executor wake cycle.
    /// </summary>
    public int MaxDrainQuantumReplication { get; set; } = (int)Scheduling.RaftOperationPriority.Replication;

    /// <summary>
    /// Maximum number of client operations (proposals, commits, rollbacks, reads)
    /// drained per executor wake cycle.  Keep this lower than the replication quantum
    /// to avoid client floods delaying leader-to-follower replication.
    /// </summary>
    public int MaxDrainQuantumClient { get; set; } = (int)Scheduling.RaftOperationPriority.Client;

    /// <summary>
    /// Maximum number of maintenance operations (restore, compaction, discovery)
    /// drained per executor wake cycle.
    /// </summary>
    public int MaxDrainQuantumMaintenance { get; set; } = (int)Scheduling.RaftOperationPriority.Maintenance;

    /// <remarks>
    /// Control queue reserve: the control queue is unbounded by design, so no explicit
    /// reserve configuration is required.  The weighted-drain scheme guarantees
    /// control traffic always drains first within each cycle (weight 8 vs client weight 2),
    /// and only the client queue is capacity-limited.  This structural separation makes
    /// a separate "reserve" concept unnecessary.
    /// </remarks>

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

    /// <summary>
    /// Returns transport security settings with legacy compatibility values applied.
    /// </summary>
    public RaftTransportSecurityOptions GetEffectiveTransportSecurity()
    {
        string? sharedSecret = string.IsNullOrWhiteSpace(TransportSecurity.SharedSecret)
            ? HttpAuthBearerToken
            : TransportSecurity.SharedSecret;

        return new RaftTransportSecurityOptions
        {
            NodeAuthenticationMode = TransportSecurity.NodeAuthenticationMode,
            SharedSecret = string.IsNullOrWhiteSpace(sharedSecret) ? null : sharedSecret,
            HeaderName = TransportSecurity.HeaderName,
            RequireTls = TransportSecurity.RequireTls,
            AllowInsecureCertificateValidation = TransportSecurity.AllowInsecureCertificateValidation,
            AllowedClockSkew = TransportSecurity.AllowedClockSkew,
            TrustedServerCertificateThumbprints = TransportSecurity.TrustedServerCertificateThumbprints,
            TrustedClientCertificateThumbprints = TransportSecurity.TrustedClientCertificateThumbprints
        };
    }
}
