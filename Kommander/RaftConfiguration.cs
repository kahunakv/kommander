
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
    /// URL scheme prepended to peer endpoints when opening gRPC channels.
    /// Override to <c>"http://"</c> in test environments that use cleartext HTTP/2
    /// (requires <c>SocketsHttpHandler.Http2UnencryptedSupport</c> to be enabled).
    /// </summary>
    public string GrpcScheme { get; set; } = "https://";

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

    // ── gRPC channel pool ─────────────────────────────────────────────────────

    /// <summary>
    /// Number of pooled gRPC channels (and matching streaming calls) created per peer URL.
    /// Each channel owns its own <c>SocketsHttpHandler</c> and therefore its own TCP/HTTP2
    /// connection, so effective per-peer concurrency is approximately
    /// <c>GrpcChannelsPerNode × MaxConcurrentStreams</c>
    /// (× additional connections per channel when <see cref="GrpcEnableMultipleHttp2Connections"/>
    /// is <see langword="true"/>).
    /// Values below 1 are clamped to 1; values above 64 are capped — each unit is a
    /// permanently-held connection and handler for the lifetime of the process.
    /// Default 4.
    /// </summary>
    public int GrpcChannelsPerNode { get; set; } = 4;

    /// <summary>
    /// When <see langword="true"/>, each pooled channel's <c>SocketsHttpHandler</c> may
    /// open multiple TCP/HTTP2 connections to the same peer instead of multiplexing all
    /// streams over one, raising the per-channel stream ceiling on saturated links.
    /// Default <see langword="false"/>.
    /// </summary>
    public bool GrpcEnableMultipleHttp2Connections { get; set; }

    private const int GrpcChannelsPerNodeMax = 64;

    // Warn at most once per process so repeated calls don't spam.
    // internal so tests can reset it between isolated assertion runs.
    internal static int grpcChannelsCapWarned;

    /// <summary>
    /// Returns <see cref="GrpcChannelsPerNode"/> clamped to [1, 64].  Values below 1 are
    /// raised to 1; values above 64 are capped because each unit is a permanently-held
    /// connection and <c>SocketsHttpHandler</c> for the process lifetime.  A one-time warning
    /// is emitted to <c>stderr</c> when the raw value exceeds the cap so misconfigured
    /// deployments are visible without requiring a structured logger.
    /// </summary>
    public int GetEffectiveGrpcChannelsPerNode()
    {
        int v = GrpcChannelsPerNode;

        if (v > GrpcChannelsPerNodeMax && Interlocked.CompareExchange(ref grpcChannelsCapWarned, 1, 0) == 0)
            Console.Error.WriteLine(
                $"[Kommander] GrpcChannelsPerNode={v} exceeds the maximum of {GrpcChannelsPerNodeMax}; " +
                $"clamped to {GrpcChannelsPerNodeMax}. Each pooled channel is a permanently-held connection and handler.");

        return Math.Clamp(v, 1, GrpcChannelsPerNodeMax);
    }

    // ── Bounded log backfill ──────────────────────────────────────────────────

    /// <summary>
    /// Number of committed entries a follower may trail the leader before the leader
    /// switches from empty heartbeats to active backfill on that follower.
    /// Lower values trigger backfill sooner; higher values tolerate more lag before
    /// kicking in. Default 10.
    /// </summary>
    public int BackfillThreshold { get; set; } = 10;

    /// <summary>
    /// Maximum number of committed entries shipped to a single stale follower per
    /// heartbeat interval. Bounds the per-round WAL read and network payload so that
    /// backfill cannot starve concurrent client writes. Default 128.
    /// </summary>
    public int MaxBackfillEntriesPerRound { get; set; } = 128;

    // ── Learner promotion ──────────────────────────────────────────────────────

    /// <summary>
    /// Maximum number of committed entries a learner may trail the leader on any partition
    /// and still be considered caught up for promotion purposes.
    /// The promotion driver checks this threshold on every <c>UpdateNodes</c> tick.
    /// Default 10.
    /// </summary>
    public int LearnerPromotionLag { get; set; } = 10;

    /// <summary>
    /// How long a learner must remain within <see cref="LearnerPromotionLag"/> entries of the
    /// leader on all partitions before the P0 leader promotes it to Voter.
    /// Prevents premature promotion of nodes that are briefly caught up but still unstable.
    /// Default 3 seconds.
    /// </summary>
    public TimeSpan LearnerPromotionStableWindow { get; set; } = TimeSpan.FromSeconds(3);

    // ── Gossip anti-entropy ───────────────────────────────────────────────────

    /// <summary>
    /// Interval between gossip rounds.  Each round contacts <see cref="GossipFanout"/>
    /// random peers and exchanges membership versions, pulling the current roster from
    /// any peer that holds a newer committed version.  Default 5 seconds.
    /// </summary>
    public TimeSpan GossipInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Number of random peers contacted per gossip round.  Higher values converge
    /// stale caches faster at the cost of additional network fan-out.
    /// Setting this to 0 disables gossip entirely.  Default 2.
    /// </summary>
    public int GossipFanout { get; set; } = 2;

    // ── SWIM failure detector ─────────────────────────────────────────────────

    /// <summary>
    /// How long to wait for a direct or indirect ping response before declaring a
    /// probe as timed out.  Smaller values detect failures faster at the cost of
    /// more false positives on slow networks.  Default 500 ms.
    /// </summary>
    public TimeSpan PingTimeout { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Number of random intermediary nodes used for indirect probing after a direct
    /// ping times out.  Reduces false positives caused by a single faulty path between
    /// the prober and the target.  Default 2.
    /// </summary>
    public int IndirectPingFanout { get; set; } = 2;

    /// <summary>
    /// How long a node can remain <c>Suspect</c> before being promoted to <c>Dead</c>.
    /// Should be large enough for a healthy node to refute suspicion via gossip before
    /// being declared dead.  Default 5 seconds.
    /// </summary>
    public TimeSpan SuspicionTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a <c>Dead</c> node must remain in that state before the P0 leader
    /// commits a <c>RemoveMember</c> entry.  A grace period absorbs transient partitions
    /// that resolve before the eviction log entry is applied.  Default 30 seconds.
    /// </summary>
    public TimeSpan DeadMemberEvictionGrace { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Interval between SWIM ping rounds.  Each round picks one random peer to probe.
    /// Setting this to zero or a negative value disables the failure detector entirely.
    /// <para>
    /// Default is <b>1 second</b>.  All built-in transports (InMemory, gRPC, REST) implement
    /// <see cref="ICommunication.SendPing"/> and <see cref="ICommunication.SendPingReq"/>,
    /// so the detector is active out of the box.  Set to <see cref="TimeSpan.Zero"/> to
    /// disable it entirely (e.g. in test clusters that control membership explicitly).
    /// </para>
    /// </summary>
    public TimeSpan PingInterval { get; set; } = TimeSpan.FromSeconds(1);

    // ── WAL compaction ────────────────────────────────────────────────────────

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
