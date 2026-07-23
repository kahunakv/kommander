
namespace Kommander;

/// <summary>
/// Complete configuration for a Kommander Raft node.
///
/// <para>Pass an instance to <see cref="RaftManager"/> at construction time.
/// All properties have production-safe defaults; the minimum required fields are
/// <see cref="Host"/>, <see cref="Port"/>, and either <see cref="NodeId"/> or
/// <see cref="NodeName"/> (from which an id is derived).</para>
///
/// <para>Properties are grouped by subsystem:</para>
/// <list type="bullet">
///   <item>Node identity — <see cref="NodeName"/>, <see cref="NodeId"/>, <see cref="Host"/>, <see cref="Port"/>.</item>
///   <item>Transport — <see cref="GrpcScheme"/>, <see cref="HttpScheme"/>, <see cref="HttpTimeout"/>,
///       <see cref="HttpVersion"/>, <see cref="TransportSecurity"/>.</item>
///   <item>Raft timing — <see cref="HeartbeatInterval"/>, <see cref="StartElectionTimeout"/>,
///       <see cref="EndElectionTimeout"/>, <see cref="VotingTimeout"/>, and related fields.</item>
///   <item>WAL scheduler — <see cref="MaxWalQueueDepthPerPartition"/>, <see cref="MaxWalBatchSize"/>,
///       <see cref="MaxWalGroupBatchPartitions"/>, etc.</item>
///   <item>gRPC channel pool — <see cref="GrpcChannelsPerNode"/>, <see cref="GrpcEnableMultipleHttp2Connections"/>.</item>
///   <item>Quiescence — <see cref="EnableQuiescence"/>, <see cref="QuiesceAfter"/>.</item>
///   <item>SWIM failure detector — <see cref="PingInterval"/>, <see cref="PingTimeout"/>, <see cref="SuspicionTimeout"/>.</item>
///   <item>Gossip — <see cref="GossipInterval"/>, <see cref="GossipFanout"/>.</item>
///   <item>WAL compaction — <see cref="CompactEveryOperations"/>, <see cref="CompactNumberEntries"/>, <see cref="MaxEntriesPerCompaction"/>.</item>
/// </list>
///
/// <para>Call <see cref="Validate"/> before starting the node to catch invariant
/// violations (e.g., quiescence requires an active SWIM detector).</para>
/// </summary>
public class RaftConfiguration
{
    private RaftTransportSecurityOptions transportSecurity = new();

    // ── Node identity ─────────────────────────────────────────────────────────

    /// <summary>
    /// Human-readable name for this node, used in log messages and as a seed for
    /// <see cref="NodeId"/> when <see cref="NodeId"/> is not set.
    /// If left <see langword="null"/> or empty, <see cref="Environment.MachineName"/>
    /// is used.  Does not need to be globally unique; <see cref="NodeId"/> is the
    /// authoritative identity.
    /// </summary>
    public string? NodeName { get; set; }

    /// <summary>
    /// Numeric identity for this node, unique within the cluster.
    /// Used in vote requests, append-entries RPCs, and membership log entries.
    /// When set to 0 or a negative value, the runtime derives the id automatically
    /// as a stable hash of <see cref="NodeName"/> (or <see cref="Environment.MachineName"/>).
    /// Set an explicit positive value when you need a deterministic id independent
    /// of the machine name.
    /// </summary>
    public int NodeId { get; set; }

    /// <summary>
    /// Hostname or IP address advertised to peer nodes for inbound gRPC and HTTP
    /// connections.  Must be reachable by all other cluster members.
    /// Example: <c>"node1.internal"</c> or <c>"10.0.0.1"</c>.
    /// </summary>
    public string? Host { get; set; }

    /// <summary>
    /// TCP port the node listens on for gRPC (and optionally REST) connections.
    /// All peer nodes must be able to reach this port.
    /// </summary>
    public int Port { get; set; }

    /// <summary>
    /// Number of data partitions created during cluster bootstrap (first-ever
    /// leader election on partition 0).  Partition 0 (the system partition) is
    /// always present and is not counted here.  The value is written into the
    /// committed partition map and ignored on subsequent restarts.
    /// Default 1.
    /// </summary>
    public int InitialPartitions { get; set; } = 1;

    // ── Transport ─────────────────────────────────────────────────────────────

    /// <summary>
    /// URL scheme prepended to peer endpoints when the REST transport sends HTTP
    /// requests (e.g. <c>"https://"</c> or <c>"http://"</c>).
    /// This field is validated against <see cref="RaftTransportSecurityOptions.RequireTls"/>:
    /// if TLS is required but the scheme is plain HTTP a startup error is thrown.
    /// For gRPC connections, use <see cref="GrpcScheme"/> instead.
    /// Default <c>"https://"</c>.
    /// </summary>
    public string? HttpScheme { get; set; } = "https://";

    /// <summary>
    /// URL scheme prepended to peer endpoints when opening gRPC channels.
    /// Override to <c>"http://"</c> in test environments that use cleartext HTTP/2
    /// (requires <c>SocketsHttpHandler.Http2UnencryptedSupport</c> to be enabled).
    /// Default <c>"https://"</c>.
    /// </summary>
    public string GrpcScheme { get; set; } = "https://";

    /// <summary>
    /// Transport security and node authentication settings for network transports.
    /// Controls TLS requirements, shared-secret bearer tokens, certificate
    /// thumbprint pinning, and allowed clock skew for HMAC-signed requests.
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
    /// Timeout in seconds applied to outbound REST HTTP requests.
    /// Tune in proportion to your network round-trip time; the default (5 s)
    /// suits LAN clusters.  gRPC calls use stream-level deadlines set by the
    /// caller rather than this global timeout.
    /// Default 5.
    /// </summary>
    public int HttpTimeout { get; set; } = 5;

    /// <summary>
    /// HTTP protocol version string sent on REST transport requests
    /// (e.g. <c>"2.0"</c> for HTTP/2, <c>"1.1"</c> for HTTP/1.1).
    /// Has no effect on gRPC channels, which always use HTTP/2.
    /// Default <c>"2.0"</c>.
    /// </summary>
    public string? HttpVersion { get; set; } = "2.0";

    // ── Raft timing ───────────────────────────────────────────────────────────

    /// <summary>
    /// How often the leader sends Raft AppendEntries heartbeats to each follower
    /// to maintain authority and reset their election timers.  Must be well below
    /// <see cref="StartElectionTimeout"/> — a common guideline is
    /// <c>HeartbeatInterval ≤ StartElectionTimeout / 5</c>.
    /// This is distinct from the SWIM <see cref="PingInterval"/>, which is used
    /// by the node-level failure detector independently of Raft partition state.
    /// Default 500 ms.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// De-duplication window for per-node heartbeat sends.  If the leader already
    /// sent a heartbeat to a specific follower within this duration, the current
    /// <c>CheckLeader</c> tick skips it.  This prevents redundant sends when
    /// <see cref="CheckLeaderInterval"/> fires more frequently than
    /// <see cref="HeartbeatInterval"/>.
    /// Default 100 ms.
    /// </summary>
    public TimeSpan RecentHeartbeat { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// How long a candidate waits after broadcasting RequestVote RPCs before
    /// checking whether a quorum of votes has arrived.  If quorum is not reached
    /// within this window the election round is abandoned and restarted with a
    /// new randomised timeout.
    /// Default 1500 ms.
    /// </summary>
    public TimeSpan VotingTimeout { get; set; } = TimeSpan.FromMilliseconds(1500);

    /// <summary>
    /// Interval at which the Raft timer service fires the <c>CheckLeader</c> pass —
    /// the heartbeat send loop (on leaders) and the election-timeout check (on
    /// followers).  Smaller values give tighter heartbeat jitter control but add
    /// timer overhead.
    /// Default 250 ms.
    /// </summary>
    public TimeSpan CheckLeaderInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Grace period after node startup before the periodic Raft and SWIM timers
    /// begin firing.  Allows the node time to restore WAL state and join the
    /// cluster before participating in elections.
    /// Default 2500 ms.
    /// </summary>
    public TimeSpan TimerInitialDelay { get; set; } = TimeSpan.FromMilliseconds(2500);

    /// <summary>
    /// Interval at which the timer service refreshes the in-memory membership view
    /// and triggers gossip exchange rounds.  This is also the cadence at which
    /// SWIM-triggered membership changes (additions, evictions) are reflected in the
    /// local routing tables.
    /// Default 5000 ms.
    /// </summary>
    public TimeSpan UpdateNodesInterval { get; set; } = TimeSpan.FromMilliseconds(5000);

    /// <summary>
    /// Lower bound of the randomised election timeout range (milliseconds).
    /// When a follower has not received a Raft heartbeat from a leader for a
    /// duration drawn from [<see cref="StartElectionTimeout"/>,
    /// <see cref="EndElectionTimeout"/>], it transitions to Candidate and starts
    /// a new election.  Randomisation staggers elections across nodes so multiple
    /// candidates do not collide repeatedly.
    /// Default 2000 ms.
    /// </summary>
    public int StartElectionTimeout { get; set; } = 2000;

    /// <summary>
    /// Upper bound of the randomised election timeout range (milliseconds).
    /// Must be greater than <see cref="StartElectionTimeout"/>.  A wider range
    /// reduces the probability of simultaneous elections in large clusters at the
    /// cost of higher worst-case failover latency.
    /// Default 4000 ms.
    /// </summary>
    public int EndElectionTimeout { get; set; } = 4000;

    /// <summary>
    /// Amount (milliseconds) added to <see cref="StartElectionTimeout"/> after
    /// each failed election round (no quorum reached).  Backing off the lower
    /// bound reduces contention when multiple nodes repeatedly collide as candidates.
    /// Default 100 ms.
    /// </summary>
    public int StartElectionTimeoutIncrement { get; set; } = 100;

    /// <summary>
    /// Amount (milliseconds) added to <see cref="EndElectionTimeout"/> after
    /// each failed election round.  Widens the randomisation range over successive
    /// failures, further reducing the chance of repeated simultaneous elections.
    /// Default 200 ms.
    /// </summary>
    public int EndElectionTimeoutIncrement { get; set; } = 200;

    /// <summary>
    /// When set, election timeouts are derived from a deterministic combination of this seed, the
    /// partition id, and the <b>local node id</b> (rather than <see cref="Random.Shared"/>), making
    /// leader election reproducible for a given seed. Mixing in the node id is essential: it gives each
    /// node in a partition its <b>own</b> sequence, so a symmetric split vote is broken by the differing
    /// per-node timeouts. Seeding on <c>seed ^ partitionId</c> alone would give every node in a partition
    /// the identical sequence — they would keep retrying at the same instant and never converge, the
    /// opposite of what randomisation is for. Leave <c>null</c> (the default) for normal randomised
    /// behaviour in production.
    /// </summary>
    public int? ElectionTimeoutSeed { get; set; }

    // ── Observability ─────────────────────────────────────────────────────────

    /// <summary>
    /// Warning threshold (milliseconds) for the per-partition state-machine executor.
    /// If processing a single operation takes longer than this value, a warning is
    /// emitted to the logger to help identify scheduling hot spots.
    /// Set to 0 to disable slow-processing warnings.
    /// Default 50 ms.
    /// </summary>
    public int SlowRaftStateMachineLog { get; set; } = 50;

    /// <summary>
    /// Intended warning threshold (milliseconds) for WAL write operations.
    /// Reserved for future use — this field is not yet wired into the WAL write path
    /// and has no effect at runtime.
    /// Default 25 ms.
    /// </summary>
    public int SlowRaftWALMachineLog { get; set; } = 25;

    // ── I/O thread pools ──────────────────────────────────────────────────────

    /// <summary>
    /// Number of background threads in the <see cref="WAL.IO.FairReadScheduler"/>
    /// pool used to serve WAL read requests (log replay, snapshot reads).
    /// Increase on storage-bound nodes or when read-heavy workloads (e.g. follower
    /// log reconstruction) saturate the default pool.
    /// Default 8.
    /// </summary>
    public int ReadIOThreads { get; set; } = 8;

    /// <summary>
    /// Number of worker threads in the <see cref="WAL.IO.FairWalScheduler"/> pool
    /// that flush WAL write batches to storage.  Each thread can process one
    /// cross-partition group-commit batch concurrently.  Values above 1 allow
    /// multiple batches to be in-flight simultaneously, but per-partition FIFO
    /// ordering is always preserved.
    /// Default 4.
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

    /// <summary>
    /// Desired number of SQLite shard databases across which partitions are distributed
    /// (<c>partitionId mod shardCount</c>).
    ///
    /// <para>This value is used <b>only</b> to seed a brand-new data directory; once a
    /// directory has been initialised the value persisted in its metadata DB is authoritative.
    /// A non-zero value that differs from the persisted one causes <see cref="WAL.SqliteWAL"/>
    /// to throw at startup (changing the shard count for an existing directory orphans all
    /// previously-written logs).  Use <c>0</c> (the default) to accept whatever value was
    /// written at directory creation time.</para>
    ///
    /// <para>Typical trade-offs:</para>
    /// <list type="bullet">
    ///   <item><c>1</c> — single shared database; maximum fsync amortization, all writes serialized.</item>
    ///   <item><see cref="Environment.ProcessorCount"/> (resolved when <c>0</c> seeds a new directory)
    ///       — balanced default: one shard per CPU thread, parallelism without per-partition overhead.</item>
    ///   <item>Large value — approaches the legacy one-file-per-partition layout with no amortization.</item>
    /// </list>
    ///
    /// Default <c>0</c> (resolved to <see cref="Environment.ProcessorCount"/> on first initialisation).
    /// </summary>
    public int SqliteWalShardCount { get; set; }

    /// <summary>
    /// Maximum number of partitions coalesced into a single <c>walAdapter.Write</c>
    /// call (cross-partition group commit).  When multiple partitions are ready
    /// simultaneously a worker drains up to this many of them and issues one
    /// storage write — for RocksDB this is one <c>db.Write</c> / one fsync
    /// regardless of partition count.
    ///
    /// <para><b>SQLite note:</b> SQLite stores each partition in a separate database
    /// file, so a group-commit batch still issues one transaction per partition file.
    /// If one partition's write fails the entire batch's status is set to
    /// <c>Errored</c>, which means partitions that committed successfully will
    /// receive a spurious error completion and their proposers will retry.  The
    /// upsert is idempotent (<c>ON CONFLICT DO UPDATE</c>) so retries are safe, but
    /// cross-partition failure propagation is wider than it was pre-D1.  RocksDB is
    /// not affected (atomic <c>WriteBatch</c>).</para>
    ///
    /// Defaults to <see cref="WAL.IO.FairWalScheduler.DefaultMaxGroupBatchPartitions"/> (64).
    /// </summary>
    public int MaxWalGroupBatchPartitions { get; set; } = WAL.IO.FairWalScheduler.DefaultMaxGroupBatchPartitions;

    /// <summary>
    /// Deferred group-commit linger window, in milliseconds. When greater than zero,
    /// a WAL worker that has dequeued its first ready partition will briefly wait to
    /// gather <em>more</em> ready partitions before issuing the single group fsync,
    /// rather than fsyncing whatever is ready at that instant
    /// (<see cref="WAL.IO.FairWalScheduler"/>'s default <em>opportunistic</em> batching).
    ///
    /// <para>This trades a bounded latency floor for denser batches. It pays off only
    /// because a single fsync is far more expensive (tens of ms) than the window: on
    /// paths where ready partitions arrive spread out — notably the follower append
    /// path, whose appends are paced by replication RPCs rather than arriving all at
    /// once like a leader's local proposes — coalescing them into one fsync removes
    /// real time from the propose quorum's critical path.</para>
    ///
    /// <para>The window is <b>adaptive</b>: the worker bails out the moment a probe
    /// finds no further partition arriving, so sequential / low-overlap load does not
    /// pay the full window. It is also bounded by <see cref="MaxWalGroupBatchPartitions"/>
    /// (a full batch fsyncs immediately).</para>
    ///
    /// <para>Defaults to <c>0</c> (disabled — behaviour is byte-for-byte the prior
    /// opportunistic batching). Enable and measure before relying on it.</para>
    /// </summary>
    public int WalGroupCommitLingerMs { get; set; }

    /// <summary>
    /// Single-fsync commit fast path. When enabled, an <c>autoCommit</c> single-round
    /// proposal releases its client ticket as soon as the <b>propose quorum is durable</b>
    /// — i.e. a quorum already holds the entry on disk — instead of waiting for the
    /// leader's own second (commit) fsync. The per-entry <c>Committed</c> record is still
    /// written afterward; only the moment the client is acknowledged moves earlier.
    ///
    /// <para>This removes one serial fsync from the client's critical path, which is what
    /// the set-p50 symptom reflects (a write blocked on two serial fsyncs of its own).
    /// It does <b>not</b> weaken durability: propose-quorum-durable is the true Raft commit
    /// point, so the load-bearing invariant "acked ⇒ durable on a quorum, and crash
    /// recovery yields the same committed prefix" is preserved. The commit frontier is
    /// reconstructed on restart from the last durable <c>CommittedCheckpoint</c> plus the
    /// leader's commit index on reconnect.</para>
    ///
    /// <para>Scope is strictly the <c>autoCommit</c> single-round path. The explicit
    /// two-phase (<c>!AutoCommit</c>) path keeps its separate durable commit, untouched.</para>
    ///
    /// <para><b>Defaults to <c>true</c>.</b> The cost of the fast path is that the on-disk
    /// entry type is no longer a reliable committed/uncommitted boundary after a crash: restore
    /// reconstructs a conservative lower bound (the contiguous committed prefix) and the true
    /// tail is re-supplied by the leader on reconnect, or re-committed once this node wins an
    /// election and commits a current-term entry. A node therefore serves a briefly truncated
    /// view of its own committed state between restore and re-supply. Set to <c>false</c> to get
    /// byte-for-byte the prior always-sync two-fsync behaviour, where the disk alone answers
    /// "what was committed".</para>
    ///
    /// <para>The flag must be set <b>uniformly across the cluster</b>: the regressed-frontier
    /// re-supply channel is a follower-reports / leader-reacts pair (see
    /// <c>RaftPartitionStateMachine.CompleteAppendLogsAsync</c>), and with the flag off a
    /// follower's heartbeat ack reports <c>-1</c> instead of its real frontier, leaving the
    /// channel half-wired.</para>
    /// </summary>
    public bool WalSingleFsyncCommit { get; set; } = true;

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

    /// <summary>
    /// When <see langword="true"/>, gRPC channels register gzip compression providers and
    /// <see cref="Communication.Grpc.GrpcCommunication.SendInstallSnapshot"/> requests gzip
    /// encoding on the unary snapshot RPC via the
    /// <c>grpc-internal-encoding-request</c> metadata header. The hot replication stream
    /// explicitly opts out of compression. Default <see langword="false"/>.
    /// </summary>
    public bool GrpcEnableSnapshotCompression { get; set; }

    /// <summary>
    /// When <see langword="true"/>, enables per-stream outbound <c>AppendLogs</c> coalescing:
    /// while a <c>WriteAsync</c> to a follower stream is in flight, subsequent
    /// <c>AppendLogs</c> items for that stream are queued and sent as a single
    /// <c>GrpcBatchRequestsRequest</c> with multiple items when the stream becomes free.
    /// <para>
    /// This collapses N semaphore acquisitions and N HTTP/2 frames into one under sustained
    /// write load, improving throughput without adding artificial latency — an isolated send
    /// (nothing queued) still goes out immediately as a batch of one.
    /// </para>
    /// <para>
    /// The coalescing is backpressure-driven: no <c>Task.Delay</c> is introduced. Items that
    /// naturally queue behind an in-flight write are batched; an idle stream is never delayed.
    /// The receiver already handles multi-item <c>GrpcBatchRequestsRequest</c> messages, so
    /// this is fully wire-compatible.
    /// </para>
    /// <para>
    /// Default <see langword="false"/>. Enable for write-heavy multi-partition workloads where
    /// per-peer send concurrency is the bottleneck. Has no effect when used with the in-memory
    /// transport.
    /// </para>
    /// </summary>
    public bool GrpcEnableAppendLogsCoalescing { get; set; }

    /// <summary>
    /// Maximum number of <c>AppendLogs</c> items the coalescing flusher drains into a
    /// single <c>GrpcBatchRequestsRequest</c> frame per write cycle.  If the queue contains
    /// more items than this cap, the flusher sends the first batch and immediately loops to
    /// drain the remainder in the next batch — keeping individual frames bounded.
    /// <para>
    /// The primary constraint is the receiver's <c>MaxReceiveMessageSize</c> (gRPC default
    /// 4 MB). A cap of 256 items matches <see cref="MaxWalBatchSize"/> and leaves ample
    /// headroom for typical log-entry sizes. Operators on very large entry payloads should
    /// reduce this value; operators on tiny entries may raise it.
    /// </para>
    /// <para>
    /// Only effective when <see cref="GrpcEnableAppendLogsCoalescing"/> is
    /// <see langword="true"/>. Default 256.
    /// </para>
    /// </summary>
    public int GrpcAppendLogsMaxCoalesceBatch { get; set; } = 256;

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

    // ── Shared executor pool ──────────────────────────────────────────────────

    /// <summary>
    /// When <see langword="true"/>, all partition executors share a bounded pool of
    /// <see cref="PartitionExecutorPoolSize"/> worker threads instead of each owning a
    /// dedicated OS thread.  This removes the approximately 1 MB-per-partition thread-stack
    /// ceiling and allows thousands of partitions to coexist on a small fixed thread count.
    ///
    /// <para>Per-partition serial execution (the single-owner invariant) is preserved by a
    /// per-partition run-lock: at most one pool thread drains a given partition at a time.
    /// Control-plane operations keep their weighted priority lane, so heartbeats and votes are
    /// never starved by client load on another partition.</para>
    ///
    /// <para>Set to <see langword="false"/> to restore the original one-thread-per-partition
    /// behaviour as an escape hatch.  Both settings are Raft-safe.
    /// Default <see langword="true"/>.</para>
    /// </summary>
    public bool EnableSharedExecutorPool { get; set; } = true;

    /// <summary>
    /// Number of worker threads in the shared executor pool when
    /// <see cref="EnableSharedExecutorPool"/> is <see langword="true"/>.
    /// <c>0</c> (the default) auto-sizes to <see cref="Environment.ProcessorCount"/>.
    /// Values below 0 are clamped to 1.  Setting this too low relative to the number of
    /// hot partitions will increase per-operation latency due to pool contention; setting it
    /// equal to the partition count recovers dedicated-thread behaviour at higher memory cost.
    /// </summary>
    public int PartitionExecutorPoolSize { get; set; }

    // ── Quiescence ───────────────────────────────────────────────────────────

    /// <summary>
    /// When <see langword="true"/>, idle partitions stop sending per-partition heartbeats
    /// after <see cref="QuiesceAfter"/> of inactivity, relying on SWIM node-level liveness
    /// to suppress follower elections.  Set to <see langword="false"/> to restore the
    /// original per-partition heartbeating on every interval, independent of idle time.
    /// Default <see langword="true"/>.
    /// </summary>
    public bool EnableQuiescence { get; set; } = true;

    /// <summary>
    /// How long a partition must remain idle (no proposals, no in-flight replication)
    /// before the leader quiesces it and suppresses further per-partition heartbeats.
    /// Approximately 3× <see cref="HeartbeatInterval"/> is a safe default (1500 ms).
    /// While quiesced, followers gate elections on SWIM node state rather than the
    /// per-partition heartbeat timer — see <see cref="EnableQuiescence"/>.
    /// </summary>
    public TimeSpan QuiesceAfter { get; set; } = TimeSpan.FromMilliseconds(1500);

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

    // ── Leader balancer ───────────────────────────────────────────────────────

    /// <summary>
    /// Master switch for the advisory leader-balancing feature.
    /// When <see langword="false"/> (the default), no load reports are built or gossiped,
    /// no transfer suggestions are sent, and the balancer loop never runs.
    /// Flip to <see langword="true"/> only after all nodes in the cluster are on a version
    /// that supports the feature; it is independently revertible at runtime via rolling restart.
    /// </summary>
    public bool EnableLeaderBalancer { get; set; }

    /// <summary>
    /// How often each node emits a <c>NodeLoadReport</c> on the gossip path.
    /// The P0 leader uses this cadence to refresh its global leadership view.
    /// Shorter intervals give fresher data at the cost of more gossip traffic.
    /// <para>Default is <b>5 seconds</b>.</para>
    /// </summary>
    public TimeSpan LeaderBalancerReportInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often the P0 leader runs a balancer planning pass, building a
    /// <c>GlobalLeadershipView</c> and dispatching any <c>LeaderMove</c> suggestions.
    /// Shorter intervals react faster but add coordinator overhead.
    /// <para>Default is <b>30 seconds</b>.</para>
    /// </summary>
    public TimeSpan LeaderBalancerInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum age of a <c>NodeLoadReport</c> before it is considered stale and excluded
    /// from the planning view.  Must be greater than <see cref="LeaderBalancerReportInterval"/>
    /// to tolerate at least one missed emission.
    /// <para>Default is <b>20 seconds</b>.</para>
    /// </summary>
    public TimeSpan LeaderBalancerReportTtl { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Minimum leader-count imbalance (above and below the ideal per-node count) that
    /// must exist before the count tier emits any moves.  A deadband of 1 means a node
    /// must be at least 2 above the natural ceiling before it is considered over-loaded.
    /// Prevents flip-flopping around perfectly-balanced distributions.
    /// <para>Default is <b>1</b>.</para>
    /// </summary>
    public int CountDeadband { get; set; } = 1;

    /// <summary>
    /// Fractional load skew threshold between the busiest and quietest live voter node
    /// that triggers load-tier swaps when the count distribution is already balanced.
    /// Skew is measured as <c>(maxLoad − minLoad) / maxLoad</c>.
    /// <para>Default is <b>0.25</b> (25 %).</para>
    /// </summary>
    public double LoadImbalanceThreshold { get; set; } = 0.25;

    /// <summary>
    /// How long a partition must remain on the same leader (ms) before it is eligible
    /// to be moved.  Prevents the balancer from immediately shuffling a partition that
    /// just won an election, giving the new leader time to stabilise.
    /// <para>Default is <b>5000 ms</b>.</para>
    /// </summary>
    public long MinLeaderStabilityMs { get; set; } = 5000;

    /// <summary>
    /// How long a partition is excluded from further moves after a transfer suggestion
    /// is confirmed or times out.  Prevents rapid oscillation on hot partitions.
    /// <para>Default is <b>60 seconds</b>.</para>
    /// </summary>
    public TimeSpan MoveCooldown { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maximum number of <c>LeaderMove</c> suggestions emitted in a single planner pass.
    /// Limits the blast radius of a bad planning decision and keeps the suggestion queue
    /// from growing unbounded during large imbalances.
    /// <para>Default is <b>4</b>.</para>
    /// </summary>
    public int MaxMovesPerPass { get; set; } = 4;

    /// <summary>
    /// Maximum number of in-flight transfer suggestions the P0 leader tracks simultaneously.
    /// Once this limit is reached no new moves are dispatched until outstanding ones confirm
    /// or time out.  Prevents the cluster from being in permanent leadership churn.
    /// <para>Default is <b>2</b>.</para>
    /// </summary>
    public int MaxConcurrentTransfers { get; set; } = 2;

    /// <summary>
    /// Weight of the EWMA ops/sec term in the composite per-partition load score:
    /// <c>Load = LeaderBalancerOpsWeight × OpsPerSecond + LeaderBalancerQueueWeight × QueueDepth</c>.
    /// Higher values make the balancer more sensitive to throughput differences between partitions.
    /// <para>Default is <b>1.0</b>.</para>
    /// </summary>
    public double LeaderBalancerOpsWeight { get; set; } = 1.0;

    /// <summary>
    /// Weight of the instantaneous queue-depth term in the composite per-partition load score.
    /// Higher values make the balancer more sensitive to pending-work backlog.
    /// <para>Default is <b>0.5</b>.</para>
    /// </summary>
    public double LeaderBalancerQueueWeight { get; set; } = 0.5;

    /// <summary>
    /// How long the P0 leader waits for a suggested move to be confirmed by the global view
    /// before declaring it dropped or failed and clearing it from the outstanding-move table.
    /// After timeout the partition is eligible again subject to <see cref="MoveCooldown"/>.
    /// <para>Default is <b>15 s</b>.</para>
    /// </summary>
    public TimeSpan SuggestionTimeout { get; set; } = TimeSpan.FromSeconds(15);

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
    /// Validates configuration invariants at startup and throws <see cref="RaftException"/>
    /// on any violation.
    /// <para>
    /// When <see cref="EnableQuiescence"/> is <see langword="true"/>, enforces two invariants:
    /// </para>
    /// <para>
    /// 1. <c>PingInterval &gt; 0</c>: quiescence relies on the SWIM failure detector to notice a
    /// dead leader node, because a quiesced follower stops watching the per-partition heartbeat
    /// timer. A non-positive <c>PingInterval</c> disables SWIM probing, so quiesced followers
    /// would never detect a dead leader and could never fail over — a liveness hazard.
    /// </para>
    /// <para>
    /// 2. <c>PingInterval &lt; StartElectionTimeout</c>: quiesced followers gate failover on
    /// SWIM <c>Suspect</c> (fires after approximately one <c>PingInterval</c>), so a
    /// <c>PingInterval</c> at or above <c>StartElectionTimeout</c> would make quiesced
    /// failover slower than normal election timeout — defeating the purpose of the reconciliation.
    /// </para>
    /// </summary>
    /// <exception cref="RaftException">Thrown when a timing invariant is violated.</exception>
    public void Validate()
    {
        if (EnableQuiescence && PingInterval <= TimeSpan.Zero)
            throw new RaftException(
                "[Kommander] EnableQuiescence=true requires SWIM to be enabled (PingInterval > 0). " +
                "Quiesced followers stop watching the per-partition heartbeat timer and detect a dead " +
                "leader only via the SWIM failure detector, so a non-positive PingInterval would leave " +
                "them unable to fail over. Set PingInterval > 0, or set EnableQuiescence=false.");

        if (EnableQuiescence && PingInterval.TotalMilliseconds >= StartElectionTimeout)
            throw new RaftException(
                $"[Kommander] EnableQuiescence=true requires PingInterval ({PingInterval.TotalMilliseconds} ms) " +
                $"< StartElectionTimeout ({StartElectionTimeout} ms). " +
                "Quiesced followers detect leader failure via SWIM Suspect (approximately one PingInterval), " +
                "so a PingInterval at or above StartElectionTimeout regresses quiesced failover latency. " +
                "Lower PingInterval, raise StartElectionTimeout, or set EnableQuiescence=false.");

        // PartitionExecutorPoolSize: no throw needed — the RaftExecutorPool constructor
        // clamps negative values to 1 and treats 0 as ProcessorCount.  This note keeps
        // validation self-documenting for readers who look here expecting to find all
        // range checks in one place.
        // Effective value: PartitionExecutorPoolSize > 0 ? PartitionExecutorPoolSize
        //                : PartitionExecutorPoolSize == 0 ? Environment.ProcessorCount : 1
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

    private volatile RaftTransportAuthenticator? _cachedAuthenticator;
    private readonly object _authenticatorLock = new();

    /// <summary>
    /// Returns a shared <see cref="RaftTransportAuthenticator"/> for this configuration's effective
    /// transport security, building it once and caching it.
    /// <para>
    /// The authenticator holds immutable signing state — the UTF-8-encoded shared secret and the
    /// SHA-256 replay-cache namespace derived from it — so reconstructing it per request (as every
    /// incoming/outgoing REST and unary gRPC auth path previously did) re-encoded and re-hashed the
    /// secret each time and also allocated a fresh effective-security options object via
    /// <see cref="GetEffectiveTransportSecurity"/>. Caching one instance removes that per-request cost;
    /// the shared static replay cache is unaffected (it lives on the type, not the instance).
    /// </para>
    /// <para><b>Live reconfiguration:</b> the cached authenticator captures the effective security at
    /// first use. If <see cref="TransportSecurity"/> (or the legacy bearer token) is mutated after the
    /// first auth operation, call <see cref="InvalidateTransportAuthenticator"/> to force a rebuild;
    /// there is intentionally no automatic invalidation, matching the "explicit and atomic replacement"
    /// contract for a security-sensitive object.</para>
    /// </summary>
    public RaftTransportAuthenticator GetTransportAuthenticator()
    {
        RaftTransportAuthenticator? cached = _cachedAuthenticator;
        if (cached is not null)
            return cached;

        lock (_authenticatorLock)
            return _cachedAuthenticator ??= new RaftTransportAuthenticator(GetEffectiveTransportSecurity());
    }

    /// <summary>
    /// Clears the cached <see cref="GetTransportAuthenticator"/> instance so the next call rebuilds it
    /// from the current <see cref="TransportSecurity"/>. Call after mutating transport-security settings.
    /// </summary>
    public void InvalidateTransportAuthenticator()
    {
        lock (_authenticatorLock)
            _cachedAuthenticator = null;
    }
}
