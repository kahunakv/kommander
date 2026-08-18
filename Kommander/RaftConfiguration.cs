
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
    /// that flush WAL write batches to storage. Each thread can hold one
    /// cross-partition group-commit batch at a time; per-partition FIFO ordering
    /// is always preserved regardless of this value (a partition is only ever
    /// drained by one worker at a time).
    ///
    /// <para>Defaults to 4 because RocksDB coalesces concurrent sync writes for us:
    /// its write path elects a <em>write-group leader</em> that performs a single
    /// WAL append + fsync covering every writer that joined the group, so multiple
    /// in-flight <c>db.Write(sync)</c> calls ride the <em>same</em> flush rather than
    /// each forcing its own fsync. With a single worker, WAL flushes are fully
    /// serialized — each write (including its fsync) completes before the next
    /// begins — so there is never a second write in flight to join the group; the
    /// device sits idle through every fsync and throughput is fsync-latency-bound.
    /// Several workers keep multiple writes in flight, letting RocksDB's group
    /// commit overlap their fsync latency; benchmarks showed a single worker
    /// materially reduced write throughput versus 4. The scheduler's per-worker
    /// coalescing of ready partitions into one <c>db.Write</c> still applies on top
    /// of this, giving two layers of batching.</para>
    ///
    /// <para>4 balances write concurrency against write-group mutex contention on
    /// the shared WAL; raise it further only if benchmarks on your storage show
    /// additional overlap. This is safe for a shared-WAL backend precisely because
    /// group commit merges the concurrent writes — it does not multiply fsyncs.</para>
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

    /// <summary>
    /// Optional per-partition application-durability floor (see
    /// <see cref="IApplicationDurabilityProvider"/>). When set, restart replay widens down to the
    /// floor (committed entries above it are redelivered via <c>OnLogRestored</c> even when a
    /// checkpoint sits above them) and WAL compaction never truncates entries above the floor —
    /// on leaders and followers alike, including the startup window before the consumer's first
    /// flush tick. <see langword="null"/> keeps the checkpoint-anchored behavior unchanged.
    /// <para>
    /// Consumers that apply committed entries asynchronously (in-memory overlay plus a background
    /// flush) need this to avoid silently losing the flush-cadence window when a node is killed
    /// after a checkpoint lands but before the flush runs.
    /// </para>
    /// </summary>
    public IApplicationDurabilityProvider? ApplicationDurabilityProvider { get; set; }

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

    // ── Leadership barrier ───────────────────────────────────────────────────

    /// <summary>
    /// How long a newly elected leader waits for its promotion-barrier no-op to commit before
    /// giving up and reverting to Follower. The barrier is proposed when the winner inherits
    /// uncommitted prior-term entries; until it commits, leadership is not published
    /// (<c>AmILeader</c> stays false) but heartbeats flow, so rivals are suppressed. If the
    /// barrier cannot commit the quorum is effectively lost for writes anyway — reverting lets
    /// the election timeout pick a (possibly different) leader instead of leaving a node that
    /// heartbeats forever but never serves. Keep this comfortably above the expected quorum
    /// round-trip under load; too low a value causes needless election churn on a slow but
    /// healthy cluster. Default 10 s.
    /// </summary>
    public TimeSpan LeadershipBarrierTimeout { get; set; } = TimeSpan.FromSeconds(10);

    // ── Read-index leadership confirmation ───────────────────────────────────

    /// <summary>
    /// Maximum time a <c>ConfirmLeadershipAsync</c> (read-index) request may wait for the
    /// quorum ack round plus the applied-frontier catch-up before failing with a negative
    /// result. A minority-partitioned leader never collects the acks, so this bounds how
    /// long a quorum-confirmed read blocks before the caller can surface a retry/redirect —
    /// it mirrors the write path's behaviour rather than serving stale local state as an
    /// authoritative success. Keep it above one heartbeat round-trip under load; expiry is
    /// enforced from the leader tick, so effective granularity is
    /// <see cref="CheckLeaderInterval"/>. Default 2 s.
    /// </summary>
    public TimeSpan LeadershipConfirmationTimeout { get; set; } = TimeSpan.FromSeconds(2);

    // ── Check-quorum ─────────────────────────────────────────────────────────

    /// <summary>
    /// When <see langword="true"/>, a leader that has not heard a same-term append/heartbeat
    /// ack from a majority of voters for <see cref="HeartbeatInterval"/> ×
    /// <see cref="CheckQuorumIntervalMultiplier"/> steps down to Follower. This does not by
    /// itself make reads safe (a stale-read window remains — that is what
    /// <c>ConfirmLeadershipAsync</c> closes); it bounds how long a deposed or isolated leader
    /// lingers, so minority-side writes fail fast and clients stop hammering a dead-end node.
    /// Suspended while the leader is quiesced: a quiesced leader stops heartbeating by design,
    /// so an absence of acks proves nothing there. Off by default to preserve existing
    /// behaviour.
    /// </summary>
    public bool EnableCheckQuorum { get; set; }

    /// <summary>
    /// Number of heartbeat intervals without a majority of same-term acks after which a leader
    /// with <see cref="EnableCheckQuorum"/> steps down. Too low a value causes spurious
    /// step-downs when acks are merely delayed under load; the window restarts whenever a
    /// majority is heard from, on promotion, and on un-quiesce. Default 8.
    /// </summary>
    public int CheckQuorumIntervalMultiplier { get; set; } = 8;

    // ── Bounded log backfill ──────────────────────────────────────────────────

    /// <summary>
    /// Master switch for leader-driven log backfill on this node's partitions. When
    /// <see langword="false"/> the leader never ships catch-up batches and never falls back to a
    /// snapshot transfer for a lagging follower: peers converge only through the live
    /// propose/commit broadcast. Default <see langword="true"/>.
    /// <para>
    /// This exists because <see cref="BackfillThreshold"/> is <b>not</b> a disable switch — it gates
    /// one of three triggers, so a large threshold suppresses backfill only while writes are
    /// flowing, and leaks the moment the node goes idle. Consumers that legitimately want no
    /// catch-up traffic for a class of peers (witness/observer topologies whose peers never need
    /// real data, and whose commit progress does not depend on their catch-up) must set this rather
    /// than raising the threshold.
    /// </para>
    /// <para>
    /// Turning this off makes lagging followers permanently stale by design: nothing else re-ships
    /// entries they missed. Leave it on unless a consumer owns the peers' catch-up story.
    /// </para>
    /// </summary>
    public bool BackfillEnabled { get; set; } = true;

    /// <summary>
    /// Number of committed entries a follower may trail the leader before the leader
    /// switches from empty heartbeats to active backfill on that follower.
    /// Lower values trigger backfill sooner; higher values tolerate more lag before
    /// kicking in. Default 10.
    /// <para>
    /// <b>This paces an enabled backfill; it does not disable one.</b> It gates the actively-behind
    /// trigger (a follower trailing by more than this many entries) and the two per-ack fast-path
    /// re-supplies. It does <b>not</b> gate the other two heartbeat triggers: the idle-tail trigger
    /// (any gap ≥ 1 once live replication goes quiet, which exists precisely because empty
    /// heartbeats cannot heal a residual tail entry) and the crash-restart regression re-supply.
    /// Raising it to <see cref="int.MaxValue"/> therefore suppresses backfill only while writes are
    /// flowing — use <see cref="BackfillEnabled"/> to turn backfill off.
    /// </para>
    /// </summary>
    public int BackfillThreshold { get; set; } = 10;

    /// <summary>
    /// How long a leader stops sending backfill batches to a peer after that peer reports
    /// <see cref="Data.RaftOperationStatus.FollowerWalSaturated"/>. Default 1 s.
    /// </summary>
    /// <remarks>
    /// Without this the saturation status is information the leader never acts on, and
    /// answering a full queue is no better than throwing at it: the follower rejects exactly
    /// as fast as the leader re-sends, the queue never gets an interval in which to drain, and
    /// the follower does not converge even after the workload stops. A measured run showed an
    /// *idle*, fully healed cluster accelerating from ~1,000 to ~7,800 rejected batches per
    /// minute over three minutes, peaking at 2,365 in one second, while no replica advanced a
    /// single log index.
    ///
    /// Only entry-carrying backfill is suppressed. Heartbeats continue, because they are what
    /// hold leadership and suppressing them would trade a stalled follower for an election.
    /// The window wants to be long enough for the WAL queue to drain a meaningful part of
    /// <see cref="MaxWalQueueDepthPerPartition"/> and short enough not to delay a genuine
    /// catch-up; a second is roughly a heartbeat pair and errs toward the latter.
    /// </remarks>
    public TimeSpan FollowerSaturationBackoff { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum number of committed entries shipped to a single stale follower per
    /// heartbeat interval. Bounds the per-round WAL read and network payload so that
    /// backfill cannot starve concurrent client writes. Default 128.
    /// </summary>
    public int MaxBackfillEntriesPerRound { get; set; } = 128;

    // ── Snapshot receive session ──────────────────────────────────────────────

    /// <summary>
    /// How long an in-progress snapshot-receive session may sit idle (no new chunk) before the
    /// receiver expires and discards it, releasing its buffered bytes. Expiry is <b>lazy</b>: it runs
    /// when the next chunk arrives (or an explicit sweep), so an abandoned session's memory is bounded
    /// by the global caps below rather than by this TTL alone. Protects the production receive path from
    /// a leader that starts a transfer and vanishes mid-stream. Must be positive. Default 30 s.
    /// </summary>
    public TimeSpan SnapshotReceiveSessionTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Global cap on the number of concurrent snapshot-receive sessions across all partitions. When a new
    /// session would exceed the cap the receiver deterministically evicts the oldest (least-recently-active,
    /// then lowest composite key) sessions to make room, disposing their buffers. Bounds per-receiver
    /// session bookkeeping regardless of how many leaders race to send. Must be positive. Default 8.
    /// </summary>
    public int SnapshotMaxPendingSessions { get; set; } = 8;

    /// <summary>
    /// Global cap on total buffered snapshot bytes across all in-progress receive sessions. A chunk that
    /// would push the total past this bound triggers deterministic eviction of other sessions; if even then
    /// it cannot fit (a single session larger than the whole budget) that session is rejected and removed.
    /// This is the primary memory bound on the receive path. Must be positive. Default 512 MiB.
    /// </summary>
    public long SnapshotMaxPendingBytes { get; set; } = 512L * 1024 * 1024;

    /// <summary>
    /// When <see langword="true"/>, snapshot chunks whose new session-metadata fields
    /// (<c>LeaderTerm</c>/<c>LeaderEndpoint</c>/<c>LastIncludedTerm</c>) are zero/empty are treated as a
    /// legacy sender that pre-dates those fields and are accepted with a warning under the old behaviour.
    /// When <see langword="false"/> (the default) new senders are expected to populate all fields. This is
    /// a temporary compatibility switch to be removed in a later breaking release. Default
    /// <see langword="false"/>.
    /// </summary>
    public bool AllowLegacySnapshotSenders { get; set; }

    /// <summary>
    /// Maximum REST request body, in bytes, that will be read and digested before its signature has
    /// been verified. Larger bodies are refused with 413 and
    /// <see cref="RaftTransportAuthenticationStatus.RequestBodyTooLarge"/>. Default 32 MiB.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This bounds work an <b>unauthenticated</b> caller can cause: verifying a signature means
    /// reading the body first, so every byte below this ceiling is streamed and buffered on the
    /// strength of an unproven credential.
    /// </para>
    /// <para>
    /// The body is digested through a small pooled buffer, so exceeding this limit is not a
    /// memory-exhaustion risk on its own — the ceiling exists to bound the buffering the downstream
    /// handler requires (which spills to a temp file past its threshold), and to give operators a
    /// limit independent of the host's.
    /// </para>
    /// <para>
    /// The default sits just above ASP.NET Core's own ~28.6 MB request-size limit, so on a default
    /// host that limit binds first and this one changes nothing. It matters when the host's limit has
    /// been raised. Lower it to the largest legitimate signed Raft request for a tighter bound —
    /// remember that snapshot chunks travel base64-encoded over REST, so a 3 MiB chunk is ~4 MiB on
    /// the wire.
    /// </para>
    /// </remarks>
    public long MaxPreAuthRequestBodyBytes { get; set; } = 32L * 1024 * 1024;

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
    /// that resolve before the eviction log entry is applied.
    /// <para>
    /// Default 2 minutes. Eviction is an expensive one-way door — a member that misses the
    /// window must run the Join flow again — so the default must comfortably exceed a routine
    /// restart (image pull, cold storage open on a large store), not just a network blip.
    /// The original 30 s default was shorter than a cold RocksDB start and evicted nodes mid
    /// <c>docker restart</c>. The eviction path additionally re-probes the endpoint at commit
    /// time, but the grace is the first line of defense.
    /// </para>
    /// </summary>
    public TimeSpan DeadMemberEvictionGrace { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// When enabled (default), a node that discovers it has been removed from the committed
    /// roster — typically dead-member auto-eviction firing across a slow restart — automatically
    /// re-runs the Join flow against the remaining members instead of parking as
    /// <c>NotMember</c> forever (suppressed pre-votes on every partition, terminal errors to
    /// clients, no self-recovery). Suppressed during a graceful <c>LeaveCluster</c> and before
    /// the node has ever finished assembling (first-time joins own their admission loop).
    /// Disable it only if an operator workflow removes live nodes remotely and expects them to
    /// stay out while still running.
    /// </summary>
    public bool EnableAutoRejoin { get; set; } = true;

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
    /// When <see langword="false"/> (the default), no transfer suggestions are sent and the
    /// balancer loop never runs. Load-report gossip is governed separately by
    /// <see cref="LoadReportsEnabled"/> — enabling the balancer implies it, but placement and
    /// <see cref="EnableLoadReports"/> keep reports flowing without the balancer.
    /// Flip to <see langword="true"/> only after all nodes in the cluster are on a version
    /// that supports the feature; it is independently revertible at runtime via rolling restart.
    /// </summary>
    public bool EnableLeaderBalancer { get; set; }

    /// <summary>
    /// Explicitly enables the gossip exchange of <c>NodeLoadReport</c>s (each node's believed
    /// partition leaderships and load figures) independently of the leader balancer. The reports
    /// feed <see cref="IRaft.GetPartitionLeaderHint"/> and the remote-node load metrics
    /// (<see cref="IRaft.GetPartitionLogOpsPerSecond"/> and friends).
    /// <para>Rarely needed directly: report exchange is already implied by
    /// <see cref="EnableLeaderBalancer"/>, <see cref="EnablePlacementRebalancer"/>, or a global
    /// <see cref="ReplicationFactor"/> &gt; 0 (see <see cref="LoadReportsEnabled"/>). Set this
    /// when none of those apply but leader hints are still wanted — e.g. placement driven purely
    /// by per-range <c>SetReplicationFactorAsync</c> overrides with the global factor at 0.</para>
    /// </summary>
    public bool EnableLoadReports { get; set; }

    /// <summary>
    /// Effective switch for load-report gossip: reports are built, gossiped, and ingested when
    /// this is <see langword="true"/>. Derived — reports flow whenever anything that consumes
    /// them is on: the leader balancer, the placement rebalancer, a non-zero global replication
    /// factor (placement deployments route with leader hints), or the explicit
    /// <see cref="EnableLoadReports"/> opt-in. Previously the exchange was gated on
    /// <see cref="EnableLeaderBalancer"/> alone, which silently starved
    /// placement-without-balancing deployments of leader hints.
    /// </summary>
    public bool LoadReportsEnabled =>
        EnableLoadReports || EnableLeaderBalancer || EnablePlacementRebalancer || ReplicationFactor > 0;

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

    // ── Replica placement (replication factor) ────────────────────────────────

    /// <summary>
    /// Desired number of voter replicas per partition range. <b>0 (the default) means full
    /// replication</b> — every roster voter hosts every range, today's behavior. When &gt; 0,
    /// initial placement assigns each range a replica set of this size (spread evenly across
    /// voters) and quorum is computed per range over its voter replicas only. A cluster with
    /// fewer voters than the factor degrades to full replication with no loss of safety.
    /// <para>Even values give a fragile quorum (3-of-4 tolerates the same single failure as
    /// 2-of-3); prefer odd values.</para>
    /// </summary>
    public int ReplicationFactor { get; set; }

    /// <summary>
    /// Master switch for the continual replica-placement rebalancer on the P0 leader.
    /// When <see langword="false"/> (the default) no rebalancing moves are planned; in-flight
    /// replica transitions (learner promotion, removal completion) are still driven to
    /// completion so a crash mid-move can always converge. Initial placement at
    /// <see cref="ReplicationFactor"/> is applied regardless of this flag.
    /// </summary>
    public bool EnablePlacementRebalancer { get; set; }

    /// <summary>
    /// Effective switch for scheduling placement-controller passes: the placement timer runs
    /// whenever anything can create placement work — a non-zero global
    /// <see cref="ReplicationFactor"/> or the rebalancer flag. Derived rather than gated on
    /// <see cref="ReplicationFactor"/> &gt; 0 alone because a supported deployment runs the
    /// global factor at 0 with per-range <c>SetReplicationFactorAsync</c> overrides (see
    /// <see cref="EnableLoadReports"/>) — such a cluster still needs passes. Mirrors
    /// <see cref="LoadReportsEnabled"/>. Not gated on <see cref="EnablePlacementRebalancer"/>
    /// alone either: the pass drives in-flight transitions (learner promotion, removal
    /// completion) even with the rebalancer off, which is the documented contract of that flag.
    /// </summary>
    public bool PlacementPassEnabled => ReplicationFactor > 0 || EnablePlacementRebalancer;

    /// <summary>
    /// How often the P0 leader runs a placement-controller pass
    /// (<c>RunPlacementPass</c>: drive in-flight transitions, plan rebalancing moves).
    /// Deliberately independent of <see cref="LeaderBalancerInterval"/> — placement used to ride
    /// the leader balancer's timer, which meant no pass ever ran with
    /// <see cref="EnableLeaderBalancer"/> off. A relocation costs ~3 passes (add learner →
    /// observe caught up → promote after <see cref="LearnerPromotionStableWindow"/>) plus a trim
    /// pass, so this interval bounds convergence speed; an idle pass costs one P0-leadership
    /// check and a map load. Non-positive disables the timer (event-driven kicks still fire).
    /// <para>Default is <b>5 seconds</b>.</para>
    /// </summary>
    public TimeSpan PlacementPassInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of new replica moves (add or remove sequences) initiated per
    /// placement-controller pass, across all priorities. Bounds the blast radius of a bad plan.
    /// Keep at least <see cref="MaxConcurrentReplicaRepairs"/> +
    /// <see cref="MaxConcurrentReplicaTransfers"/>, otherwise it binds before the per-class
    /// budgets and silently starves repairs.
    /// <para>Default is <b>4</b> (= default repairs 3 + default transfers 1).</para>
    /// </summary>
    public int MaxReplicaMovesPerPass { get; set; } = 4;

    /// <summary>
    /// Maximum number of ranges with an in-flight transitional replica (Learner catching up or
    /// Removing awaiting final drop) initiated by priority-3 <b>balance</b> moves (cosmetic skew
    /// spreading) at any time. Durability work is budgeted separately by
    /// <see cref="MaxConcurrentReplicaRepairs"/> so a low setting here never rate-limits repair.
    /// Caps concurrent backfill/snapshot transfers so rebalancing never starves client traffic.
    /// <para>Default is <b>1</b>.</para>
    /// </summary>
    public int MaxConcurrentReplicaTransfers { get; set; } = 1;

    /// <summary>
    /// Maximum number of in-flight <b>repair</b> moves (priority-1 re-replication of
    /// under-replicated ranges and priority-2 shedding of replicas stranded on evicted nodes) at
    /// any time. Split from <see cref="MaxConcurrentReplicaTransfers"/> so restoring durability
    /// after a node loss is not serialized behind the cosmetic-balance budget of 1 — at the old
    /// shared budget, draining several nodes took ~2 minutes per replica, serialized.
    /// Any in-flight transitional replica counts against <i>both</i> budgets (a transfer consumes
    /// bandwidth regardless of why it started), so worst-case concurrent transitions are bounded
    /// by the larger of the two budgets, and balance moves pause entirely during a repair wave.
    /// <para>Default is <b>3</b>.</para>
    /// </summary>
    public int MaxConcurrentReplicaRepairs { get; set; } = 3;

    /// <summary>
    /// Upper bound on how long <see cref="RaftManager.RequestLeaveAsync"/> waits for the
    /// placement pass to evacuate this node's replicas after committing the
    /// <see cref="System.ClusterMemberRole.Leaving"/> roster role. On expiry the role is rolled
    /// back to Voter and the caller gets <see cref="Data.LeaveClusterOutcome.DrainTimedOut"/>
    /// (unless the pass finished first — removal wins). Size it from the data: an evacuation
    /// costs ~3 passes per range (add learner → observe caught up for
    /// <see cref="LearnerPromotionStableWindow"/> → promote, then trim), with up to
    /// <see cref="MaxConcurrentReplicaRepairs"/> ranges in flight — minutes for real data sets,
    /// which is why this is a config knob and not the 10 s commit deadline.
    /// <para>Default is <b>2 minutes</b>.</para>
    /// </summary>
    public TimeSpan DecommissionDrainTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Minimum per-node replica-count imbalance (above the even-spread ceiling) before the
    /// placement planner emits balancing moves. Prevents ping-ponging a replica between two
    /// nodes around a perfectly balanced distribution. Under-replicated ranges bypass the
    /// deadband — restoring the replication factor is always highest priority.
    /// <para>Default is <b>1</b>.</para>
    /// </summary>
    public int ReplicaCountDeadband { get; set; } = 1;

    /// <summary>
    /// Optional locality hint (zone/rack) for the local node. When set, it is gossiped on this
    /// node's load reports and the placement planner prefers spreading a range's replicas across
    /// distinct zones. Zone-aware spread therefore needs load-report gossip
    /// (<see cref="LoadReportsEnabled"/> — implied by placement) to see remote nodes' zones.
    /// Normalized by <see cref="Validate"/>: whitespace-only values become null (the planner
    /// treats empty as "no zone" anyway, so a stray <c>""</c> would otherwise silently disable
    /// the hint while looking configured).
    /// </summary>
    public string? Zone { get; set; }

    // ── WAL compaction ────────────────────────────────────────────────────────

    /// <summary>
    /// Committed operations between automatic WAL compaction triggers per partition.
    /// Set to 0 or a negative value to disable automatic compaction.
    /// <para>
    /// <b>This schedules passes; it does not by itself truncate anything.</b> A pass can only remove
    /// entries below the partition's last checkpoint, and the checkpoint advances only when the
    /// application calls <see cref="IRaft.ReplicateCheckpoint"/> (or when a snapshot is installed).
    /// Kommander does not checkpoint on a cadence of its own. A deployment that never checkpoints
    /// therefore never compacts, however low this value is set — the passes fire, find no floor, and
    /// return. Watch for the per-partition "compaction is running but has never had a checkpoint"
    /// line, or the <c>raft.wal.compaction_passes_total</c> metric with <c>outcome=no_checkpoint</c>.
    /// </para>
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
    /// Always enforces the heartbeat-vs-election-timeout relation: both
    /// <see cref="HeartbeatInterval"/> and <see cref="CheckLeaderInterval"/> (which gates when
    /// heartbeats are actually sent) must be below <see cref="StartElectionTimeout"/>, otherwise
    /// every partition livelocks in perpetual re-elections — a leader is elected but followers
    /// time out before its next heartbeat, so no leader ever holds and terms climb forever.
    /// </para>
    /// <para>
    /// When <see cref="EnableQuiescence"/> is <see langword="true"/>, additionally enforces two invariants:
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
        // Normalize the locality hint: the planner guards with IsNullOrEmpty, so a whitespace
        // zone would silently behave as "no zone" while looking configured. Trim it to the
        // canonical form here (null = no zone) so every consumer sees the same value.
        Zone = string.IsNullOrWhiteSpace(Zone) ? null : Zone.Trim();

        // A leader only sends heartbeats from the CheckLeader timer pass, so the effective
        // heartbeat cadence is max(HeartbeatInterval, CheckLeaderInterval). If either is at or
        // above StartElectionTimeout, followers are guaranteed to hit their election timeout
        // before the next heartbeat can arrive: every partition livelocks in perpetual
        // re-elections (a leader is elected, loses authority within one follower timeout, and
        // the term climbs forever). This is a silent total-availability failure — the cluster
        // appears to assemble (a leader exists at any given instant) but no leader ever holds,
        // so fail fast at startup instead.
        if (HeartbeatInterval.TotalMilliseconds >= StartElectionTimeout)
            throw new RaftException(
                $"[Kommander] HeartbeatInterval ({HeartbeatInterval.TotalMilliseconds} ms) must be well below " +
                $"StartElectionTimeout ({StartElectionTimeout} ms); a common guideline is HeartbeatInterval <= " +
                "StartElectionTimeout / 5. With HeartbeatInterval >= StartElectionTimeout, followers always reach " +
                "their election timeout before the leader's next heartbeat, so leadership churns indefinitely. " +
                "Lower HeartbeatInterval or raise StartElectionTimeout.");

        if (CheckLeaderInterval.TotalMilliseconds >= StartElectionTimeout)
            throw new RaftException(
                $"[Kommander] CheckLeaderInterval ({CheckLeaderInterval.TotalMilliseconds} ms) must be below " +
                $"StartElectionTimeout ({StartElectionTimeout} ms). Heartbeats are sent from the CheckLeader " +
                "timer pass, so its interval bounds the real heartbeat cadence regardless of HeartbeatInterval; " +
                "at or above StartElectionTimeout, followers time out before the next heartbeat and leadership " +
                "churns indefinitely. Lower CheckLeaderInterval or raise StartElectionTimeout.");

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

        if (LeadershipConfirmationTimeout <= TimeSpan.Zero)
            throw new RaftException(
                "[Kommander] LeadershipConfirmationTimeout must be positive. ConfirmLeadershipAsync " +
                "(read-index) waits up to this long for a quorum ack round; a non-positive value would " +
                "fail every confirmation immediately, making quorum-confirmed reads permanently unavailable.");

        if (EnableCheckQuorum && CheckQuorumIntervalMultiplier < 2)
            throw new RaftException(
                "[Kommander] CheckQuorumIntervalMultiplier must be at least 2 when EnableCheckQuorum=true. " +
                "The step-down window is HeartbeatInterval * CheckQuorumIntervalMultiplier; a window of a " +
                "single heartbeat interval steps a healthy leader down whenever one ack round is merely " +
                "delayed, causing spurious leadership churn under ordinary load.");

        if (LeadershipBarrierTimeout <= TimeSpan.Zero)
            throw new RaftException(
                "[Kommander] LeadershipBarrierTimeout must be positive. A newly elected leader that " +
                "inherited uncommitted prior-term entries holds leadership unpublished until its barrier " +
                "no-op commits; a non-positive timeout would make it revert to Follower immediately on " +
                "every such promotion, so the partition could never elect a serving leader.");

        if (SnapshotReceiveSessionTtl <= TimeSpan.Zero)
            throw new RaftException(
                $"[Kommander] SnapshotReceiveSessionTtl ({SnapshotReceiveSessionTtl}) must be positive. " +
                "It bounds how long an idle snapshot-receive session is retained before its buffer is released.");

        if (SnapshotMaxPendingSessions <= 0)
            throw new RaftException(
                $"[Kommander] SnapshotMaxPendingSessions ({SnapshotMaxPendingSessions}) must be positive. " +
                "It caps the number of concurrent snapshot-receive sessions on the production receive path.");

        if (SnapshotMaxPendingBytes <= 0)
            throw new RaftException(
                $"[Kommander] SnapshotMaxPendingBytes ({SnapshotMaxPendingBytes}) must be positive. " +
                "It caps total buffered snapshot bytes across all in-progress receive sessions.");

        if (TransportSecurity.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls)
        {
            // Mirrors the Kommander.Server startup checks so embedded hosts that never parse a command
            // line get the same fail-fast. Without it the first symptom is a peer that cannot connect,
            // which reads as a network fault rather than a configuration error.
            if (TransportSecurity.AllowInsecureCertificateValidation)
                throw new RaftException(
                    "[Kommander] AllowInsecureCertificateValidation cannot be combined with " +
                    "NodeAuthenticationMode.MutualTls. mTLS derives all of its security from validating " +
                    "the peer certificate, so bypassing that validation leaves the transport unauthenticated " +
                    "while appearing to be configured for mutual TLS.");

            if (TransportSecurity.ClientCertificate is null
                && string.IsNullOrWhiteSpace(TransportSecurity.ClientCertificatePath))
            {
                throw new RaftException(
                    "[Kommander] NodeAuthenticationMode.MutualTls requires a client certificate. Set " +
                    "TransportSecurity.ClientCertificatePath (or TransportSecurity.ClientCertificate for an " +
                    "already-loaded instance); without one this node cannot complete a TLS client handshake " +
                    "against its peers.");
            }

            // Resolve now so a bad path or password surfaces here rather than at first replication.
            TransportSecurity.GetClientCertificate();
        }

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
        bool mutualTls = TransportSecurity.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls;

        // The legacy bearer token is only a shared-secret alias. Folding it in under MutualTls would
        // populate a secret the mode never consults, leaving it to be transmitted or logged for no
        // benefit.
        string? sharedSecret = mutualTls
            ? TransportSecurity.SharedSecret
            : string.IsNullOrWhiteSpace(TransportSecurity.SharedSecret)
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
            // Normalized once here so every transport compares against the same canonical form, rather
            // than each call site re-deriving it (or forgetting to).
            TrustedServerCertificateThumbprints =
                RaftClientCertificateValidator.NormalizeAll(TransportSecurity.TrustedServerCertificateThumbprints),
            TrustedClientCertificateThumbprints =
                RaftClientCertificateValidator.NormalizeAll(TransportSecurity.TrustedClientCertificateThumbprints),
            ClientCertificatePath = TransportSecurity.ClientCertificatePath,
            ClientCertificatePassword = TransportSecurity.ClientCertificatePassword,
            // This method allocates a fresh options object per call, so resolving the certificate from
            // the source instance keeps a single loaded X509Certificate2 shared across every derived
            // copy. Letting each copy lazily load its own would open one key container per copy.
            ClientCertificate = mutualTls
                ? TransportSecurity.GetClientCertificate()
                : TransportSecurity.ClientCertificate
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
