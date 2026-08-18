
using System.Collections.Concurrent;
using System.ComponentModel;

using Kommander.Communication;
using Kommander.Data;
using Kommander.Gossip;
using GossipPingRequest = Kommander.Gossip.PingRequest;
using GossipPingResponse = Kommander.Gossip.PingResponse;
using GossipPingReqRequest = Kommander.Gossip.PingReqRequest;
using GossipPingReqResponse = Kommander.Gossip.PingReqResponse;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.Logging;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
// ReSharper disable ConvertToAutoPropertyWithPrivateSetter
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable ConvertToAutoProperty
// ReSharper disable ConvertToAutoPropertyWhenPossible

namespace Kommander;

/// <summary>
/// The RaftManager class is responsible for managing the Raft distributed consensus algorithm.
/// It coordinates cluster nodes, handles log replication, voting processes, and partition management
/// associated with a Raft-based architecture.
/// </summary>
public sealed class RaftManager : IRaft, IPartitionProvider, Scheduling.IRaftTimerHost, IDisposable
{
    /// <summary>
    /// Test-only seam. When non-null, replaces the per-attempt partition call inside the
    /// <see cref="ReplicateLogs(int,string,IReadOnlyList{byte[]},bool,long,CancellationToken)"/>
    /// retry loop, so a test can return <see cref="RaftOperationStatus.ActiveProposal"/> then a
    /// terminal status to drive ≥2 iterations and assert the materialized payload list is reused
    /// across retries (not re-enumerated). Left null in production; the only production cost is one
    /// field read per replication call.
    /// </summary>
    internal Func<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)>? _replicateAttemptHookForTesting
    {
        get => replicationGateway.ReplicateAttemptHookForTesting;
        set => replicationGateway.ReplicateAttemptHookForTesting = value;
    }

    /// <summary>
    /// Test-only seam. When non-null, replaces <see cref="AmILeaderQuick"/> so coordinator
    /// harness tests (which never call <see cref="JoinCluster(CancellationToken)"/> and therefore
    /// have no system partition) can exercise P0-leader-gated paths such as the replica-placement
    /// controller pass. Left null in production; the only cost is one field read per call.
    /// </summary>
    internal Func<int, ValueTask<bool>>? _amILeaderQuickHookForTesting
    {
        get => leadershipService.AmILeaderQuickHookForTesting;
        set => leadershipService.AmILeaderQuickHookForTesting = value;
    }

    internal readonly string LocalEndpoint;

    internal readonly string LocalNodeName;

    internal readonly int LocalNodeId;

    internal readonly ILogger<IRaft> Logger;

    private readonly RaftConfiguration configuration;

    private readonly IWAL walAdapter;

    private readonly IDiscovery discovery;

    private readonly ICommunication communication;

    private readonly HybridLogicalClock hybridLogicalClock;

    private readonly ClusterHandler clusterHandler;

    //private readonly RaftBatcher raftBatcher;

    private RaftPartition? systemPartition;

    private readonly ConcurrentDictionary<int, RaftPartition> partitions = new();

    /// <summary>
    /// Partitions that are currently hot: not quiesced and receiving <c>CheckLeader</c> ticks
    /// on every <see cref="RaftConfiguration.CheckLeaderInterval"/> cycle.
    /// Populated when a partition is started; entries are removed by the partition's quiesce
    /// callback and restored when it un-quiesces.  Only used when
    /// <see cref="RaftConfiguration.EnableSharedExecutorPool"/> is on.
    /// </summary>
    private readonly ConcurrentDictionary<int, RaftPartition> _hotPartitions = new();

    /// <summary>Test-visible snapshot of the current hot-partition IDs.</summary>
    internal IEnumerable<int> HotPartitionIds => _hotPartitions.Keys;

    private readonly FairReadScheduler readScheduler;

    private readonly FairWalScheduler walScheduler;

    /// <summary>
    /// Shared executor pool for all partition executors.  Non-null when
    /// <see cref="RaftConfiguration.EnableSharedExecutorPool"/> is <see langword="true"/>.
    /// Created in the constructor; started alongside the I/O schedulers in
    /// <see cref="JoinCluster(CancellationToken)"/>; stopped after all partitions stop.
    /// </summary>
    private readonly Scheduling.RaftExecutorPool? executorPool;

    private readonly RaftSystemCoordinator systemCoordinator;

    private readonly RaftTimerService timerService;

    private int _disposed;

    private IRaftStateMachineTransfer? _stateMachineTransfer;

    private IRaftSystemStateTransfer? _systemStateTransfer;

    private IRaftPartitionStateTransfer? _partitionStateTransfer;

    // Snapshot-receive session buffers owned by SnapshotReceiver; initialized in constructor.
    private readonly SnapshotReceiver snapshotReceiver;

    // Gossip/SWIM service — owns LivenessTable; initialized in constructor.
    private readonly GossipService gossipService;

    /// <summary>
    /// SWIM failure-detector liveness table. Owned by <see cref="GossipService"/>;
    /// exposed here for callers in other subsystems (coordinator, balancer).
    /// </summary>
    internal LivenessTable Liveness => gossipService.Liveness;

    /// <summary>
    /// Optional snapshot-transfer implementation registered by the application.
    /// Accessed by <see cref="RaftSystemCoordinator"/> during <c>TrySplitPartition</c>.
    /// Null when the application has not registered one (log-shipping fallback).
    /// </summary>
    internal IRaftStateMachineTransfer? StateMachineTransfer => Volatile.Read(ref _stateMachineTransfer);

    /// <summary>
    /// Optional whole-partition state-transfer implementation registered by the application for
    /// the system partition.  Accessed by the partition state machine when a P0 follower has
    /// fallen below the WAL compaction floor and must be repaired via a full-state snapshot.
    /// Null when no system-state transfer has been registered (log-shipping only).
    /// </summary>
    internal IRaftSystemStateTransfer? SystemStateTransfer => Volatile.Read(ref _systemStateTransfer);

    /// <summary>
    /// Optional whole-partition state-transfer implementation registered by the application for
    /// user data partitions. When set, the leader-side catch-up path prefers it over the
    /// split-shaped <see cref="StateMachineTransfer"/> fallback for seeding a below-floor
    /// follower, and stamps the transfer <see cref="SnapshotKind.PartitionState"/>.
    /// Null when none has been registered.
    /// </summary>
    internal IRaftPartitionStateTransfer? PartitionStateTransfer => Volatile.Read(ref _partitionStateTransfer);

    // Activity/heartbeat state owned by NodeActivityTracker; initialized in constructor
    // after LocalEndpoint is set.
    private readonly NodeActivityTracker nodeActivityTracker;

    // Load-report state owned by LoadReportService; initialized in constructor.
    private readonly LoadReportService loadReportService;

    // Partition lifecycle delegation; no owned state. Initialized in constructor after systemCoordinator.
    private readonly PartitionLifecycleService lifecycleService;
    
    private readonly Communication.RaftTransportDispatcher transportDispatcher;

    // Inbound consensus RPC routing; no owned state. Initialized in constructor after transportDispatcher.
    private readonly RaftRpcRouter rpcRouter;

    // Leadership belief, proof, waiting and handover. Initialized in constructor.
    private readonly LeadershipService leadershipService;

    // The consensus write path: propose, quorum-wait, commit/rollback, forward. Initialized in constructor.
    private readonly ReplicationGateway replicationGateway;

    // Re-admission driver for a node the committed roster evicted. Initialized in constructor.
    private readonly AutoRejoinDriver autoRejoinDriver;

    // Roster-advance reactions: application event, follower-progress repair, eviction handling.
    // Initialized in constructor after autoRejoinDriver.
    private readonly MembershipChangeHandler membershipChangeHandler;

    // Departure protocol: decommission drain, roster removal, and the leave latches.
    // Initialized in constructor.
    private readonly ClusterLeaveService leaveService;

    // Admission protocol: both join overloads, the leader-side join handler, and the
    // promotion-blocked reasons. Initialized in constructor.
    private readonly ClusterJoinService joinService;

    // Event-notifier collaborator: owns the 11 application-facing event delegate chains.
    // Initialized as a field so it is ready before the constructor body runs (the
    // constructor subscribes internal handlers via OnXxx += which routes through the accessor).
    private readonly RaftEventNotifier eventNotifier = new();

    /// <summary>
    /// Allows to retrieve the list of known nodes within the Raft cluster
    /// </summary>
    internal List<RaftNode> Nodes { get; set; } = [];

    // Committed-map view and every routing answer derived from it; owns the committed ranges and
    // per-partition placements. Initialized in constructor.
    private readonly PartitionRoutingTable routingTable;

    /// <summary>
    /// Returns the local node's role in the committed cluster roster:
    /// <see cref="System.ClusterMemberRole.Voter"/>, <see cref="System.ClusterMemberRole.Learner"/>,
    /// <see cref="System.ClusterMemberRole.Leaving"/>, or <see cref="System.ClusterMemberRole.NotMember"/>.
    /// <para>
    /// Returns <see cref="System.ClusterMemberRole.Leaving"/> immediately when
    /// <see cref="LeaveCluster"/> has been called, even before the removal commits, so
    /// election / pre-vote gates suppress campaigning during the drain window.
    /// </para>
    /// <para>
    /// Returns <see cref="System.ClusterMemberRole.Voter"/> during the pre-seed transient
    /// (roster version 0) so existing behavior is preserved on greenfield clusters.
    /// </para>
    /// </summary>
    public System.ClusterMemberRole LocalRole
    {
        get
        {
            if (leaveService.IsLeaving)
                return System.ClusterMemberRole.Leaving;

            System.ClusterMembership roster = systemCoordinator.GetMembership();
            if (roster.MembershipVersion == 0)
                return System.ClusterMemberRole.Voter;

            System.ClusterMember? self = roster.Members.FirstOrDefault(m => m.Endpoint == LocalEndpoint);
            return self?.Role ?? System.ClusterMemberRole.NotMember;
        }
    }

    /// <summary>
    /// Returns the system partition
    /// </summary>
    internal RaftPartition? SystemPartition => systemPartition;

    /// <summary>
    /// Returns the user partitions
    /// </summary>
    internal ConcurrentDictionary<int, RaftPartition> Partitions => partitions;

    internal RaftSystemCoordinator SystemCoordinator => systemCoordinator;

    /// <summary>
    /// Exposes the timer service for tests that drive gossip or balancer passes
    /// without waiting for wall-clock timer ticks.
    /// </summary>
    internal RaftTimerService TimerService => timerService;

    /// <summary>
    /// Whether the node is fully initialized or not
    /// </summary>
    public bool IsInitialized { get; private set; }

    /// <summary>
    /// Completed once, the first time the initial partition map is applied and
    /// <see cref="IsInitialized"/> flips to <c>true</c> (see <see cref="StartUserPartitions"/>).
    /// <see cref="JoinCluster(CancellationToken)"/> awaits this instead of re-polling the boolean on a
    /// fixed 1 s tick, so a node that assembles in a few ms returns in a few ms rather than sleeping
    /// out the remainder of the tick.
    /// <para>
    /// Uses <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> so a waiter's continuation
    /// never runs inline on the thread that applied the map. This signal is awaited ONLY on the
    /// caller's own join task — never in a transport handler — so it does not recreate the
    /// handshake-path join deadlock documented on <see cref="Handshake"/> (see <c>RaftManager.cs</c>
    /// Handshake summary). Because <c>IsInitialized</c> is written before this is completed, any waiter
    /// woken by the signal observes <c>IsInitialized == true</c> on re-check.
    /// </para>
    /// </summary>
    private readonly TaskCompletionSource _initializedSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Fair read scheduler. Dispatches partition-tagged synchronous WAL reads
    /// to dedicated worker threads with fair, bounded per-partition queues.
    /// </summary>
    public IRaftReadScheduler ReadScheduler => readScheduler;

    /// <summary>
    /// WAL write scheduler. Submits partition-tagged WAL commands to the
    /// <see cref="FairWalScheduler"/> and delivers completions via
    /// <see cref="WAL.Data.RaftWalCompletion"/> callbacks.
    /// </summary>
    public IRaftWalScheduler WalScheduler => walScheduler;

    /// <summary>
    /// Whether the node has joined the Raft cluster
    /// </summary>
    public bool Joined => clusterHandler.Joined;

    /// <summary>
    /// Current WAL adapter
    /// </summary>
    public IWAL WalAdapter => walAdapter;

    /// <summary>
    /// Current Communication adapter
    /// </summary>
    public ICommunication Communication => communication;

    /// <summary>
    /// Current Discovery adapter
    /// </summary>
    public IDiscovery Discovery => discovery;

    /// <summary>
    ///
    /// </summary>
    public ClusterHandler ClusterHandler => clusterHandler;

    /// <summary>
    /// Current Raft configuration
    /// </summary>
    public RaftConfiguration Configuration => configuration;

    /// <summary>
    /// Hybrid Logical Clock
    /// </summary>
    public HybridLogicalClock HybridLogicalClock => hybridLogicalClock;
    
    /// <summary>
    /// 
    /// </summary>
    //internal RaftBatcher RaftBatcher => raftBatcher;

    /// <summary>Event when the restore process starts.</summary>
    public event Action<int>? OnRestoreStarted
    {
        add => eventNotifier.OnRestoreStarted += value;
        remove => eventNotifier.OnRestoreStarted -= value;
    }

    /// <summary>Event when the restore process finishes from a user partition.</summary>
    public event Action<int>? OnRestoreFinished
    {
        add => eventNotifier.OnRestoreFinished += value;
        remove => eventNotifier.OnRestoreFinished -= value;
    }

    /// <summary>Event when the restore process finishes from a system partition.</summary>
    public event Action<int>? OnSystemRestoreFinished
    {
        add => eventNotifier.OnSystemRestoreFinished += value;
        remove => eventNotifier.OnSystemRestoreFinished -= value;
    }

    /// <summary>Event when a replication log error is acknowledged by the application.</summary>
    public event Action<int, RaftLog>? OnReplicationError
    {
        add => eventNotifier.OnReplicationError += value;
        remove => eventNotifier.OnReplicationError -= value;
    }

    /// <summary>Event when a replication log is restored from a user partition.</summary>
    public event Func<int, RaftLog, Task<bool>>? OnLogRestored
    {
        add => eventNotifier.OnLogRestored += value;
        remove => eventNotifier.OnLogRestored -= value;
    }

    /// <summary>Event when a replication log is restored from a system partition.</summary>
    public event Func<int, RaftLog, Task<bool>>? OnSystemLogRestored
    {
        add => eventNotifier.OnSystemLogRestored += value;
        remove => eventNotifier.OnSystemLogRestored -= value;
    }

    /// <summary>Event when a replication log is received from a user partition.</summary>
    public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived
    {
        add => eventNotifier.OnReplicationReceived += value;
        remove => eventNotifier.OnReplicationReceived -= value;
    }

    /// <summary>Event when a replication log is received from a system partition.</summary>
    public event Func<int, RaftLog, Task<bool>>? OnSystemReplicationReceived
    {
        add => eventNotifier.OnSystemReplicationReceived += value;
        remove => eventNotifier.OnSystemReplicationReceived -= value;
    }

    /// <summary>Event called when a leader is elected on a partition.</summary>
    public event Func<int, string, Task<bool>>? OnLeaderChanged
    {
        add => eventNotifier.OnLeaderChanged += value;
        remove => eventNotifier.OnLeaderChanged -= value;
    }

    /// <summary>
    /// Fires when a proposal reaches commit quorum, reporting the acknowledgements (the local leader plus every
    /// acking voter) that carried it. Off in production; a test subscribes to enable observation and feed a
    /// live quorum-discipline check. Invoked on the partition executor thread.
    /// </summary>
    public event Action<IReadOnlyList<RaftCommitAckObservation>>? OnCommitAcksObserved;

    /// <summary>True while a <see cref="OnCommitAcksObserved"/> subscriber is attached (gates the commit-path emission).</summary>
    public bool CommitAckObservationEnabled => OnCommitAcksObserved is not null;

    /// <summary>Forwards a commit-acknowledgement observation to any subscriber. No-op when none is attached.</summary>
    public void ObserveCommitAcks(IReadOnlyList<RaftCommitAckObservation> acks) => OnCommitAcksObserved?.Invoke(acks);

    /// <inheritdoc/>
    public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged
    {
        add => eventNotifier.OnPartitionMapChanged += value;
        remove => eventNotifier.OnPartitionMapChanged -= value;
    }

    /// <inheritdoc/>
    public event Action<System.ClusterMembership>? OnMembershipChanged
    {
        add => eventNotifier.OnMembershipChanged += value;
        remove => eventNotifier.OnMembershipChanged -= value;
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="configuration"></param>
    /// <param name="discovery"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    /// <param name="hybridLogicalClock"></param>
    /// <param name="logger"></param>
    public RaftManager(
        RaftConfiguration configuration,
        IDiscovery discovery,
        IWAL walAdapter,
        ICommunication communication,
        HybridLogicalClock hybridLogicalClock,
        ILogger<IRaft> logger
    )
    {
        this.configuration = configuration;
        this.walAdapter = walAdapter;
        this.discovery = discovery;
        this.communication = communication;
        this.hybridLogicalClock = hybridLogicalClock;

        Logger = logger;

        configuration.Validate();

        LocalEndpoint = string.Concat(configuration.Host, ":", configuration.Port);
        LocalNodeName = string.IsNullOrEmpty(this.configuration.NodeName) ? Environment.MachineName : this.configuration.NodeName;
        LocalNodeId = this.configuration.NodeId > 0 ? this.configuration.NodeId : HashUtils.SmallSimpleHash(LocalNodeName);

        nodeActivityTracker = new NodeActivityTracker(
            () => hybridLogicalClock.TrySendOrLocalEvent(LocalNodeId),
            LocalEndpoint);

        snapshotReceiver = new SnapshotReceiver(
            () => Volatile.Read(ref _disposed) != 0,
            InstallSnapshotOnExecutorAsync,
            Logger,
            LocalEndpoint,
            SnapshotReceiver.TicksForDuration(this.configuration.SnapshotReceiveSessionTtl),
            this.configuration.SnapshotMaxPendingSessions,
            this.configuration.SnapshotMaxPendingBytes,
            static () => global::System.Diagnostics.Stopwatch.GetTimestamp(),
            () => this.configuration.AllowLegacySnapshotSenders);

        clusterHandler = new(this, discovery);

        // GossipService must be initialized before RaftSystemCoordinator because the
        // coordinator constructor reads manager.Liveness (which forwards here).
        // GossipService gets a lazy Func<> so it can reference systemCoordinator
        // after both are constructed.
        gossipService = new GossipService(
            communication,
            () => Nodes,
            () => systemCoordinator!,
            () => configuration.LoadReportsEnabled ? loadReportService!.BuildLocalLoadReport() : null,
            WakePartitionsForLeader,
            configuration,
            LocalEndpoint,
            Logger);

        joinService = new ClusterJoinService(
            this,
            configuration,
            clusterHandler,
            walAdapter,
            () => systemCoordinator!,
            partitionId => leadershipService!.AmILeaderQuick(partitionId),
            () => IsInitialized,
            () => _initializedSignal.Task,
            () => LocalRole,
            StartSystemPartition,
            (node, request) => communication.SendJoin(this, node, request),
            Logger,
            LocalEndpoint,
            LocalNodeId);

        leaveService = new ClusterLeaveService(
            this,
            configuration,
            clusterHandler,
            () => systemCoordinator!,
            partitionId => leadershipService!.AmILeaderQuick(partitionId),
            () => routingTable!.CommittedMapNamesEndpoint(LocalEndpoint),
            () => IsInitialized,
            (node, request, ct) => communication.SendLeave(this, node, request, ct),
            (node, request, ct) => communication.SendSetMemberRole(this, node, request, ct),
            TearDownAfterLeaveAsync,
            Logger,
            LocalEndpoint);

        autoRejoinDriver = new AutoRejoinDriver(
            configuration,
            discovery,
            () => systemCoordinator!.GetMembership(),
            () => LocalRole,
            () => leaveService.IsLeaving,
            () => leaveService.IsLeaveRequested,
            () => Volatile.Read(ref _disposed) != 0,
            (node, request) => communication.SendJoin(this, node, request),
            Logger,
            LocalEndpoint,
            LocalNodeId);

        // Built before the coordinator: the coordinator raises roster events, and every one of
        // them routes straight into this handler.
        membershipChangeHandler = new MembershipChangeHandler(
            this,
            eventNotifier,
            autoRejoinDriver,
            Logger,
            LocalEndpoint);

        systemCoordinator = new RaftSystemCoordinator(this, Logger);

        lifecycleService = new PartitionLifecycleService(
            systemCoordinator,
            () => IsInitialized,
            AmILeader);

        transportDispatcher = new Communication.RaftTransportDispatcher(this, communication, Logger);

        rpcRouter = new RaftRpcRouter(
            this,
            walAdapter,
            () => systemCoordinator.GetMembership(),
            () => Liveness,
            transportDispatcher.Enqueue,
            Logger,
            LocalEndpoint,
            LocalNodeId);

        leadershipService = new LeadershipService(
            this,
            walAdapter,
            () => IsInitialized,
            () => Joined,
            () => Nodes,
            (node, request, ct) => communication.GetReadIndex(this, node, request, ct),
            (node, request) => communication.Handshake(this, node, request),
            Logger,
            LocalEndpoint,
            LocalNodeId);

        routingTable = new PartitionRoutingTable(
            this,
            () => Nodes,
            () => systemCoordinator.GetMembership(),
            configuration,
            LocalEndpoint);

        replicationGateway = new ReplicationGateway(
            this,
            routingTable,
            (node, partitionId, type, logs, autoCommit, expectedGeneration, ct) =>
                communication.ForwardReplicateLogs(this, node, partitionId, type, logs, autoCommit, expectedGeneration, ct),
            Logger,
            LocalEndpoint);

        readScheduler = new(logger, configuration.ReadIOThreads);
        walScheduler = new(
            walAdapter,
            logger,
            configuration.WriteIOThreads,
            configuration.MaxWalQueueDepthPerPartition,
            configuration.MaxWalBatchSize,
            configuration.MaxGlobalWalQueueDepth,
            configuration.MaxWalGroupBatchPartitions,
            configuration.WalGroupCommitLingerMs,
            configuration.WalSingleFsyncCommit);

        loadReportService = new LoadReportService(
            this,
            walScheduler,
            systemCoordinator.GetLoadReports,
            () => hybridLogicalClock.TrySendOrLocalEvent(LocalNodeId),
            GetPartitionLeaderEndpoint,
            configuration,
            LocalEndpoint);

        if (configuration.EnableSharedExecutorPool)
        {
            executorPool = new Scheduling.RaftExecutorPool(configuration.PartitionExecutorPoolSize);

            // Start the pool here, where it is created, rather than in JoinCluster.
            // A partition executor in pool mode depends on a *running* pool: Start()
            // schedules its restore onto the pool and Stop() blocks on _stopTcs until a
            // pool thread runs the cleanup drain. Any code path that constructs partitions
            // without going through JoinCluster (e.g. driving SystemCoordinator directly)
            // would otherwise deadlock. Pool threads simply park until work arrives, so
            // starting early is cheap. Start() is idempotent.
            executorPool.Start();
        }

        // Started last, after every collaborator a tick can reach is constructed: the timer fires
        // UpdateNodes / gossip / balancer / placement callbacks that route straight back into this
        // manager, so starting it mid-wiring would let a tick observe a half-built node.
        timerService = new RaftTimerService(this, Logger, configuration);
        timerService.Start();

        OnSystemLogRestored += SystemLogRestored;
        OnSystemReplicationReceived += SystemReplicationReceived;
        OnSystemRestoreFinished += SystemRestoreFinished;
        OnLeaderChanged += SystemLeaderChanged;

        if (communication is Kommander.Communication.Grpc.GrpcCommunication)
        {
            // Establish process-wide gRPC pool defaults before any peer I/O fires so that
            // external SharedChannels consumers (e.g. Kahuna's GrpcServerBatcher) inherit
            // the operator's RaftConfiguration values rather than the library fallback (4, false).
            Kommander.Communication.Grpc.SharedChannels.Configure(
                configuration.GetEffectiveGrpcChannelsPerNode(),
                configuration.GrpcEnableMultipleHttp2Connections);
        }

        if (communication is Kommander.Communication.Rest.RestCommunication
                          or Kommander.Communication.Grpc.GrpcCommunication)
        {
            RaftTransportSecurityOptions effectiveSecurity = configuration.GetEffectiveTransportSecurity();

            if (effectiveSecurity.NodeAuthenticationMode == RaftNodeAuthenticationMode.Disabled)
            {
                Logger.LogWarning(
                    "[{Endpoint}] Node authentication is Disabled for network transport. " +
                    "Configure TransportSecurity.NodeAuthenticationMode to SharedSecret or MutualTls in production.",
                    LocalEndpoint);
            }

            if (effectiveSecurity.AllowInsecureCertificateValidation)
            {
                Logger.LogWarning(
                    "[{Endpoint}] Certificate validation is disabled (AllowInsecureCertificateValidation = true). " +
                    "Do not use this setting in production.",
                    LocalEndpoint);
            }

            if (effectiveSecurity.RequireTls
                && configuration.HttpScheme is not null
                && configuration.HttpScheme.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            {
                throw new RaftException(
                    $"[{LocalEndpoint}] RequireTls is enabled but the configured HttpScheme is plain HTTP ('{configuration.HttpScheme}'). " +
                    "Set HttpScheme to 'https://' or disable RequireTls.");
            }
        }

        //raftBatcher = new(this);
    }

    private Task<bool> SystemLeaderChanged(int partitionId, string node)
    {
        /*if (!IsInitialized && partitions.Count >= 1)
        {
            bool isInitialized = true;

            foreach (KeyValuePair<int, RaftPartition> partition in partitions)
            {
                if (string.IsNullOrEmpty(partition.Value.Leader))
                {
                    isInitialized = false;
                    break;
                }
            }
            
            IsInitialized = isInitialized;
        }*/

        if (partitionId != RaftSystemConfig.SystemPartition)
            return Task.FromResult(true);

        systemCoordinator.Send(new(RaftSystemRequestType.LeaderChanged, node));
        return Task.FromResult(true);
    }

    private Task<bool> SystemLogRestored(int partitionId, RaftLog log)
    {
        if (log.LogType == RaftSystemConfig.CheckpointLogType && log.LogData is { Length: > 0 })
        {
            // A P0 checkpoint entry carrying the full system-configuration snapshot at
            // checkpoint time. Replay delivers it before any config delta above the
            // checkpoint, so a WAL compacted past the original members/partitions records
            // still reconstructs the roster and partition map on restart.
            systemCoordinator.Send(new(RaftSystemRequestType.ConfigCheckpointRestored, log.LogData));

            return Task.FromResult(true);
        }

        if (log.LogType != RaftSystemConfig.RaftLogType || log.LogData is null)
        {
            // Post-shared-P0: non-system P0 entries are dispatched to consumer callbacks
            // upstream (RaftWriteAhead restore branch) and should never reach here.
            Logger.LogDebugSystemLogRestoredSkip(log.LogType, log.LogData is null);

            return Task.FromResult(true);
        }

        systemCoordinator.Send(new(RaftSystemRequestType.ConfigRestored, log.LogData));

        return Task.FromResult(true);
    }

    private Task<bool> SystemReplicationReceived(int partitionId, RaftLog log)
    {
        if (log.LogType != RaftSystemConfig.RaftLogType || log.LogData is null)
        {
            // Post-shared-P0: non-system P0 entries are dispatched to consumer callbacks
            // upstream (CompleteFollowerAppend dispatch) and should never reach here.
            Logger.LogDebugSystemReplicationReceivedSkip(log.LogType, log.LogData is null);

            return Task.FromResult(true);
        }

        systemCoordinator.Send(new(RaftSystemRequestType.ConfigReplicated, log.LogData));

        return Task.FromResult(true);
    }

    private void SystemRestoreFinished(int partitionId)
    {
        systemCoordinator.Send(new(RaftSystemRequestType.RestoreCompleted));
    }

    /// <summary>
    /// Starts the shared executor pool and I/O schedulers and materializes the system partition,
    /// once. Owned here rather than by <see cref="ClusterJoinService"/> because the pool, the
    /// schedulers and the partition registry all belong to this class; both join overloads call
    /// it as their first step.
    /// </summary>
    private void StartSystemPartition()
    {
        if (systemPartition is not null)
            return;

        executorPool?.Start();
        readScheduler.Start();
        walScheduler.Start();

        systemPartition = new(
            this,
            walAdapter,
            RaftSystemConfig.SystemPartition,
            0,
            0,
            Logger,
            executorPool
        );
    }

    /// <summary>
    /// Joins the cluster
    /// </summary>
    public Task JoinCluster(CancellationToken cancellationToken = default) =>
        joinService.JoinCluster(cancellationToken);

    /// <summary>
    /// Seed-based join: contacts each seed in turn until this node is admitted as a Learner,
    /// then waits for automatic promotion to Voter. See <see cref="ClusterJoinService"/> for the
    /// deadline contract.
    /// </summary>
    public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) =>
        joinService.JoinCluster(seeds, cancellationToken);

    /// <summary>
    /// Handles an inbound <see cref="JoinRequest"/> from a joining node: the leader commits the
    /// joiner as a <see cref="ClusterMemberRole.Learner"/>, a non-leader answers with a hint.
    /// </summary>
    public Task<JoinResponse> ReceiveJoin(JoinRequest request) => joinService.ReceiveJoin(request);

    /// <summary>
    /// Handles an inbound <see cref="LeaveRequest"/> from a departing node.
    /// <para>
    /// If this node is the P0 leader it commits the removal and returns
    /// <see cref="LeaveResponse.Success"/> = <c>true</c>.  If this node is not the P0 leader,
    /// it returns <see cref="LeaveResponse.LeaderHint"/> so the caller can retry against the
    /// actual leader.
    /// </para>
    /// <para>
    /// <b>Idempotency:</b> if the endpoint is not found in the roster (already removed, or was
    /// never added) this method returns <c>Success</c> so a retried leave request does not spin
    /// to timeout.
    /// </para>
    /// </summary>
    public Task<LeaveResponse> ReceiveLeave(LeaveRequest request, CancellationToken cancellationToken = default) =>
        leaveService.ReceiveLeave(request, cancellationToken);

    /// <summary>
    /// Handles an inbound <see cref="SetMemberRoleRequest"/>: a roster role transition for the
    /// decommission drain (<c>Voter → Leaving</c> to start, <c>Leaving → Voter</c> to roll back).
    /// Only the system-partition leader commits; a non-leader returns a leader hint.
    /// </summary>
    public Task<SetMemberRoleResponse> ReceiveSetMemberRole(SetMemberRoleRequest request, CancellationToken cancellationToken = default) =>
        leaveService.ReceiveSetMemberRole(request, cancellationToken);

    /// <summary>
    /// Installs a partition snapshot received from the partition leader.
    /// Called on a follower when the leader delivers one chunk of a snapshot transfer.
    ///
    /// <para>Large snapshots are split into bounded chunks by the sender.  Each chunk carries a
    /// <see cref="SnapshotRequest.SessionId"/> that identifies the transfer session; this method
    /// accumulates chunks in <see cref="_pendingSnapshots"/> until <see cref="SnapshotRequest.IsLast"/>
    /// is true, then dispatches to the correct importer based on <see cref="SnapshotRequest.Kind"/>:
    /// <see cref="SnapshotKind.Range"/> → <see cref="IRaftStateMachineTransfer.ImportRange"/>;
    /// <see cref="SnapshotKind.SystemState"/> → <see cref="IRaftSystemStateTransfer.ImportPartitionState"/>;
    /// <see cref="SnapshotKind.PartitionState"/> → <see cref="IRaftPartitionStateTransfer.ImportPartitionState"/>.
    /// Afterwards the WAL is seeded with a <c>CommittedCheckpoint</c> entry at
    /// <see cref="SnapshotRequest.SnapshotIndex"/> so normal backfill can resume from there.</para>
    ///
    /// <para>The method is idempotent at the session boundary: if the local WAL already reflects
    /// <see cref="SnapshotRequest.SnapshotIndex"/> or higher, every chunk for that transfer returns
    /// success immediately.  On any error the partial session is removed so a retry starts clean.</para>
    /// </summary>
    public Task<SnapshotResponse> ReceiveInstallSnapshot(
        SnapshotRequest request,
        CancellationToken cancellationToken = default) =>
        snapshotReceiver.ReceiveInstallSnapshot(request, cancellationToken);

    /// <summary>
    /// Routes a fully-staged snapshot to the target partition's single-writer executor for the actual
    /// install (term validation, application import, durable WAL boundary). Invoked by
    /// <see cref="SnapshotReceiver"/> on the terminal chunk. Returns failure if the partition is not
    /// hosted here yet, so the leader retries once the partition exists.
    /// </summary>
    private async Task<SnapshotResponse> InstallSnapshotOnExecutorAsync(SnapshotInstallRequest request)
    {
        if (!TryGetPartition(request.PartitionId, out RaftPartition? partition) || partition is null)
            return new SnapshotResponse(false);

        bool installed = await partition.InstallSnapshotAsync(request).ConfigureAwait(false);
        return new SnapshotResponse(installed);
    }

    /// <summary>
    /// Handles an inbound gossip digest from a peer.
    /// <para>
    /// If the sender carries a newer committed roster the request is posted to the
    /// coordinator channel so it is applied to the local membership cache in the correct
    /// serial order.  The response always carries the local committed version and, when
    /// locally newer, the full roster so the sender can catch up in the same round trip.
    /// </para>
    /// <para>
    /// This method is intentionally synchronous: the coordinator update is fire-and-forget
    /// (posted to a channel); the caller does not need to await it before returning the ACK.
    /// </para>
    /// </summary>
    public GossipAck ReceiveGossip(GossipMessage digest) =>
        gossipService.ReceiveGossip(this, digest);

    public Task GossipAsync(CancellationToken cancellationToken = default) =>
        gossipService.GossipAsync(this, cancellationToken);

    public GossipPingResponse ReceivePing(GossipPingRequest request) =>
        gossipService.ReceivePing(request);

    public Task<GossipPingReqResponse> ReceivePingReq(GossipPingReqRequest request, CancellationToken cancellationToken = default) =>
        gossipService.ReceivePingReq(this, request, cancellationToken);

    /// <summary>
    /// Runs one SWIM probe round: picks a random peer from <see cref="Nodes"/>, sends a
    /// direct <see cref="PingRequest"/>, and if it times out follows up with up to
    /// <c>IndirectPingFanout</c> indirect probes.  On total failure the peer is marked
    /// <see cref="MemberLivenessState.Suspect"/>; on success it is marked
    /// <see cref="MemberLivenessState.Alive"/>.
    /// <para>
    /// Also advances the Suspect→Dead expiry, so a peer whose suspicion age exceeds
    /// <c>SuspicionTimeout</c> transitions to Dead during this call.
    /// </para>
    /// <para>
    /// Called by <see cref="RaftTimerService.TriggerPing"/> on a periodic timer.  Tests may
    /// call it directly to drive probing deterministically without waiting for the timer.
    /// </para>
    /// </summary>
    public Task PingAsync(CancellationToken cancellationToken = default) =>
        gossipService.PingAsync(this, cancellationToken);

    /// <summary>
    /// Returns the last commit index acknowledged by <paramref name="endpoint"/> on the given partition,
    /// or -1 when no <c>CompleteAppendLogs</c> has been received yet.  Delegates to the
    /// partition executor so the read is thread-safe.
    /// </summary>
    internal async ValueTask<long> GetFollowerCommittedIndexAsync(int partitionId, string endpoint)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
            return systemPartition is not null
                ? await systemPartition.GetFollowerCommittedIndexAsync(endpoint).ConfigureAwait(false)
                : -1;

        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return await partition.GetFollowerCommittedIndexAsync(endpoint).ConfigureAwait(false);

        return -1;
    }

    /// <summary>
    /// Nullable variant: returns <c>null</c> when <paramref name="endpoint"/> has never sent a
    /// <c>CompleteAppendLogs</c> for this partition — meaning the node does not participate in it.
    /// Distinguishes "not a participant" from "participant with no committed entries yet (-1)".
    /// </summary>
    internal async ValueTask<long?> GetFollowerCommittedIndexNullableAsync(int partitionId, string endpoint)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
            return systemPartition is not null
                ? await systemPartition.GetFollowerCommittedIndexNullableAsync(endpoint).ConfigureAwait(false)
                : null;

        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return await partition.GetFollowerCommittedIndexNullableAsync(endpoint).ConfigureAwait(false);

        return null;
    }

    /// <inheritdoc/>
    public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint)
        => GetFollowerCommittedIndexNullableAsync(partitionId, followerEndpoint);

    /// <summary>
    /// Returns the endpoint of the current known leader for <paramref name="partitionId"/>,
    /// or <see langword="null"/> if unknown or no leader has been observed yet.
    /// Reads the cached Leader field — no I/O.
    /// </summary>
    internal string? GetPartitionLeaderEndpoint(int partitionId)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
            return systemPartition?.Leader;

        return partitions.TryGetValue(partitionId, out RaftPartition? p) ? p.Leader : null;
    }

    /// <summary>
    /// Start the user partitions
    /// <para>
    /// Applies the committed map: refreshes the global routing snapshot in
    /// <see cref="PartitionRoutingTable"/>, then materializes a
    /// <see cref="RaftPartition"/> only for ranges this node hosts. A range with a non-empty
    /// replica set is hosted only when the local endpoint appears in it (any role); an empty
    /// replica set means legacy full replication and is hosted unconditionally. A partition this
    /// node stopped being a replica of is drained, stopped, and its WAL reclaimed — safe because
    /// the absence from the committed replica set is exactly the final <c>RemoveReplica</c>
    /// commit, so no later configuration of the range can need this node's copy.
    /// </para>
    /// </summary>
    /// <param name="ranges"></param>
    internal void StartUserPartitions(List<RaftPartitionRange> ranges)
    {
        routingTable.ApplyCommittedMap(ranges);

        foreach (RaftPartitionRange range in ranges)
        {
            // Tombstone entries must never re-create a stopped partition.
            if (range.State == RaftPartitionState.Removed)
                continue;

            bool hostsIt = range.Replicas.Count == 0 // legacy full replication
                           || range.Replicas.Any(r => r.Endpoint == LocalEndpoint);

            if (!hostsIt)
            {
                StopUnhostedPartition(range.PartitionId);
                continue;
            }

            if (partitions.TryGetValue(range.PartitionId, out RaftPartition? partition))
            {
                // Volatile writes — visible to any thread already holding a reference.
                partition.StartRange = range.StartRange;
                partition.EndRange = range.EndRange;
                partition.RoutingMode = range.RoutingMode;
                partition.Generation = range.Generation;
                partition.State = range.State;
            }
            else
            {
                RaftPartition newPartition = new(
                    this,
                    walAdapter,
                    range.PartitionId,
                    range.StartRange,
                    range.EndRange,
                    Logger,
                    executorPool
                );
                newPartition.RoutingMode = range.RoutingMode;
                newPartition.Generation = range.Generation;
                newPartition.State = range.State;
                partitions.TryAdd(range.PartitionId, newPartition);
                // New partitions start hot; they leave the hot set via the quiesce callback.
                _hotPartitions.TryAdd(range.PartitionId, newPartition);
            }
        }

        IsInitialized = true;

        // Wake any JoinCluster waiter immediately instead of leaving it to notice on the next 1 s
        // poll tick. RunContinuationsAsynchronously (on the TCS) keeps the waiter's continuation off
        // this map-application thread. Idempotent: only the first application (the flip of
        // IsInitialized) completes it; later map changes (splits/merges) re-enter here and no-op.
        _initializedSignal.TrySetResult();

        eventNotifier.InvokePartitionMapChanged(GetPartitionMap());
    }

    /// <summary>
    /// Stops hosting a partition this node is no longer a replica of: the partition is removed
    /// from the routing dictionaries immediately (no new proposals land on it), then drained,
    /// stopped, and its WAL reclaimed in the background. Reclaiming here is safe because the
    /// committed map no longer lists this node in the range's replica set — the final
    /// <c>RemoveReplica</c> commit — so no future configuration of the range can require this
    /// node's copy. Runs off the coordinator loop so map application never blocks on a drain.
    /// </summary>
    private void StopUnhostedPartition(int partitionId)
    {
        if (!partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return;

        RemovePartition(partitionId);

        _ = Task.Run(async () =>
        {
            try
            {
                await partition.DrainAsync(CancellationToken.None).ConfigureAwait(false);
                partition.Stop();
                walAdapter.DeletePartitionWAL(partitionId);

                if (Logger.IsEnabled(LogLevel.Information))
                    Logger.LogInformation(
                        "[{Endpoint}] Stopped hosting partition {PartitionId} (no longer a replica); WAL reclaimed",
                        LocalEndpoint, partitionId);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(
                    "StopUnhostedPartition: partition {PartitionId}: {Message}",
                    partitionId, ex.Message);
            }
        });
    }

    /// <summary>
    /// Returns the peer set for one partition: the range's replica set (minus self) when the
    /// committed map assigns it one, otherwise the whole-cluster node list (legacy full
    /// replication and the system partition, which always replicates everywhere). This is the
    /// seam that makes quorum per-partition — <see cref="RaftPartitionStateMachine"/> computes
    /// every quorum from <c>host.Nodes</c> filtered by <c>host.IsVoter</c>.
    /// </summary>
    internal IReadOnlyList<RaftNode> GetPartitionPeers(int partitionId) =>
        routingTable.GetPartitionPeers(partitionId);

    /// <summary>
    /// Returns true when <paramref name="endpoint"/> counts toward <paramref name="partitionId"/>'s
    /// quorum. For a range with an assigned replica set this is membership in the range's
    /// <see cref="System.RaftReplicaRole.Voter"/> replicas — Learner and Removing replicas are
    /// peers but excluded from the quorum denominator. For legacy ranges and the system partition
    /// it falls back to the committed roster's voter set (pre-seed: everyone is a voter).
    /// </summary>
    internal bool IsPartitionVoter(int partitionId, string endpoint) =>
        routingTable.IsPartitionVoter(partitionId, endpoint);

    /// <summary>
    /// Returns whether the partition is materialized on this node — see
    /// <see cref="IRaft.HostsPartition"/> for the contract. Reads the same dictionary the
    /// per-partition APIs resolve through, so a <see langword="true"/> is authoritative at read
    /// time; it can still be invalidated by a concurrent replica move, so callers must keep
    /// treating <see cref="PartitionNotHostedException"/> as retryable.
    /// </summary>
    public bool HostsPartition(int partitionId)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
            return systemPartition is not null;

        return partitions.ContainsKey(partitionId);
    }

    /// <summary>
    /// Returns the committed replica set of <paramref name="partitionId"/>, or an empty list for
    /// legacy full replication (every roster voter hosts the range) and unknown partitions.
    /// The returned list is a snapshot; it never mutates after being returned.
    /// </summary>
    public IReadOnlyList<System.RaftReplica> GetPartitionReplicas(int partitionId) =>
        routingTable.GetPartitionReplicas(partitionId);

    /// <summary>
    /// Returns the effective replication factor for <paramref name="partitionId"/>: the range's
    /// override when set, otherwise <see cref="RaftConfiguration.ReplicationFactor"/>.
    /// 0 means full replication.
    /// </summary>
    public int GetEffectiveReplicationFactor(int partitionId) =>
        routingTable.GetEffectiveReplicationFactor(partitionId);

    /// <summary>
    /// Leaves the cluster and tears the node down. See <see cref="ClusterLeaveService.LeaveCluster"/>;
    /// this node's ordered teardown is supplied to that service as <see cref="TearDownAfterLeaveAsync"/>.
    /// </summary>
    /// <param name="dispose">If true, also disposes the manager</param>
    /// <param name="cancellationToken">
    /// When cancelled, aborts any in-progress graceful-leave attempt immediately.
    /// </param>
    public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) =>
        leaveService.LeaveCluster(dispose, cancellationToken);

    /// <summary>
    /// Stops everything this manager owns, in the one order that is safe: timer first (no new work
    /// injected), drain partition queues, stop the shared I/O schedulers while partition executors
    /// are still alive so WAL completions can be posted back, drain those completions, then stop
    /// executor threads, the pool, the dispatcher and the coordinator. Owned here rather than by
    /// the leave service because every resource in the sequence belongs to this class.
    /// </summary>
    private async Task TearDownAfterLeaveAsync(bool dispose)
    {
        // RaftTimerService.Dispose() is idempotent and safe to call again from Dispose() below.
        timerService.Dispose();

        await DrainPartitions(CancellationToken.None).ConfigureAwait(false);

        readScheduler.Stop();
        walScheduler.Stop();

        await DrainPartitions(CancellationToken.None).ConfigureAwait(false);

        foreach (RaftPartition partition in partitions.Values)
            partition.Stop();

        systemPartition?.Stop();

        // All partition executors have stopped; safe to stop the shared pool now.
        executorPool?.Stop();

        // Complete dispatcher channels now that no executor thread is producing more
        // outbound messages; workers drain the remaining buffered items then exit.
        transportDispatcher.Stop();

        // Stop system coordinator channel — no more system events will be produced.
        systemCoordinator.Stop();

        if (dispose)
            Dispose();
    }

    /// <summary>
    /// Asks the cluster to remove this node from the committed roster and reports what happened,
    /// <b>without</b> tearing the node down — see <see cref="ClusterLeaveService.RequestLeaveAsync"/>
    /// for the drain-before-removal contract and the full set of outcomes.
    /// </summary>
    /// <param name="cancellationToken">
    /// Bounds the attempt. Cancelling yields <see cref="LeaveClusterOutcome.Timeout"/> — the removal
    /// may still commit, so the caller must re-read the roster before concluding anything.
    /// </param>
    public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) =>
        leaveService.RequestLeaveAsync(cancellationToken);

    /// <summary>
    /// Upper bound on how long a teardown drain waits for each partition's <c>DrainBarrier</c>.
    /// The barrier is a low-priority (Maintenance) request, so a steady stream of higher-priority
    /// incoming AppendLogs/heartbeats from peers — heavier with the single-fsync fast path, which
    /// generates extra replication traffic — can starve it indefinitely. An unbounded wait here let
    /// teardown block forever, which under constrained cores compounded across many partitions/nodes
    /// into multi-minute suite hangs. The drain is best-effort (Stop()/Dispose() forcibly halts the
    /// executor afterward), so timing out and proceeding only abandons a few in-flight ops — exactly
    /// what a shutdown does regardless.
    /// </summary>
    private static readonly TimeSpan DrainBarrierTimeout = TimeSpan.FromSeconds(3);

    private async Task DrainPartitions(CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeoutCts = new(DrainBarrierTimeout);
        using CancellationTokenSource linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        List<Task> drainTasks = new(partitions.Count + 1);

        foreach (RaftPartition partition in partitions.Values)
            drainTasks.Add(partition.DrainAsync(linkedCts.Token));

        if (systemPartition is not null)
            drainTasks.Add(systemPartition.DrainAsync(linkedCts.Token));

        if (drainTasks.Count == 0)
            return;

        try
        {
            await Task.WhenAll(drainTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            // Barrier starved by higher-priority in-flight work; proceed to the forced stop below.
            Logger.LogWarning(
                "Teardown: partition drain barrier exceeded {TimeoutMs}ms (executor busy); proceeding to stop.",
                DrainBarrierTimeout.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Updates the internal state of the nodes
    /// </summary>
    public async Task UpdateNodes()
    {
        if (systemPartition is null && partitions.IsEmpty)
            return;

        await clusterHandler.UpdateNodes().ConfigureAwait(false);

        // Mark discovery as having run at least once. Until this is set, a partition whose Nodes set
        // is still empty must not self-elect as a single-node leader — a seed-joining node's peers are
        // only loaded here, and self-electing in that window lets it usurp the existing cluster's P0
        // leadership with an empty log (see IRaftPartitionHost.InitialNodesDiscovered).
        InitialNodesDiscovered = true;

        await systemCoordinator.CheckLearnerPromotionsAsync().ConfigureAwait(false);
        await systemCoordinator.EvictDeadMembersAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public bool InitialNodesDiscovered { get; private set; }

    // ── IRaftTimerHost ─────────────────────────────────────────────────────

    RaftPartition? Scheduling.IRaftTimerHost.SystemPartition => systemPartition;

    // Iterator rather than partitions.Values: ConcurrentDictionary.Values acquires every bucket
    // lock and materializes a fresh List on each access — this fires every CheckLeader tick
    // (default 250 ms) and would contend with MarkPartitionHot/Cool writers. The dictionary's own
    // enumerator is lock-free and allocation-light.
    IEnumerable<RaftPartition> Scheduling.IRaftTimerHost.GetUserPartitions()
    {
        foreach (KeyValuePair<int, RaftPartition> kv in partitions)
            yield return kv.Value;
    }

    /// <summary>
    /// Returns only the hot (non-quiesced) partitions for targeted <c>CheckLeader</c> ticks.
    /// Updated by <see cref="MarkPartitionHot"/> / <see cref="MarkPartitionCool"/> which are
    /// wired from each <see cref="RaftPartitionStateMachine"/>'s quiesce callback.
    /// </summary>
    IEnumerable<RaftPartition> Scheduling.IRaftTimerHost.GetHotUserPartitions()
    {
        // Same rationale as GetUserPartitions: avoid the Values snapshot per tick.
        foreach (KeyValuePair<int, RaftPartition> kv in _hotPartitions)
            yield return kv.Value;
    }

    Task Scheduling.IRaftTimerHost.UpdateNodes() => UpdateNodes();

    Task Scheduling.IRaftTimerHost.GossipAsync(CancellationToken cancellationToken) => GossipAsync(cancellationToken);

    Task Scheduling.IRaftTimerHost.PingAsync(CancellationToken cancellationToken) => PingAsync(cancellationToken);

    void Scheduling.IRaftTimerHost.TriggerBalancerPass()
    {
        systemCoordinator.Send(new System.RaftSystemRequest(System.RaftSystemRequestType.RunBalancerPass));
    }

    void Scheduling.IRaftTimerHost.TriggerPlacementPass()
    {
        // Deliberately not sent from TriggerBalancerPass: placement runs on its own
        // PlacementPassInterval cadence so it works with the leader balancer disabled.
        // The pass self-gates on P0 leadership and no-ops when no range has replicas.
        systemCoordinator.Send(new System.RaftSystemRequest(System.RaftSystemRequestType.RunPlacementPass));
    }

    /// <summary>
    /// Adds <paramref name="partitionId"/> to the hot set so it receives targeted
    /// <c>CheckLeader</c> ticks.  Called from the partition's quiesce callback when a
    /// partition transitions from quiesced → active.  Safe to call from any thread.
    /// </summary>
    internal void MarkPartitionHot(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? p))
            _hotPartitions.TryAdd(partitionId, p);
    }

    /// <summary>
    /// Removes <paramref name="partitionId"/> from the hot set.  Called from the partition's
    /// quiesce callback when it transitions to quiesced state.  Safe to call from any thread.
    /// </summary>
    internal void MarkPartitionCool(int partitionId) => _hotPartitions.TryRemove(partitionId, out _);

    /// <summary>
    /// Promotes every quiesced partition that believes <paramref name="leaderEndpoint"/> is
    /// its current leader back into the hot set so it receives a <c>CheckLeader</c> tick
    /// on the next <see cref="RaftConfiguration.CheckLeaderInterval"/> cycle instead of
    /// waiting for the coarse safety sweep.
    ///
    /// <para>Called whenever SWIM transitions <paramref name="leaderEndpoint"/> to Suspect or
    /// Dead so failover detection for quiesced followers is bounded by SWIM latency rather
    /// than by <see cref="RaftConfiguration.UpdateNodesInterval"/> (the safety-sweep period).
    /// This preserves the fast-failover guarantee quiescence depends on.</para>
    /// </summary>
    private void WakePartitionsForLeader(string leaderEndpoint)
    {
        foreach (RaftPartition p in partitions.Values)
        {
            if (string.Equals(p.Leader, leaderEndpoint, StringComparison.Ordinal))
                MarkPartitionHot(p.PartitionId);
        }
    }

    /// <summary>
    /// Evicts <paramref name="partitionId"/> from both <see cref="partitions"/> and
    /// <see cref="_hotPartitions"/> in one call.  Use this instead of
    /// <c>Partitions.TryRemove</c> at removal/merge sites so the two dictionaries never
    /// drift out of sync: a stale <c>_hotPartitions</c> entry points at a stopped executor
    /// and causes <see cref="RaftTimerService.TriggerCheckLeader"/> to throw on the next
    /// hot-set tick, silently aborting the sweep for all survivors that follow it.
    /// </summary>
    internal void RemovePartition(int partitionId)
    {
        partitions.TryRemove(partitionId, out _);
        _hotPartitions.TryRemove(partitionId, out _);
    }

    // ── Node activity / heartbeat — bodies live in NodeActivityTracker ────────

    /// <summary>
    /// Obtains the last activity known of a specific node on a specific partition.
    /// </summary>
    public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) =>
        nodeActivityTracker.GetLastNodeActivity(endpoint, partitionId);

    /// <summary>
    /// Obtains the last activity known of a specific node across all partitions.
    /// </summary>
    public HLCTimestamp GetLastNodeActivity(string endpoint) =>
        nodeActivityTracker.GetLastNodeActivity(endpoint);

    /// <summary>
    /// Updates the last activity known of a specific node on a specific partition.
    /// </summary>
    internal void UpdateLastNodeActivity(string nodeId, int partitionId, HLCTimestamp lastTimestamp) =>
        nodeActivityTracker.UpdateLastNodeActivity(nodeId, partitionId, lastTimestamp);

    /// <summary>
    /// Obtains the last heartbeat sent to a specific node for a specific partition.
    /// The throttle key must include the partition id: a single node hosts many partitions,
    /// and keying only by endpoint would let one partition's heartbeat suppress the
    /// heartbeats of every other partition to the same node (within RecentHeartbeat),
    /// starving their followers and triggering perpetual re-elections.
    /// </summary>
    internal HLCTimestamp GetLastNodeHearthbeat(string nodeId, int partitionId) =>
        nodeActivityTracker.GetLastNodeHearthbeat(nodeId, partitionId);

    /// <summary>
    /// Updates the last heartbeat sent to a node for a specific partition.
    /// </summary>
    internal void UpdateLastHeartbeat(string nodeId, int partitionId, HLCTimestamp lastTimestamp) =>
        nodeActivityTracker.UpdateLastHeartbeat(nodeId, partitionId, lastTimestamp);

    /// <summary>
    /// Returns a list of nodes in the cluster.
    /// </summary>
    public IList<RaftNode> GetNodes()
    {
        return Nodes;
    }

    /// <summary>
    /// Returns the non-local endpoints observed within the requested liveness window.
    /// </summary>
    public IReadOnlyList<string> GetActiveNodes(TimeSpan within) =>
        nodeActivityTracker.GetActiveNodes(within);

    /// <summary>
    /// Returns the raft partition for the given partition number
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    /// <summary>
    /// Test-only: returns an immutable snapshot of the given partition's consensus state, captured on the
    /// partition executor thread, or <see langword="null"/> if the partition is not hosted here. Used by
    /// the chaos harness to build a point-in-time cluster view for the invariant checker.
    /// </summary>
    internal async Task<RaftPartitionView?> GetPartitionViewAsync(int partitionId, CancellationToken cancellationToken = default)
    {
        if (!TryGetPartition(partitionId, out RaftPartition? partition) || partition is null)
            return null;
        return await partition.GetPartitionViewAsync(cancellationToken).ConfigureAwait(false);
    }

    private RaftPartition GetPartition(int partitionId)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
        {
            if (systemPartition is null)
                throw new RaftException("System partition not initialized.");

            return systemPartition;
        }

        if (!partitions.TryGetValue(partitionId, out RaftPartition? partition))
            throw BuildUnknownPartitionException(partitionId);

        return partition;
    }

    /// <summary>
    /// Classifies a data-partition miss on the local proposal path. A partition present in the
    /// committed map but not materialized here is a routing condition, not a caller error: under
    /// replica placement most ranges live on other nodes, and even a range this node hosts can be
    /// looked up in the window between the map commit and the coordinator applying it in
    /// <see cref="StartUserPartitions"/>. Both cases get the typed, retryable
    /// <see cref="PartitionNotHostedException"/> so consumers can route elsewhere without matching
    /// message strings. An id absent from the committed map (or tombstoned
    /// <see cref="RaftPartitionState.Removed"/>) keeps the plain <see cref="RaftException"/> —
    /// no node hosts it, so retrying elsewhere cannot help.
    /// </summary>
    private RaftException BuildUnknownPartitionException(int partitionId)
    {
        if (routingTable.IsLiveMappedRange(partitionId))
            return new PartitionNotHostedException(partitionId);

        return new RaftException("Invalid partition: " + partitionId);
    }

    /// <summary>
    /// Resolves the partition for an *inbound peer message*, tolerating a data partition that
    /// has not been created on this node yet. Unlike <see cref="GetPartition"/> — which throws
    /// for an unknown data partition, correctly treating that as a caller error on the local
    /// proposal path — inbound election/replication messages legitimately arrive during cluster
    /// assembly before this node has finished creating the target data partition.
    /// <para>
    /// Returning false lets the handler drop just that one message: the remote peer simply retries
    /// on its next election timeout / heartbeat. Critically, this avoids throwing a
    /// <see cref="RaftException"/> out of a coalesced endpoint batch — a single not-yet-created
    /// data partition must never poison sibling messages (including system-partition heartbeats and
    /// votes) that share the same batch, which would otherwise stall cluster assembly under load.
    /// </para>
    /// </summary>
    private bool TryGetPartition(int partitionId, out RaftPartition? partition)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
        {
            partition = systemPartition;
            return partition is not null;
        }

        return partitions.TryGetValue(partitionId, out partition);
    }

    // Partition-registry seam consumed by the extracted collaborators. Implemented explicitly so
    // the underlying helpers stay private and no new surface is added to the public facade: the
    // registry remains owned and written here, and collaborators only read through it.

    /// <inheritdoc/>
    RaftPartition? IPartitionProvider.SystemPartition => systemPartition;

    /// <inheritdoc/>
    IEnumerable<RaftPartition> IPartitionProvider.DataPartitions => partitions.Values;

    /// <inheritdoc/>
    int IPartitionProvider.DataPartitionCount => partitions.Count;

    /// <inheritdoc/>
    bool IPartitionProvider.TryGetDataPartition(int partitionId, out RaftPartition? partition) =>
        partitions.TryGetValue(partitionId, out partition);

    /// <inheritdoc/>
    bool IPartitionProvider.TryGetPartition(int partitionId, out RaftPartition? partition) =>
        TryGetPartition(partitionId, out partition);

    /// <inheritdoc/>
    RaftPartition IPartitionProvider.GetPartition(int partitionId) => GetPartition(partitionId);

    /// <inheritdoc/>
    bool IPartitionProvider.HostsPartition(int partitionId) => HostsPartition(partitionId);

    /// <summary>
    /// Sets the minimum WAL log index that compaction must not truncate below on the given
    /// partition. No-ops silently when the partition is not hosted on this node.
    /// See <see cref="IRaft.SetMinRetainIndex"/> for full semantics.
    /// </summary>
    public void SetMinRetainIndex(int partitionId, long index)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            partition.SetMinRetainIndex(index);
    }

    /// <summary>
    /// Returns the given partition's committed frontier (highest contiguously committed log id),
    /// or -1 when the partition is not hosted on this node or has committed nothing. See
    /// <see cref="IRaft.GetCommitIndex"/> for why this differs from the raw max log id.
    /// </summary>
    public long GetCommitIndex(int partitionId)
    {
        if (TryGetPartition(partitionId, out RaftPartition? partition))
            return partition!.GetCommitIndex();

        return -1;
    }

    /// <summary>
    /// Returns the number of stale <c>Proposed</c> duplicates of already-resolved ids refused on
    /// <paramref name="partitionId"/> since it last started, or -1 when the partition is not hosted
    /// on this node. See <see cref="IRaft.GetStaleProposedSkippedCount"/>: it is a floor, not a
    /// lifetime total (the count restarts with the process), and -1 and 0 mean different things and
    /// must not be conflated by a caller aggregating across nodes.
    /// </summary>
    public long GetStaleProposedSkippedCount(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return partition.GetStaleProposedSkippedCount();

        return -1;
    }

    /// <summary>
    /// Returns the leader-side snapshot-transfer status per follower for the given partition —
    /// see <see cref="IRaft.GetSnapshotStatuses"/>. Empty when the partition is not hosted here,
    /// this node is not producing snapshot transfers for it, or every follower is healthy.
    /// </summary>
    public IReadOnlyList<Data.RaftSnapshotStatus> GetSnapshotStatuses(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return partition.GetSnapshotStatuses();

        return [];
    }

    /// <summary>
    /// Returns the leader-side non-contiguous-backfill status per follower for the given partition —
    /// see <see cref="IRaft.GetBackfillStatuses"/>. Empty when the partition is not hosted here, this
    /// node is not its leader, or no follower's anchor is unserviceable.
    /// </summary>
    public IReadOnlyList<Data.RaftBackfillStatus> GetBackfillStatuses(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return partition.GetBackfillStatuses();

        return [];
    }

    /// <summary>
    /// Acquires a composable retention hold on the given partition's WAL. When the partition is not
    /// hosted on this node the call is a no-op that returns a disposable which does nothing, mirroring
    /// the silent no-op of <see cref="SetMinRetainIndex"/>. See <see cref="IRaft.AcquireRetentionHold"/>
    /// for full semantics.
    /// </summary>
    public IDisposable AcquireRetentionHold(int partitionId, long index)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return partition.AcquireRetentionHold(index);

        return NoOpRetentionHold.Instance;
    }

    /// <summary>
    /// No-op retention handle returned when a hold is requested for a partition not hosted here, so
    /// callers always receive a valid <see cref="IDisposable"/> to dispose.
    /// </summary>
    private sealed class NoOpRetentionHold : IDisposable
    {
        public static readonly NoOpRetentionHold Instance = new();

        public void Dispose() { }
    }

    internal NodeLoadReport BuildLocalLoadReport() => loadReportService.BuildLocalLoadReport();

    /// <summary>
    /// Passes the handshake to the addressed partition. Delegates to <see cref="RaftRpcRouter"/>,
    /// which drops the message when that partition is not materialized here yet.
    /// </summary>
    /// <param name="request"></param>
    public Task Handshake(HandshakeRequest request) => rpcRouter.Handshake(request);

    /// <summary>Builds this node's handshake reply for a partition.</summary>
    internal HandshakeResponse GetHandshakeResponse(int partitionId) => rpcRouter.GetHandshakeResponse(partitionId);

    /// <summary>
    /// Passes the RequestVote to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request) => rpcRouter.RequestVote(request);

    /// <summary>
    /// Passes the request to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request) => rpcRouter.Vote(request);

    /// <summary>Passes a step-down notice to the appropriate partition.</summary>
    internal void StepDownNotice(StepDownNoticeRequest request) => rpcRouter.StepDownNotice(request);

    /// <summary>Passes a leadership-transfer command to the appropriate partition.</summary>
    internal void TransferLeadership(TransferLeadershipRequest request) => rpcRouter.TransferLeadership(request);

    /// <summary>
    /// Receives an advisory leadership-transfer suggestion from the balancer running on the
    /// system-partition leader.
    /// </summary>
    internal void ReceiveTransferLeadershipSuggestion(Data.TransferLeadershipSuggestionRequest request) =>
        rpcRouter.ReceiveTransferLeadershipSuggestion(request);

    /// <summary>
    /// Sends an advisory leadership-transfer suggestion to the node owning the partition,
    /// short-circuiting to in-process delivery when that owner is this node.
    /// </summary>
    internal void SendTransferLeadershipSuggestion(string ownerEndpoint, Data.TransferLeadershipSuggestionRequest request) =>
        rpcRouter.SendTransferLeadershipSuggestion(ownerEndpoint, request);

    /// <summary>
    /// Append logs in the appropriate partition
    /// Returns the index of the last log
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void AppendLogs(AppendLogsRequest request) => rpcRouter.AppendLogs(request);

    /// <summary>
    /// Completes an append logs operation in the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request) => rpcRouter.CompleteAppendLogs(request);

    /// <summary>
    /// Replicate a single log to the follower nodes in the system partition
    /// </summary>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal Task<RaftReplicationResult> ReplicateSystemLogs(string type, byte[] data, bool autoCommit = true, CancellationToken cancellationToken = default) =>
        replicationGateway.ReplicateSystemLogs(type, data, autoCommit, cancellationToken);

    /// <summary>
    /// Replicates a single log entry to the follower nodes in the specified partition.
    /// P0 routes committed entries by log type: <c>_RaftSystem</c> entries go to the system
    /// coordinator; all other types go to consumer callbacks (<c>OnReplicationReceived</c> /
    /// <c>OnLogRestored</c>).  Passing <c>type == "_RaftSystem"</c> on partition 0 is rejected
    /// with <see cref="RaftException"/> to prevent userland from forging coordinator entries.
    /// P0 is never a valid target for create, split, merge, or remove.
    /// </summary>
    public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) =>
        replicationGateway.ReplicateLogs(partitionId, type, data, autoCommit, expectedGeneration, cancellationToken);

    /// <summary>
    /// Replicates a batch of log entries to the follower nodes in the specified partition.
    /// See the <see cref="IReadOnlyList{T}"/> overload for the full contract.
    /// </summary>
    public Task<RaftReplicationResult> ReplicateLogs(
        int partitionId,
        string type,
        IEnumerable<byte[]> logs,
        bool autoCommit = true,
        long expectedGeneration = 0,
        CancellationToken cancellationToken = default
    ) => replicationGateway.ReplicateLogs(partitionId, type, logs, autoCommit, expectedGeneration, cancellationToken);

    /// <summary>
    /// Replicates a batch of log entries to the follower nodes in the specified partition.
    /// Accepts an already-materialized list or array and avoids the intermediate copy
    /// incurred by the <see cref="IEnumerable{T}"/> overload for array and list callers.
    /// </summary>
    public Task<RaftReplicationResult> ReplicateLogs(
        int partitionId,
        string type,
        IReadOnlyList<byte[]> logs,
        bool autoCommit = true,
        long expectedGeneration = 0,
        CancellationToken cancellationToken = default
    ) => replicationGateway.ReplicateLogs(partitionId, type, logs, autoCommit, expectedGeneration, cancellationToken);

    /// <summary>
    /// Replicates a heterogeneous, per-entry-typed batch to one partition
    /// (<see cref="IRaft.ReplicateEntries"/>): a leading auto-commit group plus an optional single
    /// trailing manual group, with a per-entry generation fence.
    /// </summary>
    public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) =>
        replicationGateway.ReplicateEntries(partitionId, entries, cancellationToken);

    /// <summary>
    /// Commit logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) =>
        replicationGateway.CommitLogs(partitionId, ticketId, cancellationToken);

    /// <summary>
    /// Rollback logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="ticketId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) =>
        replicationGateway.RollbackLogs(partitionId, ticketId, cancellationToken);

    /// <summary>
    /// Replicates a checkpoint to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) =>
        replicationGateway.ReplicateCheckpoint(partitionId, cancellationToken);

    // ── Event dispatch — bodies live in RaftEventNotifier ─────────────────

    /// <summary>Fires <see cref="OnRestoreStarted"/> for the given partition.</summary>
    internal void InvokeRestoreStarted(int partitionId) =>
        eventNotifier.InvokeRestoreStarted(partitionId);

    /// <summary>Fires <see cref="OnRestoreFinished"/> for the given partition.</summary>
    internal void InvokeRestoreFinished(int partitionId) =>
        eventNotifier.InvokeRestoreFinished(partitionId);

    /// <summary>Fires <see cref="OnSystemRestoreFinished"/> for the given partition.</summary>
    internal void InvokeSystemRestoreFinished(int partitionId) =>
        eventNotifier.InvokeSystemRestoreFinished(partitionId);

    /// <summary>Fires <see cref="OnReplicationError"/> for the given partition and log entry.</summary>
    internal void InvokeReplicationError(int partitionId, RaftLog log) =>
        eventNotifier.InvokeReplicationError(partitionId, log);

    /// <summary>Fires <see cref="OnReplicationReceived"/> and returns the handler result.</summary>
    internal Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) =>
        eventNotifier.InvokeReplicationReceived(partitionId, log);

    /// <summary>Fires <see cref="OnSystemReplicationReceived"/> and returns the handler result.</summary>
    internal Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) =>
        eventNotifier.InvokeSystemReplicationReceived(partitionId, log);

    /// <summary>Fires <see cref="OnSystemLogRestored"/> and returns the handler result.</summary>
    internal Task<bool> InvokeSystemLogRestored(int partitionId, RaftLog log) =>
        eventNotifier.InvokeSystemLogRestored(partitionId, log);

    /// <summary>Fires <see cref="OnLogRestored"/> and returns the handler result.</summary>
    internal Task<bool> InvokeLogRestored(int partitionId, RaftLog log) =>
        eventNotifier.InvokeLogRestored(partitionId, log);

    /// <summary>Fires <see cref="OnLeaderChanged"/> and returns the handler result.</summary>
    internal Task<bool> InvokeLeaderChanged(int partitionId, string node) =>
        eventNotifier.InvokeLeaderChanged(partitionId, node);

    /// <summary>
    /// Returns the local endpoint
    /// </summary>
    /// <returns></returns>
    public string GetLocalEndpoint()
    {
        return LocalEndpoint;
    }
    
    /// <summary>
    /// Returns the local node id
    /// </summary>
    /// <returns></returns>
    public int GetLocalNodeId()
    {
        return LocalNodeId;
    }

    /// <summary>
    /// Returns the local node id
    /// </summary>
    /// <returns></returns>
    public string GetLocalNodeName()
    {
        return LocalNodeName;
    }

    /// <summary>
    /// Checks if the local node is the leader in the given partition. Throws the typed
    /// <see cref="PartitionNotHostedException"/> for a committed range this node does not host
    /// (the documented routing contract — see <c>TestPartitionNotHosted</c>), so callers that
    /// legitimately ask about non-hosted ranges (e.g. the P0 placement controller) must gate on
    /// <see cref="HostsPartition"/> first instead of calling blind.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    public ValueTask<bool> AmILeaderQuick(int partitionId) => leadershipService.AmILeaderQuick(partitionId);

    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="OperationCanceledException"></exception>
    /// <exception cref="RaftException"></exception>
    public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) =>
        leadershipService.AmILeader(partitionId, cancellationToken);

    /// <summary>
    /// Read-index leadership confirmation for the given partition — see
    /// <see cref="IRaft.ConfirmLeadershipAsync"/> for the contract.
    /// </summary>
    public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) =>
        leadershipService.ConfirmLeadershipAsync(partitionId, cancellationToken);

    /// <summary>
    /// Follower catch-up confirmation — see <see cref="IRaft.ConfirmLocalApplicationAsync"/> for
    /// the contract.
    /// </summary>
    public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default) =>
        leadershipService.ConfirmLocalApplicationAsync(partitionId, cancellationToken);

    /// <summary>
    /// Serves a non-leader's read-index fetch (<see cref="ICommunication.GetReadIndex"/>): runs
    /// this node's read-index confirmation round for the partition and returns the captured
    /// commit index on success.
    /// </summary>
    public ValueTask<GetReadIndexResponse> ReceiveGetReadIndex(GetReadIndexRequest request, CancellationToken cancellationToken = default) =>
        leadershipService.ReceiveGetReadIndex(request, cancellationToken);

    /// <summary>
    /// Waits for the leader to be elected in the given partition.
    /// If the leader is already elected, it returns the leader.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) =>
        leadershipService.WaitForLeader(partitionId, cancellationToken);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        CancellationToken cancellationToken = default) =>
        leadershipService.WaitForLeaderStableAsync(partitionId, minStableFor, cancellationToken);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        leadershipService.WaitForLeaderStableAsync(partitionId, minStableFor, timeout, cancellationToken);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> ForceLeaderForTestingAsync(
        int partitionId,
        CancellationToken cancellationToken = default) =>
        leadershipService.ForceLeaderForTestingAsync(partitionId, cancellationToken);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> StepDownAsync(
        int partitionId,
        CancellationToken cancellationToken = default) =>
        leadershipService.StepDownAsync(partitionId, cancellationToken);

    /// <summary>
    /// Relinquishes leadership of <paramref name="partitionId"/> WITHOUT waiting for a successor to be
    /// elected. Used only by the self-removal teardown path: a node the committed roster has already
    /// dropped from the voter set is shutting down and must not block on a handoff.
    /// </summary>
    internal Task<RaftOperationStatus> StepDownWithoutSuccessorWaitAsync(
        int partitionId,
        CancellationToken cancellationToken = default) =>
        leadershipService.StepDownWithoutSuccessorWaitAsync(partitionId, cancellationToken);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> TransferLeadershipAsync(
        int partitionId,
        string targetEndpoint,
        CancellationToken cancellationToken = default) =>
        leadershipService.TransferLeadershipAsync(partitionId, targetEndpoint, cancellationToken);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> SuspendHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default) =>
        leadershipService.SuspendHeartbeatsAsync(partitionId, cancellationToken);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task<RaftOperationStatus> ResumeHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default) =>
        leadershipService.ResumeHeartbeatsAsync(partitionId, cancellationToken);

    /// <summary>
    /// Queues a request to split a partition. Splitting is an asynchronous
    /// operation initiated by the leader of the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <exception cref="RaftException"></exception>
    public async Task SplitPartition(int partitionId)
    {
        await SplitPartitionAsync(partitionId, ct: CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(
        int sourcePartitionId,
        int targetPartitionId = 0,
        RaftSplitPlan? plan = null,
        CancellationToken ct = default) =>
        lifecycleService.SplitPartitionAsync(sourcePartitionId, targetPartitionId, plan, ct);

    /// <inheritdoc/>
    public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(
        int survivorPartitionId,
        int sourcePartitionId,
        RaftMergePlan? plan = null,
        CancellationToken ct = default) =>
        lifecycleService.MergePartitionsAsync(survivorPartitionId, sourcePartitionId, plan, ct);

    /// <inheritdoc/>
    public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(
        int partitionId,
        RaftRoutingMode mode = RaftRoutingMode.Unrouted,
        (int start, int end)? hashRange = null,
        CancellationToken ct = default) =>
        lifecycleService.CreatePartitionAsync(partitionId, mode, hashRange, ct);

    /// <inheritdoc/>
    public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(
        int partitionId,
        CancellationToken ct = default) =>
        lifecycleService.RemovePartitionAsync(partitionId, ct);

    /// <inheritdoc/>
    public Task<RaftPartitionLifecycleResult> SetReplicationFactorAsync(
        int partitionId,
        int replicationFactor,
        CancellationToken ct = default) =>
        lifecycleService.SetReplicationFactorAsync(partitionId, replicationFactor, ct);

    /// <summary>
    /// Commits an <c>AddReplica</c> mutation: adds <paramref name="endpoint"/> as a Learner
    /// replica of the range. P0-leader-only. Test/operator surface; automatic placement uses
    /// the controller.
    /// </summary>
    internal Task<RaftPartitionLifecycleResult> AddReplicaAsync(
        int partitionId, string endpoint, int nodeId = 0, CancellationToken ct = default) =>
        lifecycleService.ChangeReplicaAsync(System.RaftSystemRequestType.AddReplica, partitionId, endpoint, nodeId, ct);

    /// <summary>
    /// Commits a <c>PromoteReplica</c> mutation: promotes a Learner replica of the range to
    /// Voter — the commit point at which it enters the range's quorum. P0-leader-only.
    /// </summary>
    internal Task<RaftPartitionLifecycleResult> PromoteReplicaAsync(
        int partitionId, string endpoint, int nodeId = 0, CancellationToken ct = default) =>
        lifecycleService.ChangeReplicaAsync(System.RaftSystemRequestType.PromoteReplica, partitionId, endpoint, nodeId, ct);

    /// <summary>
    /// Commits the two-step <c>RemoveReplica</c> mutation: the replica is first marked Removing
    /// (leaving quorum), then dropped; the departing node drains and reclaims its WAL.
    /// P0-leader-only.
    /// </summary>
    internal Task<RaftPartitionLifecycleResult> RemoveReplicaAsync(
        int partitionId, string endpoint, int nodeId = 0, CancellationToken ct = default) =>
        lifecycleService.ChangeReplicaAsync(System.RaftSystemRequestType.RemoveReplica, partitionId, endpoint, nodeId, ct);

    /// <inheritdoc/>
    public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) =>
        Volatile.Write(ref _stateMachineTransfer, transfer);

    /// <inheritdoc/>
    public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) =>
        Volatile.Write(ref _systemStateTransfer, transfer);

    /// <inheritdoc/>
    public void RegisterPartitionStateTransfer(IRaftPartitionStateTransfer? transfer) =>
        Volatile.Write(ref _partitionStateTransfer, transfer);

    /// <summary>
    /// Called by the P0 coordinator when it determines that <paramref name="endpoint"/> can never
    /// be promoted (e.g., below WAL compaction floor with no snapshot transfer registered).
    /// <c>JoinCluster(seeds)</c> polls this on the local endpoint and throws
    /// <see cref="InvalidOperationException"/> immediately rather than spinning to the timeout.
    /// </summary>
    internal void SetJoinTerminalReason(string endpoint, string reason) =>
        joinService.SetJoinTerminalReason(endpoint, reason);

    internal string? GetJoinTerminalReason(string endpoint) =>
        joinService.GetJoinTerminalReason(endpoint);

    /// <inheritdoc/>
    public long GetPartitionGeneration(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return partition.Generation;

        // Non-hosted ranges (per-partition placement) still expose the committed generation so
        // callers on non-replica nodes can build a correctly-fenced forwarded proposal.
        return routingTable.GetCommittedGeneration(partitionId);
    }

    /// <inheritdoc/>
    public double GetPartitionLogOpsPerSecond(int partitionId) =>
        loadReportService.GetPartitionLogOpsPerSecond(partitionId);

    /// <inheritdoc/>
    public int GetPartitionWalQueueDepth(int partitionId) =>
        loadReportService.GetPartitionWalQueueDepth(partitionId);

    /// <inheritdoc/>
    public double GetPartitionCommitWaitMs(int partitionId) =>
        loadReportService.GetPartitionCommitWaitMs(partitionId);

    /// <inheritdoc/>
    public string? GetPartitionLeaderHint(int partitionId) =>
        loadReportService.GetPartitionLeaderHint(partitionId);

    /// <inheritdoc/>
    public System.ClusterMembership GetMembership() => systemCoordinator.GetMembership();

    /// <summary>
    /// Fires <see cref="OnMembershipChanged"/> with the new roster and checks whether this
    /// node has been removed from it, which triggers the auto-rejoin driver.
    /// Called by <see cref="RaftSystemCoordinator"/> each time <c>_cachedMembership</c>
    /// advances to a strictly higher version.
    /// </summary>
    internal void RaiseMembershipChanged(System.ClusterMembership membership) =>
        membershipChangeHandler.RaiseMembershipChanged(membership);

    /// <summary>
    /// Last-chance liveness check used by the eviction path: one direct ping bounded by
    /// <see cref="RaftConfiguration.PingTimeout"/>; a response resurrects the endpoint in the
    /// liveness table. See <see cref="GossipService.ProbeAndResurrectAsync"/>.
    /// </summary>
    internal Task<bool> ProbeEndpointAliveAsync(string endpoint, CancellationToken cancellationToken = default) =>
        gossipService.ProbeAndResurrectAsync(this, endpoint, cancellationToken);

    /// <inheritdoc/>
    public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => routingTable.GetPartitionMap();

    /// <inheritdoc/>
    public int GetNextAvailablePartitionId() => routingTable.GetNextAvailablePartitionId();

    /// <summary>
    /// Returns the number of the partition for the given partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPartitionKey(string partitionKey) => routingTable.GetPartitionKey(partitionKey);

    /// <summary>
    /// Returns the number of the partition for the given prefix partition key
    /// </summary>
    /// <param name="prefixPartitionKey"></param>
    /// <returns></returns>
    public int GetPrefixPartitionKey(string prefixPartitionKey) => routingTable.GetPrefixPartitionKey(prefixPartitionKey);
    
    internal void EnqueueResponse(string endpoint, RaftResponderRequest request) =>
        rpcRouter.EnqueueResponse(endpoint, request);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // 1. Stop and dispose the timer so no new work is injected and both
        //    Timer instances are released without waiting for GC.
        timerService.Dispose();
        snapshotReceiver.DisposePendingSnapshots();

        // 2. Drain partition queues before stopping shared schedulers. Then stop the
        //    I/O schedulers while executors are still alive, so accepted WAL work can
        //    post completions back into the owning executor. Drain once more to process
        //    those completion messages before executor threads are joined.
        DrainPartitions(CancellationToken.None).GetAwaiter().GetResult();

        readScheduler.Stop();
        walScheduler.Stop();

        DrainPartitions(CancellationToken.None).GetAwaiter().GetResult();

        foreach (RaftPartition partition in partitions.Values)
            partition.Dispose();

        systemPartition?.Dispose();

        // All partition executors have been stopped (by Dispose above); safe to stop
        // and dispose the shared executor pool now.
        executorPool?.Dispose();

        // 3. Dispose the transport dispatcher now that all partition executors have
        //    stopped; workers drain buffered responses then are hard-aborted.
        transportDispatcher.Dispose();

        // Dispose the system coordinator after the dispatcher (no more system events).
        systemCoordinator.Dispose();

        // 4. Dispose I/O schedulers after they have already been stopped above.
        readScheduler.Dispose();
        walScheduler.Dispose();

        // 4. Dispose remaining shared resources.
        hybridLogicalClock.Dispose();
        walAdapter.Dispose();
        leaveService.Dispose();

        if (discovery is IDisposable disposableDiscovery)
            disposableDiscovery.Dispose();
    }
}
