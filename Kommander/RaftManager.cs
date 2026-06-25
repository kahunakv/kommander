
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
public sealed class RaftManager : IRaft, Scheduling.IRaftTimerHost, IDisposable
{
    private static readonly TimeSpan ProposalRetryDelay = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan ProposalStatusPollDelay = TimeSpan.FromMilliseconds(10);

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

    /// <summary>
    /// Per-endpoint terminal reasons set by the P0 leader when it determines a learner can
    /// never be promoted (e.g., below the WAL compaction floor with no snapshot transfer).
    /// The entry for the local endpoint is checked inside <see cref="JoinCluster(IEnumerable{string}, CancellationToken)"/>
    /// so the joiner fails fast with a descriptive error rather than spinning to the timeout.
    /// Written by the coordinator, read by the joining node's <c>JoinCluster</c> loop.
    /// Only the local node's entry matters on the follower side; leader entries are noise-free
    /// because the leader never waits for its own promotion.
    /// </summary>
    private readonly ConcurrentDictionary<string, string> _joinTerminalReasons = new();

    /// <summary>
    /// In-progress snapshot receive sessions keyed by <see cref="SnapshotRequest.SessionId"/>.
    /// Each value is a <see cref="MemoryStream"/> accumulating chunks until <see cref="SnapshotRequest.IsLast"/>
    /// triggers the final <c>ImportRange</c> call.  Sessions are removed on completion or on any
    /// error so a retry always starts a clean accumulation.
    /// </summary>
    private readonly ConcurrentDictionary<string, MemoryStream> _pendingSnapshots = new();

    /// <summary>
    /// SWIM failure-detector liveness table for this node.
    /// Tracks Alive/Suspect/Dead state for all known peers.
    /// The P0 leader uses this table to decide which members to evict.
    /// </summary>
    internal readonly LivenessTable Liveness = new();

    /// <summary>
    /// Optional snapshot-transfer implementation registered by the application.
    /// Accessed by <see cref="RaftSystemCoordinator"/> during <c>TrySplitPartition</c>.
    /// Null when the application has not registered one (log-shipping fallback).
    /// </summary>
    internal IRaftStateMachineTransfer? StateMachineTransfer => Volatile.Read(ref _stateMachineTransfer);

    private readonly ConcurrentDictionary<(string endpoint, int partitionId), HLCTimestamp> lastActivity = new();

    private readonly ConcurrentDictionary<(string endpoint, int partitionId), HLCTimestamp> lastHearthBeat = new();

    // Monotonically increasing counter for NodeLoadReport.ReportVersion.
    // Incremented on every BuildLocalLoadReport call; never reset.
    private long _reportVersion;
    
    private readonly Communication.RaftTransportDispatcher transportDispatcher;

    /// <summary>
    /// Allows to retrieve the list of known nodes within the Raft cluster
    /// </summary>
    internal List<RaftNode> Nodes { get; set; } = [];

    /// <summary>
    /// Set to true by <see cref="LeaveCluster"/> before the removal is committed so that
    /// <see cref="LocalRole"/> immediately returns <see cref="System.ClusterMemberRole.Leaving"/>
    /// and the election / pre-vote gate suppresses campaigning on all partitions.
    /// </summary>
    private volatile bool _leaving;

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
            if (_leaving)
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

    /// <summary>
    /// Event when the restore process starts
    /// </summary>
    public event Action<int>? OnRestoreStarted;

    /// <summary>
    /// Event when the restore process finishes from a user partition
    /// </summary>
    public event Action<int>? OnRestoreFinished;

    /// <summary>
    /// Event when the restore process finishes from a system partition
    /// </summary>
    public event Action<int>? OnSystemRestoreFinished;

    /// <summary>
    /// Event when a replication log is now acknowledged by the application
    /// </summary>
    public event Action<int, RaftLog>? OnReplicationError;

    /// <summary>
    /// Event when a replication log is restored from a user partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnLogRestored;

    /// <summary>
    /// Event when a replication log is restored from a system partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnSystemLogRestored;

    /// <summary>
    /// Event when a replication log is received from a user partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived;

    /// <summary>
    /// Event when a replication log is received from a system partition
    /// </summary>
    public event Func<int, RaftLog, Task<bool>>? OnSystemReplicationReceived;

    /// <summary>
    /// Event called when a leader is elected on certain partition
    /// </summary>
    public event Func<int, string, Task<bool>>? OnLeaderChanged;

    /// <inheritdoc/>
    public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged;

    /// <inheritdoc/>
    public event Action<System.ClusterMembership>? OnMembershipChanged;

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

        clusterHandler = new(this, discovery);

        systemCoordinator = new RaftSystemCoordinator(this, Logger);
        timerService = new RaftTimerService(this, Logger, configuration);
        timerService.Start();

        transportDispatcher = new Communication.RaftTransportDispatcher(this, communication, Logger);

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
    /// Joins the cluster
    /// </summary>
    public async Task JoinCluster(CancellationToken cancellationToken = default)
    {
        // Registers itself at the discovery service
        await clusterHandler.JoinCluster(configuration).ConfigureAwait(false);

        if (systemPartition is null)
        {
            executorPool?.Start();
            readScheduler.Start();
            walScheduler.Start();

            // Add system partition
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

        // Wait for the system coordinator to replicate the initial partition map and call
        // StartUserPartitions. On a slow or loaded host this can take longer than expected;
        // impose an explicit deadline so JoinCluster never blocks indefinitely. The 60 s
        // hard timeout fires only when the caller does not supply their own cancellation.
        ValueStopwatch joinStopwatch = ValueStopwatch.StartNew();
        while (!IsInitialized)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (joinStopwatch.GetElapsedMilliseconds() > 60_000)
                throw new TimeoutException("RaftManager.JoinCluster timed out after 60 s waiting for cluster initialization. The system partition may have failed to elect a leader.");

            await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Seed-based join: contacts each seed in turn until this node is admitted as a Learner,
    /// then waits for automatic promotion to Voter (same 60 s deadline as <see cref="JoinCluster(CancellationToken)"/>).
    /// <para>
    /// Unlike the discovery-based overload this variant does NOT call <see cref="IDiscovery.Register"/>;
    /// it relies on the P0 leader learning of the new node via the committed roster entry so that
    /// the leader's next <c>UpdateNodes</c> tick includes this endpoint in its peer set and begins
    /// sending P0 heartbeats.  Once the system-partition logs are replicated the coordinator calls
    /// <see cref="StartUserPartitions"/> and <see cref="IsInitialized"/> becomes <c>true</c>.
    /// </para>
    /// </summary>
    public async Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default)
    {
        // Start schedulers and the system partition exactly as the discovery-based join does.
        if (systemPartition is null)
        {
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

        // Mark as joined so timer UpdateNodes ticks start firing once the roster has us.
        clusterHandler.MarkJoined();

        // Contact seeds until a P0 leader accepts us as a Learner.
        JoinResponse? accepted = null;
        ValueStopwatch joinStopwatch = ValueStopwatch.StartNew();

        List<string> seedList = seeds.ToList();

        while (accepted is null || !accepted.Success)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > 60_000)
                throw new TimeoutException("RaftManager.JoinCluster(seeds) timed out after 60 s before being admitted as a Learner.");
            string? leaderHint = null;

            foreach (string seed in seedList)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string target = leaderHint ?? seed;
                RaftNode node = new(target);
                try
                {
                    JoinResponse resp = await communication.SendJoin(this, node, new JoinRequest(LocalEndpoint, LocalNodeId)).ConfigureAwait(false);
                    if (resp.Success)
                    {
                        accepted = resp;
                        break;
                    }
                    if (!string.IsNullOrEmpty(resp.LeaderHint))
                    {
                        leaderHint = resp.LeaderHint;
                        // Retry immediately against the leader hint.
                        try
                        {
                            RaftNode leaderNode = new(leaderHint);
                            resp = await communication.SendJoin(this, leaderNode, new JoinRequest(LocalEndpoint, LocalNodeId)).ConfigureAwait(false);
                            if (resp.Success)
                            {
                                accepted = resp;
                                break;
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning("JoinCluster: failed to contact leader hint {Hint}: {Message}", leaderHint, ex.Message);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogWarning("JoinCluster: failed to contact seed {Seed}: {Message}", seed, ex.Message);
                }
            }

            if (accepted is null || !accepted.Success)
                await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }

        Logger.LogInfoJoinClusterAdmittedAsLearner(accepted.MembershipVersion);

        // Wait for IsInitialized (system coordinator receives partition map from P0 leader).
        while (!IsInitialized)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > 60_000)
                throw new TimeoutException("RaftManager.JoinCluster(seeds) timed out after 60 s waiting for cluster initialization.");
            await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }

        // Wait for promotion to Voter.
        while (LocalRole != System.ClusterMemberRole.Voter)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > 60_000)
                throw new TimeoutException("RaftManager.JoinCluster(seeds) timed out after 60 s waiting for Voter promotion.");

            // The P0 leader signals a terminal condition (e.g., learner is below the WAL
            // compaction floor and no snapshot transfer is registered) so the joiner fails
            // fast with a descriptive error rather than spinning to the 60 s deadline.
            string? terminalReason = GetJoinTerminalReason(LocalEndpoint);
            if (terminalReason is not null)
                throw new InvalidOperationException($"RaftManager.JoinCluster: promotion permanently blocked — {terminalReason}");

            await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }

        Logger.LogInformation("JoinCluster: promoted to Voter; join complete");
    }

    /// <summary>
    /// Handles an inbound <see cref="JoinRequest"/> from a joining node.
    /// <para>
    /// If this node is the P0 leader it commits the joiner as a <see cref="ClusterMemberRole.Learner"/>
    /// and returns <see cref="JoinResponse.Success"/> = <c>true</c>.
    /// Otherwise it returns <see cref="JoinResponse.Success"/> = <c>false</c> with the current
    /// P0 leader endpoint in <see cref="JoinResponse.LeaderHint"/> so the caller can retry directly
    /// against the leader.
    /// </para>
    /// <para>
    /// <b>Idempotency:</b> if the endpoint is already committed in the roster (e.g. a previous
    /// <c>AddMember</c> committed but the response was lost before the joiner saw it), this
    /// method returns <c>Success</c> with the current roster version rather than an error.  This
    /// prevents the joiner's retry loop from spinning to the 60 s timeout.
    /// </para>
    /// </summary>
    public async Task<JoinResponse> ReceiveJoin(JoinRequest request)
    {
        if (systemPartition is null || !IsInitialized)
            return new JoinResponse(false);

        bool isLeader = await AmILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!isLeader)
        {
            string leaderHint = systemPartition.Leader;
            return new JoinResponse(false, string.IsNullOrEmpty(leaderHint) ? null : leaderHint);
        }

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new System.RaftSystemRequest(
            System.RaftSystemRequestType.AddMember,
            request.Endpoint,
            request.NodeId,
            systemCoordinator.GetMembership().MembershipVersion,
            tcs));

        (RaftOperationStatus status, long version) = await tcs.Task.ConfigureAwait(false);

        if (status == RaftOperationStatus.Success)
            return new JoinResponse(true, null, version);

        // Another membership change was in flight or version mismatch — return current leader so
        // the caller can retry after a brief backoff.
        return new JoinResponse(false, LocalEndpoint);
    }

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
    public async Task<LeaveResponse> ReceiveLeave(LeaveRequest request, CancellationToken cancellationToken = default)
    {
        // Fail-fast when this node has already stopped — the coordinator channel is closed
        // and posting to it would block until the per-attempt CTS fires (3 s per attempt).
        if (systemCoordinator.IsStopped)
            return new LeaveResponse(false);

        if (systemPartition is null || !IsInitialized)
            return new LeaveResponse(false);

        bool isLeader = await AmILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!isLeader)
        {
            string leaderHint = systemPartition.Leader;
            return new LeaveResponse(false, string.IsNullOrEmpty(leaderHint) ? null : leaderHint);
        }

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Cancel the TCS if the caller's token fires (e.g., per-attempt timeout) so this
        // method never blocks indefinitely when the coordinator has already been stopped.
        await using CancellationTokenRegistration reg = cancellationToken.Register(
            () => tcs.TrySetCanceled(cancellationToken));

        systemCoordinator.Send(new System.RaftSystemRequest(
            System.RaftSystemRequestType.RemoveMember,
            request.Endpoint,
            request.NodeId,
            systemCoordinator.GetMembership().MembershipVersion,
            tcs));

        try
        {
            (RaftOperationStatus status, long _) = await tcs.Task.ConfigureAwait(false);

            if (status == RaftOperationStatus.Success)
                return new LeaveResponse(true);

            // InsufficientVoters is a permanent condition: removal would brick the cluster.
            // Terminal=true prevents CommitGracefulLeaveAsync from retrying.
            if (status == RaftOperationStatus.InsufficientVoters)
                return new LeaveResponse(false, null, Terminal: true);
        }
        catch (OperationCanceledException)
        {
            // Per-attempt timeout or caller cancelled — return false so caller retries or gives up.
        }

        // Concurrent change, stale version, or timeout — caller retries with current leader.
        return new LeaveResponse(false, LocalEndpoint);
    }

    /// <summary>
    /// Installs a partition snapshot received from the partition leader.
    /// Called on a follower when the leader delivers one chunk of a snapshot transfer.
    ///
    /// <para>Large snapshots are split into bounded chunks by the sender.  Each chunk carries a
    /// <see cref="SnapshotRequest.SessionId"/> that identifies the transfer session; this method
    /// accumulates chunks in <see cref="_pendingSnapshots"/> until <see cref="SnapshotRequest.IsLast"/>
    /// is true, then delegates to <see cref="IRaftStateMachineTransfer.ImportRange"/> and seeds the
    /// WAL with a <c>CommittedCheckpoint</c> entry at <see cref="SnapshotRequest.SnapshotIndex"/>
    /// so that normal backfill can resume from there.</para>
    ///
    /// <para>The method is idempotent at the session boundary: if the local WAL already reflects
    /// <see cref="SnapshotRequest.SnapshotIndex"/> or higher, every chunk for that transfer returns
    /// success immediately.  On any error the partial session is removed so a retry starts clean.</para>
    /// </summary>
    public async Task<SnapshotResponse> ReceiveInstallSnapshot(
        SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        IRaftStateMachineTransfer? transfer = Volatile.Read(ref _stateMachineTransfer);
        if (transfer is null)
            return new SnapshotResponse(false);

        // Idempotency: already at or past the snapshot index — accept without re-importing.
        long currentMax = walAdapter.GetMaxLog(request.PartitionId);
        if (currentMax >= request.SnapshotIndex)
            return new SnapshotResponse(true);

        // Accumulate the chunk into the session buffer.
        MemoryStream buf = _pendingSnapshots.GetOrAdd(request.SessionId, _ => new MemoryStream());
        await buf.WriteAsync(request.Data, cancellationToken).ConfigureAwait(false);

        // Non-final chunks are accepted; wait for IsLast before applying.
        if (!request.IsLast)
            return new SnapshotResponse(true);

        // All chunks received — apply and clean up session state.
        _pendingSnapshots.TryRemove(request.SessionId, out _);
        buf.Position = 0;

        try
        {
            await transfer.ImportRange(request.PartitionId, buf, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                "[{Endpoint}] ReceiveInstallSnapshot: ImportRange partition={PartitionId} index={Index} failed: {Message}",
                LocalEndpoint, request.PartitionId, request.SnapshotIndex, ex.Message);
            return new SnapshotResponse(false);
        }
        finally
        {
            await buf.DisposeAsync().ConfigureAwait(false);
        }

        // Seed the WAL with a CommittedCheckpoint at the snapshot index so GetMaxLog reflects
        // the installed position and normal backfill can resume from snapshotIndex + 1.
        RaftLog checkpointLog = new()
        {
            Id = request.SnapshotIndex,
            Type = RaftLogType.CommittedCheckpoint,
            Term = walAdapter.GetCurrentTerm(request.PartitionId),
        };

        walAdapter.Write([(request.PartitionId, [checkpointLog])]);

        Logger.LogInfoReceiveInstallSnapshot(LocalEndpoint, request.PartitionId, request.SnapshotIndex);

        return new SnapshotResponse(true);
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
    public GossipAck ReceiveGossip(GossipMessage digest)
    {
        ClusterMembership local = systemCoordinator.GetMembership();

        // If peer has a newer committed roster, queue an update to our local cache.
        if (digest.Roster is not null && digest.MembershipVersion > local.MembershipVersion)
            systemCoordinator.Send(new System.RaftSystemRequest(digest.Roster));

        // Apply piggybacked liveness updates; refute self-suspicion if required.
        // Wake quiesced partitions for any peer that gossip reports as Suspect/Dead so
        // their failover is bounded by gossip propagation latency, not the safety sweep.
        (bool selfSuspected, IReadOnlyList<string> newlySuspectOrDead) =
            Liveness.ApplyUpdates(LocalEndpoint, digest.LivenessUpdates);

        if (selfSuspected)
        {
            long refutedInc = Liveness.RefuteSuspicion(LocalEndpoint);
            Logger.LogInfoReceiveGossipRefuting(refutedInc);
        }

        foreach (string suspectEndpoint in newlySuspectOrDead)
            WakePartitionsForLeader(suspectEndpoint);

        // Store advisory load report when the balancer is enabled.
        if (configuration.EnableLeaderBalancer && digest.LoadReport is { } report)
            systemCoordinator.Send(new System.RaftSystemRequest(report));

        // Respond with our current roster so the sender can catch up if we are ahead.
        bool includeRoster = local.MembershipVersion > digest.MembershipVersion;
        return new GossipAck(local.MembershipVersion, includeRoster ? local : null);
    }

    /// <summary>
    /// Runs one gossip round: contacts up to <see cref="RaftConfiguration.GossipFanout"/>
    /// randomly chosen peers from the current node list, sends a <see cref="GossipMessage"/>
    /// carrying the locally committed roster and piggybacked liveness state, and applies any
    /// newer roster returned in the ACK.
    /// <para>
    /// Called by <see cref="RaftTimerService.TriggerGossip"/> on a periodic timer.  Tests
    /// may call it directly to drive gossip deterministically without waiting for the timer.
    /// </para>
    /// </summary>
    public async Task GossipAsync(CancellationToken cancellationToken = default)
    {
        ClusterMembership membership = systemCoordinator.GetMembership();

        // Always store the local load report so the local coordinator's view is always
        // complete, even when gossip is skipped due to uninitialized membership.
        // The report must be stored before the membership/fanout guards so test-driven
        // drains always have fresh data.
        System.NodeLoadReport? localReport = null;
        if (configuration.EnableLeaderBalancer)
        {
            localReport = BuildLocalLoadReport();
            systemCoordinator.Send(new System.RaftSystemRequest(localReport));
        }

        if (membership.MembershipVersion == 0 || configuration.GossipFanout <= 0)
            return;

        List<RaftNode> peers = [..Nodes];
        if (peers.Count == 0)
            return;

        IReadOnlyList<MemberLivenessEntry> livenessUpdates = Liveness.GetAll();
        GossipMessage digest = new(LocalEndpoint, membership.MembershipVersion, membership)
        {
            LivenessUpdates = livenessUpdates.Count > 0 ? livenessUpdates : null,
            LoadReport = localReport,
        };

        int fanout = Math.Min(configuration.GossipFanout, peers.Count);

        // Partial Fisher-Yates shuffle to pick fanout random peers.
        for (int i = 0; i < fanout; i++)
        {
            int j = Random.Shared.Next(i, peers.Count);
            (peers[i], peers[j]) = (peers[j], peers[i]);
        }

        for (int i = 0; i < fanout; i++)
        {
            try
            {
                GossipAck ack = await communication.SendGossip(this, peers[i], digest, cancellationToken).ConfigureAwait(false);

                // If peer returned a newer roster, update our local cache.
                if (ack.Roster is not null && ack.MembershipVersion > systemCoordinator.GetMembership().MembershipVersion)
                    systemCoordinator.Send(new System.RaftSystemRequest(ack.Roster));
            }
            catch (Exception ex)
            {
                Logger.LogDebugGossipAsyncFailed(peers[i].Endpoint, ex.Message);
            }
        }
    }

    /// <summary>
    /// Handles an inbound SWIM direct probe from a peer.
    /// The receiver is always alive if it can process the message; it returns its current
    /// incarnation so the sender can record an up-to-date Alive entry.
    /// If our own liveness entry is Suspect (e.g. carried in a prior gossip round), we
    /// refute immediately by bumping our incarnation.
    /// </summary>
    public GossipPingResponse ReceivePing(GossipPingRequest request)
    {
        long incarnation = Liveness.GetSelfIncarnation();
        // If we've been marked Suspect by another node's gossip reaching us, refute.
        if (Liveness.GetState(LocalEndpoint) >= MemberLivenessState.Suspect)
            incarnation = Liveness.RefuteSuspicion(LocalEndpoint);
        return new GossipPingResponse(true, incarnation);
    }

    /// <summary>
    /// Handles an inbound SWIM indirect probe request: relays a direct
    /// <see cref="GossipPingRequest"/> to <see cref="GossipPingReqRequest.TargetEndpoint"/> and reports
    /// whether the target was reachable within <c>PingTimeout</c>.
    /// </summary>
    public async Task<GossipPingReqResponse> ReceivePingReq(GossipPingReqRequest request, CancellationToken cancellationToken = default)
    {
        RaftNode? targetNode = Nodes.FirstOrDefault(n => n.Endpoint == request.TargetEndpoint);
        if (targetNode is null)
            return new GossipPingReqResponse(false);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(configuration.PingTimeout);

        try
        {
            GossipPingResponse resp = await communication.SendPing(this, targetNode, new GossipPingRequest(LocalEndpoint), cts.Token).ConfigureAwait(false);
            return new GossipPingReqResponse(resp.Alive);
        }
        catch
        {
            return new GossipPingReqResponse(false);
        }
    }

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
    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        // Advance Suspect → Dead before processing this round's probe results.
        // Wake quiesced partitions for any newly-Dead leaders so they detect failure within
        // the next CheckLeader tick rather than waiting for the coarse safety sweep.
        IReadOnlyList<string> newlyDead = Liveness.AdvanceExpiry(DateTimeOffset.UtcNow, configuration.SuspicionTimeout);
        foreach (string deadEndpoint in newlyDead)
            WakePartitionsForLeader(deadEndpoint);

        ClusterMembership membership = systemCoordinator.GetMembership();
        if (membership.MembershipVersion == 0)
            return;

        List<RaftNode> peers = [..Nodes];
        if (peers.Count == 0)
            return;

        // Pick one random peer to probe this round.
        int idx = Random.Shared.Next(peers.Count);
        RaftNode target = peers[idx];

        // Direct probe.
        bool alive = false;
        long incarnation = 0;

        using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
        {
            cts.CancelAfter(configuration.PingTimeout);
            try
            {
                GossipPingResponse resp = await communication.SendPing(this, target, new GossipPingRequest(LocalEndpoint), cts.Token).ConfigureAwait(false);
                alive = resp.Alive;
                incarnation = resp.Incarnation;
            }
            catch
            {
                alive = false;
            }
        }

        if (alive)
        {
            Liveness.MarkAlive(target.Endpoint, incarnation);
            return;
        }

        // Indirect probe via up to IndirectPingFanout intermediaries.
        List<RaftNode> relays = peers.Where(p => p.Endpoint != target.Endpoint).ToList();
        int fanout = Math.Min(configuration.IndirectPingFanout, relays.Count);

        // Partial Fisher-Yates shuffle for relay selection.
        for (int i = 0; i < fanout; i++)
        {
            int j = Random.Shared.Next(i, relays.Count);
            (relays[i], relays[j]) = (relays[j], relays[i]);
        }

        bool reachedViaRelay = false;
        using (CancellationTokenSource relayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
        {
            relayCts.CancelAfter(configuration.PingTimeout);
            for (int i = 0; i < fanout && !reachedViaRelay; i++)
            {
                try
                {
                    GossipPingReqResponse relayResp = await communication.SendPingReq(
                        this, relays[i],
                        new GossipPingReqRequest(LocalEndpoint, target.Endpoint),
                        relayCts.Token).ConfigureAwait(false);
                    if (relayResp.Reached)
                        reachedViaRelay = true;
                }
                catch
                {
                    // relay also unreachable — continue to next
                }
            }
        }

        if (reachedViaRelay)
        {
            // A relay confirmed the target is reachable.  PingReqResponse does not carry the
            // target's incarnation, so MarkAlive(…, 0) would be silently dropped for any node
            // whose incarnation is > 0 (e.g. after a prior refutation).  ClearSuspicion
            // transitions Suspect→Alive while preserving the existing incarnation.
            Liveness.ClearSuspicion(target.Endpoint);
        }
        else
        {
            // Both direct and indirect probes failed — suspect.
            MemberLivenessState prev = Liveness.GetState(target.Endpoint);
            Liveness.MarkSuspect(target.Endpoint);
            if (prev == MemberLivenessState.Alive)
            {
                Logger.LogWarning("PingAsync: {Endpoint} failed direct and indirect probe; marked Suspect", target.Endpoint);
                // Wake quiesced followers that believe this node is their leader so they
                // detect the failure at the next CheckLeader tick, not the 5 s safety sweep.
                WakePartitionsForLeader(target.Endpoint);
            }
        }
    }

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
    /// </summary>
    /// <param name="ranges"></param>
    internal void StartUserPartitions(List<RaftPartitionRange> ranges)
    {
        foreach (RaftPartitionRange range in ranges)
        {
            // Tombstone entries must never re-create a stopped partition.
            if (range.State == RaftPartitionState.Removed)
                continue;

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

        OnPartitionMapChanged?.Invoke(GetPartitionMap());
    }

    /// <summary>
    /// Leaves the cluster.
    /// <para>
    /// When the local node is part of a committed roster (MembershipVersion &gt; 0) this method
    /// first transitions the node to the <see cref="System.ClusterMemberRole.Leaving"/> role
    /// (suppressing elections immediately), commits a <c>RemoveMember</c> entry on P0, and
    /// waits up to 10 s for the removal to propagate back to this node before tearing down.
    /// If the cluster has no committed roster (pre-seed transient or test teardown path), or if
    /// the roster contains no other <c>Voter</c> peer, the round-trip is skipped and the node
    /// stops immediately (single-voter short-circuit — no 10 s spin).
    /// </para>
    /// </summary>
    /// <param name="dispose">If true, also disposes the manager</param>
    /// <param name="cancellationToken">
    /// When cancelled, aborts any in-progress graceful-leave attempt immediately.
    /// </param>
    public async Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default)
    {
        // Suppress elections on all partitions immediately.
        _leaving = true;

        // If we are part of a committed roster AND there is at least one other Voter peer,
        // commit the removal before stopping so surviving nodes drop us from their peer list
        // at the consensus level.
        // Short-circuit: a single-voter roster (embedded/standalone) has no quorum peer to
        // commit the removal — skipping saves the 10 s deadline spin on every dispose.
        System.ClusterMembership roster = systemCoordinator.GetMembership();
        bool hasOtherVoter = roster.Members.Any(m =>
            m.Role == System.ClusterMemberRole.Voter &&
            !string.Equals(m.Endpoint, LocalEndpoint, StringComparison.Ordinal));

        if (roster.MembershipVersion > 0 && clusterHandler.Joined && hasOtherVoter)
        {
            await CommitGracefulLeaveAsync(cancellationToken).ConfigureAwait(false);
        }

        await clusterHandler.LeaveCluster(configuration).ConfigureAwait(false);

        // Stop in the correct order: timer first (no new work injected), drain
        // partition queues, stop shared I/O schedulers while partition executors are
        // still alive so WAL completions can be posted back, drain those completions,
        // then stop executor threads. RaftTimerService.Dispose() is idempotent and
        // safe to call again from RaftManager.Dispose() if that path is taken.
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
    /// Sends a <c>RemoveMember(self)</c> to the P0 leader, retrying until the removal commits or
    /// a 10 s deadline expires.  Each individual <c>SendLeave</c> call is bounded by a 3 s
    /// per-attempt token so a stopped or unreachable node never blocks indefinitely.
    /// <para>
    /// <b>Empty-leader cap:</b> when the P0 leader cannot be resolved (election in progress or
    /// partition still starting), the loop polls at most <c>maxEmptyLeaderPolls</c> times before
    /// giving up. This prevents the 10 s spin during shutdown when the system partition is already
    /// draining and will never elect a new leader.
    /// </para>
    /// <para>
    /// <b>Cancellation:</b> <paramref name="cancellationToken"/> is observed in all waits so a
    /// tearing-down host returns promptly.
    /// </para>
    /// Failures are logged but never thrown — the caller always proceeds to stop afterwards.
    /// </summary>
    private async Task CommitGracefulLeaveAsync(CancellationToken cancellationToken)
    {
        const int deadlineMs = 10_000;
        const int attemptTimeoutMs = 3_000;
        // After this many consecutive "leader unknown" polls (5 × 200 ms = 1 s), give up.
        // The full deadlineMs only applies while we are actively contacting a known leader.
        const int maxEmptyLeaderPolls = 5;

        ValueStopwatch sw = ValueStopwatch.StartNew();
        LeaveRequest request = new(LocalEndpoint, configuration.NodeId);
        int emptyLeaderPolls = 0;

        while (sw.GetElapsedMilliseconds() < deadlineMs)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                Logger.LogInformation("LeaveCluster: graceful leave cancelled; proceeding to stop.");
                return;
            }

            try
            {
                // Find the current P0 leader. Try self first (fast path when we are the leader).
                bool amLeader = systemPartition is not null &&
                    await AmILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);

                string? leaderEndpoint = amLeader ? LocalEndpoint : systemPartition?.Leader;
                if (string.IsNullOrEmpty(leaderEndpoint))
                {
                    if (++emptyLeaderPolls >= maxEmptyLeaderPolls)
                    {
                        Logger.LogInfoLeaveClusterLeaderUnknown(emptyLeaderPolls);
                        return;
                    }

                    await Task.Delay(200, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // We have a leader — reset the empty-leader counter.
                emptyLeaderPolls = 0;

                using CancellationTokenSource attemptCts =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attemptCts.CancelAfter(attemptTimeoutMs);

                RaftNode leaderNode = new(leaderEndpoint);
                LeaveResponse resp = await communication.SendLeave(this, leaderNode, request, attemptCts.Token).ConfigureAwait(false);

                if (resp.Success)
                {
                    await WaitForRosterRemovalAsync(sw, deadlineMs, cancellationToken).ConfigureAwait(false);
                    return;
                }

                // Terminal = permanently blocked (e.g. InsufficientVoters) — give up immediately.
                // A null-hint without Terminal means the leader is unknown right now (election
                // in progress); the loop continues and retries within the 10 s deadline.
                if (resp.Terminal)
                {
                    Logger.LogInformation("LeaveCluster: leave permanently rejected; proceeding to stop.");
                    return;
                }

                // Not the leader — follow the hint if it differs from the endpoint we just tried.
                if (!string.IsNullOrEmpty(resp.LeaderHint) && resp.LeaderHint != leaderEndpoint)
                {
                    try
                    {
                        using CancellationTokenSource hintCts =
                            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        hintCts.CancelAfter(attemptTimeoutMs);

                        RaftNode hint = new(resp.LeaderHint);
                        LeaveResponse hintResp = await communication.SendLeave(this, hint, request, hintCts.Token).ConfigureAwait(false);
                        if (hintResp.Success)
                        {
                            await WaitForRosterRemovalAsync(sw, deadlineMs, cancellationToken).ConfigureAwait(false);
                            return;
                        }

                        if (hintResp.Terminal)
                        {
                            Logger.LogInfoLeaveClusterRejectedByHint(resp.LeaderHint);
                            return;
                        }
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        Logger.LogInformation("LeaveCluster: graceful leave cancelled during hint attempt; proceeding to stop.");
                        return;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning("LeaveCluster: failed to contact leader hint {Hint}: {Message}", resp.LeaderHint, ex.Message);
                    }
                }

                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Logger.LogInformation("LeaveCluster: graceful leave cancelled; proceeding to stop.");
                return;
            }
            catch (Exception ex)
            {
                Logger.LogWarning("LeaveCluster: error during graceful leave: {Message}", ex.Message);

                try
                {
                    await Task.Delay(200, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    Logger.LogInformation("LeaveCluster: graceful leave cancelled; proceeding to stop.");
                    return;
                }
            }
        }

        Logger.LogWarning("LeaveCluster: graceful leave timed out; proceeding to stop without committed removal.");
    }

    /// <summary>
    /// Polls the local roster cache until the local endpoint is absent (removal propagated) or
    /// the overall deadline or <paramref name="cancellationToken"/> is reached.
    /// Returns <c>true</c> if the removal was observed locally.
    /// </summary>
    private async Task<bool> WaitForRosterRemovalAsync(ValueStopwatch sw, int deadlineMs, CancellationToken cancellationToken)
    {
        while (sw.GetElapsedMilliseconds() < deadlineMs)
        {
            if (cancellationToken.IsCancellationRequested)
                return false;

            if (!systemCoordinator.GetMembership().Members.Any(m => m.Endpoint == LocalEndpoint))
                return true;

            try
            {
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        Logger.LogWarning("LeaveCluster: removal committed but roster propagation timed out; proceeding to stop.");
        return false;
    }

    private async Task DrainPartitions(CancellationToken cancellationToken)
    {
        List<Task> drainTasks = new(partitions.Count + 1);

        foreach (RaftPartition partition in partitions.Values)
            drainTasks.Add(partition.DrainAsync(cancellationToken));

        if (systemPartition is not null)
            drainTasks.Add(systemPartition.DrainAsync(cancellationToken));

        if (drainTasks.Count > 0)
            await Task.WhenAll(drainTasks).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates the internal state of the nodes
    /// </summary>
    public async Task UpdateNodes()
    {
        if (systemPartition is null && partitions.IsEmpty)
            return;

        await clusterHandler.UpdateNodes().ConfigureAwait(false);
        await systemCoordinator.CheckLearnerPromotionsAsync().ConfigureAwait(false);
        await systemCoordinator.EvictDeadMembersAsync().ConfigureAwait(false);
    }

    // ── IRaftTimerHost ─────────────────────────────────────────────────────

    RaftPartition? Scheduling.IRaftTimerHost.SystemPartition => systemPartition;

    IEnumerable<RaftPartition> Scheduling.IRaftTimerHost.GetUserPartitions() => partitions.Values;

    /// <summary>
    /// Returns only the hot (non-quiesced) partitions for targeted <c>CheckLeader</c> ticks.
    /// Updated by <see cref="MarkPartitionHot"/> / <see cref="MarkPartitionCool"/> which are
    /// wired from each <see cref="RaftPartitionStateMachine"/>'s quiesce callback.
    /// </summary>
    IEnumerable<RaftPartition> Scheduling.IRaftTimerHost.GetHotUserPartitions() => _hotPartitions.Values;

    Task Scheduling.IRaftTimerHost.UpdateNodes() => UpdateNodes();

    Task Scheduling.IRaftTimerHost.GossipAsync(CancellationToken cancellationToken) => GossipAsync(cancellationToken);

    Task Scheduling.IRaftTimerHost.PingAsync(CancellationToken cancellationToken) => PingAsync(cancellationToken);

    void Scheduling.IRaftTimerHost.TriggerBalancerPass() =>
        systemCoordinator.Send(new System.RaftSystemRequest(System.RaftSystemRequestType.RunBalancerPass));

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

    // ──────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Obtains the last activity known of a specific node on any partitions
    /// </summary>
    /// <param name="endpoint"></param>
    /// <returns></returns>
    public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId)
    {
        return lastActivity.TryGetValue((endpoint, partitionId), out HLCTimestamp lastTimestamp) ? lastTimestamp : HLCTimestamp.Zero;
    }

    public HLCTimestamp GetLastNodeActivity(string endpoint)
    {
        HLCTimestamp max = HLCTimestamp.Zero;
        foreach (((string ep, int _) key, HLCTimestamp ts) in lastActivity)
        {
            if (key.ep == endpoint && ts > max)
                max = ts;
        }
        return max;
    }

    /// <summary>
    /// Updates the last activity known of a specific node on a specific partition
    /// </summary>
    /// <param name="nodeId"></param>
    /// <param name="partitionId"></param>
    /// <param name="lastTimestamp"></param>
    internal void UpdateLastNodeActivity(string nodeId, int partitionId, HLCTimestamp lastTimestamp)
    {
        var key = (nodeId, partitionId);
        if (lastActivity.TryGetValue(key, out HLCTimestamp currentTimestamp))
        {
            if (lastTimestamp > currentTimestamp)
                lastActivity[key] = lastTimestamp;
        }
        else
            lastActivity.TryAdd(key, lastTimestamp);
    }
    
    /// <summary>
    /// Obtains the last heartbeat sent to a specific node for a specific partition.
    /// The throttle key must include the partition id: a single node hosts many partitions,
    /// and keying only by endpoint would let one partition's heartbeat suppress the
    /// heartbeats of every other partition to the same node (within RecentHeartbeat),
    /// starving their followers and triggering perpetual re-elections.
    /// </summary>
    /// <param name="nodeId"></param>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    internal HLCTimestamp GetLastNodeHearthbeat(string nodeId, int partitionId)
    {
        return lastHearthBeat.TryGetValue((nodeId, partitionId), out HLCTimestamp lastTimestamp) ? lastTimestamp : HLCTimestamp.Zero;
    }

    /// <summary>
    /// Updates the last heartbeat sent to a node for a specific partition.
    /// </summary>
    /// <param name="nodeId"></param>
    /// <param name="partitionId"></param>
    /// <param name="lastTimestamp"></param>
    internal void UpdateLastHeartbeat(string nodeId, int partitionId, HLCTimestamp lastTimestamp)
    {
        var key = (nodeId, partitionId);
        if (lastHearthBeat.TryGetValue(key, out HLCTimestamp currentTimestamp))
        {
            if (lastTimestamp > currentTimestamp)
                lastHearthBeat[key] = lastTimestamp;
        }
        else
            lastHearthBeat.TryAdd(key, lastTimestamp);
    }

    /// <summary>
    /// Returns a list of nodes in the cluster
    /// </summary>
    public IList<RaftNode> GetNodes()
    {
        return Nodes;
    }

    /// <summary>
    /// Returns the non-local endpoints observed within the requested liveness window.
    /// </summary>
    public IReadOnlyList<string> GetActiveNodes(TimeSpan within)
    {
        HLCTimestamp now = hybridLogicalClock.TrySendOrLocalEvent(LocalNodeId);
        List<string> active = [];

        foreach (((string endpoint, int _) key, HLCTimestamp lastSeen) in lastActivity)
        {
            if (key.endpoint == LocalEndpoint)
                continue;

            if ((now - lastSeen) <= within && !active.Contains(key.endpoint))
                active.Add(key.endpoint);
        }

        active.Sort(StringComparer.Ordinal);
        return active;
    }

    /// <summary>
    /// Returns the raft partition for the given partition number
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    private RaftPartition GetPartition(int partitionId)
    {
        if (partitionId == RaftSystemConfig.SystemPartition)
        {
            if (systemPartition is null)
                throw new RaftException("System partition not initialized.");

            return systemPartition;
        }

        //if (partitionId < 0 || partitionId > partitions.Count)
        //    throw new RaftException("Invalid partition: " + partitionId);

        if (!partitions.TryGetValue(partitionId, out RaftPartition? partition))
            throw new RaftException("Invalid partition: " + partitionId);

        return partition;
    }

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
    /// Builds an advisory load report for this node, covering only the partitions for which
    /// this node is currently leader. The report is stamped with a monotonically increasing
    /// <c>ReportVersion</c> and the local HLC time so receivers can discard stale entries.
    /// <para>
    /// Only called when <see cref="RaftConfiguration.EnableLeaderBalancer"/> is
    /// <see langword="true"/>. The report is never committed to the Raft log; it is gossiped
    /// out-of-band so a stale or dropped report only delays balancing, never violates safety.
    /// </para>
    /// </summary>
    internal NodeLoadReport BuildLocalLoadReport()
    {
        double wOps = configuration.LeaderBalancerOpsWeight;
        double wQueue = configuration.LeaderBalancerQueueWeight;
        double ticksToMs = 1000.0 / global::System.Diagnostics.Stopwatch.Frequency;
        long now = global::System.Diagnostics.Stopwatch.GetTimestamp();

        List<PartitionLoad> leaderships = [];

        foreach (KeyValuePair<int, RaftPartition> kv in partitions)
        {
            RaftPartition p = kv.Value;
            if (!string.Equals(p.Leader, LocalEndpoint, StringComparison.Ordinal))
                continue;

            long leaderSinceMs = (long)((now - p.LeaderChangedTicks) * ticksToMs);

            leaderships.Add(new PartitionLoad
            {
                PartitionId = p.PartitionId,
                Load = p.GetCurrentLoad(wOps, wQueue),
                LeaderSinceMs = leaderSinceMs > 0 ? leaderSinceMs : 0,
                LogOpsPerSecond = p.GetLogOpsPerSecond(),
                WalQueueDepth = walScheduler.GetPartitionDepth(p.PartitionId),
                CommitWaitMs = walScheduler.GetPartitionCommitWaitMs(p.PartitionId),
            });
        }

        return new NodeLoadReport
        {
            Endpoint = LocalEndpoint,
            ReportVersion = Interlocked.Increment(ref _reportVersion),
            Time = hybridLogicalClock.TrySendOrLocalEvent(LocalNodeId),
            Leaderships = leaderships,
        };
    }

    /// <summary>
    /// Passes the Handshake to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public async Task Handshake(HandshakeRequest request)
    {                
        while (request.Partition != RaftSystemConfig.SystemPartition && !IsInitialized)
            await Task.Delay(100);
        
        RaftPartition partition = GetPartition(request.Partition);

        partition.Handshake(request);
    }

    internal HandshakeResponse GetHandshakeResponse(int partitionId)
    {
        long maxLogId = walAdapter.GetMaxLog(partitionId);
        return new(LocalNodeId, maxLogId, LocalEndpoint);
    }

    /// <summary>
    /// Passes the RequestVote to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);

        partition.RequestVote(request);
    }

    /// <summary>
    /// Passes the request to the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);

        partition.Vote(request);
    }

    internal void StepDownNotice(StepDownNoticeRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);

        partition.StepDownNotice(request);
    }

    internal void TransferLeadership(TransferLeadershipRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);

        partition.TransferLeadership(request);
    }

    /// <summary>
    /// Receives an advisory leadership-transfer suggestion from the P0 balancer.
    /// Validates that this node currently leads the partition, that the partition is
    /// <see cref="System.RaftPartitionState.Active"/>, and that <paramref name="request"/>
    /// <see cref="Data.TransferLeadershipSuggestionRequest.TargetEndpoint"/> is a live voter —
    /// then fires the local transfer fire-and-forget.  Drops silently on any validation failure
    /// so a stale or misdirected suggestion is always safe.
    /// </summary>
    internal void ReceiveTransferLeadershipSuggestion(Data.TransferLeadershipSuggestionRequest request)
    {
        if (!partitions.TryGetValue(request.Partition, out RaftPartition? partition))
            return;

        // Only act if we currently lead this partition.
        if (!string.Equals(partition.Leader, LocalEndpoint, global::System.StringComparison.Ordinal))
        {
            Logger.LogDebugTransferSuggestionDroppedNotLeader(
                request.Partition, request.Term, partition.Leader ?? "(none)", request.SuggestedBy);
            return;
        }

        // Only move Active partitions.
        if (partition.State != System.RaftPartitionState.Active)
        {
            Logger.LogDebugTransferSuggestionDroppedNotActive(
                request.Partition, request.Term, partition.State, request.SuggestedBy);
            return;
        }

        // Target must be a live voter.
        System.ClusterMembership membership = systemCoordinator.GetMembership();
        bool targetIsVoter = membership.Members.Exists(m =>
            string.Equals(m.Endpoint, request.TargetEndpoint, global::System.StringComparison.Ordinal) &&
            m.Role == System.ClusterMemberRole.Voter);

        if (!targetIsVoter)
        {
            Logger.LogDebugTransferSuggestionDroppedNotVoter(
                request.Partition, request.Term, request.TargetEndpoint, request.SuggestedBy);
            return;
        }

        if (Liveness.GetState(request.TargetEndpoint) >= Gossip.MemberLivenessState.Suspect)
        {
            Logger.LogDebugTransferSuggestionDroppedSuspect(
                request.Partition, request.Term, request.TargetEndpoint, request.SuggestedBy);
            return;
        }

        // Fire-and-forget: the executor serialises the transfer; we don't await here.
        _ = partition.TransferLeadershipAsync(request.TargetEndpoint, global::System.Threading.CancellationToken.None);
    }

    /// <summary>
    /// Sends an advisory leadership-transfer suggestion to the node at
    /// <paramref name="ownerEndpoint"/> via the existing responder transport.
    /// Fire-and-forget; a failed delivery is silently ignored and the suggestion
    /// will time out in the balancer's outstanding-move tracking table.
    /// <para>
    /// When the owner is this node itself — the common case where the P0 balancer leader
    /// also leads the overloaded partition — the suggestion is delivered in-process.  The
    /// peer transport cannot be used for self-delivery: a node is not its own peer
    /// (<see cref="ClusterHandler.IsNode"/> excludes the local endpoint), so a self-addressed
    /// responder message is dropped on the wire.  Without this short-circuit the balancer
    /// could never rebalance partitions led by the P0 node.
    /// </para>
    /// </summary>
    internal void SendTransferLeadershipSuggestion(string ownerEndpoint, Data.TransferLeadershipSuggestionRequest request)
    {
        if (string.Equals(ownerEndpoint, LocalEndpoint, global::System.StringComparison.Ordinal))
        {
            ReceiveTransferLeadershipSuggestion(request);
            return;
        }

        RaftNode node = new(ownerEndpoint);
        EnqueueResponse(ownerEndpoint, new Data.RaftResponderRequest(
            Data.RaftResponderRequestType.TransferLeadershipSuggestion, node, request));
    }

    /// <summary>
    /// Append logs in the appropriate partition
    /// Returns the index of the last log
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void AppendLogs(AppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);

        partition.AppendLogs(request);
    }

    /// <summary>
    /// Completes an append logs operation in the appropriate partition
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request)
    {
        RaftPartition partition = GetPartition(request.Partition);

        partition.CompleteAppendLogs(request);
    }

    /// <summary>
    /// Replicate a single log to the follower nodes in the system partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="type"></param>
    /// <param name="data"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal async Task<RaftReplicationResult> ReplicateSystemLogs(string type, byte[] data, bool autoCommit = true, CancellationToken cancellationToken = default)
    {
        if (systemPartition is null)
            throw new RaftException("System partition not initialized.");

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await systemPartition.ReplicateLogs(type, data, autoCommit).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(systemPartition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a single log entry to the follower nodes in the specified partition.
    /// P0 routes committed entries by log type: <c>_RaftSystem</c> entries go to the system
    /// coordinator; all other types go to consumer callbacks (<c>OnReplicationReceived</c> /
    /// <c>OnLogRestored</c>).  Passing <c>type == "_RaftSystem"</c> on partition 0 is rejected
    /// with <see cref="RaftException"/> to prevent userland from forging coordinator entries.
    /// P0 is never a valid target for create, split, merge, or remove.
    /// </summary>
    public async Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default)
    {
        if (partitionId == RaftSystemConfig.SystemPartition && type == RaftSystemConfig.RaftLogType)
            throw new RaftException("System log type is reserved on the system partition");

        RaftPartition partition = GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateLogs(type, data, autoCommit, expectedGeneration).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a batch of log entries to the follower nodes in the specified partition.
    /// P0 routes committed entries by log type: <c>_RaftSystem</c> entries go to the system
    /// coordinator; all other types go to consumer callbacks (<c>OnReplicationReceived</c> /
    /// <c>OnLogRestored</c>).  Passing <c>type == "_RaftSystem"</c> on partition 0 is rejected
    /// with <see cref="RaftException"/> to prevent userland from forging coordinator entries.
    /// P0 is never a valid target for create, split, merge, or remove.
    /// </summary>
    public async Task<RaftReplicationResult> ReplicateLogs(
        int partitionId,
        string type,
        IEnumerable<byte[]> logs,
        bool autoCommit = true,
        long expectedGeneration = 0,
        CancellationToken cancellationToken = default
    )
    {
        if (partitionId == RaftSystemConfig.SystemPartition && type == RaftSystemConfig.RaftLogType)
            throw new RaftException("System log type is reserved on the system partition");

        RaftPartition partition = GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            // ReSharper disable once PossibleMultipleEnumeration
            (success, status, ticketId) = await partition.ReplicateLogs(type, logs.ToList(), autoCommit, expectedGeneration).ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, autoCommit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Commit logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="proposalIndex"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId)
    {
        RaftPartition partition = GetPartition(partitionId);

        return await partition.CommitLogs(ticketId).ConfigureAwait(false);
    }

    /// <summary>
    /// Rollback logs and notify followers in the partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="proposalIndex"></param>
    /// <returns></returns>
    public async Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId)
    {
        RaftPartition partition = GetPartition(partitionId);

        return await partition.RollbackLogs(ticketId).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a checkpoint to the follower nodes
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default)
    {
        RaftPartition partition = GetPartition(partitionId);

        bool success;
        HLCTimestamp ticketId;
        RaftOperationStatus status;

        do
        {
            (success, status, ticketId) = await partition.ReplicateCheckpoint().ConfigureAwait(false);

            if (status == RaftOperationStatus.ActiveProposal)
                await Task.Delay(ProposalRetryDelay, cancellationToken).ConfigureAwait(false);

        } while (status == RaftOperationStatus.ActiveProposal);

        if (!success)
            return new(success, status, ticketId, -1);

        return await WaitForQuorum(partition, ticketId, true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits for the replication proposal to be completed in the given partition
    /// </summary>
    /// <param name="partition"></param>
    /// <param name="ticketId"></param>
    /// <param name="autoCommit"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<RaftReplicationResult> WaitForQuorum(RaftPartition partition, HLCTimestamp ticketId, bool autoCommit, CancellationToken cancellationToken)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader != LocalEndpoint)
                return new(false, RaftOperationStatus.NodeIsNotLeader, ticketId, -1);

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                (RaftProposalTicketState state, long commitId) = await partition.GetTicketState(ticketId, autoCommit).ConfigureAwait(false);

                switch (state)
                {
                    case RaftProposalTicketState.NotFound:
                        return new(false, RaftOperationStatus.ReplicationFailed, ticketId, -1);

                    case RaftProposalTicketState.Committed:
                        return new(true, RaftOperationStatus.Success, ticketId, commitId);

                    case RaftProposalTicketState.Proposed:
                    default:
                        break;
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogError("ReplicateLogs: {Message}", e.Message);
            }

            await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        return new(false, RaftOperationStatus.ProposalTimeout, ticketId, -1);
    }

    /// <summary>
    /// Calls the restore started event
    /// </summary>
    /// <param name="partitionId"></param>
    internal void InvokeRestoreStarted(int partitionId)
    {
        if (OnRestoreStarted != null)
        {
            Action<int>? callback = OnRestoreStarted;
            callback?.Invoke(partitionId);
        }
    }

    /// <summary>
    /// Calls the restore finished event
    /// </summary>
    /// <param name="partitionId"></param>
    internal void InvokeRestoreFinished(int partitionId)
    {
        if (OnRestoreFinished != null)
        {
            Action<int>? callback = OnRestoreFinished;
            callback?.Invoke(partitionId);
        }
    }

    /// <summary>
    /// Calls the restore finished event
    /// </summary>
    /// <param name="partitionId"></param>
    internal void InvokeSystemRestoreFinished(int partitionId)
    {
        if (OnSystemRestoreFinished != null)
        {
            Action<int>? callback = OnSystemRestoreFinished;
            callback?.Invoke(partitionId);
        }
    }

    /// <summary>
    /// Calls when a replication error occurs
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    internal void InvokeReplicationError(int partitionId, RaftLog log)
    {
        if (OnReplicationError != null)
        {
            Action<int, RaftLog>? callback = OnReplicationError;
            callback?.Invoke(partitionId, log);
        }
    }

    /// <summary>
    /// Calls the replication received event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log)
    {
        if (OnReplicationReceived != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnReplicationReceived;

            bool success = await callback(partitionId, log);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the replication received event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log)
    {
        if (OnSystemReplicationReceived != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnSystemReplicationReceived;

            bool success = await callback(partitionId, log);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the replication restored event on system partitions
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeSystemLogRestored(int partitionId, RaftLog log)
    {
        if (OnSystemLogRestored != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnSystemLogRestored;

            bool success = await callback(partitionId, log).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the replication restored event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeLogRestored(int partitionId, RaftLog log)
    {
        if (OnLogRestored != null)
        {
            Func<int, RaftLog, Task<bool>> callback = OnLogRestored;

            bool success = await callback(partitionId, log).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Calls the leader changed event
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="node"></param>
    /// <returns></returns>
    internal async Task<bool> InvokeLeaderChanged(int partitionId, string node)
    {
        if (OnLeaderChanged != null)
        {
            Func<int, string, Task<bool>> callback = OnLeaderChanged;

            bool success = await callback(partitionId, node).ConfigureAwait(false);
            if (!success)
                return false;
        }

        return true;
    }

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
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <returns></returns>
    public async ValueTask<bool> AmILeaderQuick(int partitionId)
    {
        if (!IsInitialized)
            return false;

        RaftPartition partition = GetPartition(partitionId);

        if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
            return true;

        try
        {
            RaftNodeState response = await partition.GetState().ConfigureAwait(false);

            return response == RaftNodeState.Leader;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            Logger.LogError("AmILeaderQuick: {Message}", e.Message);
        }

        return false;
    }

    /// <summary>
    /// Checks if the local node is the leader in the given partition
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="OperationCanceledException"></exception>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken)
    {
        if (!IsInitialized)
            return false;

        RaftPartition partition = GetPartition(partitionId);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            if (!string.IsNullOrEmpty(partition.Leader) && partition.Leader == LocalEndpoint)
                return true;

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                RaftNodeState response = await partition.GetState(cancellationToken).ConfigureAwait(false);

                return response == RaftNodeState.Leader;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogError("AmILeader: {Message}", e.Message);
            }

            await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }

    /// <summary>
    /// Waits for the leader to be elected in the given partition
    /// If the leader is already elected, it returns the leader
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken)
    {
        RaftPartition partition = GetPartition(partitionId);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                RaftNodeState response = await partition.GetState(cancellationToken).ConfigureAwait(false);

                if (response == RaftNodeState.Leader)
                    return LocalEndpoint;

                if (string.IsNullOrEmpty(partition.Leader))
                {
                    await Task.Delay(100 + Random.Shared.Next(-50, 50), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return partition.Leader;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogError("WaitForLeader: {Message}", e.Message);
            }
        }

        throw new RaftException("Leader couldn't be found or is not decided");
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public ValueTask<string> WaitForLeaderStableAsync(
        int partitionId,
        TimeSpan minStableFor,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            throw new RaftException("Raft manager is not initialized");

        return GetPartition(partitionId).WaitForLeaderStableAsync(minStableFor, cancellationToken);
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public async Task<RaftOperationStatus> ForceLeaderForTestingAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!Joined || !IsInitialized)
            return RaftOperationStatus.Errored;

        RaftPartition partition;

        try
        {
            partition = GetPartition(partitionId);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }

        RaftOperationStatus status = await partition.ForceLeaderForTestingAsync(cancellationToken).ConfigureAwait(false);
        if (status != RaftOperationStatus.Pending)
            return status;

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrEmpty(partition.Leader))
            {
                if (partition.Leader == LocalEndpoint)
                    return RaftOperationStatus.Success;

                return RaftOperationStatus.LeaderAlreadyElected;
            }

            try
            {
                RaftNodeState nodeState = await partition.GetState(cancellationToken).ConfigureAwait(false);
                if (nodeState == RaftNodeState.Leader)
                    return RaftOperationStatus.Success;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogWarning("ForceLeaderForTestingAsync: {Message}", e.Message);
            }

            await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        return RaftOperationStatus.Pending;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public async Task<RaftOperationStatus> StepDownAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!Joined || !IsInitialized)
            return RaftOperationStatus.Errored;

        RaftPartition partition;

        try
        {
            partition = GetPartition(partitionId);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }

        RaftOperationStatus status = await partition.StepDownAsync(cancellationToken).ConfigureAwait(false);
        if (status != RaftOperationStatus.Pending)
            return status;

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrEmpty(partition.Leader))
            {
                if (partition.Leader == LocalEndpoint)
                {
                    await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return RaftOperationStatus.Success;
            }

            try
            {
                RaftNodeState nodeState = await partition.GetState(cancellationToken).ConfigureAwait(false);
                if (nodeState != RaftNodeState.Leader && !string.IsNullOrEmpty(partition.Leader) && partition.Leader != LocalEndpoint)
                    return RaftOperationStatus.Success;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogWarning("StepDownAsync: {Message}", e.Message);
            }

            await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        return RaftOperationStatus.Pending;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public async Task<RaftOperationStatus> TransferLeadershipAsync(
        int partitionId,
        string targetEndpoint,
        CancellationToken cancellationToken = default)
    {
        if (!Joined || !IsInitialized)
            return RaftOperationStatus.Errored;

        RaftPartition partition;

        try
        {
            partition = GetPartition(partitionId);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }

        RaftOperationStatus status = await partition.TransferLeadershipAsync(targetEndpoint, cancellationToken).ConfigureAwait(false);
        if (status == RaftOperationStatus.ReplicationFailed)
            status = await RetryTransferLeadershipAfterProbeAsync(partition, partitionId, targetEndpoint, cancellationToken).ConfigureAwait(false);

        if (status != RaftOperationStatus.Pending)
            return status;

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        while (stopwatch.GetElapsedMilliseconds() < 10000)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrEmpty(partition.Leader))
            {
                if (partition.Leader == targetEndpoint)
                    return RaftOperationStatus.Success;

                if (partition.Leader != LocalEndpoint)
                    return RaftOperationStatus.LeaderAlreadyElected;
            }

            try
            {
                RaftNodeState nodeState = await partition.GetState(cancellationToken).ConfigureAwait(false);
                if (nodeState != RaftNodeState.Leader && partition.Leader == targetEndpoint)
                    return RaftOperationStatus.Success;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                Logger.LogWarning("TransferLeadershipAsync: {Message}", e.Message);
            }

            await Task.Delay(ProposalStatusPollDelay, cancellationToken).ConfigureAwait(false);
        }

        return RaftOperationStatus.Pending;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public async Task<RaftOperationStatus> SuspendHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!Joined || !IsInitialized)
            return RaftOperationStatus.Errored;

        try
        {
            return await GetPartition(partitionId).SuspendHeartbeatsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public async Task<RaftOperationStatus> ResumeHeartbeatsAsync(
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!Joined || !IsInitialized)
            return RaftOperationStatus.Errored;

        try
        {
            return await GetPartition(partitionId).ResumeHeartbeatsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RaftException)
        {
            return RaftOperationStatus.Errored;
        }
    }

    private async Task<RaftOperationStatus> RetryTransferLeadershipAfterProbeAsync(
        RaftPartition partition,
        int partitionId,
        string targetEndpoint,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(targetEndpoint) || targetEndpoint == LocalEndpoint)
            return RaftOperationStatus.ReplicationFailed;

        RaftNode? targetNode = Nodes.FirstOrDefault(node => node.Endpoint == targetEndpoint);
        if (targetNode is null)
            return RaftOperationStatus.ReplicationFailed;

        HandshakeResponse response;

        try
        {
            response = await communication.Handshake(this, targetNode, new HandshakeRequest(
                LocalNodeId,
                partitionId,
                walAdapter.GetMaxLog(partitionId),
                LocalEndpoint)).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            Logger.LogWarning("TransferLeadershipAsync probe: {Message}", e.Message);
            return RaftOperationStatus.ReplicationFailed;
        }

        if (string.IsNullOrEmpty(response.Endpoint))
            response = new HandshakeResponse(response.NodeId, response.MaxLogId, targetEndpoint);

        partition.Handshake(new HandshakeRequest(
            response.NodeId,
            partitionId,
            response.MaxLogId,
            response.Endpoint));

        await partition.DrainAsync(cancellationToken).ConfigureAwait(false);

        return await partition.TransferLeadershipAsync(targetEndpoint, cancellationToken).ConfigureAwait(false);
    }

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
    public async Task<RaftPartitionLifecycleResult> SplitPartitionAsync(
        int sourcePartitionId,
        int targetPartitionId = 0,
        RaftSplitPlan? plan = null,
        CancellationToken ct = default)
    {
        if (!IsInitialized)
            throw new RaftException("System is not initialized");

        if (sourcePartitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be split");

        if (!await AmILeader(sourcePartitionId, ct).ConfigureAwait(false))
            throw new RaftException("Split cannot be initiated by follower");

        RaftSplitPlan effectivePlan = new()
        {
            // Method parameter takes precedence; fall back to plan.TargetPartitionId so a
            // caller who passes only the plan (leaving targetPartitionId at its default 0)
            // still has their explicit id honoured.
            TargetPartitionId = targetPartitionId > 0 ? targetPartitionId : (plan?.TargetPartitionId ?? 0),
            TargetRoutingMode = plan?.TargetRoutingMode ?? RaftRoutingMode.HashRange,
            HashBoundary      = plan?.HashBoundary,
            AutoCommit        = true
        };

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new System.RaftSystemRequest(sourcePartitionId, effectivePlan, tcs));

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success    = status == RaftOperationStatus.Success,
            Status     = status,
            Generation = generation
        };
    }

    /// <inheritdoc/>
    public async Task<RaftPartitionLifecycleResult> MergePartitionsAsync(
        int survivorPartitionId,
        int sourcePartitionId,
        RaftMergePlan? plan = null,
        CancellationToken ct = default)
    {
        if (!IsInitialized)
            throw new RaftException("System is not initialized");

        if (sourcePartitionId == RaftSystemConfig.SystemPartition || survivorPartitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be merged");

        if (sourcePartitionId == survivorPartitionId)
            throw new RaftException("Source and survivor partition must be different");

        if (!await AmILeader(survivorPartitionId, ct).ConfigureAwait(false))
            throw new RaftException("Merge cannot be initiated by follower: not leader of survivor partition");

        if (!await AmILeader(sourcePartitionId, ct).ConfigureAwait(false))
            throw new RaftException("Merge cannot be initiated by follower: not leader of source partition");

        // Use the caller's plan when provided (carries any extended fields added in the future).
        // Fall back to a default plan built from the validated method parameters.
        RaftMergePlan effectivePlan = plan ?? new()
        {
            SurvivorPartitionId = survivorPartitionId,
            SourcePartitionId   = sourcePartitionId
        };

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new System.RaftSystemRequest(effectivePlan, tcs));

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success    = status == RaftOperationStatus.Success,
            Status     = status,
            Generation = generation
        };
    }

    /// <inheritdoc/>
    public async Task<RaftPartitionLifecycleResult> CreatePartitionAsync(
        int partitionId,
        RaftRoutingMode mode = RaftRoutingMode.Unrouted,
        (int start, int end)? hashRange = null,
        CancellationToken ct = default)
    {
        if (!IsInitialized)
            throw new RaftException("System is not initialized");

        if (partitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be created");

        if (!await AmILeader(RaftSystemConfig.SystemPartition, ct).ConfigureAwait(false))
            throw new RaftException("CreatePartition cannot be initiated by a follower");

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new System.RaftSystemRequest(
            partitionId, mode, hashRange?.start, hashRange?.end, tcs));

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success = status == RaftOperationStatus.Success,
            Status = status,
            Generation = generation
        };
    }

    /// <inheritdoc/>
    public async Task<RaftPartitionLifecycleResult> RemovePartitionAsync(
        int partitionId,
        CancellationToken ct = default)
    {
        if (!IsInitialized)
            throw new RaftException("System is not initialized");

        if (partitionId == RaftSystemConfig.SystemPartition)
            throw new RaftException("System partition cannot be removed");

        if (!await AmILeader(RaftSystemConfig.SystemPartition, ct).ConfigureAwait(false))
            throw new RaftException("RemovePartition cannot be initiated by a follower");

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(
            new System.RaftSystemRequest(System.RaftSystemRequestType.RemovePartition, partitionId)
            {
                Completion = tcs
            });

        (RaftOperationStatus status, long generation) = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        return new RaftPartitionLifecycleResult
        {
            Success = status == RaftOperationStatus.Success,
            Status = status,
            Generation = generation
        };
    }

    /// <inheritdoc/>
    public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) =>
        Volatile.Write(ref _stateMachineTransfer, transfer);

    /// <summary>
    /// Called by the P0 coordinator when it determines that <paramref name="endpoint"/> can never
    /// be promoted (e.g., below WAL compaction floor with no snapshot transfer registered).
    /// <c>JoinCluster(seeds)</c> polls this on the local endpoint and throws
    /// <see cref="InvalidOperationException"/> immediately rather than spinning to the timeout.
    /// </summary>
    internal void SetJoinTerminalReason(string endpoint, string reason) =>
        _joinTerminalReasons[endpoint] = reason;

    internal string? GetJoinTerminalReason(string endpoint) =>
        _joinTerminalReasons.TryGetValue(endpoint, out string? reason) ? reason : null;

    /// <inheritdoc/>
    public long GetPartitionGeneration(int partitionId)
    {
        if (partitions.TryGetValue(partitionId, out RaftPartition? partition))
            return partition.Generation;

        return 0;
    }

    /// <summary>
    /// Returns the best <see cref="System.NodeLoadReport"/> for the given partition from the
    /// gossip cache: prefers the report from the endpoint that currently leads P; falls back
    /// to the report with the greatest HLC <c>Time</c> among those containing P; returns
    /// <c>null</c> when no gossip report mentions P at all.
    /// Cross-endpoint <c>ReportVersion</c> is not used as a tiebreak.
    /// </summary>
    private System.NodeLoadReport? FindBestLoadReport(int partitionId)
    {
        string? leaderEndpoint = GetPartitionLeaderEndpoint(partitionId);
        IReadOnlyList<System.NodeLoadReport> reports = systemCoordinator.GetLoadReports();

        System.NodeLoadReport? best = null;

        if (leaderEndpoint is not null)
        {
            foreach (System.NodeLoadReport r in reports)
            {
                if (string.Equals(r.Endpoint, leaderEndpoint, StringComparison.Ordinal))
                {
                    best = r;
                    break;
                }
            }
        }

        // Fallback: among all reports that mention P, pick the one with the greatest HLC Time.
        // Cross-endpoint ReportVersion is not a valid tiebreak.
        if (best is null)
        {
            foreach (System.NodeLoadReport r in reports)
            {
                foreach (System.PartitionLoad l in r.Leaderships)
                {
                    if (l.PartitionId == partitionId && (best is null || r.Time > best.Time))
                    {
                        best = r;
                        break;
                    }
                }
            }
        }

        return best;
    }

    /// <inheritdoc/>
    public double GetPartitionLogOpsPerSecond(int partitionId)
    {
        // Local fast-path: no gossip lag.
        if (partitions.TryGetValue(partitionId, out RaftPartition? p) &&
            string.Equals(p.Leader, LocalEndpoint, StringComparison.Ordinal))
            return p.GetLogOpsPerSecond();

        System.NodeLoadReport? best = FindBestLoadReport(partitionId);
        if (best is null) return 0.0;

        foreach (System.PartitionLoad l in best.Leaderships)
        {
            if (l.PartitionId == partitionId)
                return l.LogOpsPerSecond;
        }

        return 0.0;
    }

    /// <inheritdoc/>
    public int GetPartitionWalQueueDepth(int partitionId)
    {
        // Local fast-path: no gossip lag.
        if (partitions.TryGetValue(partitionId, out RaftPartition? p) &&
            string.Equals(p.Leader, LocalEndpoint, StringComparison.Ordinal))
            return walScheduler.GetPartitionDepth(partitionId);

        System.NodeLoadReport? best = FindBestLoadReport(partitionId);
        if (best is null) return 0;

        foreach (System.PartitionLoad l in best.Leaderships)
        {
            if (l.PartitionId == partitionId)
                return l.WalQueueDepth;
        }

        return 0;
    }

    /// <inheritdoc/>
    public double GetPartitionCommitWaitMs(int partitionId)
    {
        // Local fast-path: no gossip lag.
        if (partitions.TryGetValue(partitionId, out RaftPartition? p) &&
            string.Equals(p.Leader, LocalEndpoint, StringComparison.Ordinal))
            return walScheduler.GetPartitionCommitWaitMs(partitionId);

        System.NodeLoadReport? best = FindBestLoadReport(partitionId);
        if (best is null) return 0.0;

        foreach (System.PartitionLoad l in best.Leaderships)
        {
            if (l.PartitionId == partitionId)
                return l.CommitWaitMs;
        }

        return 0.0;
    }

    /// <inheritdoc/>
    public System.ClusterMembership GetMembership() => systemCoordinator.GetMembership();

    /// <summary>
    /// Fires <see cref="OnMembershipChanged"/> with the new roster.
    /// Called by <see cref="RaftSystemCoordinator"/> each time <c>_cachedMembership</c>
    /// advances to a strictly higher version.
    /// </summary>
    internal void RaiseMembershipChanged(System.ClusterMembership membership) =>
        OnMembershipChanged?.Invoke(membership);

    /// <inheritdoc/>
    public IReadOnlyList<RaftPartitionRange> GetPartitionMap()
    {
        List<RaftPartitionRange> snapshot = new(partitions.Count);

        foreach (KeyValuePair<int, RaftPartition> kv in partitions)
        {
            RaftPartition p = kv.Value;
            snapshot.Add(new RaftPartitionRange
            {
                PartitionId  = p.PartitionId,
                StartRange   = p.StartRange,
                EndRange     = p.EndRange,
                RoutingMode  = p.RoutingMode,
                Generation   = p.Generation,
                State        = p.State,
            });
        }

        return snapshot;
    }

    /// <summary>
    /// Returns the number of the partition for the given partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPartitionKey(string partitionKey)
    {
        int rangeId = (int)HashUtils.InversePrefixedStaticHash(partitionKey, '/');
        if (rangeId < 0)
            rangeId = -rangeId;

        foreach (KeyValuePair<int, RaftPartition> partition in partitions)
        {
            if (partition.Value.RoutingMode == RaftRoutingMode.HashRange &&
                partition.Value.StartRange <= rangeId && partition.Value.EndRange >= rangeId)
                return partition.Key;
        }

        throw new RaftException("Couldn't find partition range for: " + partitionKey + " " + rangeId);
    }
    
    /// <summary>
    /// Returns the number of the partition for the given partition key
    /// </summary>
    /// <param name="partitionKey"></param>
    /// <returns></returns>
    public int GetPrefixPartitionKey(string prefixPartitionKey)
    {
        int rangeId = (int)HashUtils.SimpleHash(prefixPartitionKey);
        if (rangeId < 0)
            rangeId = -rangeId;

        foreach (KeyValuePair<int, RaftPartition> partition in partitions)
        {
            if (partition.Value.RoutingMode == RaftRoutingMode.HashRange &&
                partition.Value.StartRange <= rangeId && partition.Value.EndRange >= rangeId)
                return partition.Key;
        }

        throw new RaftException("Couldn't find partition range for: " + prefixPartitionKey + " " + rangeId);
    }
    
    internal void EnqueueResponse(string endpoint, RaftResponderRequest request) =>
        transportDispatcher.Enqueue(endpoint, request);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // 1. Stop and dispose the timer so no new work is injected and both
        //    Timer instances are released without waiting for GC.
        timerService.Dispose();

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
    }
}
