
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Kommander.System.Protos;
using Google.Protobuf;
using Kommander.Data;
using Kommander.Logging;
using Microsoft.Extensions.Logging;
using Kommander.Discovery;

namespace Kommander.System;

/// <summary>
/// Non-actor replacement for <c>RaftSystemActor</c>.  Owns a single-consumer
/// <see cref="Channel{T}"/> so all system-partition events are processed serially
/// without any Nixie dependency.
/// </summary>
internal sealed class RaftSystemCoordinator : IDisposable
{
    private const int MaxRetries = 10;

    private readonly Dictionary<string, string> systemConfiguration = new();

    private readonly RaftManager manager;

    private readonly ILogger<IRaft> logger;

    private string? leaderNode;

    private readonly Channel<RaftSystemRequest> _channel =
        Channel.CreateUnbounded<RaftSystemRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    // ── Test injection points ──────────────────────────────────────────────
    // Null in production. Tests set these to intercept replication and partition
    // activation without requiring a real Raft quorum or system partition.
    internal Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>>? ReplicateOverride;
    internal Action<List<RaftPartitionRange>>? StartPartitionsOverride;

    /// <summary>
    /// Test hook for <see cref="ReplicateCheckpointForPartition"/>.
    /// When set, replaces the call to <see cref="RaftManager.ReplicateCheckpoint"/> so unit
    /// tests can assert checkpoint replication without a live Raft quorum.
    /// </summary>
    internal Func<int, CancellationToken, Task<RaftReplicationResult>>? ReplicateCheckpointOverride;

    /// <summary>
    /// Delay between replication retries. Defaults to 5 seconds in production;
    /// tests set this to <see cref="TimeSpan.Zero"/> for fast retry cycles.
    /// </summary>
    internal TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    // Queue of drain sentinels.  Each DrainAsync() call enqueues one TCS here
    // and sends a DrainSentinel request; the loop completes it in FIFO order.
    private readonly ConcurrentQueue<TaskCompletionSource> _drainQueue = new();

    // ── Load-report store ──────────────────────────────────────────────────
    // All writes and internal reads are exclusive to the single-consumer loop.
    // GetLoadReports() copies a snapshot for external callers on other threads.
    private readonly LoadReportStore _loadReportStore = new();

    // ── Balancer controller ────────────────────────────────────────────────
    // Cooldown/outstanding tables owned by LeaderBalancer; initialized in constructor.
    private readonly LeaderBalancer _leaderBalancer;

    // ── Membership state ───────────────────────────────────────────────────
    // Cached view of the committed roster.  Only updated from within the single-
    // consumer loop so no locking is required.
    private ClusterMembership _cachedMembership = new() { MembershipVersion = 0, Members = [] };

    // The single-consumer channel loop already serialises membership changes: Receive
    // is awaited to completion before the next message is dequeued, so two changes can
    // never interleave through the normal path.  This flag is belt-and-suspenders for
    // code paths that bypass the loop (e.g. the MembershipChangePendingForTest hook)
    // and as a test affordance to simulate an in-flight change.
    private bool _membershipChangePending;

    /// <summary>Test hook — lets tests inject the pending-change flag directly.</summary>
    internal bool MembershipChangePendingForTest
    {
        get => _membershipChangePending;
        set => _membershipChangePending = value;
    }

    // ── Partition map and split/merge collaborators ────────────────────────
    private readonly SplitMergeController _splitMerge;
    private readonly PartitionMapService _partitionMap;

    // Exposed so tests can await clean loop exit after Stop().
    internal Task LoopTask => _loop;

    public RaftSystemCoordinator(RaftManager manager, ILogger<IRaft> logger)
    {
        this.manager = manager;
        this.logger = logger;

        _leaderBalancer = new LeaderBalancer(
            _loadReportStore,
            () => leaderNode,
            GetMembership,
            () => manager.HybridLogicalClock.TrySendOrLocalEvent(manager.LocalNodeId),
            () => manager.WalAdapter.GetCurrentTerm(RaftSystemConfig.SystemPartition),
            systemConfiguration,
            manager.SendTransferLeadershipSuggestion,
            manager.Liveness,
            manager.Configuration,
            manager.LocalEndpoint);

        _splitMerge = new SplitMergeController(
            systemConfiguration,
            Replicate,
            ReplicateCheckpointForPartition,
            Send,
            StartPartitions,
            () => manager.StateMachineTransfer,
            partitionId => manager.WalAdapter.GetMaxLog(partitionId),
            partitionId => manager.Partitions.TryGetValue(partitionId, out RaftPartition? p) ? p : null,
            manager.RemovePartition,
            partitionId => { manager.WalAdapter.DeletePartitionWAL(partitionId); },
            () => RetryDelay,
            MaxRetries,
            logger);

        _partitionMap = new PartitionMapService(
            systemConfiguration,
            Replicate,
            Send,
            StartPartitions,
            partitionId => { manager.WalAdapter.DeletePartitionWAL(partitionId); },
            partitionId => manager.Partitions.TryGetValue(partitionId, out RaftPartition? p) ? p : null,
            manager.RemovePartition,
            manager.Configuration.InitialPartitions,
            TrySeedInitialMembership,
            _splitMerge.PendingSplits,
            _splitMerge.PendingMerges,
            () => RetryDelay,
            MaxRetries,
            logger);

        _loop = Task.Run(RunAsync);
    }

    /// <summary>
    /// Enqueues a request for serial processing. Safe to call from any thread.
    /// </summary>
    internal void Send(RaftSystemRequest request) =>
        _channel.Writer.TryWrite(request);

    /// <summary>
    /// Returns a <see cref="Task"/> that completes only after all requests
    /// currently in the channel have been processed by the background loop.
    /// Used by tests instead of a fixed <c>Task.Delay</c>.
    /// </summary>
    internal Task DrainAsync()
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _drainQueue.Enqueue(tcs);
        _channel.Writer.TryWrite(new RaftSystemRequest(RaftSystemRequestType.DrainSentinel));
        return tcs.Task;
    }

    // ── Background loop ────────────────────────────────────────────────────

    private async Task RunAsync()
    {
        ChannelReader<RaftSystemRequest> reader = _channel.Reader;

        try
        {
            while (await reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                while (reader.TryRead(out RaftSystemRequest? request))
                {
                    try
                    {
                        await Receive(request, _cts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(
                            "[RaftSystemCoordinator] Unhandled exception processing {Type}: {Message}\n{StackTrace}",
                            request.Type, ex.Message, ex.StackTrace);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[RaftSystemCoordinator] RunAsync faulted: {Message}\n{StackTrace}",
                ex.Message, ex.StackTrace);
        }

        // Drain any messages written before the channel was completed.
        // The token is already cancelled at this point so Receive will return
        // quickly without starting any new replication or split work.
        while (reader.TryRead(out RaftSystemRequest? remaining))
        {
            try
            {
                await Receive(remaining, _cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(
                    "[RaftSystemCoordinator] drain {Type}: {Message}",
                    remaining.Type, ex.Message);
            }
        }
    }

    // ── Message handler ────────────────────────────────────────────────────

    private async Task Receive(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return;
        switch (message.Type)
        {
            case RaftSystemRequestType.ConfigRestored:
            {
                if (message.LogData is null)
                {
                    logger.LogWarning("Restored message is null");
                    return;
                }

                RaftSystemMessage systemMessage = Unserialize(message.LogData);
                systemConfiguration[systemMessage.Key] = systemMessage.Value;
                logger.LogInfoRestoredSystemConfiguration(systemMessage.Key);

                if (systemMessage.Key == RaftSystemConfigKeys.Members)
                    ApplyMembershipFromCache();
            }
            break;

            case RaftSystemRequestType.ConfigReplicated:
            {
                if (message.LogData is null)
                {
                    logger.LogWarning("Replication message is null");
                    return;
                }

                RaftSystemMessage systemMessage = Unserialize(message.LogData);
                systemConfiguration[systemMessage.Key] = systemMessage.Value;
                logger.LogInfoReplicatedSystemConfiguration(systemMessage.Key);

                // Live replication: dispatch to the correct subsystem based on the key.
                // Followers must never attempt to drive Phase 2 commits, and leaders already
                // track in-progress operations via _pendingSplits/_pendingMerges.
                if (systemMessage.Key == RaftSystemConfigKeys.Partitions)
                    _partitionMap.InitializePartitions(crashRecovery: false);
                else if (systemMessage.Key == RaftSystemConfigKeys.Members)
                    ApplyMembershipFromCache();
            }
            break;

            case RaftSystemRequestType.LeaderChanged:
                leaderNode = message.LeaderNode;

                if (string.IsNullOrEmpty(leaderNode))
                {
                    logger.LogInformation("Waiting for leader on system partition...");
                    return;
                }

                if (manager.LocalEndpoint == leaderNode)
                    await _partitionMap.TrySetInitialPartitions(cancellationToken).ConfigureAwait(false);
                else
                    // Follower path: just apply the current map. Crash-recovery re-enqueuing
                    // (SplitPartitionCommit / MergePartitionCommit) is the new leader's
                    // responsibility — it runs via RestoreCompleted after WAL replay, before
                    // leadership is determined. Running it here on every leader election would
                    // register stale _pendingSplits/_pendingMerges entries on followers that
                    // would cause double-commits if this node later wins leadership.
                    _partitionMap.InitializePartitions(crashRecovery: false);
                break;

            case RaftSystemRequestType.SplitPartition:
                await _splitMerge.TrySplitPartition(message.PartitionId, message.SplitPlan, message.Completion, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.SplitPartitionCommit:
                await _splitMerge.TrySplitPartitionCommit(message.PartitionId, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.MergePartition:
                await _splitMerge.TryMergePartitions(message.MergePlan!, message.Completion, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.MergePartitionCommit:
                await _splitMerge.TryMergePartitionCommit(message.PartitionId, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RestoreCompleted:
                // WAL restore finished: apply the persisted map and run crash-recovery
                // re-enqueuing to resume any Splitting/Draining phases left incomplete
                // before the previous shutdown.
                _partitionMap.InitializePartitions(crashRecovery: true);
                break;

            case RaftSystemRequestType.CreatePartition:
                await _partitionMap.TryCreatePartition(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RemovePartition:
                await _partitionMap.TryRemovePartition(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.AddMember:
                await TryAddMember(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.PromoteMember:
                await TryPromoteMember(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RemoveMember:
                await TryRemoveMember(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.ApplyGossipRoster:
                ApplyGossipRoster(message);
                break;

            case RaftSystemRequestType.ApplyGossipLoadReport:
                ApplyGossipLoadReport(message);
                break;

            case RaftSystemRequestType.RunBalancerPass:
                await RunBalancerPassAsync(cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.DrainSentinel:
                if (_drainQueue.TryDequeue(out TaskCompletionSource? tcs))
                    tcs.TrySetResult();
                break;

            default:
                throw new NotImplementedException($"Unhandled RaftSystemRequestType: {message.Type}");
        }
    }

    // ── Partition helpers ──────────────────────────────────────────────────

    private Task<RaftReplicationResult> Replicate(string type, byte[] data, bool autoCommit, CancellationToken ct) =>
        ReplicateOverride is { } fn
            ? fn(type, data, autoCommit, ct)
            : manager.ReplicateSystemLogs(type, data, autoCommit, ct);

    private Task<RaftReplicationResult> ReplicateCheckpointForPartition(int partitionId, CancellationToken ct) =>
        ReplicateCheckpointOverride is { } fn
            ? fn(partitionId, ct)
            : manager.ReplicateCheckpoint(partitionId, ct);

    private void StartPartitions(List<RaftPartitionRange> ranges) =>
        (StartPartitionsOverride ?? manager.StartUserPartitions)(ranges);

    // ── Membership seeding ─────────────────────────────────────────────────

    /// <summary>
    /// Called by the P0 leader after the partition map is established.
    /// If no <c>members</c> record exists yet, writes the current discovery set
    /// (all peers plus self) as the initial roster, all <see cref="ClusterMemberRole.Voter"/>.
    /// This preserves today's static-discovery behavior on greenfield clusters while
    /// providing a committed roster that future membership changes can build on.
    /// <para>
    /// <b>Known limitation:</b> the roster captures only the nodes visible to the P0 leader via
    /// <see cref="IDiscovery.GetNodes"/> at the instant of seeding. A node that registers in
    /// discovery after the seed is committed is absent from the roster. Because role gating
    /// suppresses elections/votes for non-voters, that node will be
    /// <see cref="ClusterMemberRole.NotMember"/> and cannot participate until it is explicitly
    /// added via the Join RPC.
    /// </para>
    /// </summary>
    private async Task TrySeedInitialMembership(CancellationToken cancellationToken)
    {
        if (systemConfiguration.ContainsKey(RaftSystemConfigKeys.Members) || _cachedMembership.MembershipVersion > 0)
            return;

        List<RaftNode> peers = manager.Discovery.GetNodes();

        List<ClusterMember> allMembers =
        [
            new()
            {
                Endpoint = manager.LocalEndpoint,
                NodeId = manager.LocalNodeId,
                Role = ClusterMemberRole.Voter,
                JoinedVersion = 1
            },
            // Peer NodeIds are provisional (0) because discovery only yields endpoints.
            // The Join RPC will replace these with each node's real configured NodeId when it
            // self-reports; the roster-derived peer set keys on Endpoint, not NodeId.
            // Self-exclusion and dedup guard against discovery backends that list the local
            // endpoint or return duplicates — both would corrupt quorum math.
            ..peers
                .Where(n => n.Endpoint != manager.LocalEndpoint)
                .DistinctBy(n => n.Endpoint)
                .Select(n => new ClusterMember
                {
                    Endpoint = n.Endpoint,
                    NodeId = 0,
                    Role = ClusterMemberRole.Voter,
                    JoinedVersion = 1
                })
        ];

        ClusterMembership seed = new() { MembershipVersion = 1, Members = allMembers };
        string json = JsonSerializer.Serialize(seed);

        RaftSystemMessage message = new() { Key = RaftSystemConfigKeys.Members, Value = json };

        if (cancellationToken.IsCancellationRequested)
            return;

        RaftReplicationResult result;
        try
        {
            result = await Replicate(
                RaftSystemConfig.RaftLogType, Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("[RaftSystemCoordinator] TrySeedInitialMembership aborted on shutdown");
            return;
        }

        if (result.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning(
                "[RaftSystemCoordinator] TrySeedInitialMembership: Failed to seed initial roster: {Status}",
                result.Status);
            return;
        }

        systemConfiguration[RaftSystemConfigKeys.Members] = json;
        _cachedMembership = seed;
        manager.RaiseMembershipChanged(seed);
        logger.LogInfoSeededInitialMembership(allMembers.Count);
    }

    // ── Membership helpers ─────────────────────────────────────────────────

    /// <summary>
    /// Returns the current cached roster. Returns an empty <see cref="ClusterMembership"/>
    /// (version 0, no members) when no roster has been committed yet.
    /// </summary>
    internal ClusterMembership GetMembership() => _cachedMembership;

    /// <summary>
    /// Applies a gossiped roster to the local cache when its version exceeds the locally
    /// committed version.  Called from the coordinator channel loop so it is serialized
    /// against all other cache mutations; the Raft log is never written.
    /// </summary>
    private void ApplyGossipRoster(RaftSystemRequest request)
    {
        ClusterMembership? roster = request.GossipedRoster;
        if (roster is null || roster.MembershipVersion <= _cachedMembership.MembershipVersion)
            return;

        logger.LogDebugGossipUpdatingCache(_cachedMembership.MembershipVersion, roster.MembershipVersion);

        _cachedMembership = roster;
        manager.RaiseMembershipChanged(roster);
    }

    // Load-report methods delegate to _loadReportStore (bodies live in LoadReportStore).

    private void ApplyGossipLoadReport(RaftSystemRequest request) =>
        _loadReportStore.Apply(request);

    /// <summary>
    /// Returns a snapshot of all load reports currently retained.
    /// The P0 balancer controller calls this at the start of each planning pass.
    /// </summary>
    internal IReadOnlyList<NodeLoadReport> GetLoadReports() =>
        _loadReportStore.GetAll();

    private void EvictStaleLoadReports(TimeSpan ttl, Time.HLCTimestamp now) =>
        _loadReportStore.EvictStale(ttl, now);

    // ── Balancer controller ────────────────────────────────────────────────

    /// <summary>
    /// Executes one leader-balancer planning pass.
    /// Only acts when this node is the P0 leader and <c>EnableLeaderBalancer</c> is on.
    ///
    /// <para><b>Pass sequence:</b>
    /// <list type="number">
    ///   <item>Clear P0 imbalance gauges and return early when not P0 or flag is off.</item>
    ///   <item>Reconcile outstanding moves: confirm view-visible completions (enter cooldown)
    ///         or expire timed-out ones (enter cooldown for one cycle to prevent hot-looping).</item>
    ///   <item>Reduce retained load reports → <see cref="GlobalLeadershipView"/>.  Skip pass if
    ///         the view is incomplete (increments <c>raft.balancer.skipped_passes_total</c>).</item>
    ///   <item>Update imbalance gauges (even on a skipped pass, after reconciliation).</item>
    ///   <item>Cap new moves by <see cref="RaftConfiguration.MaxConcurrentTransfers"/> — number
    ///         of already-outstanding slots consumed.</item>
    ///   <item>Dispatch each planned move as a
    ///         <see cref="Data.TransferLeadershipSuggestionRequest"/> to the partition's current owner
    ///         via <see cref="RaftManager.SendTransferLeadershipSuggestion"/>.</item>
    /// </list></para>
    ///
    /// <para>The controller holds no durable state — if P0 leadership moves the new leader
    /// resets cooldowns and the outstanding table and re-derives reality from the next pass.</para>
    /// </summary>
    private Task RunBalancerPassAsync(CancellationToken cancellationToken) =>
        _leaderBalancer.RunPassAsync(cancellationToken);

    private static void UpdateImbalanceGauges(GlobalLeadershipView view) =>
        RaftSystemCoordinatorHelpers.UpdateImbalanceGauges(view);

    /// <summary>
    /// Resets the local membership cache to an empty roster.
    /// Intended for tests that need to simulate a node that missed committed membership
    /// entries, allowing the gossip anti-entropy path to be exercised in isolation.
    /// Must not be called in production code.
    /// </summary>
    internal void ResetMembershipCacheForTest() =>
        _cachedMembership = new ClusterMembership { MembershipVersion = 0, Members = [] };

    /// <summary>
    /// Returns the current count of outstanding move suggestions for test assertions.
    /// Must not be called from production code — races with the coordinator loop.
    /// </summary>
    internal int OutstandingMoveCountForTest => _leaderBalancer.OutstandingMoveCountForTest;

    /// <summary>
    /// Injects a <see cref="RaftPartitionMap"/> into the coordinator's system-configuration
    /// store via the <c>ConfigRestored</c> channel message.
    /// Allows unit tests to seed the partition map without running a full Raft log.
    /// Must only be used from tests; call <see cref="DrainAsync"/> afterwards.
    /// </summary>
    internal void SetPartitionMapForTest(RaftPartitionMap map)
    {
        string json = global::System.Text.Json.JsonSerializer.Serialize(map);
        RaftSystemMessage proto = new() { Key = RaftSystemConfigKeys.Partitions, Value = json };
        Send(new RaftSystemRequest(RaftSystemRequestType.ConfigRestored, Serialize(proto)));
    }

    /// <summary>
    /// Injects a <see cref="ClusterMembership"/> into the coordinator's cache via the
    /// <c>ConfigRestored</c> channel message so tests can drive balancer passes without
    /// calling <c>JoinCluster</c>.
    /// Must only be used from tests; call <see cref="DrainAsync"/> afterwards.
    /// </summary>
    internal void SetMembershipForTest(ClusterMembership membership)
    {
        string json = global::System.Text.Json.JsonSerializer.Serialize(membership);
        RaftSystemMessage proto = new() { Key = RaftSystemConfigKeys.Members, Value = json };
        Send(new RaftSystemRequest(RaftSystemRequestType.ConfigRestored, Serialize(proto)));
    }

    // ── Learner promotion driver ───────────────────────────────────────────────
    // Keyed by learner endpoint; records when the learner first appeared caught-up on all partitions.
    private readonly Dictionary<string, DateTimeOffset> _learnerCaughtUpSince = new();

    // Endpoints for which a terminal "below WAL floor, no transfer" signal has already been sent.
    // Prevents re-logging and re-signalling on every promotion tick.
    private readonly HashSet<string> _terminalBelowFloorEndpoints = new();

    /// <summary>
    /// Called from <c>RaftManager.UpdateNodes</c> on every <see cref="RaftTimerService"/>
    /// <c>UpdateNodes</c> tick.  Runs only when this node is the P0 leader.
    /// <para>
    /// For each Learner in the committed roster it measures the per-partition lag on every
    /// partition (system + user).  For partitions this node leads, lag is read directly from
    /// <c>lastCommitIndexes</c>.  For partitions led by another node the driver queries that
    /// node via <see cref="ICommunication.GetRemoteFollowerLag"/> — in the in-memory transport
    /// this is a direct call; the gRPC transport returns <see langword="null"/> (skip) until a
    /// dedicated RPC is added.  A learner that stays within
    /// <see cref="RaftConfiguration.LearnerPromotionLag"/> entries on <em>all</em> checked
    /// partitions for at least <see cref="RaftConfiguration.LearnerPromotionStableWindow"/> is
    /// promoted to Voter.  At most one membership change is in flight at a time.
    /// </para>
    /// </summary>
    internal async Task CheckLearnerPromotionsAsync(CancellationToken cancellationToken = default)
    {
        // Only the P0 leader runs the driver.
        bool amP0Leader = await manager.AmILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!amP0Leader)
        {
            _learnerCaughtUpSince.Clear();
            _terminalBelowFloorEndpoints.Clear();
            return;
        }

        // Belt-and-suspenders: a membership change is already committed or being replicated.
        if (_membershipChangePending)
            return;

        ClusterMembership roster = _cachedMembership;
        List<ClusterMember> learners = roster.Members
            .Where(m => m.Role == ClusterMemberRole.Learner)
            .ToList();

        if (learners.Count == 0)
        {
            _learnerCaughtUpSince.Clear();
            _terminalBelowFloorEndpoints.Clear();
            return;
        }

        // Collect all partition ids (system + user) to measure lag on.
        List<int> allPartitionIds = new(manager.Partitions.Count + 1) { RaftSystemConfig.SystemPartition };
        allPartitionIds.AddRange(manager.Partitions.Keys);

        DateTimeOffset now = DateTimeOffset.UtcNow;

        foreach (ClusterMember learner in learners)
        {
            string endpoint = learner.Endpoint;
            bool caughtUp = true;

            foreach (int partitionId in allPartitionIds)
            {
                bool isPartitionLeader = await manager.AmILeaderQuick(partitionId).ConfigureAwait(false);

                long leaderCommitted;
                long? learnerCommittedNullable;

                if (isPartitionLeader)
                {
                    // Happy path: we lead this partition and can read lastCommitIndexes directly.
                    leaderCommitted = await manager.GetFollowerCommittedIndexAsync(partitionId, manager.LocalEndpoint).ConfigureAwait(false);
                    if (leaderCommitted < 0)
                        continue;

                    // Nullable: null means the learner has never acked this partition at all —
                    // i.e., the node was not configured with this partition. Skip the lag check
                    // so a node with fewer partitions than the P0 leader is not blocked.
                    learnerCommittedNullable = await manager.GetFollowerCommittedIndexNullableAsync(partitionId, endpoint).ConfigureAwait(false);
                }
                else
                {
                    // This partition is led by another node. Query that node remotely so the
                    // promotion gate covers ALL partitions, not just those the P0 leader leads.
                    // If the transport returns null (gRPC before a dedicated RPC is added, or
                    // the leader is unknown/unreachable) we skip the partition — same as before.
                    string? leaderEndpoint = manager.GetPartitionLeaderEndpoint(partitionId);
                    if (string.IsNullOrEmpty(leaderEndpoint))
                        continue;

                    RaftNode leaderNode = new(leaderEndpoint);

                    // Ask the remote leader for its own committed index (follower = leaderEndpoint
                    // maps to localCommittedIndex on that node) and for the learner's lag.
                    long? remoteLeaderCommitted = await manager.Communication.GetRemoteFollowerLag(
                        manager, leaderNode, partitionId, leaderEndpoint).ConfigureAwait(false);
                    if (remoteLeaderCommitted is null || remoteLeaderCommitted.Value < 0)
                        continue;

                    leaderCommitted = remoteLeaderCommitted.Value;
                    learnerCommittedNullable = await manager.Communication.GetRemoteFollowerLag(
                        manager, leaderNode, partitionId, endpoint).ConfigureAwait(false);
                }

                // null here has two interpretations that we cannot distinguish today:
                //   (a) the learner genuinely has no assignment for this partition (future
                //       per-partition placement) — skipping is correct.
                //   (b) replication just started and the learner has not yet sent a
                //       CompleteAppendLogs ack — skipping creates an early-promotion window
                //       where the learner may be promoted before receiving a single entry on
                //       a freshly-replicated partition.
                // In the current join-all-partitions model (b) is the common case, so this
                // skip is a known gap. The impact is mitigated by PreVote (the new Voter
                // cannot win an election until its log is fresh enough) and by the backfill
                // path that replays outstanding entries after promotion. When per-partition
                // placement is introduced, callers should supply an explicit "expected
                // partition set" so (a) and (b) can be told apart and (b) blocked.
                if (learnerCommittedNullable is null)
                    continue;

                long lag = leaderCommitted - learnerCommittedNullable.Value;
                if (lag > manager.Configuration.LearnerPromotionLag)
                {
                    // If the learner is below the WAL compaction floor and no snapshot transfer
                    // is registered it can never catch up via log replay. Signal the joiner once
                    // so JoinCluster fails fast with a clear error instead of timing out.
                    if (!_terminalBelowFloorEndpoints.Contains(endpoint))
                    {
                        long floor = manager.WalAdapter.GetLastCheckpoint(partitionId);
                        bool canRepairViaSnapshot = manager.StateMachineTransfer is not null
                            || (partitionId == RaftSystemConfig.SystemPartition && manager.SystemStateTransfer is not null);
                        if (floor > 0 && learnerCommittedNullable.Value < floor && !canRepairViaSnapshot)
                        {
                            string reason =
                                $"learner is below WAL compaction floor on partition {partitionId} " +
                                $"(learnerIndex={learnerCommittedNullable.Value}, floor={floor}). " +
                                "Register IRaftStateMachineTransfer (or IRaftSystemStateTransfer for P0) to enable snapshot-based catch-up.";

                            logger.LogWarning(
                                "[RaftSystemCoordinator] Learner {Endpoint} permanently blocked: {Reason}",
                                endpoint, reason);

                            _terminalBelowFloorEndpoints.Add(endpoint);

                            // Route the terminal signal to the actual joiner. In-process (InMemoryCommunication)
                            // this calls SetJoinTerminalReason directly on the target manager. Over gRPC/REST the
                            // default ICommunication stub is a no-op (joiner times out after 60 s — a future task
                            // adds the wire notification).
                            await manager.Communication.NotifyJoinBlocked(manager, endpoint, reason, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    caughtUp = false;
                    break;
                }
            }

            if (caughtUp)
            {
                if (!_learnerCaughtUpSince.TryGetValue(endpoint, out DateTimeOffset since))
                {
                    _learnerCaughtUpSince[endpoint] = now;
                    // Not stable yet — stable window starts now.
                }
                else if (now - since >= manager.Configuration.LearnerPromotionStableWindow)
                {
                    // Stable long enough — promote.
                    logger.LogInfoPromotingLearnerToVoter(endpoint, now - since);
                    _learnerCaughtUpSince.Remove(endpoint);

                    Send(new RaftSystemRequest(
                        RaftSystemRequestType.PromoteMember,
                        endpoint,
                        learner.NodeId,
                        roster.MembershipVersion));

                    // Promote one at a time — exit; next tick handles remaining learners.
                    return;
                }
            }
            else
            {
                // Not caught up — reset the stable window for this learner.
                _learnerCaughtUpSince.Remove(endpoint);
            }
        }
    }

    /// <summary>
    /// P0-leader-only eviction driver: for each member whose SWIM liveness state has
    /// been <see cref="Gossip.MemberLivenessState.Dead"/> for at least
    /// <c>DeadMemberEvictionGrace</c>, commits a single <c>RemoveMember</c> entry.
    /// One eviction per call; caller retries on the next tick.
    ///
    /// <para>
    /// Only the P0 leader evicts — followers update their own liveness table but never
    /// commit membership changes.  The quorum-safety precondition in
    /// <see cref="TryRemoveMember"/> prevents eviction from draining the voter set below
    /// a viable majority.
    /// </para>
    /// </summary>
    internal async Task EvictDeadMembersAsync()
    {
        bool amP0Leader = await manager.AmILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!amP0Leader)
            return;

        if (_membershipChangePending)
            return;

        ClusterMembership roster = _cachedMembership;
        if (roster.MembershipVersion == 0)
            return;

        DateTimeOffset now = DateTimeOffset.UtcNow;
        TimeSpan grace = manager.Configuration.DeadMemberEvictionGrace;

        IReadOnlyList<string> evictable = manager.Liveness.GetEvictable(now, grace);
        if (evictable.Count == 0)
            return;

        foreach (string endpoint in evictable)
        {
            // Only evict members that are still in the roster.
            ClusterMember? member = roster.Members.FirstOrDefault(m => m.Endpoint == endpoint);
            if (member is null)
            {
                manager.Liveness.Remove(endpoint);
                continue;
            }

            // Don't evict self — a running node cannot be dead.
            if (endpoint == manager.LocalEndpoint)
            {
                manager.Liveness.Remove(endpoint);
                continue;
            }

            logger.LogWarning(
                "[RaftSystemCoordinator] Evicting Dead member {Endpoint} (dead > {Grace:g}); roster version {Version}",
                endpoint, grace, roster.MembershipVersion);

            Send(new RaftSystemRequest(RaftSystemRequestType.RemoveMember, endpoint, member.NodeId, roster.MembershipVersion));

            // One eviction per tick — the coordinator loop processes them serially.
            return;
        }
    }

    /// <summary>
    /// Reads the <see cref="RaftSystemConfigKeys.Members"/> entry from
    /// <c>systemConfiguration</c> and updates <see cref="_cachedMembership"/>.
    /// A no-op when the key is absent (pre-seed transient).
    /// </summary>
    private void ApplyMembershipFromCache()
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Members, out string? membersJson))
            return;

        ClusterMembership? membership = JsonSerializer.Deserialize<ClusterMembership>(membersJson);
        if (membership is null)
        {
            logger.LogError("ApplyMembershipFromCache: Failed to parse membership record");
            return;
        }

        // Accept only monotonically newer versions so that a slow ConfigReplicated replay
        // does not clobber a locally-updated cache that is already at a higher version.
        if (membership.MembershipVersion > _cachedMembership.MembershipVersion)
        {
            _cachedMembership = membership;
            manager.RaiseMembershipChanged(membership);
        }
    }

    /// <summary>
    /// Shared pre-flight checks for all membership mutations.
    /// Returns false and completes <paramref name="completion"/> with the appropriate
    /// failure status if the request should be rejected without replication.
    /// </summary>
    private bool ValidateMembershipRequest(
        long expectedVersion,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion)
    {
        if (_membershipChangePending)
        {
            logger.LogWarning("[RaftSystemCoordinator] Membership change rejected: another change is in flight");
            completion?.TrySetResult((RaftOperationStatus.ConcurrentMembershipChange, 0));
            return false;
        }

        if (_cachedMembership.MembershipVersion != expectedVersion)
        {
            logger.LogWarning(
                "[RaftSystemCoordinator] Membership change rejected: expected version {Expected} but current is {Current}",
                expectedVersion, _cachedMembership.MembershipVersion);
            completion?.TrySetResult((RaftOperationStatus.StaleMembership, _cachedMembership.MembershipVersion));
            return false;
        }

        return true;
    }

    /// <summary>
    /// Replicates a new membership record, updates the local cache, and resolves
    /// <paramref name="completion"/>. Handles the retry/backoff loop identically to
    /// partition-map mutations.
    /// <para>
    /// <see cref="_membershipChangePending"/> is NOT cleared here — callers wrap this call in
    /// <c>try/finally { _membershipChangePending = false; }</c> so the flag is always
    /// released even if this method throws unexpectedly.
    /// </para>
    /// </summary>
    private async Task ReplicateMembership(
        ClusterMembership newMembership,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        string json = JsonSerializer.Serialize(newMembership);
        RaftSystemMessage sysMessage = new() { Key = RaftSystemConfigKeys.Members, Value = json };

        try
        {
            for (int i = 0; i < MaxRetries; i++)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    completion?.TrySetCanceled(cancellationToken);
                    return;
                }

                RaftReplicationResult result = await Replicate(
                    RaftSystemConfig.RaftLogType, Serialize(sysMessage), true, cancellationToken
                ).ConfigureAwait(false);

                if (result.Status != RaftOperationStatus.Success)
                {
                    logger.LogWarning(
                        "ReplicateMembership: replication failed {Status} {LogIndex} Retry={Retry}",
                        result.Status, result.LogIndex, i);

                    if (result.Status == RaftOperationStatus.NodeIsNotLeader)
                    {
                        completion?.TrySetResult((result.Status, 0));
                        return;
                    }

                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] ReplicateMembership delay aborted on shutdown");
                        return;
                    }

                    if (i <= 8) continue;

                    completion?.TrySetResult((result.Status, 0));
                    return;
                }

                break;
            }

            systemConfiguration[RaftSystemConfigKeys.Members] = json;
            _cachedMembership = newMembership;
            manager.RaiseMembershipChanged(newMembership);

            // Raft §6 self-removal: if this node is no longer a voter in the new
            // configuration, step down from every partition it currently leads so
            // followers can elect a new leader without waiting for a heartbeat timeout.
            if (!newMembership.Members.Any(m => m.Endpoint == manager.LocalEndpoint && m.Role == ClusterMemberRole.Voter))
                _ = StepDownSelfRemovedAsync();

            completion?.TrySetResult((RaftOperationStatus.Success, newMembership.MembershipVersion));
        }
        catch (OperationCanceledException)
        {
            completion?.TrySetCanceled(cancellationToken);
            throw;
        }
        catch (Exception)
        {
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            throw;
        }
    }

    /// <summary>
    /// Steps down from every partition this node currently leads after the committed
    /// membership record has removed it from the voter set.  Called fire-and-forget
    /// from <see cref="ReplicateMembership"/> so the completion TCS is resolved without
    /// waiting for election convergence; failures are logged and swallowed.
    /// </summary>
    private async Task StepDownSelfRemovedAsync()
    {
        List<int> partitionIds = [RaftSystemConfig.SystemPartition, ..manager.Partitions.Keys];

        foreach (int partitionId in partitionIds)
        {
            try
            {
                if (await manager.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    await manager.StepDownWithoutSuccessorWaitAsync(partitionId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning("StepDownSelfRemoved: partition {PartitionId}: {Message}", partitionId, ex.Message);
            }
        }
    }

    /// <summary>
    /// Appends a new node as a <see cref="ClusterMemberRole.Learner"/> to the committed roster.
    /// Rejected if another change is in flight or the expected version is stale.
    /// </summary>
    private async Task TryAddMember(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (!ValidateMembershipRequest(message.ExpectedMembershipVersion, completion))
            return;

        string endpoint = message.MemberEndpoint ?? "";

        if (string.IsNullOrEmpty(endpoint))
        {
            logger.LogWarning("TryAddMember: Endpoint is null or empty; rejecting");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (_cachedMembership.Members.Any(m => m.Endpoint == endpoint))
        {
            // Idempotent: a previous AddMember committed but the JoinResponse was lost, so the
            // joiner retried. Treat this as success so the caller can proceed past the admission
            // loop without waiting for the 60 s timeout. Return the current version so the joiner
            // can log the correct roster version.
            logger.LogInfoAddMemberAlreadyInRoster(endpoint, _cachedMembership.MembershipVersion);
            completion?.TrySetResult((RaftOperationStatus.Success, _cachedMembership.MembershipVersion));
            return;
        }

        long newVersion = _cachedMembership.MembershipVersion + 1;

        ClusterMembership newMembership = new()
        {
            MembershipVersion = newVersion,
            Members =
            [
                .._cachedMembership.Members,
                new ClusterMember
                {
                    Endpoint = endpoint,
                    NodeId = message.MemberNodeId,
                    Role = ClusterMemberRole.Learner,
                    JoinedVersion = newVersion
                }
            ]
        };

        _membershipChangePending = true;
        logger.LogInfoAddMemberAddingLearner(endpoint, newVersion);

        try
        {
            await ReplicateMembership(newMembership, completion, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _membershipChangePending = false;
        }
    }

    /// <summary>
    /// Promotes a committed <see cref="ClusterMemberRole.Learner"/> to
    /// <see cref="ClusterMemberRole.Voter"/>. The node enters quorum at the commit point.
    /// </summary>
    private async Task TryPromoteMember(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (!ValidateMembershipRequest(message.ExpectedMembershipVersion, completion))
            return;

        string endpoint = message.MemberEndpoint ?? "";

        ClusterMember? member = _cachedMembership.Members.FirstOrDefault(m => m.Endpoint == endpoint);
        if (member is null)
        {
            logger.LogError("TryPromoteMember: Endpoint {Endpoint} not found in roster", endpoint);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (member.Role != ClusterMemberRole.Learner)
        {
            logger.LogError(
                "TryPromoteMember: Endpoint {Endpoint} is not a Learner (Role={Role})",
                endpoint, member.Role);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        long newVersion = _cachedMembership.MembershipVersion + 1;

        ClusterMembership newMembership = new()
        {
            MembershipVersion = newVersion,
            Members = _cachedMembership.Members
                .Select(m => m.Endpoint == endpoint
                    ? new ClusterMember { Endpoint = m.Endpoint, NodeId = m.NodeId, Role = ClusterMemberRole.Voter, JoinedVersion = m.JoinedVersion }
                    : m)
                .ToList()
        };

        _membershipChangePending = true;
        logger.LogInfoPromoteMemberToVoter(endpoint, newVersion);

        try
        {
            await ReplicateMembership(newMembership, completion, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _membershipChangePending = false;
        }
    }

    /// <summary>
    /// Removes a node from the committed roster (graceful leave or failure-driven eviction).
    /// Quorum shrinks at the commit point; single-server safety is preserved because
    /// changes are applied one node at a time.
    /// </summary>
    private async Task TryRemoveMember(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (!ValidateMembershipRequest(message.ExpectedMembershipVersion, completion))
            return;

        string endpoint = message.MemberEndpoint ?? "";

        ClusterMember? member = _cachedMembership.Members.FirstOrDefault(m => m.Endpoint == endpoint);
        if (member is null)
        {
            // Idempotent: the endpoint is already absent — a previous RemoveMember committed but
            // the response was lost before the caller saw it. Treat as success so the caller's
            // retry loop does not spin to timeout.
            logger.LogInfoRemoveMemberNotInRoster(endpoint, _cachedMembership.MembershipVersion);
            completion?.TrySetResult((RaftOperationStatus.Success, _cachedMembership.MembershipVersion));
            return;
        }

        // Quorum-safety precondition: at least 1 voter must remain after removal so the
        // cluster can still commit entries (single-node commit is supported — see commit 3fe6cae).
        // We do NOT gate non-Voter (Learner/Leaving) removals — those don't affect quorum.
        if (member.Role == ClusterMemberRole.Voter)
        {
            int remainingVoters = _cachedMembership.Members.Count(m => m.Role == ClusterMemberRole.Voter && m.Endpoint != endpoint);
            if (remainingVoters < 1)
            {
                // Would leave zero voters: cluster becomes permanently unavailable.
                // Return InsufficientVoters so the caller gives up immediately rather than
                // retrying — this is a permanent condition, not a transient one.
                logger.LogError("TryRemoveMember: Refusing to remove {Endpoint} — would leave {Remaining} voter(s), making the cluster unavailable",
                    endpoint, remainingVoters);
                completion?.TrySetResult((RaftOperationStatus.InsufficientVoters, 0));
                return;
            }
        }

        long newVersion = _cachedMembership.MembershipVersion + 1;

        ClusterMembership newMembership = new()
        {
            MembershipVersion = newVersion,
            Members = _cachedMembership.Members.Where(m => m.Endpoint != endpoint).ToList()
        };

        _membershipChangePending = true;
        logger.LogInfoRemoveMember(endpoint, newVersion);

        try
        {
            await ReplicateMembership(newMembership, completion, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _membershipChangePending = false;
        }
    }

    // ── Static helpers (bodies live in RaftSystemCoordinatorHelpers) ──────────

    internal static List<RaftPartitionRange> DivideIntoRanges(int numberOfRanges) =>
        RaftSystemCoordinatorHelpers.DivideIntoRanges(numberOfRanges);

    internal static byte[] Serialize(RaftSystemMessage message) =>
        RaftSystemCoordinatorHelpers.Serialize(message);

    private static RaftSystemMessage Unserialize(byte[] serializedData) =>
        RaftSystemCoordinatorHelpers.Unserialize(serializedData);

    private int _stopped;

    /// <summary>
    /// Returns true once <see cref="Stop"/> has been called.
    /// Used by <c>RaftManager.ReceiveLeave</c> to fail-fast without posting
    /// to the coordinator channel, which is already completed at that point.
    /// </summary>
    internal bool IsStopped => _stopped != 0;

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>
    /// Signals the coordinator to stop: cancels in-flight async work immediately,
    /// then completes the channel so the background loop drains and exits.
    /// Idempotent — safe to call multiple times.
    /// </summary>
    internal void Stop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
            return;

        // Cancel first so any in-flight ReplicateSystemLogs / Task.Delay returns promptly.
        _cts.Cancel();
        _channel.Writer.TryComplete();
    }

    public void Dispose()
    {
        Stop(); // cancels _cts and completes channel; loop exits promptly
        try { _loop.Wait(TimeSpan.FromSeconds(5)); } catch { /* ignore shutdown races */ }
        _cts.Dispose();
    }
}
