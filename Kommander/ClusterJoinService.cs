
using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.System;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// The admission protocol: both ways this node gets into a cluster (discovery-based and
/// seed-based), the leader-side handler that admits somebody else, and the diagnostics a stalled
/// join needs.
/// <para>
/// Every wait here is bounded <b>unconditionally</b> — including when the caller passes a cancelable
/// token. A caller whose token never actually fires (an xUnit test-context token with no configured
/// timeout, say) would otherwise have no liveness bound at all, so a cluster that failed to assemble
/// hung forever and silently. The caller's token still cancels earlier than either bound.
/// </para>
/// <para>
/// There are two bounds, because the waits fail for two different reasons. Admission and
/// initialization are bounded by <see cref="RaftConfiguration.JoinTimeout"/>, measured from the start
/// of the join: a cluster that cannot admit or initialize this node within it is wedged, and failing
/// loudly with a state dump beats hanging. The seed join's wait for promotion to Voter is a catch-up
/// whose length follows the data size and the write rate, so it is bounded by progress instead
/// (<see cref="RaftConfiguration.JoinPromotionStallTimeout"/>, judged by
/// <see cref="JoinCatchUpProgress"/>). A single fixed 60 s bound over all three waits aborted every
/// replacement of a node on a loaded cluster while the learner was still catching up (Caraxes,
/// 2026-10-14).
/// </para>
/// <para>
/// A seed join that fails after this node was admitted withdraws the node from the roster before
/// it throws (<see cref="ClusterLeaveService.WithdrawFailedJoinAsync"/>); otherwise the failed join
/// left a Learner in the roster that the survivors keep tracking for as long as it stays. A join
/// canceled by the caller does not withdraw: the caller owns that decision.
/// </para>
/// <para>
/// Concurrency: the terminal-reason map is the only shared state and is a concurrent dictionary
/// because it is written by the coordinator's decision path and read by the joiner's wait loop on
/// a different thread.
/// </para>
/// </summary>
internal sealed class ClusterJoinService
{
    /// <summary>Interval between the Information lines that report a long promotion wait.</summary>
    private const long PromotionReportIntervalMs = 10_000;

    /// <summary>
    /// Per-endpoint terminal reasons set by the system-partition leader when it determines a
    /// learner can never be promoted (e.g., below the WAL compaction floor with no snapshot
    /// transfer). The entry for the local endpoint is checked inside the seed-join wait loops so
    /// the joiner fails fast with a descriptive error rather than spinning to the timeout.
    /// Written by the coordinator, read by the joining node's loop. Only the local node's entry
    /// matters on the follower side; leader entries are noise-free because the leader never waits
    /// for its own promotion.
    /// </summary>
    private readonly ConcurrentDictionary<string, string> _joinTerminalReasons = new();

    private readonly IPartitionProvider partitionProvider;
    private readonly RaftConfiguration configuration;
    private readonly ClusterHandler clusterHandler;
    private readonly IWAL walAdapter;
    private readonly Func<RaftSystemCoordinator> getCoordinator;
    private readonly Func<int, ValueTask<bool>> amILeaderQuick;
    private readonly Func<bool> isInitialized;
    private readonly Func<Task> getInitializedSignal;
    private readonly Func<ClusterMemberRole> getLocalRole;
    private readonly Action startSystemPartition;
    private readonly Func<Task> refreshNodes;
    private readonly Func<RaftNode, JoinRequest, Task<JoinResponse>> sendJoin;
    private readonly Func<long> getSnapshotProgressMarker;
    private readonly Func<IReadOnlyList<string>, CancellationToken, Task<bool>> withdrawFailedJoin;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;
    private readonly int localNodeId;

    internal ClusterJoinService(
        IPartitionProvider partitionProvider,
        RaftConfiguration configuration,
        ClusterHandler clusterHandler,
        IWAL walAdapter,
        Func<RaftSystemCoordinator> getCoordinator,
        Func<int, ValueTask<bool>> amILeaderQuick,
        Func<bool> isInitialized,
        Func<Task> getInitializedSignal,
        Func<ClusterMemberRole> getLocalRole,
        Action startSystemPartition,
        Func<Task> refreshNodes,
        Func<RaftNode, JoinRequest, Task<JoinResponse>> sendJoin,
        Func<long> getSnapshotProgressMarker,
        Func<IReadOnlyList<string>, CancellationToken, Task<bool>> withdrawFailedJoin,
        ILogger<IRaft> logger,
        string localEndpoint,
        int localNodeId)
    {
        this.partitionProvider = partitionProvider;
        this.configuration = configuration;
        this.clusterHandler = clusterHandler;
        this.walAdapter = walAdapter;
        this.getCoordinator = getCoordinator;
        this.amILeaderQuick = amILeaderQuick;
        this.isInitialized = isInitialized;
        this.getInitializedSignal = getInitializedSignal;
        this.getLocalRole = getLocalRole;
        this.startSystemPartition = startSystemPartition;
        this.refreshNodes = refreshNodes;
        this.sendJoin = sendJoin;
        this.getSnapshotProgressMarker = getSnapshotProgressMarker;
        this.withdrawFailedJoin = withdrawFailedJoin;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
        this.localNodeId = localNodeId;
    }

    /// <summary>Records a permanent block on the given endpoint's promotion.</summary>
    internal void SetJoinTerminalReason(string endpoint, string reason) =>
        _joinTerminalReasons[endpoint] = reason;

    /// <summary>Returns the permanent block recorded for an endpoint, or null when it can still be promoted.</summary>
    internal string? GetJoinTerminalReason(string endpoint) =>
        _joinTerminalReasons.TryGetValue(endpoint, out string? reason) ? reason : null;

    /// <summary>
    /// Runs one discovery refresh synchronously at join time so the first election is not gated on
    /// the periodic node-update timer.
    /// <para>
    /// <see cref="Consensus.ElectionCoordinator"/> refuses a self-election while the node's peer set
    /// is empty and discovery has not yet reported (<c>InitialNodesDiscovered</c>). That flag is set
    /// only by <see cref="RaftManager.UpdateNodes"/>, which the timer drives from construction time
    /// and which skips every tick that precedes <see cref="ClusterHandler.Joined"/>. When the
    /// embedder's construction-to-join gap exceeds <see cref="RaftConfiguration.TimerInitialDelay"/>
    /// the first tick is wasted and the system partition cannot elect until the second tick, a full
    /// <see cref="RaftConfiguration.UpdateNodesInterval"/> (5 s by default) later. Running the same
    /// refresh here, after <c>Joined</c> and the system partition exist, makes the first election
    /// depend on the election timeout alone while keeping the gate's safety semantics untouched: a
    /// seed-joining node still cannot self-elect before discovery has reported its peers.
    /// </para>
    /// <para>
    /// Must be called only after <see cref="ClusterHandler.Joined"/> is set and
    /// <see cref="RaftManager.StartSystemPartition"/> ran, or the refresh is a no-op that does not
    /// set the flag. Failures are logged and swallowed: the periodic timer retries discovery on its
    /// own cadence, and a transient discovery error must not fail a join that
    /// <see cref="Discovery.IDiscovery.Register"/> already accepted.
    /// </para>
    /// </summary>
    private async Task RefreshNodesAtJoinAsync()
    {
        try
        {
            await refreshNodes().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                "JoinCluster: join-time node discovery failed ({Message}); the periodic UpdateNodes timer will retry",
                ex.Message);
        }
    }

    /// <summary>
    /// One-line snapshot of how far cluster assembly has progressed, emitted on every join wait
    /// tick and embedded in the timeout message.
    /// <para>
    /// A join stalled at <c>userPartitions=0</c> has two very different causes that look identical
    /// from the partition count alone, so the local P0 log frontier is reported alongside it.
    /// <c>p0MaxLog=0</c> means the entries never arrived — the leader is heartbeating us but never
    /// shipping the committed range (a leader-side replication/backfill problem).
    /// <c>p0MaxLog&gt;0</c> with <c>userPartitions=0</c> means they did arrive but were never applied — they are sitting in the WAL unconverted to
    /// <c>Committed</c>, or the coordinator never processed the resulting <c>ConfigReplicated</c>
    /// (a follower-side apply problem). Without this split, a stalled join is indistinguishable
    /// between "nothing was sent" and "everything was sent and dropped on the floor".
    /// </para>
    /// </summary>
    private string DescribeAssemblyState()
    {
        RaftPartition? systemPartition = partitionProvider.SystemPartition;

        string systemLeader = systemPartition is null
            ? "no-system-partition"
            : string.IsNullOrEmpty(systemPartition.Leader) ? "none" : systemPartition.Leader;

        string systemState = systemPartition is null ? "n/a" : systemPartition.State.ToString();

        // Direct WAL-backend reads: synchronous, and deliberately NOT routed through the read
        // scheduler or the partition executor, either of which may be the thing that is stuck.
        string p0MaxLog = "n/a";
        string p0Term = "n/a";

        if (systemPartition is not null)
        {
            try
            {
                p0MaxLog = walAdapter.GetMaxLog(RaftSystemConfig.SystemPartition).ToString();
                p0Term = walAdapter.GetCurrentTerm(RaftSystemConfig.SystemPartition).ToString();
            }
            catch (Exception ex)
            {
                // A diagnostic must never be the reason a join fails.
                p0MaxLog = "err:" + ex.GetType().Name;
            }
        }

        return string.Concat(
            "local=", localEndpoint,
            " role=", getLocalRole().ToString(),
            " systemLeader=", systemLeader,
            " systemState=", systemState,
            " p0MaxLog=", p0MaxLog,
            " p0Term=", p0Term,
            " userPartitions=", partitionProvider.DataPartitionCount.ToString(),
            "/", configuration.InitialPartitions.ToString(),
            " initialized=", isInitialized() ? "true" : "false");
    }

    /// <summary>
    /// Joins the cluster via the discovery service.
    /// </summary>
    internal async Task JoinCluster(CancellationToken cancellationToken = default)
    {
        // Registers itself at the discovery service
        await clusterHandler.JoinCluster(configuration).ConfigureAwait(false);

        startSystemPartition();

        // Discovery has to run once before the system partition may self-elect; do it now instead
        // of waiting for the node-update timer (see RefreshNodesAtJoinAsync).
        await RefreshNodesAtJoinAsync().ConfigureAwait(false);

        // Wait for the system coordinator to replicate the initial partition map and start the
        // user partitions. On a slow or loaded host this can take longer than expected; the
        // unconditional ceiling (see the type summary) keeps it from blocking indefinitely.
        long joinTimeoutMs = (long)configuration.JoinTimeout.TotalMilliseconds;
        ValueStopwatch joinStopwatch = ValueStopwatch.StartNew();
        Task initializedTask = getInitializedSignal();
        while (!isInitialized())
        {
            cancellationToken.ThrowIfCancellationRequested();

            long elapsedMs = joinStopwatch.GetElapsedMilliseconds();

            // Emit a per-tick snapshot so a stalled join leaves an evolving trail in the log
            // (leader appears? partitions climb?) instead of a single opaque throw at the deadline.
            logger.LogWarning(
                "JoinCluster: waiting for initialization ({ElapsedMs} ms elapsed): {State}",
                elapsedMs, DescribeAssemblyState());

            if (elapsedMs > joinTimeoutMs)
                throw new TimeoutException(
                    $"RaftManager.JoinCluster timed out after {configuration.JoinTimeout.TotalSeconds:0.#} s (JoinTimeout) waiting for cluster initialization. State: {DescribeAssemblyState()}");

            // Return the instant initialization signals; otherwise re-log and ceiling-check on the
            // next 1 s tick. Replaces a flat Task.Delay(1000) that made a node which assembles in a
            // few ms still sleep out the rest of the tick. On cancellation the Delay completes
            // (canceled) and the loop-top ThrowIfCancellationRequested throws — earlier than the
            // ceiling, unchanged. The orphaned canceled Delay raises no UnobservedTaskException.
            await Task.WhenAny(initializedTask, Task.Delay(1000, cancellationToken)).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Seed-based join: contacts each seed in turn until this node is admitted as a Learner,
    /// then waits for automatic promotion to Voter.
    /// <para>
    /// Unlike the discovery-based overload this variant does NOT call <see cref="Discovery.IDiscovery.Register"/>;
    /// it relies on the system-partition leader learning of the new node via the committed roster
    /// entry so that the leader's next <c>UpdateNodes</c> tick includes this endpoint in its peer
    /// set and begins sending heartbeats. Once the system-partition logs are replicated the
    /// coordinator starts the user partitions and initialization completes.
    /// </para>
    /// <para>
    /// Admission and initialization are bounded by <see cref="RaftConfiguration.JoinTimeout"/> from
    /// the start of the join; the promotion wait by catch-up progress (see the type summary). Any
    /// failure after admission other than the caller's cancellation withdraws the node from the
    /// roster before the exception propagates.
    /// </para>
    /// </summary>
    internal async Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default)
    {
        // Start schedulers and the system partition exactly as the discovery-based join does.
        startSystemPartition();

        // Mark as joined so timer UpdateNodes ticks start firing once the roster has us.
        clusterHandler.MarkJoined();

        // Same join-time discovery refresh as the discovery-based overload: it loads whatever
        // discovery knows so the election gate has a real peer set to judge, without waiting for
        // the first effective timer tick.
        await RefreshNodesAtJoinAsync().ConfigureAwait(false);

        // Contact seeds until a leader accepts us as a Learner.
        JoinResponse? accepted = null;
        string? admittedBy = null;
        long joinTimeoutMs = (long)configuration.JoinTimeout.TotalMilliseconds;
        ValueStopwatch joinStopwatch = ValueStopwatch.StartNew();

        List<string> seedList = [.. seeds];

        while (accepted is null || !accepted.Success)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > joinTimeoutMs)
                throw new TimeoutException($"RaftManager.JoinCluster(seeds) timed out after {configuration.JoinTimeout.TotalSeconds:0.#} s (JoinTimeout) before being admitted as a Learner.");

            string? leaderHint = null;

            foreach (string seed in seedList)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string target = leaderHint ?? seed;
                RaftNode node = new(target);
                try
                {
                    JoinResponse resp = await sendJoin(node, new JoinRequest(localEndpoint, localNodeId)).ConfigureAwait(false);
                    if (resp.Success)
                    {
                        accepted = resp;
                        admittedBy = target;
                        break;
                    }
                    if (!string.IsNullOrEmpty(resp.LeaderHint))
                    {
                        leaderHint = resp.LeaderHint;
                        // Retry immediately against the leader hint.
                        try
                        {
                            RaftNode leaderNode = new(leaderHint);
                            resp = await sendJoin(leaderNode, new JoinRequest(localEndpoint, localNodeId)).ConfigureAwait(false);
                            if (resp.Success)
                            {
                                accepted = resp;
                                admittedBy = leaderHint;
                                break;
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogWarning("JoinCluster: failed to contact leader hint {Hint}: {Message}", leaderHint, ex.Message);
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning("JoinCluster: failed to contact seed {Seed}: {Message}", seed, ex.Message);
                }
            }

            if (accepted is null || !accepted.Success)
                await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }

        logger.LogInfoJoinClusterAdmittedAsLearner(accepted.MembershipVersion);

        try
        {
            await WaitForInitializationAsync(joinStopwatch, joinTimeoutMs, cancellationToken).ConfigureAwait(false);
            await WaitForPromotionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // This node is in the committed roster as a Learner. Throwing without taking it back out
            // left the survivors tracking a member that will never be promoted, and the operator's
            // replacement made the cluster worse than before (Caraxes, 2026-10-14).
            logger.LogWarning(
                "JoinCluster: the join failed after this node was admitted ({Message}); withdrawing it from the roster",
                ex.Message);

            List<string> contacts = admittedBy is null ? seedList : [admittedBy, .. seedList];

            try
            {
                await withdrawFailedJoin(contacts, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception withdrawEx)
            {
                // The join's own failure is the error the caller must see; the withdrawal is best effort.
                logger.LogWarning("JoinCluster: withdrawal after the failed join threw ({Message})", withdrawEx.Message);
            }

            throw;
        }

        logger.LogInformation("JoinCluster: promoted to Voter; join complete");
    }

    /// <summary>
    /// The seed join's wait for initialization (the system coordinator receives the partition map
    /// from the leader). Bounded by <see cref="RaftConfiguration.JoinTimeout"/> on the join's own
    /// stopwatch, which started before admission: this is the liveness part of the join.
    /// <para>
    /// Also checks for a terminal block: if the leader cannot backfill this node (WAL is compacted,
    /// no snapshot transfer registered), initialization will never complete and the permanent-block
    /// reason must surface rather than spin to the timeout.
    /// </para>
    /// <para>
    /// Unlike the discovery-based overload, this loop deliberately keeps the raw 500 ms poll rather
    /// than awaiting the initialization signal. The seed-join learner path races the terminal-block
    /// decision against promotion, and the terminal reason is polled here every tick; waking early
    /// on the signal perturbs that interleaving enough to let a below-floor learner slip past the
    /// block. This path is also not latency-sensitive the way single-node boot is — a lone in-memory
    /// node is leader-from-seed via the discovery overload, never a seed-joining learner — so the
    /// poll stays.
    /// </para>
    /// </summary>
    private async Task WaitForInitializationAsync(ValueStopwatch joinStopwatch, long joinTimeoutMs, CancellationToken cancellationToken)
    {
        while (!isInitialized())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > joinTimeoutMs)
                throw new TimeoutException(
                    $"RaftManager.JoinCluster(seeds) timed out after {configuration.JoinTimeout.TotalSeconds:0.#} s (JoinTimeout) waiting for cluster initialization. State: {DescribeAssemblyState()}");
            string? terminalReasonInit = GetJoinTerminalReason(localEndpoint);
            if (terminalReasonInit is not null)
                throw new InvalidOperationException($"RaftManager.JoinCluster: promotion permanently blocked — {terminalReasonInit}");
            await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The seed join's wait for promotion to Voter. Not bounded by time: it fails only when the
    /// learner makes no catch-up progress for <see cref="RaftConfiguration.JoinPromotionStallTimeout"/>
    /// (see <see cref="JoinCatchUpProgress"/> for what counts), or when the leader records a terminal
    /// block for this endpoint. A catch-up that keeps moving is waited for however long it takes;
    /// an Information line every <see cref="PromotionReportIntervalMs"/> keeps a long one visible.
    /// <para>
    /// The stall clock starts when this wait starts, not when the join did, so a slow admission
    /// cannot eat into the catch-up's allowance.
    /// </para>
    /// </summary>
    private async Task WaitForPromotionAsync(CancellationToken cancellationToken)
    {
        long stallMs = (long)configuration.JoinPromotionStallTimeout.TotalMilliseconds;
        JoinCatchUpProgress progress = new();
        ValueStopwatch promotionStopwatch = ValueStopwatch.StartNew();
        long nextReportMs = PromotionReportIntervalMs;

        while (getLocalRole() != ClusterMemberRole.Voter)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // The leader signals a terminal condition (e.g., learner is below the WAL compaction
            // floor and no snapshot transfer is registered) so the joiner fails fast with a
            // descriptive error rather than spinning to the stall timeout.
            string? terminalReason = GetJoinTerminalReason(localEndpoint);
            if (terminalReason is not null)
                throw new InvalidOperationException($"RaftManager.JoinCluster: promotion permanently blocked — {terminalReason}");

            long nowMs = promotionStopwatch.GetElapsedMilliseconds();
            progress.Observe(SampleCommitFrontiers(), SampleSnapshotProgress(), nowMs);

            long quietMs = progress.MillisecondsSinceProgress(nowMs);
            if (quietMs >= stallMs)
                throw new TimeoutException(
                    $"RaftManager.JoinCluster(seeds) gave up waiting for Voter promotion: the learner made no catch-up progress " +
                    $"for {quietMs / 1000.0:0.#} s (JoinPromotionStallTimeout {configuration.JoinPromotionStallTimeout.TotalSeconds:0.#} s) " +
                    $"after {nowMs / 1000.0:0.#} s of waiting. Last progress: {progress.LastProgress} at {progress.LastProgressMs / 1000.0:0.#} s. " +
                    $"Commit frontiers: {progress.DescribeFrontiers()}. State: {DescribeAssemblyState()}");

            if (nowMs >= nextReportMs && logger.IsEnabled(LogLevel.Information))
            {
                nextReportMs = nowMs + PromotionReportIntervalMs;
                logger.LogInformation(
                    "JoinCluster: still catching up as a Learner after {ElapsedSeconds:0} s; last progress {LastProgress} {QuietSeconds:0.#} s ago; commit frontiers {Frontiers}",
                    nowMs / 1000.0, progress.LastProgress, quietMs / 1000.0, progress.DescribeFrontiers());
            }

            await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The published commit frontier of every partition this node hosts now, system partition
    /// included. Lock-free reads of each partition's durability-gated frontier; never routed through
    /// an executor, which may be the thing that is busy installing a snapshot.
    /// </summary>
    private List<(int PartitionId, long CommitIndex)> SampleCommitFrontiers()
    {
        List<(int PartitionId, long CommitIndex)> frontiers = new(partitionProvider.DataPartitionCount + 1);

        try
        {
            RaftPartition? systemPartition = partitionProvider.SystemPartition;
            if (systemPartition is not null)
                frontiers.Add((systemPartition.PartitionId, systemPartition.GetCommitIndex()));

            foreach (RaftPartition partition in partitionProvider.DataPartitions)
                frontiers.Add((partition.PartitionId, partition.GetCommitIndex()));
        }
        catch (Exception ex)
        {
            // A partition removed or disposed mid-enumeration: this sample shows less, the next one
            // more. A diagnostic read must never be the reason a join fails.
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("JoinCluster: commit frontier sample failed ({Message})", ex.Message);
        }

        return frontiers;
    }

    /// <summary>
    /// The snapshot receiver's progress marker, or null when it cannot be read. Null rather than a
    /// placeholder value: the marker is compared for inequality, so a placeholder would read as progress.
    /// </summary>
    private long? SampleSnapshotProgress()
    {
        try
        {
            return getSnapshotProgressMarker();
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("JoinCluster: snapshot progress sample failed ({Message})", ex.Message);

            return null;
        }
    }

    /// <summary>
    /// Handles an inbound <see cref="JoinRequest"/> from a joining node.
    /// <para>
    /// If this node is the system-partition leader it commits the joiner as a
    /// <see cref="ClusterMemberRole.Learner"/> and returns success. Otherwise it returns the
    /// current leader endpoint in <see cref="JoinResponse.LeaderHint"/> so the caller can retry
    /// directly against the leader.
    /// </para>
    /// <para>
    /// <b>Idempotency:</b> if the endpoint is already committed in the roster (e.g. a previous
    /// <c>AddMember</c> committed but the response was lost before the joiner saw it), this
    /// returns success with the current roster version rather than an error. This prevents the
    /// joiner's retry loop from spinning to the join timeout.
    /// </para>
    /// </summary>
    internal async Task<JoinResponse> ReceiveJoin(JoinRequest request)
    {
        RaftPartition? systemPartition = partitionProvider.SystemPartition;

        if (systemPartition is null || !isInitialized())
            return new JoinResponse(false);

        bool isLeader = await amILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!isLeader)
        {
            string leaderHint = systemPartition.Leader;
            return new JoinResponse(false, string.IsNullOrEmpty(leaderHint) ? null : leaderHint);
        }

        RaftSystemCoordinator systemCoordinator = getCoordinator();

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        systemCoordinator.Send(new RaftSystemRequest(
            RaftSystemRequestType.AddMember,
            request.Endpoint,
            request.NodeId,
            systemCoordinator.GetMembership().MembershipVersion,
            tcs));

        (RaftOperationStatus status, long version) = await tcs.Task.ConfigureAwait(false);

        if (status == RaftOperationStatus.Success)
            return new JoinResponse(true, null, version);

        // Another membership change was in flight or version mismatch — return current leader so
        // the caller can retry after a brief backoff.
        return new JoinResponse(false, localEndpoint);
    }
}
