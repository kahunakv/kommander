
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
/// Every wait here is bounded by <see cref="JoinHardCeilingMs"/> <b>unconditionally</b> — including
/// when the caller passes a cancelable token. A caller whose token never actually fires (an xUnit
/// test-context token with no configured timeout, say) would otherwise have no liveness bound at
/// all, so a cluster that failed to assemble hung forever and silently. A cluster that cannot
/// initialize within the ceiling is broken; failing loudly with a state dump beats hanging. The
/// caller's token still cancels earlier than the ceiling.
/// </para>
/// <para>
/// Concurrency: the terminal-reason map is the only shared state and is a concurrent dictionary
/// because it is written by the coordinator's decision path and read by the joiner's wait loop on
/// a different thread.
/// </para>
/// </summary>
internal sealed class ClusterJoinService
{
    /// <summary>
    /// Hard upper bound (ms) on how long any join overload will wait for cluster assembly before
    /// throwing a <see cref="TimeoutException"/>. Enforced unconditionally as a liveness safety
    /// net so a stalled assembly can never hang indefinitely and silently. Normal assembly
    /// completes in well under this bound; a join that exceeds it indicates a genuinely wedged
    /// cluster.
    /// </summary>
    private const long JoinHardCeilingMs = 60_000;

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
    private readonly Func<RaftNode, JoinRequest, Task<JoinResponse>> sendJoin;
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
        Func<RaftNode, JoinRequest, Task<JoinResponse>> sendJoin,
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
        this.sendJoin = sendJoin;
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
    /// One-line snapshot of how far cluster assembly has progressed, emitted on every join wait
    /// tick and embedded in the timeout message.
    /// <para>
    /// Stage 2 has two very different causes that look identical from the partition count alone, so
    /// the local P0 log frontier is reported alongside it. <c>p0MaxLog=0</c> means the entries never
    /// arrived — the leader is heartbeating us but never shipping the committed range (a leader-side
    /// replication/backfill problem). <c>p0MaxLog&gt;0</c> with <c>userPartitions=0</c> means they
    /// did arrive but were never applied — they are sitting in the WAL unconverted to
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

        // Wait for the system coordinator to replicate the initial partition map and start the
        // user partitions. On a slow or loaded host this can take longer than expected; the
        // unconditional ceiling (see the type summary) keeps it from blocking indefinitely.
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

            if (elapsedMs > JoinHardCeilingMs)
                throw new TimeoutException(
                    $"RaftManager.JoinCluster timed out after {JoinHardCeilingMs / 1000} s waiting for cluster initialization. State: {DescribeAssemblyState()}");

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
    /// </summary>
    internal async Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default)
    {
        // Start schedulers and the system partition exactly as the discovery-based join does.
        startSystemPartition();

        // Mark as joined so timer UpdateNodes ticks start firing once the roster has us.
        clusterHandler.MarkJoined();

        // Contact seeds until a leader accepts us as a Learner.
        JoinResponse? accepted = null;
        ValueStopwatch joinStopwatch = ValueStopwatch.StartNew();

        List<string> seedList = [.. seeds];

        while (accepted is null || !accepted.Success)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > JoinHardCeilingMs)
                throw new TimeoutException($"RaftManager.JoinCluster(seeds) timed out after {JoinHardCeilingMs / 1000} s before being admitted as a Learner.");

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

        // Wait for initialization (the system coordinator receives the partition map from the
        // leader). Also check for a terminal block: if the leader cannot backfill this node (WAL is
        // compacted, no snapshot transfer registered), initialization will never complete and we
        // must surface the permanent-block reason rather than spinning to the timeout.
        //
        // Unlike the discovery-based overload, this loop deliberately keeps the raw 500 ms poll
        // rather than awaiting the initialization signal. The seed-join learner path races the
        // terminal-block decision against promotion, and the terminal reason is polled here every
        // tick; waking early on the signal perturbs that interleaving enough to let a below-floor
        // learner slip past the block. This path is also not latency-sensitive the way single-node
        // boot is — a lone in-memory node is leader-from-seed via the discovery overload, never a
        // seed-joining learner — so the poll stays.
        while (!isInitialized())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > JoinHardCeilingMs)
                throw new TimeoutException(
                    $"RaftManager.JoinCluster(seeds) timed out after {JoinHardCeilingMs / 1000} s waiting for cluster initialization. State: {DescribeAssemblyState()}");
            string? terminalReasonInit = GetJoinTerminalReason(localEndpoint);
            if (terminalReasonInit is not null)
                throw new InvalidOperationException($"RaftManager.JoinCluster: promotion permanently blocked — {terminalReasonInit}");
            await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }

        // Wait for promotion to Voter.
        while (getLocalRole() != ClusterMemberRole.Voter)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (joinStopwatch.GetElapsedMilliseconds() > JoinHardCeilingMs)
                throw new TimeoutException($"RaftManager.JoinCluster(seeds) timed out after {JoinHardCeilingMs / 1000} s waiting for Voter promotion.");

            // The leader signals a terminal condition (e.g., learner is below the WAL compaction
            // floor and no snapshot transfer is registered) so the joiner fails fast with a
            // descriptive error rather than spinning to the 60 s deadline.
            string? terminalReason = GetJoinTerminalReason(localEndpoint);
            if (terminalReason is not null)
                throw new InvalidOperationException($"RaftManager.JoinCluster: promotion permanently blocked — {terminalReason}");

            await Task.Delay(500, cancellationToken).ConfigureAwait(false);
        }

        logger.LogInformation("JoinCluster: promoted to Voter; join complete");
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
    /// joiner's retry loop from spinning to the 60 s timeout.
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
