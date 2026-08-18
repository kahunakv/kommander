
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// The departure protocol: everything involved in taking this node out of the committed roster —
/// the operator-driven decommission (<see cref="RequestLeaveAsync"/>, which evacuates replicas
/// first), the shutdown-coupled leave (<see cref="LeaveCluster"/>), and the leader-side handlers
/// that commit another node's removal or role transition.
/// <para>
/// Two latches live here and mean different things. <see cref="IsLeaving"/> suppresses
/// campaigning and is deliberately <i>not</i> sticky: a node whose leave was refused is still a
/// full member and must keep contending, so the flag is raised only once departure is real (or
/// unconditionally by <see cref="LeaveCluster"/>, where teardown follows regardless).
/// <see cref="IsLeaveRequested"/> <i>is</i> sticky and never clears, even for an attempt that
/// reported failure: the removal can still land later, and auto-rejoin would silently undo an
/// operator-ordered decommission.
/// </para>
/// <para>
/// Ordering matters throughout: drain → role transition → roster removal → teardown. Committing
/// the removal before the replicas are evacuated strands ranges on a node that is about to stop;
/// tearing down before the removal commits leaves survivors counting a dead node toward quorum.
/// </para>
/// <para>
/// Concurrency: <see cref="RequestLeaveAsync"/> is serialized by a semaphore so two callers cannot
/// run overlapping commit loops for the same removal — the second then observes the node already
/// absent and answers idempotently. The teardown path deliberately does not take it: shutdown must
/// never queue behind an API-initiated attempt.
/// </para>
/// </summary>
internal sealed class ClusterLeaveService : IDisposable
{
    /// <summary>
    /// Raised by <see cref="LeaveCluster"/> before the removal is committed so that the local role
    /// immediately reports <see cref="ClusterMemberRole.Leaving"/> and the election / pre-vote gate
    /// suppresses campaigning on all partitions.
    /// <para>
    /// <see cref="RequestLeaveAsync"/> raises it only once the removal has committed: until then
    /// the node is still a full member, and it may need to win the system-partition election in
    /// order to commit its own removal at all.
    /// </para>
    /// </summary>
    private volatile bool _leaving;

    /// <summary>
    /// Set by the first <see cref="RequestLeaveAsync"/> and never cleared, even when that attempt
    /// fails. Departure was requested by an operator, so this node must never re-admit itself: the
    /// removal can still land after a timed-out attempt, and auto-rejoin would silently undo the
    /// decommission the operator asked for. Election suppression (<see cref="_leaving"/>) is
    /// deliberately <i>not</i> sticky — a node that was refused is still a full member and must keep
    /// campaigning.
    /// </summary>
    private volatile bool _leaveRequested;

    /// <summary>
    /// Serializes <see cref="RequestLeaveAsync"/> so two concurrent callers cannot run overlapping
    /// commit loops for the same removal; the second one then observes the node already absent and
    /// answers idempotently. The teardown path deliberately does not take it: shutdown must never
    /// queue behind an API-initiated attempt.
    /// </summary>
    private readonly SemaphoreSlim leaveRequestLock = new(1, 1);

    private readonly IPartitionProvider partitionProvider;
    private readonly RaftConfiguration configuration;
    private readonly ClusterHandler clusterHandler;
    private readonly Func<RaftSystemCoordinator> getCoordinator;
    private readonly Func<int, ValueTask<bool>> amILeaderQuick;
    private readonly Func<bool> committedMapNamesLocalEndpoint;
    private readonly Func<bool> isInitialized;
    private readonly Func<RaftNode, LeaveRequest, CancellationToken, Task<LeaveResponse>> sendLeave;
    private readonly Func<RaftNode, SetMemberRoleRequest, CancellationToken, Task<SetMemberRoleResponse>> sendSetMemberRole;
    private readonly Func<bool, Task> tearDownAsync;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;

    internal ClusterLeaveService(
        IPartitionProvider partitionProvider,
        RaftConfiguration configuration,
        ClusterHandler clusterHandler,
        Func<RaftSystemCoordinator> getCoordinator,
        Func<int, ValueTask<bool>> amILeaderQuick,
        Func<bool> committedMapNamesLocalEndpoint,
        Func<bool> isInitialized,
        Func<RaftNode, LeaveRequest, CancellationToken, Task<LeaveResponse>> sendLeave,
        Func<RaftNode, SetMemberRoleRequest, CancellationToken, Task<SetMemberRoleResponse>> sendSetMemberRole,
        Func<bool, Task> tearDownAsync,
        ILogger<IRaft> logger,
        string localEndpoint)
    {
        this.partitionProvider = partitionProvider;
        this.configuration = configuration;
        this.clusterHandler = clusterHandler;
        this.getCoordinator = getCoordinator;
        this.amILeaderQuick = amILeaderQuick;
        this.committedMapNamesLocalEndpoint = committedMapNamesLocalEndpoint;
        this.isInitialized = isInitialized;
        this.sendLeave = sendLeave;
        this.sendSetMemberRole = sendSetMemberRole;
        this.tearDownAsync = tearDownAsync;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
    }

    /// <summary>
    /// True once this node has begun leaving; suppresses campaigning on every partition.
    /// Not sticky across a refused attempt — see the type summary.
    /// </summary>
    internal bool IsLeaving => _leaving;

    /// <summary>
    /// True once an operator has asked this node to leave, and never cleared afterwards.
    /// Blocks auto-rejoin from undoing a decommission that lands late.
    /// </summary>
    internal bool IsLeaveRequested => _leaveRequested;

    /// <summary>
    /// Handles an inbound <see cref="LeaveRequest"/>: only the system-partition leader commits the
    /// <c>RemoveMember</c>; a non-leader answers with a leader hint so the caller can retry against
    /// the real leader.
    /// <para>
    /// <b>Idempotency:</b> if the endpoint is not found in the roster (already removed, or was
    /// never added) this returns success so a retried leave request does not spin to timeout.
    /// </para>
    /// </summary>
    internal async Task<LeaveResponse> ReceiveLeave(LeaveRequest request, CancellationToken cancellationToken = default)
    {
        RaftSystemCoordinator systemCoordinator = getCoordinator();

        // Fail-fast when this node has already stopped — the coordinator channel is closed
        // and posting to it would block until the per-attempt CTS fires (3 s per attempt).
        if (systemCoordinator.IsStopped)
            return new LeaveResponse(false);

        RaftPartition? systemPartition = partitionProvider.SystemPartition;

        if (systemPartition is null || !isInitialized())
            return new LeaveResponse(false);

        bool isLeader = await amILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
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

        systemCoordinator.Send(new RaftSystemRequest(
            RaftSystemRequestType.RemoveMember,
            request.Endpoint,
            request.NodeId,
            systemCoordinator.GetMembership().MembershipVersion,
            tcs));

        try
        {
            (RaftOperationStatus status, long version) = await tcs.Task.ConfigureAwait(false);

            if (status == RaftOperationStatus.Success)
                return new LeaveResponse(true, null, false, version);

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
        return new LeaveResponse(false, localEndpoint);
    }

    /// <summary>
    /// Handles an inbound <see cref="SetMemberRoleRequest"/>: a roster role transition for the
    /// decommission drain (<c>Voter → Leaving</c> to start, <c>Leaving → Voter</c> to roll back).
    /// <para>
    /// Mirrors <see cref="ReceiveLeave"/>: only the system-partition leader commits; a non-leader
    /// returns a leader hint. The coordinator's verdict travels back in
    /// <see cref="SetMemberRoleResponse.Status"/> so the caller can tell a retryable rejection
    /// (concurrent change) from a permanent one (insufficient voters, drain already in flight)
    /// and — critically for the rollback race — from
    /// <see cref="RaftOperationStatus.MemberNotFound"/>, which means the placement pass already
    /// committed the final removal and the node must treat itself as departed.
    /// </para>
    /// </summary>
    internal async Task<SetMemberRoleResponse> ReceiveSetMemberRole(SetMemberRoleRequest request, CancellationToken cancellationToken = default)
    {
        RaftSystemCoordinator systemCoordinator = getCoordinator();

        // Fail-fast when this node has already stopped — the coordinator channel is closed
        // and posting to it would block until the per-attempt CTS fires.
        if (systemCoordinator.IsStopped)
            return new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored);

        RaftPartition? systemPartition = partitionProvider.SystemPartition;

        if (systemPartition is null || !isInitialized())
            return new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored);

        bool isLeader = await amILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);
        if (!isLeader)
        {
            string leaderHint = systemPartition.Leader;
            return new SetMemberRoleResponse(false, string.IsNullOrEmpty(leaderHint) ? null : leaderHint, RaftOperationStatus.NodeIsNotLeader);
        }

        TaskCompletionSource<(RaftOperationStatus Status, long Generation)> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using CancellationTokenRegistration reg = cancellationToken.Register(
            () => tcs.TrySetCanceled(cancellationToken));

        systemCoordinator.Send(new RaftSystemRequest(
            RaftSystemRequestType.SetMemberRole,
            request.Endpoint,
            request.NodeId,
            systemCoordinator.GetMembership().MembershipVersion,
            request.TargetRole,
            tcs));

        try
        {
            (RaftOperationStatus status, long version) = await tcs.Task.ConfigureAwait(false);

            if (status == RaftOperationStatus.Success)
                return new SetMemberRoleResponse(true, null, RaftOperationStatus.Success, version);

            // Terminal verdicts carry no hint — retrying against any leader cannot change them.
            if (status is RaftOperationStatus.InsufficientVoters or RaftOperationStatus.MemberNotFound or RaftOperationStatus.DrainInProgress)
                return new SetMemberRoleResponse(false, null, status, version);

            // Concurrent change or stale version — caller retries with the current leader.
            return new SetMemberRoleResponse(false, localEndpoint, status, version);
        }
        catch (OperationCanceledException)
        {
            // Per-attempt timeout or caller cancelled — caller retries or gives up.
        }

        return new SetMemberRoleResponse(false, localEndpoint, RaftOperationStatus.Errored);
    }

    /// <summary>
    /// Leaves the cluster and tears the node down.
    /// <para>
    /// When the local node is part of a committed roster (MembershipVersion &gt; 0) this first
    /// sets the <b>local</b> <see cref="_leaving"/> latch — the local role then reports
    /// <see cref="ClusterMemberRole.Leaving"/>, suppressing elections immediately; this is not a
    /// committed roster change and no replica drain happens — commits a <c>RemoveMember</c> entry
    /// on the system partition, and waits up to 10 s for the removal to propagate back to this
    /// node before tearing down. Use <see cref="RequestLeaveAsync"/> for a decommission that
    /// evacuates replicas first.
    /// If the cluster has no committed roster (pre-seed transient or test teardown path), or if
    /// the roster contains no other <c>Voter</c> peer, the round-trip is skipped and the node
    /// stops immediately (single-voter short-circuit — no 10 s spin).
    /// </para>
    /// </summary>
    /// <param name="dispose">If true, also disposes the manager</param>
    /// <param name="cancellationToken">
    /// When cancelled, aborts any in-progress graceful-leave attempt immediately.
    /// </param>
    internal async Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default)
    {
        // Suppress elections on all partitions immediately: teardown follows unconditionally, so
        // this node must not win a leadership it is about to abandon.
        _leaving = true;

        // If we are part of a committed roster AND there is at least one other Voter peer,
        // commit the removal before stopping so surviving nodes drop us from their peer list
        // at the consensus level.
        // Short-circuit: a single-voter roster (embedded/standalone) has no quorum peer to
        // commit the removal — skipping saves the 10 s deadline spin on every dispose.
        ClusterMembership roster = getCoordinator().GetMembership();
        bool hasOtherVoter = roster.Members.Any(m =>
            m.Role == ClusterMemberRole.Voter &&
            !string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal));

        if (roster.MembershipVersion > 0 && clusterHandler.Joined && hasOtherVoter)
        {
            await CommitGracefulLeaveAsync(cancellationToken).ConfigureAwait(false);
        }

        await clusterHandler.LeaveCluster(configuration).ConfigureAwait(false);

        // Teardown ordering belongs to the manager, which owns every resource being stopped;
        // it is handed in as a callback rather than reimplemented here.
        await tearDownAsync(dispose).ConfigureAwait(false);
    }

    /// <summary>
    /// Asks the cluster to remove this node from the committed roster and reports what happened,
    /// <b>without</b> tearing the node down.
    /// <para>
    /// This is the decommission primitive an operator drives at runtime: the caller learns whether
    /// the removal committed, was permanently refused (removing the last voter), or could not be
    /// attempted, and only then decides whether to stop the process. <see cref="LeaveCluster"/>
    /// remains the shutdown-coupled variant that always proceeds to stop.
    /// </para>
    /// <para>
    /// <b>Drain-before-removal:</b> when the committed partition map names this endpoint in any
    /// replica set, the method first commits the <see cref="ClusterMemberRole.Leaving"/>
    /// roster role and waits (up to <see cref="RaftConfiguration.DecommissionDrainTimeout"/>) for
    /// the placement pass to evacuate every replica onto survivors; only then does it commit the
    /// removal, and the result carries <see cref="LeaveClusterResult.Drained"/> = true. A drain
    /// that cannot start is refused (<see cref="LeaveClusterOutcome.RefusedDrainInProgress"/> /
    /// <see cref="LeaveClusterOutcome.RefusedInsufficientVoters"/>) and the node keeps serving; a
    /// drain that times out rolls the role back to Voter
    /// (<see cref="LeaveClusterOutcome.DrainTimedOut"/>) — unless the placement pass finished the
    /// evacuation and committed the removal first, in which case removal wins and the result is
    /// <see cref="LeaveClusterOutcome.Committed"/>. Full-replication clusters (no replica sets)
    /// and deployments with <see cref="RaftConfiguration.EnablePlacementRebalancer"/> off (no
    /// evacuation machinery exists there) keep the historical direct-removal behaviour unchanged.
    /// </para>
    /// <para>
    /// Campaigning is left alone until the removal commits, and suppressed for good afterwards: a
    /// node that was refused — or whose attempt failed — is still a full member and must keep
    /// participating, while one that left must not contend for leadership it no longer has a claim
    /// to.
    /// </para>
    /// <para>
    /// Concurrent calls are serialized and idempotent: the second caller observes the node already
    /// absent from the roster and returns <see cref="LeaveClusterOutcome.NotAMember"/>.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">
    /// Bounds the attempt. Cancelling yields <see cref="LeaveClusterOutcome.Timeout"/> — the removal
    /// may still commit, so the caller must re-read the roster before concluding anything.
    /// </param>
    internal async Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default)
    {
        if (partitionProvider.SystemPartition is null || !isInitialized())
            return new(LeaveClusterOutcome.NotInitialized, 0);

        await leaveRequestLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            // Recorded before anything can short-circuit: even an attempt answered "you are already
            // out" is an operator asking this node to go, and a node that then re-admitted itself
            // would undo the decommission just as surely as one that never left.
            _leaveRequested = true;

            RaftSystemCoordinator systemCoordinator = getCoordinator();
            ClusterMembership roster = systemCoordinator.GetMembership();

            // No committed roster yet (pre-seed transient): there is no membership entry to remove,
            // and the node cannot be counted towards anyone's quorum until seeding completes.
            if (roster.MembershipVersion == 0 || !clusterHandler.Joined)
                return new(LeaveClusterOutcome.NotInitialized, roster.MembershipVersion);

            if (!roster.Members.Any(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)))
                return new(LeaveClusterOutcome.NotAMember, roster.MembershipVersion);

            // ── Decommission drain ────────────────────────────────────────────────────
            // Only when the committed map actually names this endpoint in a replica set (or a
            // previous drain is being resumed — the role is already Leaving). Full-replication
            // clusters (RF 0) have nothing to evacuate: every survivor already holds the data,
            // so they keep the historical direct-removal path bit-for-bit.
            //
            // Also gated on EnablePlacementRebalancer: evacuation is planned by the placement
            // rebalancer (priority-1 repair adds + the trim of the leaver's replica), so with
            // the flag off a committed Leaving role would sit un-evacuated until the drain
            // timeout and roll back — a guaranteed 2-minute wedge. Such deployments keep the
            // historical direct removal (and its historical consequence: repairs are manual).
            bool drained = false;
            ClusterMemberRole selfRole =
                roster.Members.First(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)).Role;

            if (configuration.EnablePlacementRebalancer &&
                (committedMapNamesLocalEndpoint() || selfRole == ClusterMemberRole.Leaving))
            {
                LeaveClusterResult? drainOutcome = await DrainBeforeLeaveAsync(selfRole, cancellationToken).ConfigureAwait(false);
                if (drainOutcome is { } terminal)
                {
                    // A departure completed by the placement pass (removal won the race) must
                    // stop campaigning for good, exactly like the normal removal path below.
                    if (terminal.Left)
                        _leaving = true;

                    return terminal;
                }

                drained = true;
            }

            // Campaigning is deliberately NOT suppressed while the attempt is in flight. The node
            // is still a full member until the removal commits, and the removal is committed by the
            // system-partition leader — so a node that has to win that election first (the last
            // voter answering its own request, or any node during a leadership gap) must be allowed
            // to. Suppressing here deadlocked exactly that case into a timeout.
            LeaveClusterResult result = await CommitGracefulLeaveAsync(cancellationToken).ConfigureAwait(false);

            // A leader acknowledgement can be lost in transit or arrive after the deadline, leaving
            // an attempt reported as timed out even though the removal committed. Re-reading the
            // roster resolves that in the caller's favour whenever the new roster is already here.
            if (!result.Left &&
                !systemCoordinator.GetMembership().Members.Any(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)))
                result = new(LeaveClusterOutcome.Committed, systemCoordinator.GetMembership().MembershipVersion);

            // Only a departure that actually happened stops the node campaigning, and it stops it
            // for good: it is out of the roster and must not contend for leadership it no longer
            // has a claim to.
            if (result.Left)
                _leaving = true;

            return drained ? result with { Drained = true } : result;
        }
        finally
        {
            leaveRequestLock.Release();
        }
    }

    /// <summary>
    /// The drain leg of <see cref="RequestLeaveAsync"/>: commits <c>Voter → Leaving</c>, then
    /// waits for the placement pass to evacuate every replica this node hosts.
    /// <para>
    /// Returns <c>null</c> when the drain completed and the caller should proceed to the normal
    /// <c>RemoveMember</c> commit; returns a terminal <see cref="LeaveClusterResult"/> for every
    /// other outcome — a refusal (nothing committed, the node keeps serving as a Voter), a
    /// timeout (role rolled back to Voter so the campaign gates release the node), or a
    /// departure already completed by the placement pass (removal won the rollback race).
    /// </para>
    /// <para>
    /// Resumable: when <paramref name="selfRole"/> is already <c>Leaving</c> (a previous attempt
    /// crashed or timed out without managing to roll back) the commit step is skipped and the
    /// wait picks the drain up where it left off.
    /// </para>
    /// </summary>
    private async Task<LeaveClusterResult?> DrainBeforeLeaveAsync(ClusterMemberRole selfRole, CancellationToken cancellationToken)
    {
        RaftSystemCoordinator systemCoordinator = getCoordinator();

        if (selfRole == ClusterMemberRole.Voter)
        {
            // Local fast-path guards for an immediate, actionable answer; the leader re-enforces
            // both authoritatively inside TrySetMemberRole.
            ClusterMembership roster = systemCoordinator.GetMembership();

            if (roster.Members.Any(m => m.Role == ClusterMemberRole.Leaving))
                return new(LeaveClusterOutcome.RefusedDrainInProgress, roster.MembershipVersion);

            if (!roster.Members.Any(m => m.Role == ClusterMemberRole.Voter &&
                                         !string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)))
                return new(LeaveClusterOutcome.RefusedInsufficientVoters, roster.MembershipVersion);

            (RaftOperationStatus status, _) = await CommitRoleTransitionAsync(ClusterMemberRole.Leaving, cancellationToken).ConfigureAwait(false);

            switch (status)
            {
                case RaftOperationStatus.Success:
                    break;

                case RaftOperationStatus.InsufficientVoters:
                    return new(LeaveClusterOutcome.RefusedInsufficientVoters, systemCoordinator.GetMembership().MembershipVersion);

                case RaftOperationStatus.DrainInProgress:
                    return new(LeaveClusterOutcome.RefusedDrainInProgress, systemCoordinator.GetMembership().MembershipVersion);

                case RaftOperationStatus.MemberNotFound:
                    // Already out of the roster — a previous removal committed. Departed.
                    return new(LeaveClusterOutcome.Committed, systemCoordinator.GetMembership().MembershipVersion);

                default:
                    // Timeout / no leader / pre-drain peer (the RPC failed loudly). The commit
                    // may still have landed — proceed only if the roster already shows Leaving,
                    // otherwise report an unconfirmed attempt: nothing changed, the node keeps
                    // serving. This is also the mixed-version escape hatch: a cluster whose P0
                    // leader predates SetMemberRole refuses the drain here instead of silently
                    // removing the node undrained.
                    ClusterMember? self = systemCoordinator.GetMembership().Members
                        .FirstOrDefault(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal));

                    if (self?.Role != ClusterMemberRole.Leaving)
                        return new(LeaveClusterOutcome.Timeout, systemCoordinator.GetMembership().MembershipVersion);

                    break;
            }
        }

        // ── Wait for evacuation ──────────────────────────────────────────────
        // The placement pass on the P0 leader does the work; this loop only observes committed
        // state. Two exits besides timeout: the map stops naming us (drained — proceed to the
        // final removal), or the roster stops naming us (the pass finished the drain AND
        // committed the removal itself — crash-resumption path, nothing left to do).
        ValueStopwatch drainStopwatch = ValueStopwatch.StartNew();
        long drainDeadlineMs = (long)configuration.DecommissionDrainTimeout.TotalMilliseconds;

        while (true)
        {
            if (!systemCoordinator.GetMembership().Members.Any(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)))
                return new(LeaveClusterOutcome.Committed, systemCoordinator.GetMembership().MembershipVersion, Drained: true);

            if (!committedMapNamesLocalEndpoint())
                return null;

            if (cancellationToken.IsCancellationRequested || drainStopwatch.GetElapsedMilliseconds() > drainDeadlineMs)
                return await RollBackDrainAsync().ConfigureAwait(false);

            try
            {
                await Task.Delay(250, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return await RollBackDrainAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Rolls a timed-out or cancelled drain back to <c>Voter</c> so the campaign gates release
    /// the node. Runs on <see cref="CancellationToken.None"/> deliberately: the rollback must be
    /// attempted even when the caller's token already fired, or the node is left durably
    /// <c>Leaving</c> and never campaigns again.
    /// <para>
    /// Removal-wins precedence: if the placement pass committed the final <c>RemoveMember</c>
    /// while the timeout was firing, the rollback observes
    /// <see cref="RaftOperationStatus.MemberNotFound"/> (or the roster no longer names us) and
    /// reports the leave as <b>completed</b> — the node must never keep serving while out of the
    /// committed roster.
    /// </para>
    /// </summary>
    private async Task<LeaveClusterResult> RollBackDrainAsync()
    {
        (RaftOperationStatus status, _) = await CommitRoleTransitionAsync(ClusterMemberRole.Voter, CancellationToken.None).ConfigureAwait(false);

        ClusterMembership after = getCoordinator().GetMembership();
        bool stillPresent = after.Members.Any(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal));

        if (status == RaftOperationStatus.MemberNotFound || !stillPresent)
            return new(LeaveClusterOutcome.Committed, after.MembershipVersion, Drained: true);

        if (status != RaftOperationStatus.Success)
            // The rollback itself could not be confirmed (e.g. no reachable P0 leader). The node
            // may be durably Leaving: it keeps serving its ranges but cannot campaign, and the
            // next placement pass keeps evacuating it. Retrying RequestLeaveAsync resumes the
            // drain; the warning is the operator's cue that this node needs attention.
            logger.LogWarning("RequestLeaveAsync: drain timed out and the rollback to Voter was not confirmed ({Status}); the roster may still show this node as Leaving.", status);

        return new(LeaveClusterOutcome.DrainTimedOut, after.MembershipVersion);
    }

    /// <summary>
    /// Commits a roster role transition for the local node, retrying against the current P0
    /// leader until it commits or a 10 s deadline expires — the same discipline as
    /// <see cref="CommitGracefulLeaveAsync"/> (self fast-path when this node is the leader,
    /// 3 s per-attempt bound, capped polling while the leader is unknown).
    /// <para>
    /// Returns the coordinator's verdict. Terminal verdicts
    /// (<see cref="RaftOperationStatus.InsufficientVoters"/>,
    /// <see cref="RaftOperationStatus.DrainInProgress"/>,
    /// <see cref="RaftOperationStatus.MemberNotFound"/>) are returned immediately;
    /// retryable rejections keep looping within the deadline. An expired deadline returns
    /// <see cref="RaftOperationStatus.ProposalTimeout"/> — the commit may still have landed, so
    /// callers must re-read the roster before concluding anything.
    /// </para>
    /// </summary>
    private async Task<(RaftOperationStatus Status, long Version)> CommitRoleTransitionAsync(
        ClusterMemberRole targetRole,
        CancellationToken cancellationToken)
    {
        const int deadlineMs = 10_000;
        const int attemptTimeoutMs = 3_000;
        const int maxEmptyLeaderPolls = 5;

        ValueStopwatch sw = ValueStopwatch.StartNew();
        SetMemberRoleRequest request = new(localEndpoint, configuration.NodeId, targetRole);
        int emptyLeaderPolls = 0;

        while (sw.GetElapsedMilliseconds() < deadlineMs)
        {
            if (cancellationToken.IsCancellationRequested)
                return (RaftOperationStatus.OperationCancelled, 0);

            try
            {
                RaftPartition? systemPartition = partitionProvider.SystemPartition;

                bool amLeader = systemPartition is not null &&
                    await amILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);

                string? leaderEndpoint = amLeader ? localEndpoint : systemPartition?.Leader;

                if (string.IsNullOrEmpty(leaderEndpoint))
                {
                    if (++emptyLeaderPolls >= maxEmptyLeaderPolls)
                        return (RaftOperationStatus.NodeIsNotLeader, 0);

                    await Task.Delay(200, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                emptyLeaderPolls = 0;

                using CancellationTokenSource attemptCts =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attemptCts.CancelAfter(attemptTimeoutMs);

                // Self fast-path mirrors CommitGracefulLeaveAsync: the transport cannot always
                // route to the local endpoint.
                SetMemberRoleResponse resp = amLeader
                    ? await ReceiveSetMemberRole(request, attemptCts.Token).ConfigureAwait(false)
                    : await sendSetMemberRole(new RaftNode(leaderEndpoint), request, attemptCts.Token).ConfigureAwait(false);

                if (resp.Success)
                    return (RaftOperationStatus.Success, resp.MembershipVersion);

                if (resp.Status is RaftOperationStatus.InsufficientVoters
                    or RaftOperationStatus.DrainInProgress
                    or RaftOperationStatus.MemberNotFound)
                    return (resp.Status, resp.MembershipVersion);

                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return (RaftOperationStatus.OperationCancelled, 0);
            }
            catch (Exception ex)
            {
                logger.LogWarning("CommitRoleTransition({TargetRole}): {Message}", targetRole, ex.Message);

                try
                {
                    await Task.Delay(200, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return (RaftOperationStatus.OperationCancelled, 0);
                }
            }
        }

        return (RaftOperationStatus.ProposalTimeout, 0);
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
    /// Failures are logged and reported through the returned outcome, never thrown — the shutdown
    /// caller always proceeds to stop afterwards regardless of what happened here.
    /// </summary>
    private async Task<LeaveClusterResult> CommitGracefulLeaveAsync(CancellationToken cancellationToken)
    {
        const int deadlineMs = 10_000;
        const int attemptTimeoutMs = 3_000;
        // After this many consecutive "leader unknown" polls (5 × 200 ms = 1 s), give up.
        // The full deadlineMs only applies while we are actively contacting a known leader.
        const int maxEmptyLeaderPolls = 5;

        ValueStopwatch sw = ValueStopwatch.StartNew();
        LeaveRequest request = new(localEndpoint, configuration.NodeId);
        int emptyLeaderPolls = 0;

        while (sw.GetElapsedMilliseconds() < deadlineMs)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogInformation("LeaveCluster: graceful leave cancelled.");
                return Unconfirmed(LeaveClusterOutcome.Timeout);
            }

            try
            {
                RaftPartition? systemPartition = partitionProvider.SystemPartition;

                // Find the current P0 leader. Try self first (fast path when we are the leader).
                bool amLeader = systemPartition is not null &&
                    await amILeaderQuick(RaftSystemConfig.SystemPartition).ConfigureAwait(false);

                string? leaderEndpoint = amLeader ? localEndpoint : systemPartition?.Leader;

                if (string.IsNullOrEmpty(leaderEndpoint))
                {
                    if (++emptyLeaderPolls >= maxEmptyLeaderPolls)
                    {
                        logger.LogInfoLeaveClusterLeaderUnknown(emptyLeaderPolls);
                        return Unconfirmed(LeaveClusterOutcome.NoLeader);
                    }

                    await Task.Delay(200, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // We have a leader — reset the empty-leader counter.
                emptyLeaderPolls = 0;

                using CancellationTokenSource attemptCts =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attemptCts.CancelAfter(attemptTimeoutMs);

                // When we are the leader, apply the removal here rather than sending it to
                // ourselves: the transport cannot always reach the local endpoint (a standalone or
                // embedded node has no peer registry to route through), and a self-directed round
                // trip would fail there and spin to the deadline instead of getting the leader's
                // real answer — including the refusal that protects the last voter.
                LeaveResponse resp = amLeader
                    ? await ReceiveLeave(request, attemptCts.Token).ConfigureAwait(false)
                    : await sendLeave(new RaftNode(leaderEndpoint), request, attemptCts.Token).ConfigureAwait(false);

                if (resp.Success)
                {
                    await WaitForRosterRemovalAsync(sw, deadlineMs, cancellationToken).ConfigureAwait(false);
                    return Committed(resp);
                }

                // Terminal = permanently blocked (e.g. InsufficientVoters) — give up immediately.
                // A null-hint without Terminal means the leader is unknown right now (election
                // in progress); the loop continues and retries within the 10 s deadline.
                if (resp.Terminal)
                {
                    logger.LogInformation("LeaveCluster: leave permanently rejected.");
                    return Unconfirmed(LeaveClusterOutcome.RefusedInsufficientVoters);
                }

                // Not the leader — follow the hint if it differs from the endpoint we just tried.
                if (!string.IsNullOrEmpty(resp.LeaderHint) && resp.LeaderHint != leaderEndpoint)
                {
                    try
                    {
                        using CancellationTokenSource hintCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        hintCts.CancelAfter(attemptTimeoutMs);

                        LeaveResponse hintResp = string.Equals(resp.LeaderHint, localEndpoint, StringComparison.Ordinal)
                            ? await ReceiveLeave(request, hintCts.Token).ConfigureAwait(false)
                            : await sendLeave(new RaftNode(resp.LeaderHint), request, hintCts.Token).ConfigureAwait(false);
                        if (hintResp.Success)
                        {
                            await WaitForRosterRemovalAsync(sw, deadlineMs, cancellationToken).ConfigureAwait(false);
                            return Committed(hintResp);
                        }

                        if (hintResp.Terminal)
                        {
                            logger.LogInfoLeaveClusterRejectedByHint(resp.LeaderHint);
                            return Unconfirmed(LeaveClusterOutcome.RefusedInsufficientVoters);
                        }
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        logger.LogInformation("LeaveCluster: graceful leave cancelled during hint attempt.");
                        return Unconfirmed(LeaveClusterOutcome.Timeout);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning("LeaveCluster: failed to contact leader hint {Hint}: {Message}", resp.LeaderHint, ex.Message);
                    }
                }

                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                logger.LogInformation("LeaveCluster: graceful leave cancelled.");
                return Unconfirmed(LeaveClusterOutcome.Timeout);
            }
            catch (Exception ex)
            {
                logger.LogWarning("LeaveCluster: error during graceful leave: {Message}", ex.Message);

                try
                {
                    await Task.Delay(200, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    logger.LogInformation("LeaveCluster: graceful leave cancelled.");
                    return Unconfirmed(LeaveClusterOutcome.Timeout);
                }
            }
        }

        logger.LogWarning("LeaveCluster: graceful leave timed out without a committed removal.");
        return Unconfirmed(LeaveClusterOutcome.Timeout);

        // The leader reports the version it committed the removal at, which is authoritative even
        // when the new roster has not propagated back here yet; fall back to the local view when an
        // older peer omits it.
        LeaveClusterResult Committed(LeaveResponse response) => new(
            LeaveClusterOutcome.Committed,
            response.MembershipVersion > 0
                ? response.MembershipVersion
                : getCoordinator().GetMembership().MembershipVersion);

        LeaveClusterResult Unconfirmed(LeaveClusterOutcome outcome) =>
            new(outcome, getCoordinator().GetMembership().MembershipVersion);
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

            if (!getCoordinator().GetMembership().Members.Any(m => m.Endpoint == localEndpoint))
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

        logger.LogWarning("LeaveCluster: removal committed but roster propagation timed out; proceeding to stop.");
        return false;
    }

    /// <summary>Idempotent; releases the request serialization semaphore.</summary>
    public void Dispose() => leaveRequestLock.Dispose();
}
