
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
/// by <see cref="LeaveCluster"/> once its commit attempt has ended, where teardown follows
/// regardless). Neither path raises it while the removal is still being committed: the node may
/// have to win the system-partition election to commit its own removal.
/// <see cref="IsLeaveRequested"/> is sticky while a departure can still land: an attempt that
/// timed out may still commit later, and auto-rejoin would silently undo an operator-ordered
/// decommission. It is <i>released</i> only when the attempt provably ended with the node a full
/// Voter and nothing in flight — a refusal that committed nothing, or a drain whose rollback to
/// Voter was confirmed. A node in that state is a member like any other and must auto-rejoin
/// after a later eviction; keeping the latch there parked an evicted ex-leaver as NotMember
/// forever while it still led its ranges (Kahuna nightly, 2026-10-11).
/// A drain whose <c>Voter → Leaving</c> commit was never confirmed (the request timed out
/// against an unreachable or mid-election P0 leader) keeps the latch too, because the commit may
/// still land — but it arms the roster observer, so the next committed roster decides: one that
/// still names the node <c>Voter</c> releases the latch; one that names it <c>Leaving</c>
/// confirms the drain. Without that, a timed-out request left the latch raised with nothing able
/// to release it, and a later eviction parked the node exactly as above (Kahuna nightly,
/// 2026-10-13).
/// </para>
/// <para>
/// An <b>eviction voids a decommission</b>. When a committed roster excludes this node, the
/// roster's own departure record (<see cref="ClusterMembership.Departures"/>, written by the P0
/// leader at the removal) says whether the node was evicted as a dead member or removed because
/// it asked. Evicted while a <see cref="RequestLeaveAsync"/> departure was in force — whatever
/// the drain's state: never committed, committed late with nobody driving it, or mid-drain —
/// releases the latch, because nothing the request sent can land after an exclusion (a role
/// change for an absent member is refused, a removal of an absent member is a no-op) and the
/// node still hosts every replica it had. The auto-rejoin that follows is the only exit: a
/// NotMember node keeps leading its ranges, receives no P0 entries, and so can never feed the
/// learners the planner adds to move those ranges off it. Deciding this from the member list
/// alone was impossible, and inferring it from local state left a hole per shape (Kahuna
/// nightly, 2026-10-27: a drain-leg commit that landed after the request timed out, then an
/// eviction while <c>Leaving</c>). Removed by request keeps the latch for good. A roster with no
/// record (an older leader) falls back to the version-adjacency rule of the unconfirmed drain
/// request.
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
    /// While raised, the local role reports <see cref="ClusterMemberRole.Leaving"/> and the
    /// election / pre-vote gate suppresses campaigning on all partitions.
    /// <para>
    /// Never raised while a removal is still being committed: until then the node is a full
    /// member, and it may need to win the system-partition election in order to commit its own
    /// removal at all. <see cref="RequestLeaveAsync"/> raises it once the removal has committed;
    /// <see cref="LeaveCluster"/> raises it once its commit attempt has ended, whatever the
    /// outcome, because teardown follows.
    /// </para>
    /// </summary>
    private volatile bool _leaving;

    /// <summary>
    /// Set by <see cref="RequestLeaveAsync"/> or <see cref="LeaveCluster"/> while a departure is
    /// in force: requested, and possibly still able to land. While set, this node must never
    /// re-admit itself — the removal can still commit after a timed-out attempt, and auto-rejoin
    /// would silently undo the departure that was asked for.
    /// <para>
    /// Released by <see cref="ReleaseLeaveRequest"/> when the attempt provably committed nothing
    /// and has nothing in flight (a refusal), or when a drain's rollback to <c>Voter</c> is
    /// confirmed — either by the rollback commit itself (<see cref="RollBackDrainAsync"/>) or by a
    /// later committed roster that names this node <c>Voter</c> again
    /// (<see cref="ObserveCommittedRoster"/>, which covers a rollback whose acknowledgement was
    /// lost, and a drain request whose <c>Leaving</c> commit never landed — see
    /// <see cref="_drainLegUnconfirmed"/>), or when a committed roster records this node
    /// <i>evicted</i> while a decommission was in force (see <see cref="_departureIsDecommission"/>).
    /// Never released by <see cref="LeaveCluster"/>: teardown follows there regardless.
    /// Election suppression (<see cref="_leaving"/>) is a separate latch — a node that was refused
    /// is still a full member and must keep campaigning.
    /// </para>
    /// </summary>
    private volatile bool _leaveRequested;

    /// <summary>
    /// The roster version at (or after) which a <see cref="RequestLeaveAsync"/> drain committed
    /// this node's role as <see cref="ClusterMemberRole.Leaving"/> (or found it already so);
    /// 0 while no drain is in force. It arms <see cref="ObserveCommittedRoster"/>: only a roster
    /// <i>newer</i> than this version that names the node <c>Voter</c> can mean "the drain was
    /// rolled back". A version rather than a flag, because a roster event older than the Leaving
    /// commit can be delivered after the commit's acknowledgement (the node sent the request from
    /// an older cache) and still names the node Voter — the ordinary pre-drain state, which proves
    /// nothing about the attempt.
    /// </summary>
    private long _drainRoleCommittedVersion;

    /// <summary>
    /// True while the drain's <c>Voter → Leaving</c> commit is unconfirmed: the request timed out
    /// (or found no leader) and no committed roster has shown this node <c>Leaving</c> since.
    /// Distinguishes that state from a drain that did commit <c>Leaving</c>: a roster that
    /// <i>excludes</i> the node after a confirmed drain is the drain's own removal (the latch must
    /// stay), while the same roster after an unconfirmed request can only be an eviction (the
    /// pass removes <c>Leaving</c> members only, and none was ever committed), so the latch is
    /// released and the node auto-rejoins. Cleared when <c>Leaving</c> is confirmed (by the
    /// commit's answer or by a committed roster) and when the latch is released.
    /// </summary>
    private volatile bool _drainLegUnconfirmed;

    /// <summary>
    /// True while the raised latch belongs to a <see cref="RequestLeaveAsync"/> attempt: an
    /// operator decommission of a node that keeps running. Only such a departure is voided by an
    /// eviction (<see cref="ObserveCommittedRoster"/>): the node is alive, was not asked out by
    /// the eviction, and still hosts its replicas, so it must auto-rejoin. The other two raisers
    /// clear it — <see cref="LeaveCluster"/> tears the node down whatever happens, and
    /// <see cref="WithdrawFailedJoinAsync"/> exists to undo an admission, so neither may be undone
    /// by a rejoin. Cleared with the latch.
    /// </summary>
    private volatile bool _departureIsDecommission;

    /// <summary>
    /// The version of the roster whose departure record this node last acted on, so the record is
    /// logged once rather than on every roster a NotMember node learns through gossip.
    /// </summary>
    private long _departureObservedAt;

    /// <summary>
    /// Set by <see cref="SettleExclusion"/> when a committed roster records this node evicted
    /// while a <see cref="RequestLeaveAsync"/> attempt is running; cleared when the next attempt
    /// starts. The attempt's waits read it instead of the roster: the auto-rejoin that the same
    /// roster event starts can re-admit the node (as a Learner) before the drain loop's next poll,
    /// at which point the roster names the node again and the eviction is no longer visible there
    /// — the attempt would then run on to its drain deadline and try to roll back a role the node
    /// no longer holds. The outcome must be <see cref="LeaveClusterOutcome.Evicted"/> either way.
    /// </summary>
    private volatile bool _evictedDuringRequest;

    /// <summary>
    /// How long an attempt waits for the local roster cache to reach a version the P0 leader
    /// reported before deciding, from that roster's departure record, whether an exclusion was an
    /// eviction. The leader's answer carries the version; the roster itself reaches an excluded
    /// node only by gossip, so a short wait is usually enough and the fallback (the historical
    /// "departed" answer) is reached only when gossip is cut too.
    /// </summary>
    private static readonly TimeSpan ExclusionRecordWait = TimeSpan.FromSeconds(3);

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
    /// True while an operator-requested departure is in force. Blocks auto-rejoin from undoing a
    /// decommission that lands late. Released when the attempt provably left the node a full
    /// Voter with nothing in flight — see <see cref="_leaveRequested"/>.
    /// </summary>
    internal bool IsLeaveRequested => _leaveRequested;

    /// <summary>
    /// Observes every advance of the committed roster (called from the membership-change handler,
    /// before the auto-rejoin check for the same roster). Releases the leave latch when a drain
    /// that committed <c>Leaving</c> is seen rolled back to <c>Voter</c>: the only transition that
    /// produces a <c>Voter</c> role after a committed <c>Leaving</c> is the rollback
    /// (<c>SetMemberRole</c>), or an explicit operator re-admission followed by promotion — and in
    /// both cases the node is a full member again by intent.
    /// <para>
    /// Also settles a drain request whose <c>Leaving</c> commit was never confirmed
    /// (<see cref="_drainLegUnconfirmed"/>): a newer roster naming the node <c>Voter</c> releases
    /// the latch as above; a roster naming the node <c>Leaving</c> confirms the drain, re-raises
    /// the latch if a roster in between had released it, and arms the watermark at that version.
    /// </para>
    /// <para>
    /// A roster that <i>excludes</i> the node is settled by its departure record
    /// (<see cref="SettleExclusion"/>): evicted releases the latch of a decommission, removed by
    /// request keeps it. The record is what makes this independent of how the roster arrived —
    /// an evicted node is partitioned while it is evicted and usually learns the roster through
    /// a gossip push that skips versions, which defeated every rule keyed on the version itself.
    /// </para>
    /// <para>
    /// This is the path that catches a rollback whose acknowledgement was lost:
    /// <see cref="RollBackDrainAsync"/> may return with the roster still showing <c>Leaving</c>
    /// while the commit lands moments later. Ordering is guaranteed by the handler, which runs
    /// this before <see cref="AutoRejoinDriver.MaybeStart"/> for the same roster version, so a
    /// release is always visible to the eviction check that follows it.
    /// </para>
    /// </summary>
    internal void ObserveCommittedRoster(ClusterMembership membership)
    {
        if (_leaving)
            return;

        ClusterMember? self = membership.Members
            .FirstOrDefault(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal));

        // A committed Leaving role can only come from this node's own drain request. It confirms
        // a request whose acknowledgement was lost (the latch was kept, the watermark armed), and
        // it re-raises a latch that a Voter roster released in between: the drain is in force and
        // its removal must not be undone by an auto-rejoin.
        if (self?.Role == ClusterMemberRole.Leaving)
        {
            _leaveRequested = true;
            _drainLegUnconfirmed = false;
            ArmDrainRoleWatermark(membership.MembershipVersion);
            return;
        }

        if (!_leaveRequested)
            return;

        if (self is null)
        {
            SettleExclusion(membership);
            return;
        }

        long armedAt = Volatile.Read(ref _drainRoleCommittedVersion);

        if (armedAt == 0 || membership.MembershipVersion <= armedAt)
            return;

        if (self.Role == ClusterMemberRole.Voter)
            ReleaseLeaveRequest($"committed roster v{membership.MembershipVersion} names this node Voter again after a drain");
    }

    /// <summary>
    /// Decides what a committed roster that excludes this node means for a raised leave latch.
    /// Only a <see cref="RequestLeaveAsync"/> departure is open to question (see
    /// <see cref="_departureIsDecommission"/>); the roster's departure record then answers it:
    /// <list type="bullet">
    /// <item><see cref="ClusterDepartureReason.Evicted"/>: the request is void. Nothing it sent can
    /// land after the exclusion, and the node still hosts its replicas, so the latch is released
    /// and the auto-rejoin check that the handler runs right after this call re-admits it.</item>
    /// <item><see cref="ClusterDepartureReason.Requested"/>: the departure the node asked for
    /// happened (its own removal, or the placement pass finishing a drain the request had given up
    /// on). The latch stays for good.</item>
    /// <item>No record (a roster committed by a leader that predates it): the version-adjacency
    /// rule of the unconfirmed drain request — excluded by the very next roster after a request
    /// whose <c>Leaving</c> never committed can only be an eviction, because versions advance by
    /// one per commit and the pass removes <c>Leaving</c> members only. A roster that skipped
    /// versions proves nothing and keeps the latch, as before the record existed.</item>
    /// </list>
    /// </summary>
    private void SettleExclusion(ClusterMembership membership)
    {
        if (!_departureIsDecommission)
            return;

        ClusterDeparture? departure = membership.DepartureOf(localEndpoint);

        if (departure is not null)
        {
            if (departure.Reason == ClusterDepartureReason.Evicted)
            {
                _evictedDuringRequest = true;
                ReleaseLeaveRequest($"committed roster v{membership.MembershipVersion} records this node evicted as a dead member at v{departure.Version}; nothing from the leave request can land after an eviction");
                return;
            }

            if (Interlocked.Exchange(ref _departureObservedAt, departure.Version) != departure.Version && logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("[{LocalEndpoint}] Leave request honoured: committed roster v{Version} records this node removed by request at v{DepartureVersion}; it stays out of the cluster and will not auto-rejoin", localEndpoint, membership.MembershipVersion, departure.Version);

            return;
        }

        long armedAt = Volatile.Read(ref _drainRoleCommittedVersion);

        if (_drainLegUnconfirmed && armedAt != 0 && membership.MembershipVersion == armedAt + 1)
            ReleaseLeaveRequest($"committed roster v{membership.MembershipVersion} evicted this node and no roster since the unconfirmed drain request (v{armedAt}) named it Leaving");
    }

    /// <summary>
    /// True when the committed roster this node holds records it evicted as a dead member. Waits
    /// up to <see cref="ExclusionRecordWait"/> for the local cache to reach
    /// <paramref name="reportedVersion"/> (the version a P0 leader answered with when it said the
    /// node is not a member) so the decision is made on the roster that excluded the node rather
    /// than on a stale one; a cache that never gets there answers false, which keeps the
    /// historical "departed" reading of the outcome.
    /// </summary>
    private async Task<bool> WasEvictedAsync(long reportedVersion)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();

        while (true)
        {
            if (_evictedDuringRequest)
                return true;

            ClusterMembership roster = getCoordinator().GetMembership();

            if (roster.MembershipVersion >= reportedVersion
                && !roster.Members.Any(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)))
                return roster.DepartureOf(localEndpoint)?.Reason == ClusterDepartureReason.Evicted;

            if (sw.GetElapsedMilliseconds() >= (long)ExclusionRecordWait.TotalMilliseconds)
                return false;

            await Task.Delay(50).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Releases the departure latch so the node behaves as the full Voter it is — including
    /// auto-rejoin after a later eviction. Only called when the attempt provably committed nothing
    /// that can still land. Idempotent.
    /// </summary>
    private void ReleaseLeaveRequest(string reason)
    {
        if (!_leaveRequested)
            return;

        _leaveRequested = false;
        _departureIsDecommission = false;
        _drainLegUnconfirmed = false;
        Volatile.Write(ref _drainRoleCommittedVersion, 0);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("[{LocalEndpoint}] Leave request released: {Reason}; the node is a full member by right and auto-rejoins after an eviction", localEndpoint, reason);
    }

    /// <summary>Overall bound on <see cref="WithdrawFailedJoinAsync"/>; each attempt has its own shorter bound.</summary>
    private static readonly TimeSpan WithdrawDeadline = TimeSpan.FromSeconds(10);

    /// <summary>Bound on one <c>LeaveRequest</c> round trip inside <see cref="WithdrawFailedJoinAsync"/>.</summary>
    private static readonly TimeSpan WithdrawAttemptTimeout = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Takes a node whose seed join failed after admission back out of the committed roster, so the
    /// failure does not leave a Learner the survivors keep tracking. Called by
    /// <see cref="ClusterJoinService"/> just before it throws; returns true when a P0 leader
    /// acknowledged the removal.
    /// <para>
    /// Why it cannot use <see cref="RequestLeaveAsync"/>: a join can fail before this node has
    /// initialized (no partition map, possibly no known leader), and that path refuses an
    /// uninitialized node. This one only needs the transport: it sends a <see cref="LeaveRequest"/>
    /// to the system-partition leader this node knows of, else to each of
    /// <paramref name="contacts"/> (the join's seeds and the endpoint that admitted it), and follows
    /// one leader hint per answer. The leader-side handler is idempotent, so a retry after a lost
    /// reply is answered with success.
    /// </para>
    /// <para>
    /// Raises <see cref="IsLeaveRequested"/> first and leaves it raised: a removal sent here can
    /// commit after the deadline, and auto-rejoin must not re-admit the node into the state the
    /// withdrawal exists to undo. The node does not campaign either way — it never became a Voter.
    /// </para>
    /// <para>
    /// Best effort and bounded by <see cref="WithdrawDeadline"/>: a failed withdrawal is logged and
    /// the caller throws its original error regardless. The caller's token ends it earlier.
    /// </para>
    /// </summary>
    internal async Task<bool> WithdrawFailedJoinAsync(IReadOnlyList<string> contacts, CancellationToken cancellationToken)
    {
        // Not a decommission: an eviction of the never-promoted learner must not re-admit it.
        _departureIsDecommission = false;
        _leaveRequested = true;

        LeaveRequest request = new(localEndpoint, configuration.NodeId);
        ValueStopwatch sw = ValueStopwatch.StartNew();

        while (sw.GetElapsedMilliseconds() < (long)WithdrawDeadline.TotalMilliseconds)
        {
            if (cancellationToken.IsCancellationRequested)
                return false;

            // Known P0 leader first (it saves a hint round trip), then every contact in turn.
            List<string> targets = new(contacts.Count + 1);

            string? knownLeader = partitionProvider.SystemPartition?.Leader;
            if (!string.IsNullOrEmpty(knownLeader) && !string.Equals(knownLeader, localEndpoint, StringComparison.Ordinal))
                targets.Add(knownLeader);

            foreach (string contact in contacts)
            {
                if (!string.Equals(contact, localEndpoint, StringComparison.Ordinal) && !targets.Contains(contact))
                    targets.Add(contact);
            }

            foreach (string target in targets)
            {
                LeaveResponse response = await TrySendWithdrawalAsync(target, request, cancellationToken).ConfigureAwait(false);

                if (!response.Success
                    && !string.IsNullOrEmpty(response.LeaderHint)
                    && !string.Equals(response.LeaderHint, target, StringComparison.Ordinal)
                    && !string.Equals(response.LeaderHint, localEndpoint, StringComparison.Ordinal))
                    response = await TrySendWithdrawalAsync(response.LeaderHint, request, cancellationToken).ConfigureAwait(false);

                if (response.Success)
                {
                    logger.LogWarning(
                        "[{LocalEndpoint}] JoinCluster: withdrew this node from the roster after the failed join (membership version {Version})",
                        localEndpoint, response.MembershipVersion);
                    return true;
                }

                // A learner's removal is never refused for quorum reasons; a Terminal answer means
                // the leader will not remove it, so retrying changes nothing.
                if (response.Terminal)
                {
                    logger.LogWarning(
                        "[{LocalEndpoint}] JoinCluster: the leader refused to withdraw this node after the failed join; it stays in the roster",
                        localEndpoint);
                    return false;
                }
            }

            try
            {
                await Task.Delay(250, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        logger.LogWarning(
            "[{LocalEndpoint}] JoinCluster: could not withdraw this node from the roster within {DeadlineSeconds} s after the failed join; " +
            "no system-partition leader acknowledged the removal (a removal already sent may still commit). Remove it with RemoveMember if it stays",
            localEndpoint, WithdrawDeadline.TotalSeconds);

        return false;
    }

    /// <summary>
    /// One withdrawal round trip, bounded by <see cref="WithdrawAttemptTimeout"/>. A transport failure
    /// or timeout is a failed answer, never an exception: the caller moves on to the next target.
    /// </summary>
    private async Task<LeaveResponse> TrySendWithdrawalAsync(string target, LeaveRequest request, CancellationToken cancellationToken)
    {
        try
        {
            using CancellationTokenSource attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(WithdrawAttemptTimeout);

            return await sendLeave(new RaftNode(target), request, attemptCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}] JoinCluster: withdrawal request to {Target} failed: {Message}",
                    localEndpoint, target, ex.Message);

            return new LeaveResponse(false);
        }
    }

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
    /// When the local node is part of a committed roster (MembershipVersion &gt; 0) this commits a
    /// <c>RemoveMember</c> entry on the system partition and waits up to 10 s for the removal to
    /// propagate back to this node. No replica drain happens; use <see cref="RequestLeaveAsync"/>
    /// for a decommission that evacuates replicas first.
    /// </para>
    /// <para>
    /// The node keeps campaigning until that commit attempt ends — it can be the only node able to
    /// lead the system partition, and so the only one able to commit its own removal. Only then is
    /// the <b>local</b> <see cref="_leaving"/> latch set (the local role reports
    /// <see cref="ClusterMemberRole.Leaving"/> and elections are suppressed; this is not a
    /// committed roster change), and the node tears down whether or not the removal committed.
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
        // Departure is decided before anything else runs: teardown follows whatever the commit
        // attempt reports, so the roster that excludes this node must never start an auto-rejoin
        // — not even one that evicts it while the attempt runs.
        _departureIsDecommission = false;
        _leaveRequested = true;

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
            // Campaigning stays open while the attempt runs. The removal is committed by the
            // system-partition leader, and this node can be the only one able to become it: when
            // the previous leader left with its last entry acknowledged by this node alone, every
            // other voter's log is behind and loses the election. Suppressing here left that
            // partition leaderless for the whole deadline, and the node then stopped with its
            // removal uncommitted — the survivors counted a dead node towards quorum for good.
            await CommitGracefulLeaveAsync(cancellationToken).ConfigureAwait(false);
        }

        // The attempt is over, committed or not, and teardown is next: from here the node must not
        // win a leadership it is about to abandon.
        _leaving = true;

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
    /// A node the cluster evicts as dead while the attempt runs is reported as
    /// <see cref="LeaveClusterOutcome.Evicted"/>, not as a departure: nothing was evacuated, the
    /// request is void, and the node re-admits itself through auto-rejoin.
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
            // would undo the decommission just as surely as one that never left. Marked as a
            // decommission: the node keeps running, so an eviction voids the request instead of
            // parking the node as NotMember (see SettleExclusion).
            _leaveRequested = true;
            _departureIsDecommission = true;
            _evictedDuringRequest = false;

            RaftSystemCoordinator systemCoordinator = getCoordinator();
            ClusterMembership roster = systemCoordinator.GetMembership();

            // No committed roster yet (pre-seed transient): there is no membership entry to remove,
            // and the node cannot be counted towards anyone's quorum until seeding completes.
            // Nothing was sent, so nothing can land later: release the latch.
            if (roster.MembershipVersion == 0 || !clusterHandler.Joined)
            {
                ReleaseLeaveRequest("the node has no committed roster to leave yet");
                return new(LeaveClusterOutcome.NotInitialized, roster.MembershipVersion);
            }

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

                    // A refusal committed nothing and sent nothing that can still land: the node
                    // is the same full Voter it was before the call. (A DrainTimedOut release is
                    // decided inside RollBackDrainAsync, where the rollback's confirmation is
                    // known; a Timeout keeps the latch — the Leaving commit may still land —
                    // but arms ObserveCommittedRoster, which releases it on the next committed
                    // roster that names the node Voter or evicts it before any Leaving landed.)
                    if (terminal.Outcome is LeaveClusterOutcome.RefusedDrainInProgress or LeaveClusterOutcome.RefusedInsufficientVoters)
                        ReleaseLeaveRequest($"the drain was refused ({terminal.Outcome}) and nothing was committed");

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
            // roster resolves that in the caller's favour whenever the new roster is already here
            // — unless that roster records an eviction, which is not the departure asked for.
            if (!result.Left &&
                !systemCoordinator.GetMembership().Members.Any(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)))
            {
                ClusterMembership excluded = systemCoordinator.GetMembership();
                result = excluded.DepartureOf(localEndpoint)?.Reason == ClusterDepartureReason.Evicted
                    ? new(LeaveClusterOutcome.Evicted, excluded.MembershipVersion)
                    : new(LeaveClusterOutcome.Committed, excluded.MembershipVersion);
            }

            // An eviction recorded while the attempt ran is never a departure: the node still
            // hosts its replicas and re-admits itself (the latch was released on that roster).
            if (_evictedDuringRequest)
                result = new(LeaveClusterOutcome.Evicted, systemCoordinator.GetMembership().MembershipVersion);

            // Only a departure that actually happened stops the node campaigning, and it stops it
            // for good: it is out of the roster and must not contend for leadership it no longer
            // has a claim to.
            if (result.Left)
                _leaving = true;

            // The leader refused the removal outright (last voter): nothing committed, nothing in
            // flight. Timeout / NoLeader deliberately keep the latch — a RemoveMember sent during
            // the attempt may still be applied by the leader after the deadline.
            if (result.Terminal)
                ReleaseLeaveRequest($"the removal was refused ({result.Outcome}) and nothing was committed");

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
    /// timeout (role rolled back to Voter so the campaign gates release the node), a
    /// departure already completed by the placement pass (removal won the rollback race), or an
    /// eviction while draining (<see cref="LeaveClusterOutcome.Evicted"/>: not a departure; the
    /// node still hosts its replicas and re-admits itself).
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

            (RaftOperationStatus status, long leavingVersion) = await CommitRoleTransitionAsync(ClusterMemberRole.Leaving, cancellationToken).ConfigureAwait(false);

            switch (status)
            {
                case RaftOperationStatus.Success:
                    // The leader's version of the Leaving commit; the local cache may still lag
                    // behind it, so the watermark takes the newer of the two.
                    ArmDrainRoleWatermark(leavingVersion);
                    break;

                case RaftOperationStatus.InsufficientVoters:
                    return new(LeaveClusterOutcome.RefusedInsufficientVoters, systemCoordinator.GetMembership().MembershipVersion);

                case RaftOperationStatus.DrainInProgress:
                    return new(LeaveClusterOutcome.RefusedDrainInProgress, systemCoordinator.GetMembership().MembershipVersion);

                case RaftOperationStatus.MemberNotFound:
                    // Already out of the roster. Departed — unless the roster that excluded the
                    // node records an eviction, in which case nothing was asked for and the node
                    // re-admits itself (the latch release happens on that roster's event).
                    if (await WasEvictedAsync(leavingVersion).ConfigureAwait(false))
                        return new(LeaveClusterOutcome.Evicted, systemCoordinator.GetMembership().MembershipVersion);

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
                    {
                        // The latch stays raised (the commit may still land) but must not stay
                        // raised forever: arm the roster observer at the current version. The
                        // next committed roster settles it — Voter or an eviction before any
                        // Leaving releases the latch, Leaving confirms the drain. Without this
                        // the node had no release path at all, and a later eviction parked it
                        // as a NotMember range leader (Kahuna nightly, 2026-10-13).
                        _drainLegUnconfirmed = true;
                        ArmDrainRoleWatermark(0);

                        logger.LogWarning("[{LocalEndpoint}] RequestLeaveAsync: the Voter -> Leaving commit was not confirmed ({Status}); the roster still names this node Voter. The leave request stays in force until the next committed roster settles it.", localEndpoint, status);

                        return new(LeaveClusterOutcome.Timeout, systemCoordinator.GetMembership().MembershipVersion);
                    }

                    break;
            }
        }

        // From here the committed roster names this node Leaving (committed above, confirmed by
        // the roster after a lost acknowledgement, or resumed from a previous attempt). Arm the
        // roster observer: a roster newer than this one naming the node Voter can now only be
        // the rollback. (Idempotent after the Success branch above.)
        _drainLegUnconfirmed = false;
        ArmDrainRoleWatermark(0);

        // ── Wait for evacuation ──────────────────────────────────────────────
        // The placement pass on the P0 leader does the work; this loop only observes committed
        // state. Two exits besides timeout: the map stops naming us (drained — proceed to the
        // final removal), or the roster stops naming us (the pass finished the drain AND
        // committed the removal itself — crash-resumption path, nothing left to do).
        ValueStopwatch drainStopwatch = ValueStopwatch.StartNew();
        long drainDeadlineMs = (long)configuration.DecommissionDrainTimeout.TotalMilliseconds;

        while (true)
        {
            // Read before the roster: the rejoin that follows an eviction can already have put
            // the node back into the roster as a Learner (see the field).
            if (_evictedDuringRequest)
                return new(LeaveClusterOutcome.Evicted, systemCoordinator.GetMembership().MembershipVersion);

            ClusterMembership roster = systemCoordinator.GetMembership();

            if (!roster.Members.Any(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal)))
            {
                // Excluded mid-drain. The pass finishing the drain and committing the removal
                // itself is the expected exit; an eviction (the node was unreachable for longer
                // than the grace) is not a departure at all — the replicas were never evacuated —
                // and must not be reported as one, or the caller stops a node that still hosts
                // them while the latch (already released on that roster's event) lets it rejoin.
                if (roster.DepartureOf(localEndpoint)?.Reason == ClusterDepartureReason.Evicted)
                    return new(LeaveClusterOutcome.Evicted, roster.MembershipVersion);

                return new(LeaveClusterOutcome.Committed, roster.MembershipVersion, Drained: true);
            }

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
    /// Arms <see cref="ObserveCommittedRoster"/> at the newer of <paramref name="committedVersion"/>
    /// (the leader's answer for the Leaving commit, 0 when unknown) and the local roster version.
    /// Never lowers an existing watermark.
    /// </summary>
    private void ArmDrainRoleWatermark(long committedVersion)
    {
        long local = getCoordinator().GetMembership().MembershipVersion;
        long watermark = Math.Max(Math.Max(committedVersion, local), 1);

        long current = Volatile.Read(ref _drainRoleCommittedVersion);
        if (watermark > current)
            Volatile.Write(ref _drainRoleCommittedVersion, watermark);
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
    /// committed roster. The one exception is an exclusion the roster records as an
    /// <i>eviction</i>: the drain did not complete and the node is about to re-admit itself, so
    /// the outcome is <see cref="LeaveClusterOutcome.Evicted"/>, never a departure.
    /// </para>
    /// <para>
    /// A <b>confirmed</b> rollback (the commit succeeded, or the roster already names this node
    /// <c>Voter</c>) releases the leave latch: the operator's intent was not honoured, no removal
    /// from this attempt can land (the drain never reached the <c>RemoveMember</c> step, and the
    /// placement pass only removes a <c>Leaving</c> member), and the node is a full Voter that
    /// must auto-rejoin after a later eviction. An unconfirmed rollback keeps the latch — the
    /// node may still be <c>Leaving</c> and the pass may still finish its removal; if the
    /// rollback lands later, <see cref="ObserveCommittedRoster"/> releases it then.
    /// </para>
    /// </summary>
    private async Task<LeaveClusterResult> RollBackDrainAsync()
    {
        // Evicted while the drain waited: there is no Leaving role left to roll back (the node is
        // out, or already re-admitted as a Learner), and the request is void.
        if (_evictedDuringRequest)
            return new(LeaveClusterOutcome.Evicted, getCoordinator().GetMembership().MembershipVersion);

        (RaftOperationStatus status, long reportedVersion) = await CommitRoleTransitionAsync(ClusterMemberRole.Voter, CancellationToken.None).ConfigureAwait(false);

        ClusterMembership after = getCoordinator().GetMembership();
        ClusterMember? self = after.Members.FirstOrDefault(m => string.Equals(m.Endpoint, localEndpoint, StringComparison.Ordinal));

        if (status == RaftOperationStatus.MemberNotFound || self is null)
        {
            // Out of the roster. Removal-wins is the expected reading — unless the roster that
            // excluded the node records an eviction: then the drain never completed, the
            // replicas are still here, and the node re-admits itself.
            if (await WasEvictedAsync(reportedVersion).ConfigureAwait(false))
                return new(LeaveClusterOutcome.Evicted, getCoordinator().GetMembership().MembershipVersion);

            return new(LeaveClusterOutcome.Committed, getCoordinator().GetMembership().MembershipVersion, Drained: true);
        }

        if (status == RaftOperationStatus.Success || self.Role == ClusterMemberRole.Voter)
        {
            ReleaseLeaveRequest("the drain timed out and the rollback to Voter was confirmed");
            return new(LeaveClusterOutcome.DrainTimedOut, after.MembershipVersion);
        }

        // The rollback itself could not be confirmed (e.g. no reachable P0 leader). The node
        // may be durably Leaving: it keeps serving its ranges but cannot campaign, and the
        // next placement pass keeps evacuating it. Retrying RequestLeaveAsync resumes the
        // drain; the warning is the operator's cue that this node needs attention. The leave
        // latch stays raised until a committed roster shows the rollback landed.
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
