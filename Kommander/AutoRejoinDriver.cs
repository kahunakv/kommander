
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Gets an evicted node back into the cluster. Eviction is otherwise a one-way door: the startup
/// join already completed, so nothing would ever invoke the Join RPC again and the node parks as
/// <see cref="ClusterMemberRole.NotMember"/> forever — pre-votes suppressed on every partition,
/// terminal errors to clients.
/// <para>
/// Concurrency: at most one rejoin loop runs at a time, guarded by an interlocked flag rather than
/// a lock, because the trigger fires from the coordinator's roster-event path and must never
/// block there. A roster update that races the loop's exit simply re-triggers — every roster
/// change flows through the same entry point — so no re-check is needed on the way out.
/// </para>
/// </summary>
internal sealed class AutoRejoinDriver
{
    /// <summary>
    /// Guard flag: non-zero while <see cref="AutoRejoinLoopAsync"/> is running so roster updates
    /// arriving mid-rejoin don't spawn a second loop.
    /// </summary>
    private int _autoRejoinRunning;

    /// <summary>
    /// True once any committed roster observed by this node — including one replayed from the
    /// WAL during startup restore — contained the local endpoint. This, not initialization state,
    /// is what distinguishes "an evicted member" from "a node that was never admitted": a node
    /// that <b>boots into</b> an evicted roster applies the self-excluding record during restore,
    /// before initialization completes, and no further roster change ever arrives to re-fire the
    /// edge-triggered check — an initialization gate therefore deadlocked it as NotMember forever.
    /// The restore replays the earlier self-including roster first (it precedes the eviction
    /// record in log order), so this flag is always set by the time the eviction record is applied.
    /// </summary>
    private volatile bool _wasRosterMember;

    private readonly RaftConfiguration configuration;
    private readonly IDiscovery discovery;
    private readonly Func<ClusterMembership> getMembership;
    private readonly Func<ClusterMemberRole> getLocalRole;
    private readonly Func<bool> isLeaving;
    private readonly Func<bool> isLeaveRequested;
    private readonly Func<bool> isDisposed;
    private readonly Func<RaftNode, JoinRequest, Task<JoinResponse>> sendJoin;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;
    private readonly int localNodeId;

    internal AutoRejoinDriver(
        RaftConfiguration configuration,
        IDiscovery discovery,
        Func<ClusterMembership> getMembership,
        Func<ClusterMemberRole> getLocalRole,
        Func<bool> isLeaving,
        Func<bool> isLeaveRequested,
        Func<bool> isDisposed,
        Func<RaftNode, JoinRequest, Task<JoinResponse>> sendJoin,
        ILogger<IRaft> logger,
        string localEndpoint,
        int localNodeId)
    {
        this.configuration = configuration;
        this.discovery = discovery;
        this.getMembership = getMembership;
        this.getLocalRole = getLocalRole;
        this.isLeaving = isLeaving;
        this.isLeaveRequested = isLeaveRequested;
        this.isDisposed = isDisposed;
        this.sendJoin = sendJoin;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
        this.localNodeId = localNodeId;
    }

    /// <summary>
    /// Records that a committed roster included the local endpoint. Must be called <i>before</i>
    /// the corresponding <see cref="MaybeStart"/> check for a later roster: during startup restore
    /// the self-including roster and the eviction record arrive back-to-back on the coordinator
    /// loop, and this flag must already be visible when the eviction record's event fires.
    /// </summary>
    internal void MarkRosterMember() => _wasRosterMember = true;

    /// <summary>
    /// Starts the rejoin loop when a committed roster no longer contains this node.
    /// <para>
    /// Deliberately NOT triggered when: auto-rejoin is disabled; the node initiated a graceful
    /// leave (the removal is intentional — including an attempt that reported failure, because the
    /// removal may still land later and re-admitting would undo an operator-ordered decommission);
    /// this node has never been in a committed roster (first-time joins own their admission retry
    /// loop in the seed-join path, and a booting learner or a bare test manager routinely observes
    /// rosters that don't include it); or the roster is the pre-seed version 0.
    /// </para>
    /// <para>
    /// Known residual: a node whose replayed history contains <em>only</em> self-excluding rosters
    /// (possible after a snapshot install that post-dates its own eviction) never marks itself a
    /// roster member and still parks; it needs an explicit seed join.
    /// </para>
    /// </summary>
    internal void MaybeStart(ClusterMembership membership)
    {
        if (!configuration.EnableAutoRejoin)
            return;
        if (isLeaving() || isLeaveRequested() || isDisposed())
            return;
        if (membership.MembershipVersion == 0 || !_wasRosterMember)
            return;
        if (membership.Members.Any(m => m.Endpoint == localEndpoint))
            return;
        if (Interlocked.CompareExchange(ref _autoRejoinRunning, 1, 0) != 0)
            return;

        _ = Task.Run(AutoRejoinLoopAsync);
    }

    /// <summary>
    /// Background rejoin loop for an evicted member: sends the (idempotent) Join RPC to the
    /// remaining roster members and discovery peers with exponential backoff until this node is
    /// back in the committed roster (role leaves <c>NotMember</c> when the re-admission commit
    /// reaches us via replication or gossip), or the node starts leaving / disposing. Re-uses
    /// the same admission path as a first-time seed join, so the node re-enters as a Learner
    /// and is promoted back to Voter by the normal learner-promotion machinery once caught up
    /// — an evicted-but-live node is typically already caught up, so promotion is quick.
    /// </summary>
    private async Task AutoRejoinLoopAsync()
    {
        try
        {
            logger.LogWarning(
                "[{LocalEndpoint}] Removed from committed roster while running (likely dead-member eviction across a restart); starting auto-rejoin",
                localEndpoint);

            TimeSpan backoff = TimeSpan.FromSeconds(1);
            TimeSpan maxBackoff = TimeSpan.FromSeconds(30);

            while (!isLeaving() && !isDisposed())
            {
                ClusterMemberRole role = getLocalRole();
                if (role != ClusterMemberRole.NotMember)
                {
                    if (logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation("[{LocalEndpoint}] Auto-rejoin complete: local role is {Role}", localEndpoint, role);
                    return;
                }

                // Candidate targets: the members of the roster that excluded us (they are the
                // live cluster) plus discovery peers as a fallback.
                List<string> targets = getMembership().Members
                    .Select(m => m.Endpoint)
                    .Concat(discovery.GetNodes().Select(n => n.Endpoint))
                    .Where(e => e != localEndpoint)
                    .Distinct()
                    .ToList();

                foreach (string target in targets)
                {
                    if (isLeaving() || isDisposed())
                        return;

                    if (await TrySendJoinAsync(target).ConfigureAwait(false))
                        break;
                }

                await Task.Delay(backoff).ConfigureAwait(false);
                backoff = backoff * 2 > maxBackoff ? maxBackoff : backoff * 2;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning("[{LocalEndpoint}] Auto-rejoin loop failed: {Message}", localEndpoint, ex.Message);
        }
        finally
        {
            Interlocked.Exchange(ref _autoRejoinRunning, 0);
            // A roster update that raced our exit re-triggers via MaybeStart; no re-check is
            // needed here because every roster change goes through the membership-change handler.
        }
    }

    /// <summary>
    /// One Join attempt against <paramref name="target"/>, following a single leader hint,
    /// mirroring the seed-join contact loop. Returns true when a leader accepted the join
    /// (the committed roster entry then reaches this node asynchronously).
    /// </summary>
    private async Task<bool> TrySendJoinAsync(string target)
    {
        try
        {
            JoinResponse resp = await sendJoin(
                new RaftNode(target), new JoinRequest(localEndpoint, localNodeId)).ConfigureAwait(false);

            if (resp.Success)
                return true;

            if (!string.IsNullOrEmpty(resp.LeaderHint))
            {
                resp = await sendJoin(
                    new RaftNode(resp.LeaderHint), new JoinRequest(localEndpoint, localNodeId)).ConfigureAwait(false);
                return resp.Success;
            }
        }
        catch (Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[{LocalEndpoint}] Auto-rejoin: join attempt against {Target} failed: {Message}", localEndpoint, target, ex.Message);
        }

        return false;
    }
}
