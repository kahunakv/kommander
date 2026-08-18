
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Reacts to every advance of the committed cluster roster: notifies the application, repairs
/// per-follower replication progress for members the new roster (re)admits, and hands an eviction
/// of this node to the auto-rejoin driver.
/// <para>
/// Concurrency: this runs only on the system coordinator's event path, which delivers roster
/// versions one at a time in increasing order. That single-threadedness is what lets
/// <see cref="_lastSeenJoinedVersions"/> be a plain dictionary with no synchronization; nothing
/// else may call into this type.
/// </para>
/// </summary>
internal sealed class MembershipChangeHandler
{
    /// <summary>
    /// The <see cref="ClusterMember.JoinedVersion"/> last observed per endpoint, used to detect
    /// (re)admissions. Keying on JoinedVersion rather than a previous-roster diff makes detection
    /// robust to version jumps: the membership cache is monotonic and a node may apply v1 → v3
    /// directly (per-key config replication carries only the latest roster), which would hide the
    /// intermediate eviction from a set diff — but a readmitted member always carries a strictly
    /// higher JoinedVersion.
    /// </summary>
    private readonly Dictionary<string, long> _lastSeenJoinedVersions = new(StringComparer.Ordinal);

    private readonly IPartitionProvider partitionProvider;
    private readonly RaftEventNotifier eventNotifier;
    private readonly AutoRejoinDriver autoRejoinDriver;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;

    internal MembershipChangeHandler(
        IPartitionProvider partitionProvider,
        RaftEventNotifier eventNotifier,
        AutoRejoinDriver autoRejoinDriver,
        ILogger<IRaft> logger,
        string localEndpoint)
    {
        this.partitionProvider = partitionProvider;
        this.eventNotifier = eventNotifier;
        this.autoRejoinDriver = autoRejoinDriver;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
    }

    /// <summary>
    /// Fires the application-facing membership event with the new roster and checks whether this
    /// node has been removed from it, which triggers the auto-rejoin driver. Called by the system
    /// coordinator each time its cached membership advances to a strictly higher version.
    /// </summary>
    internal void RaiseMembershipChanged(ClusterMembership membership)
    {
        eventNotifier.RaiseMembershipChanged(membership);

        ResetProgressForReadmittedMembers(membership);

        // Record self-inclusion BEFORE the rejoin check: during startup restore the
        // self-including roster and the eviction record arrive back-to-back on the coordinator
        // loop, and the flag must be visible when the eviction record's event fires.
        if (membership.Members.Any(m => m.Endpoint == localEndpoint))
            autoRejoinDriver.MarkRosterMember();
        else
            autoRejoinDriver.MaybeStart(membership);
    }

    /// <summary>
    /// Detects members the advancing roster (re)admits — a new endpoint, or a known endpoint whose
    /// <see cref="ClusterMember.JoinedVersion"/> advanced — and posts a replication-progress
    /// reset to every local partition (user partitions and the system partition alike). A leader's
    /// retained progress for a member predates its (re)admission and may describe a log the member
    /// no longer holds — an evicted node typically rejoins with reset state, and a leader that
    /// still "remembers" it as caught-up neither un-quiesces nor backfills it, starving the member
    /// indefinitely. The first observed roster only records the baseline: a node with no baseline
    /// has no retained progress worth resetting (leaders clear per-follower state on election).
    /// </summary>
    private void ResetProgressForReadmittedMembers(ClusterMembership membership)
    {
        bool hasBaseline = _lastSeenJoinedVersions.Count > 0;

        foreach (ClusterMember member in membership.Members)
        {
            string endpoint = member.Endpoint;
            if (string.IsNullOrEmpty(endpoint))
                continue;

            bool known = _lastSeenJoinedVersions.TryGetValue(endpoint, out long seenVersion);
            _lastSeenJoinedVersions[endpoint] = member.JoinedVersion;

            if (endpoint == localEndpoint || !hasBaseline)
                continue;

            bool readmitted = known ? member.JoinedVersion > seenVersion : true;
            if (!readmitted)
                continue;

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "[{LocalEndpoint}] Roster v{Version} (re)admits {Endpoint} (joinedVersion={JoinedVersion}); resetting per-follower replication progress on all local partitions",
                    localEndpoint, membership.MembershipVersion, endpoint, member.JoinedVersion);

            partitionProvider.SystemPartition?.ResetFollowerProgress(endpoint);

            foreach (RaftPartition partition in partitionProvider.DataPartitions)
                partition.ResetFollowerProgress(endpoint);
        }
    }
}
