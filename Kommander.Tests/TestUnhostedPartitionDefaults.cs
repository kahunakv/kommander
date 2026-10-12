using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// What a <see cref="RaftManager"/> owes callers before it hosts anything: every event must be
/// subscribable and unsubscribable, an application callback fired directly must reach its
/// subscriber, identity queries must answer the configuration, RequireTls must refuse a cleartext
/// HTTP scheme at construction, and every per-partition query for a partition the node does not
/// host must answer its documented default instead of throwing.
/// </summary>
public class TestUnhostedPartitionDefaults
{
    [Fact]
    public void RequireTlsWithPlainHttpScheme_IsRefusedAtConstruction()
    {
        RaftConfiguration configuration = BaseConfiguration();
        configuration.HttpScheme = "http://";
        configuration.TransportSecurity = new() { RequireTls = true };

        RaftException exception = Assert.Throws<RaftException>(() => BuildManager(configuration));

        Assert.Contains("RequireTls", exception.Message);
        Assert.Contains("HttpScheme", exception.Message);
    }

    /// <summary>
    /// Subscribing and unsubscribing must work for every event, and the commit-observation gate
    /// must track its own subscriber: the commit path builds and forwards no observation list
    /// while nobody listens.
    /// </summary>
    [Fact]
    public void Events_SubscribeAndUnsubscribe_CommitObservationGateTracksItsSubscriber()
    {
        using RaftManager manager = BuildManager(BaseConfiguration());

        Assert.False(manager.CommitAckObservationEnabled);

        manager.OnRestoreStarted += NoopPartition;
        manager.OnRestoreFinished += NoopPartition;
        manager.OnSystemRestoreFinished += NoopPartition;
        manager.OnReplicationError += NoopReplicationError;
        manager.OnLogRestored += NoopLogRestored;
        manager.OnSystemLogRestored += NoopLogRestored;
        manager.OnReplicationReceived += NoopLogRestored;
        manager.OnSystemReplicationReceived += NoopLogRestored;
        manager.OnLeaderChanged += NoopLeaderChanged;
        manager.OnLeadershipLost += NoopLeadershipLost;
        manager.OnPartitionMapChanged += NoopPartitionMap;
        manager.OnMembershipChanged += NoopMembership;
        manager.OnCommitAcksObserved += NoopCommitAcks;

        Assert.True(manager.CommitAckObservationEnabled);

        manager.OnRestoreStarted -= NoopPartition;
        manager.OnRestoreFinished -= NoopPartition;
        manager.OnSystemRestoreFinished -= NoopPartition;
        manager.OnReplicationError -= NoopReplicationError;
        manager.OnLogRestored -= NoopLogRestored;
        manager.OnSystemLogRestored -= NoopLogRestored;
        manager.OnReplicationReceived -= NoopLogRestored;
        manager.OnSystemReplicationReceived -= NoopLogRestored;
        manager.OnLeaderChanged -= NoopLeaderChanged;
        manager.OnLeadershipLost -= NoopLeadershipLost;
        manager.OnPartitionMapChanged -= NoopPartitionMap;
        manager.OnMembershipChanged -= NoopMembership;
        manager.OnCommitAcksObserved -= NoopCommitAcks;

        Assert.False(manager.CommitAckObservationEnabled);
    }

    /// <summary>
    /// A replication error fired directly at the manager reaches the application subscriber with
    /// the partition and entry intact — the acknowledgement path applications rely on.
    /// </summary>
    [Fact]
    public void ReplicationErrorFiredDirectly_ReachesTheSubscriber()
    {
        using RaftManager manager = BuildManager(BaseConfiguration());

        List<(int Partition, long LogId)> seen = [];
        manager.OnReplicationError += (partition, log) => seen.Add((partition, log.Id));

        RaftLog log = new() { Id = 42, Term = 3 };
        manager.InvokeReplicationError(7, log);

        (int partition, long logId) = Assert.Single(seen);
        Assert.Equal(7, partition);
        Assert.Equal(42, logId);
    }

    /// <summary>
    /// Identity and node-activity queries answer from configuration and local state, with no
    /// activity recorded for an endpoint never heard from.
    /// </summary>
    [Fact]
    public void IdentityQueries_AnswerFromConfiguration()
    {
        using RaftManager manager = BuildManager(BaseConfiguration());

        Assert.Equal("localhost:8452", manager.GetLocalEndpoint());
        Assert.Equal(4242, manager.GetLocalNodeId());
        Assert.Equal("identity-node", manager.GetLocalNodeName());
        Assert.Equal(HLCTimestamp.Zero, manager.GetLastNodeActivity("never-seen.invalid:9999", partitionId: 0));
    }

    /// <summary>
    /// Per-partition queries for a partition this node does not host return their documented
    /// defaults: -1 for the commit index and the stale-propose counter (distinct from a real 0),
    /// empty status lists, and no-op registration handles that are safe to dispose — never an
    /// exception, because routing may legitimately ask a non-replica node.
    /// </summary>
    [Fact]
    public void UnhostedPartitionQueries_ReturnDocumentedDefaults_NeverThrow()
    {
        using RaftManager manager = BuildManager(BaseConfiguration());

        Assert.False(manager.HostsPartition(0), "no system partition exists before join");
        Assert.False(manager.HostsPartition(1));

        Assert.Equal(-1, manager.GetCommitIndex(0));
        Assert.Equal(-1, manager.GetCommitIndex(77));
        Assert.Equal(-1, manager.GetStaleProposedSkippedCount(77));
        Assert.Empty(manager.GetSnapshotStatuses(77));
        Assert.Empty(manager.GetBackfillStatuses(77));
        Assert.Empty(manager.GetPartitionMap());

        manager.SetMinRetainIndex(77, 500);

        IDisposable hold = manager.AcquireRetentionHold(77, 500);
        hold.Dispose();
        hold.Dispose();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static void NoopPartition(int partition) { }

    private static void NoopReplicationError(int partition, RaftLog log) { }

    private static Task<bool> NoopLogRestored(int partition, RaftLog log) => Task.FromResult(true);

    private static Task<bool> NoopLeaderChanged(int partition, string leader) => Task.FromResult(true);

    private static Task NoopLeadershipLost(int partition, long term) => Task.CompletedTask;

    private static void NoopPartitionMap(IReadOnlyList<RaftPartitionRange> ranges) { }

    private static void NoopMembership(ClusterMembership membership) { }

    private static void NoopCommitAcks(IReadOnlyList<RaftCommitAckObservation> acks) { }

    private static RaftConfiguration BaseConfiguration() => new()
    {
        NodeName = "identity-node",
        NodeId = 4242,
        Host = "localhost",
        Port = 8452,
        InitialPartitions = 1,
        HeartbeatInterval = TimeSpan.FromMilliseconds(50),
        RecentHeartbeat = TimeSpan.FromMilliseconds(25),
        VotingTimeout = TimeSpan.FromMilliseconds(250),
        CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
        UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
        TimerInitialDelay = TimeSpan.FromMilliseconds(25),
        StartElectionTimeout = 100,
        EndElectionTimeout = 250,
        EnableQuiescence = false,
    };

    private static RaftManager BuildManager(RaftConfiguration configuration) => new(
        configuration,
        new StaticDiscovery([]),
        new InMemoryWAL(NullLogger<IRaft>.Instance),
        // The RequireTls scheme guard is a transport-startup check that only runs for the
        // HTTP-based transports; an in-memory transport never sees a scheme at all.
        new Kommander.Communication.Rest.RestCommunication(),
        new HybridLogicalClock(),
        NullLogger<IRaft>.Instance);
}
