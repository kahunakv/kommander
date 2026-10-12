using Kommander;
using Kommander.System;

namespace Kommander.Tests;

/// <summary>
/// Routing answers the committed partition map owes every node — including nodes that host
/// nothing: the placement replicas of a range (distinguishing "no placement" from "placement
/// with no replicas"), the voter split inside a placement, tombstones ignored by endpoint
/// lookups and map snapshots, key routing across the hash space, and the hard refusal when no
/// range can serve a key at all.
/// </summary>
public class TestRoutingTableLookups
{
    private const string Self = "node-a:8001";
    private const string VoterB = "node-b:8002";
    private const string LearnerC = "node-c:8003";
    private const string Departed = "node-d:8004";

    /// <summary>
    /// A range with an assigned replica set answers its placement (self excluded from the peer
    /// list, voters split from learners for quorum); a legacy range with no replica set answers
    /// "no placement", which forwarding reads as "fall through to local diagnosis" rather than
    /// "empty placement with nowhere to forward".
    /// </summary>
    [Fact]
    public void PlacementReplicas_ResolvedFromTheCommittedMap_LegacyRangeAnswersNoPlacement()
    {
        PartitionRoutingTable table = BuildTable(
        [
            NewRange(1, 0, 1_000, replicas:
            [
                NewReplica(Self, RaftReplicaRole.Voter),
                NewReplica(VoterB, RaftReplicaRole.Voter),
                NewReplica(LearnerC, RaftReplicaRole.Learner),
            ]),
            NewRange(2, 1_001, 2_000, replicas: []),
        ]);

        IReadOnlyList<RaftReplica>? placed = table.TryGetPlacementReplicas(1);
        Assert.NotNull(placed);
        Assert.Equal(3, placed.Count);

        Assert.True(table.TryGetPlacementReplicas(2) is null, "a legacy full-replication range has no placement to forward to");
        Assert.True(table.TryGetPlacementReplicas(999) is null, "an unknown range has no placement either");
        Assert.Empty(table.GetPartitionReplicas(2));

        Assert.Equal([VoterB, LearnerC], table.GetPartitionPeers(1).Select(n => n.Endpoint));
        Assert.True(table.IsPartitionVoter(1, VoterB));
        Assert.False(table.IsPartitionVoter(1, LearnerC), "learners receive replication but never count toward quorum");
    }

    /// <summary>
    /// Endpoint membership ignores tombstones: a Removed range's replica set is history, not a
    /// placement, so an endpoint that only a tombstone still names must not read as hosted by
    /// the committed map.
    /// </summary>
    [Fact]
    public void CommittedMapNamesEndpoint_IgnoresTombstonedRanges()
    {
        PartitionRoutingTable table = BuildTable(
        [
            NewRange(1, 0, 1_000, replicas: [NewReplica(VoterB, RaftReplicaRole.Voter)]),
            NewRange(2, 1_001, 2_000, state: RaftPartitionState.Removed,
                replicas: [NewReplica(Departed, RaftReplicaRole.Voter)]),
        ]);

        Assert.True(table.CommittedMapNamesEndpoint(VoterB));
        Assert.False(table.CommittedMapNamesEndpoint(Departed), "the tombstone's replica set is historical");
        Assert.False(table.CommittedMapNamesEndpoint("ghost.nowhere:1"));
        Assert.False(table.IsLiveMappedRange(2));
        Assert.True(table.IsLiveMappedRange(1));
    }

    /// <summary>
    /// The map snapshot handed out is a copy: a caller mutating the returned ranges or their
    /// replica lists must not be able to rewrite the table's committed view, and tombstones are
    /// filtered out of it entirely.
    /// </summary>
    [Fact]
    public void PartitionMapSnapshot_FiltersTombstones_AndCopiesReplicas()
    {
        List<RaftReplica> liveReplicas = [NewReplica(VoterB, RaftReplicaRole.Voter)];
        PartitionRoutingTable table = BuildTable(
        [
            NewRange(1, 0, 1_000, replicas: liveReplicas),
            NewRange(2, 1_001, 2_000, state: RaftPartitionState.Removed, replicas: [NewReplica(Departed, RaftReplicaRole.Voter)]),
        ]);

        IReadOnlyList<RaftPartitionRange> snapshot = table.GetPartitionMap();

        RaftPartitionRange? range = Assert.Single(snapshot);
        Assert.Equal(1, range.PartitionId);
        Assert.Equal(liveReplicas.Count, range.Replicas.Count);

        range.Replicas.Clear();
        // The committed range's replica list is the same instance the test built it from, so
        // clearing the source list here would mutate the committed view itself — only the
        // SNAPSHOT's copy may be cleared; that is the immunity under test.

        // The committed view must be immune to caller mutations of a snapshot.
        IReadOnlyList<RaftPartitionRange> again = table.GetPartitionMap();
        Assert.Single(again);
        Assert.Single(again[0].Replicas);
    }

    /// <summary>
    /// With a range covering the whole hash space, every prefix key resolves to it; with a range
    /// covering a narrow slice, keys whose hash lands outside are refused with the routing
    /// exception rather than silently mis-routed to the nearest range.
    /// </summary>
    [Fact]
    public void PrefixKeyRouting_ResolvesInsideTheCoveredSpace_AndRefusesOutsideIt()
    {
        PartitionRoutingTable full = BuildTable([NewRange(1, 0, int.MaxValue, replicas: [NewReplica(Self, RaftReplicaRole.Voter)])]);
        for (int i = 0; i < 5; i++)
            Assert.Equal(1, full.GetPrefixPartitionKey($"tenant-{i}"));

        PartitionRoutingTable narrow = BuildTable([NewRange(7, 0, 10, replicas: [NewReplica(Self, RaftReplicaRole.Voter)])]);

        bool refusedAny = false;
        for (int i = 0; i < 64 && !refusedAny; i++)
        {
            try
            {
                _ = narrow.GetPrefixPartitionKey($"tenant-{i}");
            }
            catch (RaftException)
            {
                refusedAny = true;
            }
        }

        Assert.True(refusedAny, "a range covering 11 of 2^32 hash values must refuse some keys");

        // The plain hash router has the same contract over the same narrow space.
        bool partitionKeyRefused = false;
        for (int i = 0; i < 64 && !partitionKeyRefused; i++)
        {
            try
            {
                _ = narrow.GetPartitionKey($"some-key-{i}");
            }
            catch (RaftException)
            {
                partitionKeyRefused = true;
            }
        }

        Assert.True(partitionKeyRefused, "the plain key router must refuse keys outside the covered space too");
    }

    /// <summary>
    /// With neither a committed map nor hosted partitions, no key can be routed and both routers
    /// must say so with the routing exception instead of inventing an answer.
    /// </summary>
    [Fact]
    public void WithoutAMapOrHostedPartitions_KeyRoutingRefusesEveryKey()
    {
        PartitionRoutingTable table = BuildTable([]);

        Assert.Throws<RaftException>(() => table.GetPartitionKey("some-key"));
        Assert.Throws<RaftException>(() => table.GetPrefixPartitionKey("some-prefix"));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static RaftReplica NewReplica(string endpoint, RaftReplicaRole role) => new()
    {
        Endpoint = endpoint,
        NodeId = endpoint.GetHashCode(StringComparison.Ordinal),
        Role = role,
        SinceGeneration = 1,
    };

    private static RaftPartitionRange NewRange(
        int partitionId, int start, int end, RaftPartitionState state = RaftPartitionState.Active, List<RaftReplica>? replicas = null) => new()
    {
        PartitionId = partitionId,
        StartRange = start,
        EndRange = end,
        RoutingMode = RaftRoutingMode.HashRange,
        Generation = 1,
        State = state,
        Replicas = replicas ?? [],
    };

    private static PartitionRoutingTable BuildTable(List<RaftPartitionRange> ranges)
    {
        PartitionRoutingTable table = new(
            EmptyPartitionProvider.Instance,
            getNodes: () => [new RaftNode(VoterB)],
            getMembership: () => new ClusterMembership { MembershipVersion = 3 },
            configuration: new RaftConfiguration(),
            localEndpoint: Self);

        table.ApplyCommittedMap(ranges, highestPartitionIdEver: ranges.Count);
        return table;
    }

    /// <summary>
    /// Provider with no hosted partitions: the committed map is the only source these tests
    /// consult, and none of them should ever fall through to the registry.
    /// </summary>
    private sealed class EmptyPartitionProvider : IPartitionProvider
    {
        public static readonly EmptyPartitionProvider Instance = new();

        public RaftPartition? SystemPartition => null;
        public IEnumerable<RaftPartition> DataPartitions => [];
        public int DataPartitionCount => 0;
        public bool TryGetDataPartition(int partitionId, out RaftPartition? partition) { partition = null; return false; }
        public bool TryGetPartition(int partitionId, out RaftPartition? partition) { partition = null; return false; }

        public RaftPartition GetPartition(int partitionId) =>
            throw new NotSupportedException("no partitions are hosted by this provider");

        public bool HostsPartition(int partitionId) => false;
    }
}
