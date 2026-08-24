using Kommander.System;
using Kommander.Time;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Pure unit tests for the degraded-node behaviour of <see cref="LeaderBalancePlanner"/>: the gate
/// that keeps a slow node from receiving leadership in any tier, and the drain tier that evacuates
/// the leaderships it already holds.
///
/// <para>The slow set is passed in directly rather than derived from
/// <see cref="SlowNodeDetector"/>, so these tests fail for planner reasons only.</para>
/// </summary>
public sealed class TestSlowNodeAvoidancePlanner
{
    private static readonly HLCTimestamp T0 = new(0, 1_000_000, 0);
    private static readonly global::System.DateTimeOffset Now = global::System.DateTimeOffset.UtcNow;
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ClusterMember Voter(string endpoint) =>
        new() { Endpoint = endpoint, Role = ClusterMemberRole.Voter };

    private static RaftPartitionMap ActiveMap(params int[] partitionIds) =>
        new()
        {
            MapVersion = 1,
            Partitions = partitionIds
                .Select(id => new RaftPartitionRange { PartitionId = id, State = RaftPartitionState.Active })
                .ToList(),
        };

    private static NodeLoadReport Report(
        string endpoint,
        params (int pid, double load, long sinceMs)[] leaderships) =>
        new()
        {
            Endpoint = endpoint,
            ReportVersion = 1,
            Time = T0,
            Leaderships = leaderships
                .Select(l => new PartitionLoad { PartitionId = l.pid, Load = l.load, LeaderSinceMs = l.sinceMs })
                .ToList(),
        };

    private static GlobalLeadershipView BuildView(IEnumerable<NodeLoadReport> reports)
    {
        List<ClusterMember> members = reports.Select(r => Voter(r.Endpoint)).ToList();

        return GlobalLeadershipView.Build(
            reports,
            members,
            aliveEndpoints: new HashSet<string>(members.Select(m => m.Endpoint)),
            Ttl,
            T0 + TimeSpan.FromSeconds(1));
    }

    private static RaftConfiguration Config(int maxMovesPerPass = 4, long minStabilityMs = 5000) => new()
    {
        Host = "localhost",
        Port = 9000,
        CountDeadband = 1,
        LoadImbalanceThreshold = 0.25,
        MinLeaderStabilityMs = minStabilityMs,
        MaxMovesPerPass = maxMovesPerPass,
        MoveCooldown = TimeSpan.FromSeconds(60),
        LeaderBalancerReportTtl = Ttl,
    };

    private static readonly Dictionary<int, global::System.DateTimeOffset> NoCooldown = new();

    private static readonly string[] HealthyDestinations = ["c:1", "d:1"];

    private static HashSet<string> Slow(params string[] endpoints) => new(endpoints, StringComparer.Ordinal);

    // ── Gate: count tier ──────────────────────────────────────────────────────

    [Fact]
    public void CountTier_NeverTargetsASlowNode()
    {
        // a:1 is over-loaded; b:1 and c:1 are both empty, but b:1 is degraded.
        var reports = new[]
        {
            Report("a:1", (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000),
                          (4, 1.0, 10_000), (5, 1.0, 10_000), (6, 1.0, 10_000)),
            Report("b:1"),
            Report("c:1"),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4, 5, 6), Config(), NoCooldown, Now, Slow("b:1"));

        Assert.NotEmpty(moves);
        Assert.All(moves, m => Assert.NotEqual("b:1", m.ToEndpoint));
        Assert.All(moves, m => Assert.Equal("c:1", m.ToEndpoint));
    }

    [Fact]
    public void CountTier_WithEverySpareNodeSlow_EmitsNothing()
    {
        var reports = new[]
        {
            Report("a:1", (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000),
                          (4, 1.0, 10_000), (5, 1.0, 10_000), (6, 1.0, 10_000)),
            Report("b:1"),
            Report("c:1"),
        };

        // Both destinations are degraded. Standing still beats moving leadership onto a bad disk.
        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4, 5, 6), Config(), NoCooldown, Now, Slow("b:1", "c:1"));

        Assert.Empty(moves);
    }

    // ── Gate: load tier ───────────────────────────────────────────────────────

    [Fact]
    public void LoadTier_NeverTargetsASlowNode()
    {
        // Counts are equal at 2 each, so the count tier stays quiet and the load tier runs.
        // a:1 carries all the load; b:1 is the coolest node but is degraded.
        //
        // b:1's own leaderships are pinned below MinLeaderStabilityMs on purpose: without that the
        // drain tier would legitimately claim this pass and the load tier would never be reached.
        var reports = new[]
        {
            Report("a:1", (1, 10.0, 10_000), (2, 10.0, 10_000)),
            Report("b:1", (3, 1.0, 100), (4, 1.0, 100)),
            Report("c:1", (5, 2.0, 10_000), (6, 2.0, 10_000)),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4, 5, 6), Config(), NoCooldown, Now, Slow("b:1"));

        // A count-neutral swap between the two healthy nodes, with the degraded node untouched.
        Assert.Equal(2, moves.Count);
        Assert.All(moves, m => Assert.NotEqual("b:1", m.ToEndpoint));
        Assert.All(moves, m => Assert.NotEqual("b:1", m.FromEndpoint));
        Assert.All(moves, m => Assert.False(m.IsDrain));
    }

    // ── Drain tier ────────────────────────────────────────────────────────────

    [Fact]
    public void DrainTier_EvacuatesASlowNodesLeaderships()
    {
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000), (2, 3.0, 10_000)),
            Report("b:1", (3, 1.0, 10_000), (4, 1.0, 10_000)),
            Report("c:1", (5, 1.0, 10_000), (6, 1.0, 10_000)),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4, 5, 6), Config(), NoCooldown, Now, Slow("a:1"));

        Assert.Equal(2, moves.Count);
        Assert.All(moves, m => Assert.Equal("a:1", m.FromEndpoint));
        Assert.All(moves, m => Assert.True(m.IsDrain, "a drain move must be tagged as one"));
        Assert.Equal([1, 2], moves.Select(m => m.PartitionId).Order().ToArray());

        // The hottest partition leaves first, so the worst offender is relieved soonest.
        Assert.Equal(1, moves[0].PartitionId);
    }

    [Fact]
    public void DrainTier_SpreadsAcrossHealthyDestinations()
    {
        // Two healthy nodes start equally cool, so successive picks must alternate rather than
        // pile every drained leadership onto whichever node sorted first.
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000), (2, 5.0, 10_000)),
            Report("b:1", (3, 1.0, 10_000)),
            Report("c:1", (4, 1.0, 10_000)),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4), Config(), NoCooldown, Now, Slow("a:1"));

        Assert.Equal(2, moves.Count);
        Assert.Equal(["b:1", "c:1"], moves.Select(m => m.ToEndpoint).Order().ToArray());
    }

    [Fact]
    public void DrainTier_RespectsTheMoveCapAndSpreadsOverPasses()
    {
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000), (2, 4.0, 10_000), (3, 3.0, 10_000),
                          (4, 2.0, 10_000), (5, 1.0, 10_000)),
            Report("b:1", (6, 1.0, 10_000)),
            Report("c:1", (7, 1.0, 10_000)),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4, 5, 6, 7), Config(maxMovesPerPass: 2),
            NoCooldown, Now, Slow("a:1"));

        Assert.Equal(2, moves.Count);
        Assert.All(moves, m => Assert.Equal("a:1", m.FromEndpoint));
    }

    [Fact]
    public void DrainTier_SuppressesTheCountAndLoadTiers()
    {
        // b:1 is badly over-loaded on count, which the count tier would normally act on. The pass
        // belongs to the drain instead: rebalancing counts against a node being emptied is churn.
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000)),
            Report("b:1", (2, 1.0, 10_000), (3, 1.0, 10_000), (4, 1.0, 10_000),
                          (5, 1.0, 10_000), (6, 1.0, 10_000)),
            Report("c:1"),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4, 5, 6), Config(), NoCooldown, Now, Slow("a:1"));

        Assert.Single(moves);
        Assert.True(moves[0].IsDrain);
        Assert.Equal("a:1", moves[0].FromEndpoint);
    }

    [Fact]
    public void DrainTier_HonoursTheStabilityGate()
    {
        // The slow node's leaderships are all too young to move. A degraded disk is a reason to
        // move leadership, not a reason to move it unsafely.
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 100), (2, 5.0, 100)),
            Report("b:1", (3, 1.0, 10_000)),
            Report("c:1", (4, 1.0, 10_000)),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4), Config(), NoCooldown, Now, Slow("a:1"));

        Assert.Empty(moves);
    }

    [Fact]
    public void DrainTier_HonoursCooldown()
    {
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000), (2, 5.0, 10_000)),
            Report("b:1", (3, 1.0, 10_000)),
            Report("c:1", (4, 1.0, 10_000)),
        };

        Dictionary<int, global::System.DateTimeOffset> cooldown = new()
        {
            [1] = Now + TimeSpan.FromSeconds(30),
            [2] = Now + TimeSpan.FromSeconds(30),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4), Config(), cooldown, Now, Slow("a:1"));

        Assert.Empty(moves);
    }

    [Fact]
    public void DrainTier_SkipsNonActivePartitions()
    {
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000), (2, 5.0, 10_000)),
            Report("b:1", (3, 1.0, 10_000)),
            Report("c:1", (4, 1.0, 10_000)),
        };

        // Partition 2 is absent from the map, so it is not Active and must not move.
        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 3, 4), Config(), NoCooldown, Now, Slow("a:1"));

        Assert.Single(moves);
        Assert.Equal(1, moves[0].PartitionId);
    }

    [Fact]
    public void DrainTier_WithEveryNodeSlow_EmitsNothing()
    {
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000)),
            Report("b:1", (2, 1.0, 10_000)),
            Report("c:1", (3, 1.0, 10_000)),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3), Config(), NoCooldown, Now, Slow("a:1", "b:1", "c:1"));

        Assert.Empty(moves);
    }

    [Fact]
    public void DrainTier_DrainsEverySlowNodeDeterministically()
    {
        var reports = new[]
        {
            Report("a:1", (1, 5.0, 10_000)),
            Report("b:1", (2, 5.0, 10_000)),
            Report("c:1", (3, 1.0, 10_000)),
            Report("d:1", (4, 1.0, 10_000)),
        };

        IReadOnlyList<LeaderMove> moves = LeaderBalancePlanner.Plan(
            BuildView(reports), ActiveMap(1, 2, 3, 4), Config(), NoCooldown, Now, Slow("a:1", "b:1"));

        Assert.Equal(2, moves.Count);
        Assert.Equal(["a:1", "b:1"], moves.Select(m => m.FromEndpoint).Order().ToArray());
        Assert.All(moves, m => Assert.Contains(m.ToEndpoint, HealthyDestinations));
    }

    // ── Regression: no slow nodes ─────────────────────────────────────────────

    [Fact]
    public void NoSlowNodes_LeavesTheOrdinaryPlanUnchanged()
    {
        var reports = new[]
        {
            Report("a:1", (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000),
                          (4, 1.0, 10_000), (5, 1.0, 10_000), (6, 1.0, 10_000)),
            Report("b:1"),
            Report("c:1"),
        };

        GlobalLeadershipView view = BuildView(reports);
        RaftPartitionMap map = ActiveMap(1, 2, 3, 4, 5, 6);

        IReadOnlyList<LeaderMove> withoutArgument =
            LeaderBalancePlanner.Plan(view, map, Config(), NoCooldown, Now);

        IReadOnlyList<LeaderMove> withEmptySet =
            LeaderBalancePlanner.Plan(view, map, Config(), NoCooldown, Now, Slow());

        Assert.NotEmpty(withoutArgument);
        Assert.Equal(withoutArgument, withEmptySet);
        Assert.All(withoutArgument, m => Assert.False(m.IsDrain));
    }
}
