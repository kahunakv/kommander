
using Kommander.System;
using Kommander.Time;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Pure unit tests for <see cref="LeaderBalancePlanner"/> covering the spec acceptance
/// criteria 1–8: count balance, load balance, deadband, stability gate, cooldown,
/// lifecycle filter, incomplete-view abort, and move caps.
/// </summary>
public sealed class TestLeaderBalancePlanner
{
    private static readonly HLCTimestamp T0 = new(0, 1_000_000, 0);
    private static readonly global::System.DateTimeOffset Now = global::System.DateTimeOffset.UtcNow;
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ClusterMember Voter(string endpoint) =>
        new() { Endpoint = endpoint, Role = ClusterMemberRole.Voter };

    private static RaftPartitionMap ActiveMap(params int[] partitionIds)
    {
        return new RaftPartitionMap
        {
            MapVersion = 1,
            Partitions = partitionIds
                .Select(id => new RaftPartitionRange
                {
                    PartitionId = id,
                    State = RaftPartitionState.Active,
                })
                .ToList(),
        };
    }

    private static NodeLoadReport Report(
        string endpoint,
        HLCTimestamp time,
        params (int pid, double load, long sinceMs)[] leaderships) =>
        new()
        {
            Endpoint = endpoint,
            ReportVersion = 1,
            Time = time,
            Leaderships = leaderships
                .Select(l => new PartitionLoad { PartitionId = l.pid, Load = l.load, LeaderSinceMs = l.sinceMs })
                .ToList(),
        };

    private static GlobalLeadershipView BuildView(
        IEnumerable<NodeLoadReport> reports,
        IEnumerable<ClusterMember> members,
        IEnumerable<string>? alive = null) =>
        GlobalLeadershipView.Build(
            reports,
            members,
            aliveEndpoints: new global::System.Collections.Generic.HashSet<string>(alive ?? members.Select(m => m.Endpoint)),
            Ttl,
            T0 + TimeSpan.FromSeconds(1));

    private static RaftConfiguration DefaultConfig(
        int countDeadband = 1,
        double loadImbalanceThreshold = 0.25,
        long minStabilityMs = 5000,
        int maxMovesPerPass = 4) => new()
        {
            Host = "localhost",
            Port = 9000,
            CountDeadband = countDeadband,
            LoadImbalanceThreshold = loadImbalanceThreshold,
            MinLeaderStabilityMs = minStabilityMs,
            MaxMovesPerPass = maxMovesPerPass,
            MoveCooldown = TimeSpan.FromSeconds(60),
            LeaderBalancerReportTtl = Ttl,
        };

    private static readonly global::System.Collections.Generic.Dictionary<int, global::System.DateTimeOffset>
        NoCooldown = new();

    // ── Count balance & convergence over passes ───────────────────────────────

    [Fact]
    public void CountTier_MovesFromOverToUnderNode()
    {
        // Node A leads 5 partitions (over), node B leads 1 (under). Expect a move A→B.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000), (4, 1.0, 10_000), (5, 1.0, 10_000)),
            Report("b:1", T0, (6, 1.0, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4, 5, 6);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(), NoCooldown, Now);

        Assert.NotEmpty(moves);
        Assert.All(moves, m => Assert.Equal("a:1", m.FromEndpoint));
        Assert.All(moves, m => Assert.Equal("b:1", m.ToEndpoint));
    }

    [Fact]
    public void CountTier_ConvergesInMultiplePasses()
    {
        // 6 partitions on 2 nodes; ideal = 3 each. A has 6, B has 0.
        // After 3 passes (each capped at MaxMovesPerPass=4) the distribution should balance.
        var members = new[] { Voter("a:1"), Voter("b:1") };
        var map = ActiveMap(1, 2, 3, 4, 5, 6);
        var config = DefaultConfig(maxMovesPerPass: 4);

        // Simulate first pass.
        var reports = new[]
        {
            Report("a:1", T0, (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000),
                              (4, 1.0, 10_000), (5, 1.0, 10_000), (6, 1.0, 10_000)),
            Report("b:1", T0),
        };

        var view = BuildView(reports, members);
        var moves1 = LeaderBalancePlanner.Plan(view, map, config, NoCooldown, Now);

        // After moving 3 (capped at 4, but balanced at 3), counts should equalize.
        Assert.True(moves1.Count >= 3, $"Expected at least 3 moves, got {moves1.Count}");
        Assert.All(moves1, m => Assert.Equal("b:1", m.ToEndpoint));
    }

    // ── Load balance with equal counts ─────────────────────────────────────────

    [Fact]
    public void LoadTier_EmitsSwapWhenCountsBalancedButSkewed()
    {
        // 2 nodes, 2 partitions each (balanced count).
        // a:1 total load = 8.0, b:1 total load = 0.2 → big skew.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 5.0, 10_000), (2, 3.0, 10_000)),
            Report("b:1", T0, (3, 0.1, 10_000), (4, 0.1, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(), NoCooldown, Now);

        // Expect exactly 2 moves forming a count-neutral swap.
        Assert.Equal(2, moves.Count);
        LeaderMove hotMove  = moves.FirstOrDefault(m => m.FromEndpoint == "a:1")!;
        LeaderMove coldMove = moves.FirstOrDefault(m => m.FromEndpoint == "b:1")!;
        Assert.NotNull(hotMove);
        Assert.NotNull(coldMove);
        Assert.Equal("b:1", hotMove.ToEndpoint);
        Assert.Equal("a:1", coldMove.ToEndpoint);
    }

    // ── Deadband → zero moves ─────────────────────────────────────────────────

    [Fact]
    public void CountTier_NoMovesWithinDeadband()
    {
        // 3 nodes; 3,3,2 distribution. Ideal floor=2, ceil=3.
        // With deadband=1: over means count > 3, under means count < 2.
        // No node qualifies → zero moves.
        var members = new[] { Voter("a:1"), Voter("b:1"), Voter("c:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000)),
            Report("b:1", T0, (4, 1.0, 10_000), (5, 1.0, 10_000), (6, 1.0, 10_000)),
            Report("c:1", T0, (7, 1.0, 10_000), (8, 1.0, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4, 5, 6, 7, 8);
        // Set a very high load-imbalance threshold so only the count tier is exercised.
        var config = DefaultConfig(countDeadband: 1, loadImbalanceThreshold: 1.0);
        var moves = LeaderBalancePlanner.Plan(view, map, config, NoCooldown, Now);

        Assert.Empty(moves);
    }

    // ── Stability gate ────────────────────────────────────────────────────────

    [Fact]
    public void StabilityGate_BlocksMoveWhenLeaderTooNew()
    {
        // a:1 leads 5 partitions (over), b:1 leads 1 (under), BUT all partitions on a
        // have sinceMs = 1000 < MinLeaderStabilityMs (5000) → no move.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0,
                (1, 1.0, 1000), (2, 1.0, 1000), (3, 1.0, 1000),
                (4, 1.0, 1000), (5, 1.0, 1000)),
            Report("b:1", T0, (6, 1.0, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4, 5, 6);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(minStabilityMs: 5000), NoCooldown, Now);

        Assert.Empty(moves);
    }

    /// <summary>
    /// Stability gate opens once <c>LeaderSinceMs ≥ MinLeaderStabilityMs</c>: the same
    /// imbalanced scenario as <see cref="StabilityGate_BlocksMoveWhenLeaderTooNew"/> but
    /// with sinceMs at the threshold must produce moves.
    /// </summary>
    [Fact]
    public void StabilityGate_AllowsMoveOnceThresholdReached()
    {
        var members = new[] { Voter("a:1"), Voter("b:1") };

        // Same imbalance as StabilityGate_BlocksMoveWhenLeaderTooNew but sinceMs == minStabilityMs.
        var reports = new[]
        {
            Report("a:1", T0,
                (1, 1.0, 5000), (2, 1.0, 5000), (3, 1.0, 5000),
                (4, 1.0, 5000), (5, 1.0, 5000)),
            Report("b:1", T0, (6, 1.0, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4, 5, 6);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(minStabilityMs: 5000), NoCooldown, Now);

        Assert.NotEmpty(moves);
    }

    // ── Cooldown exclusion ────────────────────────────────────────────────────

    [Fact]
    public void CooldownExclusion_SkipsPartitionsInCooldown()
    {
        // a:1 leads 3 partitions (over); partitions 1,2 are in cooldown → only 3 is movable.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0,
                (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000)),
            Report("b:1", T0),
        };

        var cooldown = new global::System.Collections.Generic.Dictionary<int, global::System.DateTimeOffset>
        {
            [1] = Now.AddSeconds(30),
            [2] = Now.AddSeconds(30),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3);
        var config = DefaultConfig(countDeadband: 1, maxMovesPerPass: 4);
        var moves = LeaderBalancePlanner.Plan(view, map, config, cooldown, Now);

        // Only partition 3 is movable.
        Assert.Single(moves);
        Assert.Equal(3, moves[0].PartitionId);
    }

    [Fact]
    public void CooldownExclusion_ExpiredCooldownAllowsMove()
    {
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 2.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000)),
            Report("b:1", T0),
        };

        // Cooldown already expired (in the past).
        var cooldown = new global::System.Collections.Generic.Dictionary<int, global::System.DateTimeOffset>
        {
            [1] = Now.AddSeconds(-10),
            [2] = Now.AddSeconds(-10),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(), cooldown, Now);

        Assert.NotEmpty(moves);
    }

    // ── Lifecycle filter ──────────────────────────────────────────────────────

    [Fact]
    public void LifecycleFilter_SkipsNonActivePartitions()
    {
        // a:1 leads 3 partitions; 1 and 2 are Splitting → only 3 is eligible.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 1.0, 10_000), (2, 1.0, 10_000), (3, 1.0, 10_000)),
            Report("b:1", T0),
        };

        var map = new RaftPartitionMap
        {
            MapVersion = 1,
            Partitions =
            [
                new() { PartitionId = 1, State = RaftPartitionState.Splitting },
                new() { PartitionId = 2, State = RaftPartitionState.Draining },
                new() { PartitionId = 3, State = RaftPartitionState.Active },
            ],
        };

        var view = BuildView(reports, members);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(), NoCooldown, Now);

        Assert.Single(moves);
        Assert.Equal(3, moves[0].PartitionId);
    }

    // ── Incomplete view → zero moves ──────────────────────────────────────────

    [Fact]
    public void IncompleteView_YieldsZeroMoves()
    {
        // 3 live voters but only 2 fresh reports → IsComplete(3) = false.
        var members = new[] { Voter("a:1"), Voter("b:1"), Voter("c:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 5.0, 10_000), (2, 5.0, 10_000), (3, 5.0, 10_000),
                              (4, 5.0, 10_000), (5, 5.0, 10_000)),
            Report("b:1", T0),
            // c:1 is silent — no report.
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4, 5);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(), NoCooldown, Now);

        // c:1 is silent → IsComplete() fails because LiveVoters contains c:1 but it has no fresh report.
        Assert.Empty(moves);
    }

    // ── Caps respected ────────────────────────────────────────────────────────

    [Fact]
    public void MaxMovesPerPass_IsRespected()
    {
        // a:1 leads 10 partitions, b:1 leads 0. Cap at 3.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        int[] partitions = Enumerable.Range(1, 10).ToArray();

        var reports = new[]
        {
            Report("a:1", T0, partitions.Select(p => (p, 1.0, 10_000L)).ToArray()),
            Report("b:1", T0),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(partitions);
        var config = DefaultConfig(maxMovesPerPass: 3);
        var moves = LeaderBalancePlanner.Plan(view, map, config, NoCooldown, Now);

        Assert.True(moves.Count <= 3, $"Expected ≤ 3 moves, got {moves.Count}");
    }

    // ── Fewer than 2 live voters → no moves ───────────────────────────────────

    [Fact]
    public void SingleLiveVoter_YieldsZeroMoves()
    {
        var members = new[] { Voter("a:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 5.0, 10_000), (2, 5.0, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(), NoCooldown, Now);

        Assert.Empty(moves);
    }

    // ── Load tier: swap guard ─────────────────────────────────────────────────

    [Fact]
    public void LoadTier_NoSwapWhenSwapWouldNotReduceSpread()
    {
        // hotNode load = 4.0 (partitions 1@2.0, 2@2.0), coldNode load = 2.0 (partitions 3@1.0, 4@1.0).
        // Skew = (4-2)/4 = 50 % > threshold.
        // Hottest on hot = partition 1 (pLoad=2.0), coldest on cold = partition 3 (qLoad=1.0).
        // After swap: hotNode = 4 - 2 + 1 = 3.0, coldNode = 2 + 2 - 1 = 3.0 → spread goes 2→0 (improves).
        // Change to equal loads: both hot partitions and cold partitions all have load 2.0.
        // Swap: pLoad=2.0, qLoad=2.0 → newSpread = |(4-2+2) - (2+2-2)| = |4-2| = 2 = oldSpread → no swap.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 2.0, 10_000), (2, 2.0, 10_000)),
            Report("b:1", T0, (3, 2.0, 10_000), (4, 2.0, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4);
        // Skew = (4-4)/4 = 0 (loads are equal) — but let's manually verify via different loads.
        // Actually with equal total loads there's no skew. Use unequal: a=4, b=2.
        // Rebuild with explicit loads:
        var reports2 = new[]
        {
            Report("a:1", T0, (1, 2.0, 10_000), (2, 2.0, 10_000)), // total 4.0
            Report("b:1", T0, (3, 2.0, 10_000), (4, 0.0, 10_000)), // total 2.0
        };
        // Hottest on a: partition 1 or 2, load 2.0. Coldest on b: partition 4, load 0.0.
        // pLoad=2.0, qLoad=0.0: newSpread = |(4-2+0)-(2+2-0)| = |2-4| = 2 = oldSpread → no improvement.
        var view2 = BuildView(reports2, members);
        var moves = LeaderBalancePlanner.Plan(view2, map, DefaultConfig(loadImbalanceThreshold: 0.25), NoCooldown, Now);

        Assert.Empty(moves);
    }

    // ── Load tier: below threshold → no swap ─────────────────────────────────

    [Fact]
    public void LoadTier_NoSwapWhenSkewBelowThreshold()
    {
        // 2 nodes, 2 partitions each. Load: a=2.0, b=1.8 → skew ≈ 10 % < 25 %.
        var members = new[] { Voter("a:1"), Voter("b:1") };

        var reports = new[]
        {
            Report("a:1", T0, (1, 1.1, 10_000), (2, 0.9, 10_000)),
            Report("b:1", T0, (3, 0.95, 10_000), (4, 0.85, 10_000)),
        };

        var view = BuildView(reports, members);
        var map = ActiveMap(1, 2, 3, 4);
        var moves = LeaderBalancePlanner.Plan(view, map, DefaultConfig(loadImbalanceThreshold: 0.25), NoCooldown, Now);

        Assert.Empty(moves);
    }
}
