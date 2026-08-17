using Kommander.System.Placement;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Unit tests for the pure <see cref="PlacementPlanner"/>: initial even-spread assignment,
/// repair/trim/balance priorities, single-mover and budget caps, and stability (a balanced
/// view must yield no moves — churn costs more than perfection).
/// </summary>
public sealed class TestPlacementPlanner
{
    private static CandidateNode Node(string endpoint, bool alive = true, string? zone = null) =>
        new() { Endpoint = endpoint, Alive = alive, Zone = zone };

    private static RangePlacement Range(
        int id, int rf, string[] voters, string[]? learners = null, bool transitional = false, string? leader = null) =>
        new()
        {
            PartitionId = id,
            ReplicationFactor = rf,
            VoterEndpoints = voters,
            LearnerEndpoints = learners ?? [],
            HasTransitionalReplica = transitional,
            LeaderEndpoint = leader
        };

    // ── AssignInitial ─────────────────────────────────────────────────────────

    [Fact]
    public void AssignInitial_SixNodesFourRangesRf3_EvenSpread()
    {
        // The spec's canonical example: 6 nodes, 4 ranges, RF=3 — each range gets three distinct
        // replicas, each node hosts exactly two ranges (12 replicas / 6 nodes), no node hosts all.
        List<CandidateNode> nodes = [.. Enumerable.Range(1, 6).Select(i => Node($"n{i}:1"))];

        Dictionary<int, List<CandidateNode>> assignment =
            PlacementPlanner.AssignInitial([1, 2, 3, 4], nodes, 3);

        Assert.Equal(4, assignment.Count);

        Dictionary<string, int> perNode = nodes.ToDictionary(n => n.Endpoint, _ => 0);
        foreach ((int partitionId, List<CandidateNode> replicas) in assignment)
        {
            Assert.Equal(3, replicas.Count);
            Assert.Equal(3, replicas.Select(r => r.Endpoint).Distinct().Count()); // anti-affinity
            foreach (CandidateNode replica in replicas)
                perNode[replica.Endpoint]++;
        }

        Assert.All(perNode.Values, count => Assert.Equal(2, count));
    }

    [Fact]
    public void AssignInitial_SubRfCluster_DegradesToFullReplication()
    {
        // 3 nodes at RF=3: full replication is equivalent and cheaper — empty assignment keeps
        // the legacy empty-Replicas encoding.
        List<CandidateNode> nodes = [Node("a:1"), Node("b:1"), Node("c:1")];

        Assert.Empty(PlacementPlanner.AssignInitial([1, 2], nodes, 3));
        Assert.Empty(PlacementPlanner.AssignInitial([1, 2], [Node("a:1")], 3));
        Assert.Empty(PlacementPlanner.AssignInitial([1, 2], nodes, 0));
    }

    [Fact]
    public void AssignInitial_NoZones_KeepsLegacyRoundRobinExactly()
    {
        // With no zones anywhere the historical cursor round-robin must be preserved
        // bit-for-bit: 5 nodes, RF=3 → p1: n1,n2,n3; p2: n4,n5,n1; p3: n2,n3,n4.
        List<CandidateNode> nodes = [.. Enumerable.Range(1, 5).Select(i => Node($"n{i}:1"))];

        Dictionary<int, List<CandidateNode>> assignment =
            PlacementPlanner.AssignInitial([1, 2, 3], nodes, 3);

        Assert.Equal(["n1:1", "n2:1", "n3:1"], assignment[1].Select(n => n.Endpoint));
        Assert.Equal(["n4:1", "n5:1", "n1:1"], assignment[2].Select(n => n.Endpoint));
        Assert.Equal(["n2:1", "n3:1", "n4:1"], assignment[3].Select(n => n.Endpoint));
    }

    [Fact]
    public void AssignInitial_ZonesKnown_EachRangeSpansDistinctZones()
    {
        // 6 nodes across 3 zones (2 per zone), 4 ranges, RF=3: every range must land on three
        // distinct zones AND the even spread must hold (each node hosts exactly 2 ranges) —
        // zone spread bends to the load balance, and here both are satisfiable.
        List<CandidateNode> nodes =
        [
            Node("n1:1", zone: "z-a"), Node("n2:1", zone: "z-b"), Node("n3:1", zone: "z-c"),
            Node("n4:1", zone: "z-a"), Node("n5:1", zone: "z-b"), Node("n6:1", zone: "z-c"),
        ];

        Dictionary<int, List<CandidateNode>> assignment =
            PlacementPlanner.AssignInitial([1, 2, 3, 4], nodes, 3);

        Assert.Equal(4, assignment.Count);

        Dictionary<string, int> perNode = nodes.ToDictionary(n => n.Endpoint, _ => 0);
        foreach ((int _, List<CandidateNode> replicas) in assignment)
        {
            Assert.Equal(3, replicas.Select(r => r.Endpoint).Distinct().Count()); // anti-affinity
            Assert.Equal(3, replicas.Select(r => r.Zone).Distinct().Count());     // zone spread
            foreach (CandidateNode replica in replicas)
                perNode[replica.Endpoint]++;
        }

        Assert.All(perNode.Values, count => Assert.Equal(2, count));
    }

    [Fact]
    public void AssignInitial_PartialZones_StillAssignsAndStaysEven()
    {
        // Best-effort at bootstrap: some nodes' zones are not yet gossiped. Zoneless nodes
        // still get assigned, anti-affinity holds, and loads stay even (12 replicas / 6 nodes).
        List<CandidateNode> nodes =
        [
            Node("n1:1", zone: "z-a"), Node("n2:1"), Node("n3:1", zone: "z-b"),
            Node("n4:1"), Node("n5:1", zone: "z-a"), Node("n6:1"),
        ];

        Dictionary<int, List<CandidateNode>> assignment =
            PlacementPlanner.AssignInitial([1, 2, 3, 4], nodes, 3);

        Assert.Equal(4, assignment.Count);

        Dictionary<string, int> perNode = nodes.ToDictionary(n => n.Endpoint, _ => 0);
        foreach ((int _, List<CandidateNode> replicas) in assignment)
        {
            Assert.Equal(3, replicas.Select(r => r.Endpoint).Distinct().Count());
            foreach (CandidateNode replica in replicas)
                perNode[replica.Endpoint]++;
        }

        Assert.All(perNode.Values, count => Assert.Equal(2, count));
    }

    [Fact]
    public void AssignInitial_IsDeterministic()
    {
        List<CandidateNode> nodes = [Node("b:1"), Node("a:1"), Node("d:1"), Node("c:1")];

        Dictionary<int, List<CandidateNode>> first = PlacementPlanner.AssignInitial([1, 2, 3], nodes, 3);
        Dictionary<int, List<CandidateNode>> second = PlacementPlanner.AssignInitial([1, 2, 3], [.. nodes.AsEnumerable().Reverse()], 3);

        foreach (int id in first.Keys)
            Assert.Equal(
                first[id].Select(n => n.Endpoint).Order(),
                second[id].Select(n => n.Endpoint).Order());
    }

    // ── Plan: repair ──────────────────────────────────────────────────────────

    [Fact]
    public void Plan_UnderReplicatedRange_AddsOnLeastLoadedNode()
    {
        PlacementView view = new()
        {
            Ranges =
            [
                Range(1, 3, ["a:1", "b:1"]),                   // under-replicated (2 < 3)
                Range(2, 3, ["a:1", "b:1", "c:1"]),
                Range(3, 3, ["a:1", "b:1", "c:1"])
            ],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 2,
            TransferBudget = 2
        };

        List<PlacementMove> moves = PlacementPlanner.Plan(view);

        PlacementMove repair = Assert.Single(moves);
        Assert.Equal(1, repair.PartitionId);
        Assert.Equal(PlacementMoveKind.AddReplica, repair.Kind);
        Assert.Equal("d:1", repair.Endpoint); // least loaded (0 replicas) and not already a replica
    }

    [Fact]
    public void Plan_ReplicaOnDeadNode_TriggersRepairElsewhere()
    {
        PlacementView view = new()
        {
            Ranges = [Range(1, 3, ["a:1", "b:1", "dead:1"])],
            Nodes = [Node("a:1"), Node("b:1"), Node("dead:1", alive: false), Node("c:1")],
            MaxMoves = 2,
            TransferBudget = 2
        };

        List<PlacementMove> moves = PlacementPlanner.Plan(view);

        // Only 2 healthy voters < RF 3: re-replicate onto the live spare. The dead replica is
        // trimmed on a later pass once the add promotes (single mover per range).
        PlacementMove repair = Assert.Single(moves);
        Assert.Equal(PlacementMoveKind.AddReplica, repair.Kind);
        Assert.Equal("c:1", repair.Endpoint);
    }

    [Fact]
    public void Plan_EvictedNodeReplica_ShedOnceRfSatisfied()
    {
        // "gone:1" is no longer in the roster at all, but three healthy voters already satisfy
        // RF — shed the dead-weight replica.
        PlacementView view = new()
        {
            Ranges = [Range(1, 3, ["a:1", "b:1", "c:1", "gone:1"])],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1")],
            MaxMoves = 2,
            TransferBudget = 2
        };

        List<PlacementMove> moves = PlacementPlanner.Plan(view);

        PlacementMove trim = Assert.Single(moves);
        Assert.Equal(PlacementMoveKind.RemoveReplica, trim.Kind);
        Assert.Equal("gone:1", trim.Endpoint);
    }

    // ── Plan: trim ────────────────────────────────────────────────────────────

    [Fact]
    public void Plan_OverReplicatedRange_RemovesMostLoadedNonLeader()
    {
        PlacementView view = new()
        {
            Ranges =
            [
                Range(1, 3, ["a:1", "b:1", "c:1", "d:1"], leader: "d:1"), // 4 voters > RF 3
                Range(2, 3, ["d:1", "b:1", "c:1"]),
                Range(3, 3, ["d:1", "b:1", "c:1"])
            ],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 2,
            TransferBudget = 2
        };

        List<PlacementMove> moves = PlacementPlanner.Plan(view);

        // d:1 is the most loaded (3 ranges) but leads range 1 — the trim prefers a non-leader
        // victim, so the next-loaded of {b,c} goes.
        PlacementMove trim = Assert.Single(moves);
        Assert.Equal(1, trim.PartitionId);
        Assert.Equal(PlacementMoveKind.RemoveReplica, trim.Kind);
        Assert.Equal("b:1", trim.Endpoint);
    }

    // ── Plan: stability and caps ──────────────────────────────────────────────

    [Fact]
    public void Plan_BalancedView_YieldsNoMoves()
    {
        // The even-spread optimum from AssignInitial must be a fixed point of the planner.
        List<CandidateNode> nodes = [.. Enumerable.Range(1, 6).Select(i => Node($"n{i}:1"))];
        Dictionary<int, List<CandidateNode>> assignment = PlacementPlanner.AssignInitial([1, 2, 3, 4], nodes, 3);

        PlacementView view = new()
        {
            Ranges = [.. assignment.Select(kv => Range(kv.Key, 3, [.. kv.Value.Select(n => n.Endpoint)]))],
            Nodes = nodes,
            MaxMoves = 4,
            TransferBudget = 4
        };

        Assert.Empty(PlacementPlanner.Plan(view));
    }

    [Fact]
    public void Plan_TransitionalRange_IsNeverTouched()
    {
        PlacementView view = new()
        {
            Ranges = [Range(1, 3, ["a:1"], learners: ["b:1"], transitional: true)], // under-replicated but mid-move
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 4,
            TransferBudget = 4
        };

        Assert.Empty(PlacementPlanner.Plan(view));
    }

    [Fact]
    public void Plan_ZeroBudgets_YieldNoMoves()
    {
        PlacementView view = new()
        {
            Ranges = [Range(1, 3, ["a:1", "b:1"])],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 4,
            TransferBudget = 0,
            RepairBudget = 0
        };

        Assert.Empty(PlacementPlanner.Plan(view));
    }

    // ── Plan: repair vs balance budgets ───────────────────────────────────────

    [Fact]
    public void Plan_ZeroTransferBudget_RepairStillEmitted()
    {
        // Durability work must not be rate-limited by the cosmetic-balance budget: a repair
        // proceeds even when the transfer budget is exhausted.
        PlacementView view = new()
        {
            Ranges = [Range(1, 3, ["a:1", "b:1"])],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 4,
            TransferBudget = 0,
            RepairBudget = 1
        };

        PlacementMove repair = Assert.Single(PlacementPlanner.Plan(view));
        Assert.Equal(PlacementMoveKind.AddReplica, repair.Kind);
    }

    [Fact]
    public void Plan_RepairBudget_AllowsConcurrentRepairs()
    {
        // The old shared budget of 1 serialized a multi-node drain to ~one relocation per
        // 3-pass cycle. Three under-replicated ranges with RepairBudget=3 must all repair in
        // one plan even though the balance budget is 1.
        PlacementView view = new()
        {
            Ranges =
            [
                Range(1, 3, ["a:1", "b:1"]),
                Range(2, 3, ["a:1", "b:1"]),
                Range(3, 3, ["a:1", "b:1"])
            ],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 4,
            TransferBudget = 1,
            RepairBudget = 3
        };

        List<PlacementMove> moves = PlacementPlanner.Plan(view);

        Assert.Equal(3, moves.Count);
        Assert.All(moves, m => Assert.Equal(PlacementMoveKind.AddReplica, m.Kind));
    }

    [Fact]
    public void Plan_RepairsConsumeTransferBudget_BalanceYieldsDuringRepairWave()
    {
        // Any emitted repair consumes transfer bandwidth, so with TransferBudget=1 a single
        // repair suppresses the cosmetic trim of the over-replicated range in the same plan.
        PlacementView view = new()
        {
            Ranges =
            [
                Range(1, 3, ["a:1", "b:1"]),               // under-replicated → repair
                Range(2, 3, ["a:1", "b:1", "c:1", "d:1"])  // over-replicated → cosmetic trim
            ],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 4,
            TransferBudget = 1,
            RepairBudget = 3
        };

        PlacementMove move = Assert.Single(PlacementPlanner.Plan(view));
        Assert.Equal(1, move.PartitionId);
        Assert.Equal(PlacementMoveKind.AddReplica, move.Kind);
    }

    [Fact]
    public void Plan_EvictedShed_DrawsOnRepairBudget()
    {
        // Shedding a replica stranded on an evicted node completes a repair — it must proceed
        // on the repair budget even with the transfer budget exhausted.
        PlacementView view = new()
        {
            Ranges = [Range(1, 3, ["a:1", "b:1", "c:1", "gone:1"])],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1")],
            MaxMoves = 4,
            TransferBudget = 0,
            RepairBudget = 1
        };

        PlacementMove shed = Assert.Single(PlacementPlanner.Plan(view));
        Assert.Equal(PlacementMoveKind.RemoveReplica, shed.Kind);
        Assert.Equal("gone:1", shed.Endpoint);
    }

    [Fact]
    public void Plan_CosmeticTrim_DrawsOnTransferBudgetOnly()
    {
        // A trim of a cosmetically-excess healthy voter is balance work: it proceeds with the
        // repair budget at 0, and is blocked when the transfer budget is 0.
        PlacementView Trim(int transferBudget) => new()
        {
            Ranges = [Range(1, 3, ["a:1", "b:1", "c:1", "d:1"])],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 4,
            TransferBudget = transferBudget,
            RepairBudget = 0
        };

        PlacementMove trim = Assert.Single(PlacementPlanner.Plan(Trim(1)));
        Assert.Equal(PlacementMoveKind.RemoveReplica, trim.Kind);

        Assert.Empty(PlacementPlanner.Plan(Trim(0)));
    }

    [Fact]
    public void Plan_MovesAreCappedByMaxMoves()
    {
        PlacementView view = new()
        {
            Ranges =
            [
                Range(1, 3, ["a:1", "b:1"]),
                Range(2, 3, ["a:1", "b:1"]),
                Range(3, 3, ["a:1", "b:1"])
            ],
            Nodes = [Node("a:1"), Node("b:1"), Node("c:1"), Node("d:1")],
            MaxMoves = 2,
            TransferBudget = 8
        };

        Assert.Equal(2, PlacementPlanner.Plan(view).Count);
    }

    // ── Plan: zone spread ─────────────────────────────────────────────────────

    [Fact]
    public void Plan_RepairPrefersUncoveredZone()
    {
        PlacementView view = new()
        {
            Ranges = [Range(1, 3, ["a:1", "b:1"])], // both replicas in zone z1
            Nodes =
            [
                Node("a:1", zone: "z1"),
                Node("b:1", zone: "z1"),
                Node("c:1", zone: "z1"),
                Node("d:1", zone: "z2")
            ],
            MaxMoves = 1,
            TransferBudget = 1
        };

        List<PlacementMove> moves = PlacementPlanner.Plan(view);

        PlacementMove repair = Assert.Single(moves);
        Assert.Equal("d:1", repair.Endpoint); // z2 not yet covered beats equally-loaded z1 node
    }
}
