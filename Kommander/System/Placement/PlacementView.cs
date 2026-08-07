
namespace Kommander.System.Placement;

/// <summary>
/// Immutable input to <see cref="PlacementPlanner"/>: a point-in-time snapshot of every
/// placeable range, the candidate host nodes, and the planning knobs. Built by the P0
/// placement controller from the committed map, the roster, and SWIM liveness; the planner
/// itself is pure so it can be unit-tested deterministically.
/// </summary>
public sealed class PlacementView
{
    /// <summary>Ranges eligible for placement decisions (Active, non-legacy, RF &gt; 0).</summary>
    public required IReadOnlyList<RangePlacement> Ranges { get; init; }

    /// <summary>
    /// Candidate host nodes: committed roster voters. Nodes SWIM considers dead are still
    /// listed (their replicas must be counted as lost) but flagged not alive so they are never
    /// chosen as placement targets.
    /// </summary>
    public required IReadOnlyList<CandidateNode> Nodes { get; init; }

    /// <summary>Replica-count imbalance tolerated above the even-spread ceiling before balancing moves are emitted.</summary>
    public int ReplicaCountDeadband { get; init; } = 1;

    /// <summary>Maximum number of moves emitted in one plan.</summary>
    public int MaxMoves { get; init; } = 2;

    /// <summary>
    /// Remaining transitional-replica slots (max concurrent transfers minus ranges already
    /// mid-move). Each emitted move consumes one; 0 yields an empty plan.
    /// </summary>
    public int TransferBudget { get; init; } = 1;
}

/// <summary>One range's current placement as seen by the planner.</summary>
public sealed class RangePlacement
{
    public required int PartitionId { get; init; }

    /// <summary>Effective replication factor for this range (per-range override or global).</summary>
    public required int ReplicationFactor { get; init; }

    /// <summary>Endpoints of the range's Voter replicas.</summary>
    public required IReadOnlyList<string> VoterEndpoints { get; init; }

    /// <summary>Endpoints of the range's Learner replicas (in-flight adds).</summary>
    public IReadOnlyList<string> LearnerEndpoints { get; init; } = [];

    /// <summary>
    /// True when the range has a transitional replica (Learner or Removing). The planner never
    /// emits a move for such a range — single mover per range.
    /// </summary>
    public bool HasTransitionalReplica { get; init; }

    /// <summary>
    /// Endpoint currently leading the range, when known. Used only as a tiebreak: trims prefer
    /// a non-leader victim so the controller rarely needs a leadership transfer first.
    /// </summary>
    public string? LeaderEndpoint { get; init; }
}

/// <summary>One candidate host node as seen by the planner.</summary>
public sealed class CandidateNode
{
    public required string Endpoint { get; init; }

    public int NodeId { get; init; }

    /// <summary>False when SWIM reports the node Suspect/Dead; never a placement target then.</summary>
    public bool Alive { get; init; } = true;

    /// <summary>Optional locality hint; the planner spreads a range's replicas across distinct zones when possible.</summary>
    public string? Zone { get; init; }
}

/// <summary>The kind of committed mutation a planned move maps to.</summary>
public enum PlacementMoveKind
{
    /// <summary>Add the endpoint as a Learner replica of the range (<c>AddReplica</c>).</summary>
    AddReplica,

    /// <summary>Remove the endpoint's replica of the range (<c>RemoveReplica</c>, two-step).</summary>
    RemoveReplica
}

/// <summary>One planned placement mutation.</summary>
public sealed record PlacementMove(int PartitionId, PlacementMoveKind Kind, string Endpoint);
