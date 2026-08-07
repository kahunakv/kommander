
namespace Kommander.System;

/// <summary>
/// One replica of a partition range in the committed partition map: the node that hosts it
/// and its role in the range's consensus group. Mutated only through committed
/// <c>AddReplica</c> / <c>PromoteReplica</c> / <c>RemoveReplica</c> map changes on P0.
/// </summary>
public sealed class RaftReplica
{
    /// <summary>Node endpoint (host:port) — the <see cref="RaftNode"/> keying identity.</summary>
    public string Endpoint { get; set; } = "";

    /// <summary>Node id of the hosting node; provisional (0) when only the endpoint is known.</summary>
    public int NodeId { get; set; }

    /// <summary>Role of this replica in the range's consensus group.</summary>
    public RaftReplicaRole Role { get; set; }

    /// <summary>
    /// The range's <see cref="RaftPartitionRange.Generation"/> at which this replica entered the
    /// set (or last changed role). Serves as the idempotent re-drive point for controllers after
    /// a P0 leader change — the same fence token that guards split/merge cutovers.
    /// </summary>
    public long SinceGeneration { get; set; }
}
