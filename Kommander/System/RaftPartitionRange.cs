
namespace Kommander.System;

/// <summary>
/// Represents a Raft partition range with a unique identifier, start, and end range values.
/// </summary>
public sealed class RaftPartitionRange
{
    public int PartitionId { get; set; }

    public int StartRange { get; set; }

    public int EndRange { get; set; }

    /// <summary>Monotonically increasing counter; bumped on every map mutation touching this entry.</summary>
    public long Generation { get; set; }

    /// <summary>Lifecycle state of this partition entry.</summary>
    public RaftPartitionState State { get; set; }

    /// <summary>Routing mode; Unrouted entries are excluded from GetPartitionKey hash routing.</summary>
    public RaftRoutingMode RoutingMode { get; set; }
}