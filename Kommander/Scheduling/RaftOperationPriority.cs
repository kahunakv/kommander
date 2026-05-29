
namespace Kommander.Scheduling;

/// <summary>
/// Numeric weight assigned to each <see cref="RaftOperationKind"/> for weighted-fair
/// drain scheduling inside a <c>RaftPartitionExecutor</c>.
///
/// Higher weight means more operations are drained per scheduling quantum.  The ratios
/// follow the plan: Control 8, Replication 4, Client 2, Maintenance 1.
/// </summary>
public enum RaftOperationPriority
{
    /// <summary>Control-plane operations (weight 8).</summary>
    Control = 8,

    /// <summary>Replication-plane operations (weight 4).</summary>
    Replication = 4,

    /// <summary>Client-originated operations (weight 2).</summary>
    Client = 2,

    /// <summary>Background maintenance operations (weight 1).</summary>
    Maintenance = 1,
}
