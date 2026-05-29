namespace Kommander.Scheduling;

/// <summary>
/// Provides <see cref="RaftTimerService"/> with the cluster runtime context it
/// needs to post timer-driven events, without coupling the timer service to the
/// full <see cref="RaftManager"/> type.
/// </summary>
public interface IRaftTimerHost
{
    /// <summary>Whether the node has joined the Raft cluster.</summary>
    bool Joined { get; }

    /// <summary>The system partition, or <c>null</c> when not yet initialized.</summary>
    RaftPartition? SystemPartition { get; }

    /// <summary>All user partitions currently known to the runtime.</summary>
    IEnumerable<RaftPartition> GetUserPartitions();

    /// <summary>Refreshes the cluster node registry via discovery.</summary>
    Task UpdateNodes();
}
