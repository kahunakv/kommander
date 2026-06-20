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

    /// <summary>
    /// Runs one gossip round: contacts up to <c>GossipFanout</c> random peers,
    /// exchanges membership versions, and applies any newer roster to the local cache.
    /// </summary>
    Task GossipAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs one SWIM probe round: picks a random peer, probes it directly and (on
    /// timeout) via indirect relays, then updates the local liveness table.
    /// </summary>
    Task PingAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueues one leader-balancer pass into the system coordinator's channel.
    /// The pass runs inside the coordinator's single-consumer loop so it is
    /// serialized with all other coordinator state mutations.
    /// No-op when <see cref="RaftConfiguration.EnableLeaderBalancer"/> is false
    /// or when the node is not the P0 leader.
    /// </summary>
    void TriggerBalancerPass();
}
