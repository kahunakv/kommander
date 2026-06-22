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

    /// <summary>
    /// The subset of user partitions that are currently hot: not quiesced and eligible
    /// for targeted <c>CheckLeader</c> ticks.  Called by
    /// <see cref="RaftTimerService.TriggerCheckLeader"/> on every normal interval when
    /// <see cref="RaftConfiguration.EnableSharedExecutorPool"/> is on, replacing the
    /// O(M) full sweep with an O(active) scan.
    ///
    /// <para>The default implementation falls back to <see cref="GetUserPartitions()"/> so
    /// existing <see cref="IRaftTimerHost"/> stubs are not broken when they don't override
    /// this method.</para>
    /// </summary>
    IEnumerable<RaftPartition> GetHotUserPartitions() => GetUserPartitions();

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
