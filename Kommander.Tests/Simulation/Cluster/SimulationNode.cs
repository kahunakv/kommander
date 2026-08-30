using Kommander.Data;
using Kommander.Discovery;
using Kommander.Tests.Simulation.Time;
using Kommander.Tests.Simulation.Transport;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation.Cluster;

/// <summary>
/// One node of a simulated cluster. It owns a real <see cref="RaftManager"/> built on the
/// production state machine, the production in-memory WAL, and the production in-memory
/// transport. Only three things are replaced, and each is an input/output edge rather than
/// protocol logic:
///
/// <list type="number">
///   <item>the clock — a <see cref="VirtualTickSource"/> shared by the whole cluster;</item>
///   <item>the timers — <see cref="RaftConfiguration.EnableInternalTimers"/> is off, so the
///     harness posts every check-leader tick itself;</item>
///   <item>the wire — a <see cref="SimulatedTransport"/> that can hold and reorder messages.</item>
/// </list>
///
/// <para><b>Why not a model.</b> Recent Kommander defects lived in the interaction between real
/// guard variables — commit frontiers, compaction floors, inherited tails, promotion fences. A
/// simplified protocol model would re-verify textbook Raft and find none of them. So the
/// simulation runs the real code and fakes only what it must.</para>
/// </summary>
public sealed class SimulationNode : IAsyncDisposable
{
    private int disposed;

    private SimulationNode(int nodeIndex, string endpoint, RaftManager manager, IWAL wal)
    {
        NodeIndex = nodeIndex;
        Endpoint = endpoint;
        Manager = manager;
        Wal = wal;
    }

    /// <summary>Zero-based index of this node within the cluster.</summary>
    public int NodeIndex { get; }

    /// <summary>Host and port that peers address this node by.</summary>
    public string Endpoint { get; }

    /// <summary>The real Raft node under test.</summary>
    public RaftManager Manager { get; }

    /// <summary>This node's write-ahead log.</summary>
    public IWAL Wal { get; }

    /// <summary>Lifecycle state as the harness last set it.</summary>
    public SimulationNodeLifecycleStatus LifecycleStatus { get; private set; } =
        SimulationNodeLifecycleStatus.Stopped;

    /// <summary>
    /// Builds a node. The node is constructed but not yet joined; the cluster joins every node
    /// after the routing table is published, because a node that campaigns before its peers are
    /// addressable elects itself with an empty log.
    /// </summary>
    public static SimulationNode Create(
        int nodeIndex,
        SimulationClusterOptions options,
        VirtualTickSource clock,
        SimulatedTransport transport,
        ILogger<IRaft> logger)
    {
        int port = options.BasePort + nodeIndex;
        string endpoint = $"localhost:{port}";

        List<RaftNode> peers = [];
        for (int peerIndex = 0; peerIndex < options.NodeCount; peerIndex++)
        {
            if (peerIndex == nodeIndex)
                continue;

            peers.Add(new RaftNode($"localhost:{options.BasePort + peerIndex}"));
        }

        RaftConfiguration configuration = new()
        {
            NodeName = $"node{nodeIndex + 1}",
            NodeId = nodeIndex + 1,
            Host = "localhost",
            Port = port,
            InitialPartitions = options.PartitionCount,

            // The determinism seams. TickSource makes every elapsed-time gate a function of
            // simulated time; EnableInternalTimers hands the tick itself to the harness.
            TickSource = clock,
            EnableInternalTimers = false,

            // Election randomness is drawn from the seed, not from the process clock, so two
            // runs of one seed draw the same timeout on the same node.
            ElectionTimeoutSeed = unchecked((int)options.Seed) + nodeIndex,

            StartElectionTimeout = options.StartElectionTimeoutMs,
            EndElectionTimeout = options.EndElectionTimeoutMs,
            HeartbeatInterval = TimeSpan.FromMilliseconds(options.HeartbeatIntervalMs),
            RecentHeartbeat = TimeSpan.FromMilliseconds(options.HeartbeatIntervalMs / 2),
            VotingTimeout = TimeSpan.FromMilliseconds(options.StartElectionTimeoutMs * 2),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(options.HeartbeatIntervalMs / 2),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(options.HeartbeatIntervalMs * 2),
            TimerInitialDelay = TimeSpan.FromMilliseconds(options.HeartbeatIntervalMs / 2),

            // Quiescence parks a partition after an idle window measured on the same tick
            // source. A smoke run holds time still for long stretches, so leave it off until a
            // scenario family exercises it deliberately.
            EnableQuiescence = false,
        };

        options.ConfigureNode?.Invoke(configuration);

        IWAL wal = new InMemoryWAL(logger);

        RaftManager manager = new(
            configuration,
            new StaticDiscovery(peers),
            wal,
            transport,
            new HybridLogicalClock(),
            logger);

        return new SimulationNode(nodeIndex, endpoint, manager, wal);
    }

    /// <summary>
    /// Begins joining the cluster and returns the join task.
    ///
    /// <para>The node is marked running before the join completes, and that is deliberate.
    /// Joining is not instantaneous here: it finishes only once the system partition elects a
    /// leader and commits the partition map, and with the internal timers off that election
    /// needs ticks the harness supplies. A node that only became tickable after its join
    /// returned could never join at all.</para>
    /// </summary>
    public async Task<Task> BeginStartAsync(CancellationToken cancellationToken)
    {
        await Manager.UpdateNodes().ConfigureAwait(false);
        LifecycleStatus = SimulationNodeLifecycleStatus.Running;
        return Manager.JoinCluster(cancellationToken);
    }

    /// <summary>
    /// Posts one check-leader tick into every partition executor on this node. This is what the
    /// wall-clock timer does in production; in a simulation the harness owns the call, so
    /// "a check-leader interval elapsed" becomes an explicit event.
    /// </summary>
    public void TickCheckLeader()
    {
        if (LifecycleStatus != SimulationNodeLifecycleStatus.Running)
            return;

        Manager.TimerService.TriggerCheckLeader();
    }

    /// <summary>
    /// Refreshes the membership view, the way the update-nodes timer does in production.
    ///
    /// <para>The harness calls <see cref="RaftManager.UpdateNodes"/> directly rather than the
    /// timer trigger. The trigger declines while the node has not finished joining, and it runs
    /// fire-and-forget so the step barrier could not wait for it. During a join the refresh is
    /// exactly what is needed: the routing table is empty until it runs, and a partition with no
    /// peers cannot campaign.</para>
    /// </summary>
    public async Task TickUpdateNodesAsync()
    {
        if (LifecycleStatus != SimulationNodeLifecycleStatus.Running)
            return;

        await Manager.UpdateNodes().ConfigureAwait(false);
    }

    /// <summary>
    /// Stops consuming ticks without tearing the node down. Queued messages and timers stay
    /// where they are, so a later <see cref="Resume"/> processes the backlog in one burst —
    /// the shape a real <c>SIGSTOP</c> produces, and the shape that found several defects.
    /// </summary>
    public void Pause() => LifecycleStatus = SimulationNodeLifecycleStatus.Paused;

    /// <summary>Resumes tick consumption after <see cref="Pause"/>.</summary>
    public void Resume()
    {
        if (LifecycleStatus == SimulationNodeLifecycleStatus.Paused)
            LifecycleStatus = SimulationNodeLifecycleStatus.Running;
    }

    /// <summary>
    /// Reads one partition's consensus state on the partition executor thread, so no mutable
    /// state-machine field is read by the harness thread.
    /// Returns null when the partition is not materialized on this node yet.
    /// </summary>
    public Task<RaftPartitionView?> GetPartitionViewAsync(int partitionId, CancellationToken cancellationToken) =>
        Manager.GetPartitionViewAsync(partitionId, cancellationToken);

    /// <summary>
    /// Waits until every partition executor on this node has drained its queues.
    ///
    /// <para>This is the step barrier. The executors still run on real threads, so a snapshot
    /// taken while they are busy would capture a half-applied step. Draining first makes each
    /// step boundary a settled state, which is what the invariant checks are written against.</para>
    /// </summary>
    public async Task DrainAsync(CancellationToken cancellationToken)
    {
        IPartitionProvider provider = Manager;

        List<Task> barriers = [];
        if (provider.SystemPartition is not null)
            barriers.Add(provider.SystemPartition.DrainAsync(cancellationToken));

        foreach (RaftPartition partition in provider.DataPartitions)
            barriers.Add(partition.DrainAsync(cancellationToken));

        if (barriers.Count > 0)
            await Task.WhenAll(barriers).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        LifecycleStatus = SimulationNodeLifecycleStatus.Stopped;

        try
        {
            await Manager.LeaveCluster(dispose: true, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Teardown is best-effort: a run that already failed must still release its nodes.
            Manager.Dispose();
        }
    }
}
