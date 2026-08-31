using Kommander.Data;
using Kommander.Discovery;
using Kommander.Tests.Simulation.Time;
using Kommander.Tests.Simulation.Transport;
using Kommander.Tests.Simulation.WAL;
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

    private SimulationNode(
        int nodeIndex,
        string endpoint,
        RaftManager manager,
        IWAL wal,
        SimulationClusterOptions options,
        VirtualTickSource clock,
        SimulatedTransport transport,
        ILogger<IRaft> logger)
    {
        NodeIndex = nodeIndex;
        Endpoint = endpoint;
        Manager = manager;
        Wal = wal;
        SimulatedWal = wal as SimulatedWAL;
        this.options = options;
        this.clock = clock;
        this.transport = transport;
        this.logger = logger;
        drivenScheduling = options.DrivenScheduling;
    }

    /// <summary>True when this node owns no scheduling threads. Changes how it is torn down.</summary>
    private readonly bool drivenScheduling;

    // The inputs a restart rebuilds from. A restarted node is a new process on the same machine:
    // new manager, same configuration, same disk.
    private readonly SimulationClusterOptions options;
    private readonly VirtualTickSource clock;
    private readonly SimulatedTransport transport;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Unix-millisecond epoch the simulated hybrid logical clock counts from. 2026-01-01, chosen
    /// only because it is far from zero: a hybrid logical clock rejects a non-positive physical
    /// component, and a value near zero would leave no room for the counter arithmetic.
    /// </summary>
    private const long SimulatedEpochMilliseconds = 1_767_225_600_000;

    /// <summary>Zero-based index of this node within the cluster.</summary>
    public int NodeIndex { get; }

    /// <summary>Host and port that peers address this node by.</summary>
    public string Endpoint { get; }

    /// <summary>
    /// The real Raft node under test. Replaced by <see cref="BeginRestartAsync"/>, because a
    /// restarted node is a new process: the old manager's in-memory state is exactly what a restart
    /// is supposed to lose.
    /// </summary>
    public RaftManager Manager { get; private set; }

    /// <summary>This node's write-ahead log.</summary>
    public IWAL Wal { get; }

    /// <summary>
    /// The same store typed as the simulated one, or null when the scenario asked for a plain
    /// in-memory log. This is the handle a scenario injects a storage fault through.
    /// </summary>
    public SimulatedWAL? SimulatedWal { get; }

    /// <summary>Lifecycle state as the harness last set it.</summary>
    public SimulationNodeLifecycleStatus LifecycleStatus { get; private set; } =
        SimulationNodeLifecycleStatus.Stopped;

    /// <summary>
    /// A paused node counts as live: its process is frozen at the wire, but the object is intact
    /// and reading it tells the harness what a stopped node still believes, which is often the
    /// point of the scenario. A crashed or stopped node does not: its executors are shut down and
    /// any call into one throws rather than answering.
    /// </summary>
    /// <summary>
    /// How many times this node has crashed. A monotonic counter rather than a flag, so an observer
    /// can tell that a crash happened even if it never saw the node in the crashed state.
    /// </summary>
    public int CrashCount { get; private set; }

    /// <summary>True while this node has a <see cref="RaftManager"/> that can be asked anything.</summary>
    public bool HasLiveManager =>
        LifecycleStatus is SimulationNodeLifecycleStatus.Running or SimulationNodeLifecycleStatus.Paused;

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
        string endpoint = $"localhost:{options.BasePort + nodeIndex}";

        // The simulated store reads the cluster clock, not the process clock: a durability window
        // measured in real time would make a crash land somewhere different on every run, which is
        // the one thing the whole harness exists to prevent.
        IWAL wal = options.UseSimulatedWal
            ? new SimulatedWAL(logger, () => clock.LogicalMilliseconds)
              { WriteLatencyMilliseconds = options.WalWriteLatencyMilliseconds }
            : new InMemoryWAL(logger);

        RaftManager manager = BuildManager(nodeIndex, options, clock, transport, logger, wal);

        return new SimulationNode(nodeIndex, endpoint, manager, wal, options, clock, transport, logger);
    }

    /// <summary>
    /// Builds a manager over an existing store. Separated from <see cref="Create"/> so a restart
    /// rebuilds the node without rebuilding its disk: the store is precisely what must survive.
    /// </summary>
    private static RaftManager BuildManager(
        int nodeIndex,
        SimulationClusterOptions options,
        VirtualTickSource clock,
        SimulatedTransport transport,
        ILogger<IRaft> logger,
        IWAL wal)
    {
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
            Port = options.BasePort + nodeIndex,
            InitialPartitions = options.PartitionCount,

            // The three determinism seams. TickSource makes every elapsed-time gate a function of
            // simulated time. EnableInternalTimers hands the tick itself to the harness.
            // EnableInternalSchedulingThreads, when the scenario asks for driven scheduling,
            // removes the node's own threads so its executors, write-ahead log and outbound
            // transport advance only when the harness advances them — which is what makes two runs
            // of one seed reach the same state at every step.
            TickSource = clock,
            EnableInternalTimers = false,
            EnableInternalSchedulingThreads = !options.DrivenScheduling,
            EnableSharedExecutorPool = true,

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

        return new RaftManager(
            configuration,
            new StaticDiscovery(peers),
            wal,
            transport,
            // In driven mode the hybrid logical clock reads simulated time too. It is the last
            // thing in a node that would otherwise read the wall clock on its own, and its value
            // reaches log entries and freshness gates, so leaving it real would keep a run
            // irreproducible however carefully the rest was driven. The epoch is arbitrary and
            // fixed: only the difference between readings matters.
            options.DrivenScheduling
                ? new HybridLogicalClock(() => SimulatedEpochMilliseconds + clock.LogicalMilliseconds)
                : new HybridLogicalClock(),
            logger);
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
    /// Starts the membership refresh and returns its task without awaiting it.
    ///
    /// <para>Used when the harness drives the node's schedulers. <c>UpdateNodes</c> awaits
    /// partition operations of its own — learner promotion and dead-member eviction both ask the
    /// system partition — and those complete only when the driver runs. Awaiting it on the driving
    /// thread would park the driver inside the very call it has not yet driven. Production has the
    /// same shape: the timer fires this and does not wait for it.</para>
    /// </summary>
    public Task BeginTickUpdateNodes() =>
        LifecycleStatus == SimulationNodeLifecycleStatus.Running
            ? Manager.UpdateNodes()
            : Task.CompletedTask;

    /// <summary>
    /// Stops the node the way <c>SIGSTOP</c> stops a process: it consumes no ticks, and the
    /// consensus traffic addressed to it is stored rather than delivered or lost.
    ///
    /// <para><b>Why the wire has to freeze too.</b> An earlier version of this method only stopped
    /// the ticks. Its peers kept getting answered, so nothing about the run looked stopped, and the
    /// backlog that makes a resume interesting never formed. A paused process does not decline
    /// messages, it fails to read them; they wait in its socket and arrive together when it wakes.
    /// Several shipped defects live in exactly that burst.</para>
    ///
    /// <para><b>What still answers.</b> The handshake and the control-plane calls, because their
    /// replies are synchronous and carry real state — holding one fabricates an answer rather than
    /// delaying it. A paused node is therefore silent on the consensus path and still responsive on
    /// the control plane. It is a modelled limitation, not a property of a stopped process.</para>
    /// </summary>
    public void Pause()
    {
        if (LifecycleStatus != SimulationNodeLifecycleStatus.Running)
            return;

        LifecycleStatus = SimulationNodeLifecycleStatus.Paused;
        transport.FreezeEndpoint(Endpoint);
    }

    /// <summary>
    /// Wakes the node after <see cref="Pause"/>. Everything stored while it slept is delivered on
    /// the next step, oldest first.
    /// </summary>
    public void Resume()
    {
        if (LifecycleStatus != SimulationNodeLifecycleStatus.Paused)
            return;

        LifecycleStatus = SimulationNodeLifecycleStatus.Running;
        transport.ThawEndpoint(Endpoint);
    }

    /// <summary>Messages waiting for this node while it is paused or crashed.</summary>
    public int FrozenBacklog => transport.FrozenBacklog(Endpoint);

    /// <summary>
    /// Kills the node the way a power cut kills one: the process is gone, and the disk keeps only
    /// what it had fsynced.
    ///
    /// <para>The manager is disposed outright rather than asked to leave. A graceful leave is the
    /// opposite of a crash — it commits a roster change on the way out, which is exactly the
    /// courtesy a crashed node does not extend.</para>
    ///
    /// <para>The store survives, and that is the point of the whole exercise. Everything inside its
    /// fsync window is lost, so a run that wrote commit markers on the single-fsync fast path comes
    /// back missing them. Traffic addressed to the node is refused rather than stored, because a
    /// dead process keeps nothing — see <see cref="Transport.SimulatedTransport.MarkDown"/>.</para>
    /// </summary>
    public void Crash()
    {
        if (LifecycleStatus == SimulationNodeLifecycleStatus.Crashed)
            return;

        LifecycleStatus = SimulationNodeLifecycleStatus.Crashed;
        CrashCount++;

        // Frozen so nothing in flight can reach a disposed manager, and marked down so the traffic
        // already stored for it dies with the process. A crash is not a deep pause: the socket goes
        // with the process, and a restart must not read messages written to the life before it.
        transport.FreezeEndpoint(Endpoint);
        transport.MarkDown(Endpoint);

        Manager.Dispose();
        SimulatedWal?.Crash();
    }

    /// <summary>
    /// Rebuilds the node's process over the same disk, ready to be started again.
    ///
    /// <para>A restart builds a new <see cref="RaftManager"/>. Reusing the old one would keep the
    /// in-memory frontiers, terms and leader beliefs a restart is supposed to lose, and the
    /// scenario would then prove only that a node which never forgot anything still agrees with
    /// itself. What survives is the store, which is what survives a real restart.</para>
    ///
    /// <para>Rebuilding and starting are separate steps because the new manager has to be in the
    /// routing table before it refreshes its membership, and only the cluster owns that table. The
    /// caller rebuilds, republishes, then calls <see cref="BeginStartAsync"/>.</para>
    /// </summary>
    public void Rebuild()
    {
        if (LifecycleStatus == SimulationNodeLifecycleStatus.Running)
            throw new InvalidOperationException($"{Endpoint} is already running.");

        Manager = BuildManager(NodeIndex, options, clock, transport, logger, Wal);
        transport.ThawEndpoint(Endpoint);
        transport.MarkUp(Endpoint);
    }

    /// <summary>
    /// Reads one partition's consensus state on the partition executor thread, so no mutable
    /// state-machine field is read by the harness thread.
    /// Returns null when the partition is not materialized on this node yet.
    /// </summary>
    public Task<RaftPartitionView?> GetPartitionViewAsync(int partitionId, CancellationToken cancellationToken) =>
        HasLiveManager
            ? Manager.GetPartitionViewAsync(partitionId, cancellationToken)
            : Task.FromResult<RaftPartitionView?>(null);

    /// <summary>
    /// Waits until every partition executor on this node has drained its queues.
    ///
    /// <para>This is the step barrier. The executors run on real threads, so a snapshot taken
    /// while they are busy would capture a half-applied step. Draining first makes each step
    /// boundary a settled state, which is what the invariant checks are written against.</para>
    /// </summary>
    public async Task DrainAsync(CancellationToken cancellationToken)
    {
        if (!HasLiveManager)
            return;

        IPartitionProvider provider = Manager;

        List<Task> barriers = [];
        if (provider.SystemPartition is not null)
            barriers.Add(provider.SystemPartition.DrainAsync(cancellationToken));

        foreach (RaftPartition partition in provider.DataPartitions)
            barriers.Add(partition.DrainAsync(cancellationToken));

        if (barriers.Count > 0)
            await Task.WhenAll(barriers).ConfigureAwait(false);
    }

    /// <summary>
    /// True while this node's system coordinator has a queued or in-flight request. A step that
    /// ended while this is true would leave the partition proposals its messages produce unstarted.
    /// </summary>
    public bool HasPendingCoordinatorWork => HasLiveManager && Manager.SystemCoordinator.HasPendingWork;

    /// <summary>
    /// Set by the cluster before teardown when a graceful leave cannot work. See
    /// <see cref="SimulationCluster.DisposeAsync"/> for the condition.
    /// </summary>
    internal bool SkipGracefulLeave { get; set; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        bool alreadyCrashed = LifecycleStatus == SimulationNodeLifecycleStatus.Crashed;
        LifecycleStatus = SimulationNodeLifecycleStatus.Stopped;

        // A crashed node has no manager left to leave gracefully; its store is all that remains.
        if (alreadyCrashed)
        {
            ReleaseStore();
            return;
        }

        // Faults are cleared before anything else. A graceful leave waits for a roster change to
        // commit, and a node still refusing writes can never commit one, so a fault left set by a
        // scenario turns every shutdown after it into a full timeout.
        SimulatedWal?.ClearFaults();

        // A driven node is disposed outright rather than asked to leave gracefully. A graceful
        // leave waits for roster changes to commit, and in driven mode nobody is left to drive
        // them: every one of its waits runs to its full timeout. Measured at roughly twelve
        // seconds per node, against a hundred milliseconds for the run it was tearing down.
        // Nothing is lost by skipping it — the whole cluster is being discarded, and graceful
        // leave has its own tests.
        if (drivenScheduling || SkipGracefulLeave)
        {
            Manager.Dispose();
            ReleaseStore();
            return;
        }

        try
        {
            await Manager.LeaveCluster(dispose: true, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Teardown is best-effort: a run that already failed must still release its nodes.
            Manager.Dispose();
        }

        ReleaseStore();
    }

    /// <summary>
    /// Ends the store's life. The simulated store ignores an ordinary <c>Dispose</c> so that a crash
    /// does not destroy the disk it is supposed to leave behind, so teardown has to say so
    /// explicitly.
    /// </summary>
    private void ReleaseStore()
    {
        if (SimulatedWal is not null)
            SimulatedWal.DisposeStore();
        else
            Wal.Dispose();
    }
}
