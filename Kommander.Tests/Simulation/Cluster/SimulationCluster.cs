using Kommander.Data;
using Kommander.Tests.Simulation.Time;
using Kommander.Tests.Simulation.Transport;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation.Cluster;

/// <summary>
/// A cluster of real Kommander nodes running under simulated time.
///
/// <para><b>The step model.</b> A run is a sequence of steps. One step advances simulated time,
/// posts the ticks that time makes due, releases the network messages the scenario chose, and
/// then settles. Only after settling is the state stable enough to snapshot and to check
/// invariants against. Nothing in a step waits on the process clock for its timing.</para>
///
/// <para><b>What is still shared with production.</b> Everything that decides anything: the
/// partition state machine, the write-ahead log, the executor, the replication and election
/// collaborators. The harness owns the clock, the timer ticks, and the wire.</para>
///
/// <para><b>Known residual.</b> Executor and write-ahead-log work still runs on real threads
/// inside a step. Step boundaries are therefore deterministic while the interleaving within one
/// step is not. The externally driven schedulers that would close this gap exist in the library
/// and are tested, but a node cannot yet run on a single thread: a partition executor blocks on
/// each operation it dispatches, so an operation that awaits anything outside the executor
/// deadlocks a single-threaded driver. See the determinism-boundary notes in the specification.</para>
/// </summary>
public sealed class SimulationCluster : IAsyncDisposable
{
    private readonly List<SimulationNode> nodes = [];
    private int disposed;

    private SimulationCluster(SimulationClusterOptions options, VirtualTickSource clock, SimulatedTransport transport)
    {
        Options = options;
        Clock = clock;
        Transport = transport;
    }

    /// <summary>Parameters this cluster was built from.</summary>
    public SimulationClusterOptions Options { get; }

    /// <summary>The one clock every node reads. Only the harness advances it.</summary>
    public VirtualTickSource Clock { get; }

    /// <summary>The wire between the nodes.</summary>
    public SimulatedTransport Transport { get; }

    /// <summary>The nodes, in index order.</summary>
    public IReadOnlyList<SimulationNode> Nodes => nodes;

    /// <summary>Number of steps applied so far.</summary>
    public int StepNumber { get; private set; }

    /// <summary>Simulated time at which the next membership refresh is due.</summary>
    private long nextUpdateNodesAtMilliseconds;

    /// <summary>
    /// Builds and joins a cluster.
    ///
    /// <para>Order matters. Every node is constructed first, then the routing table is published,
    /// and only then does any node join. A node that campaigns before its peers are addressable
    /// wins a term with an empty log and the cluster spends the run recovering from it.</para>
    /// </summary>
    public static async Task<SimulationCluster> StartAsync(
        SimulationClusterOptions options,
        ILogger<IRaft> logger,
        CancellationToken cancellationToken)
    {
        VirtualTickSource clock = new();
        SimulatedTransport transport = new();
        SimulationCluster cluster = new(options, clock, transport);

        for (int nodeIndex = 0; nodeIndex < options.NodeCount; nodeIndex++)
            cluster.nodes.Add(SimulationNode.Create(nodeIndex, options, clock, transport, logger));

        Dictionary<string, IRaft> routingTable = cluster.nodes.ToDictionary(
            node => node.Endpoint,
            node => (IRaft)node.Manager);

        transport.SetNodes(routingTable);

        List<Task> joins = [];
        foreach (SimulationNode node in cluster.nodes)
            joins.Add(await node.BeginStartAsync(cancellationToken).ConfigureAwait(false));

        // Joining is a simulated process, not a prelude to one. It completes only after the system
        // partition elects a leader and commits the partition map, and with the internal timers
        // off that election happens only while the harness is stepping. So step the cluster until
        // every join has returned.
        Task allJoined = Task.WhenAll(joins);

        for (int step = 0; step < JoinStepBudget && !allJoined.IsCompleted; step++)
            await cluster.StepAsync(JoinStepMilliseconds, cancellationToken).ConfigureAwait(false);

        if (!allJoined.IsCompleted)
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
            throw new TimeoutException(
                $"Cluster join did not complete within {JoinStepBudget} simulated steps.");
        }

        await allJoined.ConfigureAwait(false);
        cluster.StepNumber = 0;
        return cluster;
    }

    /// <summary>
    /// Steps allowed for the join phase. Generous, because the join covers a system-partition
    /// election plus the partition-map commit, and a scenario that cannot get past it has a real
    /// defect rather than a tight budget.
    /// </summary>
    private const int JoinStepBudget = 400;

    /// <summary>Simulated milliseconds each join step advances.</summary>
    private const long JoinStepMilliseconds = 25;

    /// <summary>
    /// Runs one simulation step: advance time, refresh membership when due, tick every running
    /// node, deliver held messages, then settle.
    ///
    /// <para><paramref name="deliverMessages"/> is false when a scenario wants time to pass while
    /// the network stays silent — the shape that expires an election timeout.</para>
    /// </summary>
    public async Task StepAsync(
        long advanceMilliseconds,
        CancellationToken cancellationToken,
        bool deliverMessages = true)
    {
        if (advanceMilliseconds > 0)
            Clock.AdvanceBy(advanceMilliseconds);

        // The membership refresh runs on its own cadence in production, and it is not optional:
        // the routing table is empty until it has run once, and a partition with no peers can
        // neither campaign nor replicate.
        if (Clock.LogicalMilliseconds >= nextUpdateNodesAtMilliseconds)
        {
            nextUpdateNodesAtMilliseconds = Clock.LogicalMilliseconds + Options.UpdateNodesIntervalMs;

            foreach (SimulationNode node in nodes)
                await node.TickUpdateNodesAsync().ConfigureAwait(false);
        }

        foreach (SimulationNode node in nodes)
            node.TickCheckLeader();

        if (deliverMessages)
            await Transport.DeliverAll().ConfigureAwait(false);

        await SettleAsync(deliverMessages, cancellationToken).ConfigureAwait(false);
        StepNumber++;
    }

    /// <summary>
    /// Rounds a step will spend waiting for the cluster to stop moving before it declares the
    /// state settled. A round that changes nothing ends the settle early, so the bound only
    /// matters for a step that genuinely keeps producing work.
    /// </summary>
    private const int MaxSettleRounds = 60;

    /// <summary>
    /// Waits until one step's work has stopped producing more work.
    ///
    /// <para><b>Why a settle loop and not a single drain.</b> One tick starts a chain: the local
    /// executor sends a vote request, the peer's executor answers it, the answer travels back
    /// through the outbound dispatcher, and the original node counts it. Each hop lands on a
    /// different thread. A single drain barrier covers only the first hop, so a step that ended
    /// there would cut every round trip in half and no election could conclude.</para>
    ///
    /// <para>The loop ends once a round delivers no message, moves no executor, and leaves no
    /// system-coordinator work outstanding. The one-millisecond wait is a scheduling yield that
    /// lets those threads run; it is not scenario timing. No timeout, election, or backoff in the
    /// cluster is measured against it — those all read the simulated clock, which this method
    /// never advances.</para>
    /// </summary>
    private async Task SettleAsync(bool deliverMessages, CancellationToken cancellationToken)
    {
        long previousDelivered = -1;

        for (int round = 0; round < MaxSettleRounds; round++)
        {
            await DrainNodesAsync(cancellationToken).ConfigureAwait(false);

            if (deliverMessages)
                await Transport.DeliverAll().ConfigureAwait(false);

            long delivered = Transport.DeliveredCount;
            bool coordinatorBusy = nodes.Any(node => node.HasPendingCoordinatorWork);

            if (delivered == previousDelivered && !coordinatorBusy)
                return;

            previousDelivered = delivered;
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Waits for every node's partition executors to drain, in node order.</summary>
    private async Task DrainNodesAsync(CancellationToken cancellationToken)
    {
        foreach (SimulationNode node in nodes)
            await node.DrainAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs <paramref name="stepCount"/> steps of <paramref name="advanceMilliseconds"/> each, and
    /// stops early once <paramref name="until"/> holds. Returns true when the condition was met.
    ///
    /// <para>This is the deterministic replacement for a polling wait. The bound is a step count,
    /// not a wall-clock deadline, so the same scenario runs the same number of steps on a fast
    /// machine and a loaded one.</para>
    /// </summary>
    public async Task<bool> RunUntilAsync(
        Func<Task<bool>> until,
        int stepCount,
        long advanceMilliseconds,
        CancellationToken cancellationToken)
    {
        for (int step = 0; step < stepCount; step++)
        {
            await StepAsync(advanceMilliseconds, cancellationToken).ConfigureAwait(false);

            if (await until().ConfigureAwait(false))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Settles the cluster without advancing simulated time and without delivering held messages.
    /// </summary>
    public Task DrainAsync(CancellationToken cancellationToken) =>
        SettleAsync(deliverMessages: false, cancellationToken);

    /// <summary>
    /// Reads one partition's view from every running node. A node that has not materialized the
    /// partition yet contributes nothing, which is the correct reading of "this node holds no
    /// opinion" rather than a fabricated follower entry.
    /// </summary>
    public async Task<IReadOnlyList<RaftPartitionView>> GetPartitionViewsAsync(
        int partitionId,
        CancellationToken cancellationToken)
    {
        List<RaftPartitionView> views = [];

        foreach (SimulationNode node in nodes)
        {
            if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Stopped)
                continue;

            RaftPartitionView? view = await node.GetPartitionViewAsync(partitionId, cancellationToken)
                .ConfigureAwait(false);

            if (view is not null)
                views.Add(view);
        }

        return views;
    }

    /// <summary>
    /// Captures the cluster state for invariant checks and failure reports.
    /// Reads each node through its executor, so no half-written state-machine field is observed.
    /// </summary>
    public async Task<SimulationSnapshot> CaptureSnapshotAsync(int partitionId, CancellationToken cancellationToken)
    {
        List<SimulationNodeSnapshot> nodeSnapshots = [];
        Dictionary<int, SimulationPartitionLeaderSnapshot> leaders = [];

        foreach (SimulationNode node in nodes)
        {
            RaftPartitionView? view = node.LifecycleStatus == SimulationNodeLifecycleStatus.Stopped
                ? null
                : await node.GetPartitionViewAsync(partitionId, cancellationToken).ConfigureAwait(false);

            // The committed prefix is summarized by its frontier, not entry by entry. The
            // invariants compare frontiers and agreement at an index, and materializing every
            // committed entry of every node on every step would dominate the run's cost and grow
            // without bound as a run proceeds.
            List<SimulationWalLogSummary> committed = view is null || view.CommitIndex <= 0
                ? []
                :
                [
                    new SimulationWalLogSummary
                    {
                        PartitionId = partitionId,
                        LogId = view.CommitIndex,
                        Term = view.Term,
                        LogType = "committed-frontier",
                    },
                ];

            nodeSnapshots.Add(new SimulationNodeSnapshot
            {
                NodeId = node.NodeIndex,
                LifecycleStatus = node.LifecycleStatus,
                CurrentTerm = view?.Term ?? 0,
                KnownLeader = view?.Leader,
                CommittedLogs = committed,
                ProposedLogs = [],
                RolledBackLogs = [],
            });

            if (view is not null && view.Role == RaftNodeState.Leader)
            {
                leaders[partitionId] = new SimulationPartitionLeaderSnapshot
                {
                    PartitionId = partitionId,
                    LeaderNodeId = node.NodeIndex,
                    Term = view.Term,
                };
            }
        }

        return new SimulationSnapshot
        {
            LogicalTick = Clock.LogicalMilliseconds,
            StepNumber = StepNumber,
            Nodes = nodeSnapshots,
            PartitionLeaders = leaders,
            PendingNetworkMessages = Transport.GetPendingSnapshots(),
            PendingWalOperations = [],
            PendingTimers = [],
            SchedulerQueueDepths = new Dictionary<string, int>(),
            ClientProposals = [],
        };
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        // Release any message still held, so a node blocked on a reply can finish its teardown.
        Transport.HoldMessages = false;
        await Transport.DeliverAll().ConfigureAwait(false);

        foreach (SimulationNode node in nodes)
            await node.DisposeAsync().ConfigureAwait(false);
    }
}
