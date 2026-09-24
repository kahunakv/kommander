using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Tests.Simulation.Time;
using Kommander.Tests.Simulation.Transport;
using Kommander.Tests.Simulation.WAL;
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
/// <para><b>Two modes.</b> By default the nodes keep their own scheduling threads, and a step is
/// deterministic only at its boundary. With
/// <see cref="SimulationClusterOptions.DrivenScheduling"/> the nodes own no threads at all and the
/// harness advances their executors, write-ahead logs and outbound transports itself; two runs of
/// one seed then reach the same state at every step.</para>
///
/// <para><b>The rule driven mode imposes.</b> Nothing the driving thread calls may be awaited
/// directly — not a state read, not a client proposal. Both wait on work that only the driving
/// thread can perform, so both go through <see cref="DriveAsync"/>. Each node's drain is likewise
/// started and kept in flight rather than awaited: a drain can be waiting for another node, and a
/// driver that awaited one would be parked inside the very node it must leave.</para>
///
/// <para><b>What is still real.</b> A settle round that moves nothing waits a real millisecond so
/// the in-flight drains can advance. A yield there was measured and is not enough. The state
/// sequence is therefore reproducible while the wall-clock shape of a run is not.</para>
/// </summary>
public sealed class SimulationCluster : IAsyncDisposable
{
    private readonly List<SimulationNode> nodes = [];
    private int disposed;

    /// <summary>
    /// Node-local invariant violations the library reported for this cluster's nodes, oldest
    /// first. Written from executor threads, so every access takes <see cref="violationGate"/>.
    /// </summary>
    private readonly List<RaftInvariantViolation> libraryViolations = [];
    private readonly object violationGate = new();

    /// <summary>Snapshot imports judged unnecessary, oldest first; see <see cref="UnnecessarySnapshotImports"/>.</summary>
    private readonly List<UnnecessarySnapshotImport> unnecessarySnapshotImports = [];
    private readonly Action<RaftInvariantViolation> onLibraryViolation;

    private SimulationCluster(SimulationClusterOptions options, VirtualTickSource clock, SimulatedTransport transport)
    {
        Options = options;
        Clock = clock;
        Transport = transport;
        onLibraryViolation = RecordLibraryViolation;
    }

    /// <summary>
    /// Every violation the library's own invariant checks reported on one of this cluster's nodes,
    /// oldest first. Empty on a healthy run.
    ///
    /// <para><b>Why the harness listens to these at all.</b> The library asserts the rules a node
    /// can check on its own — its term never regresses, its frontiers never move backwards — at the
    /// place each value is written, and reacts with a log line and, in a debug build, an exception
    /// the partition executor catches. Neither reaches a run's verdict. FINDING 5 was such a
    /// violation: a restarting node's term regressed inside its restore, the executor logged it and
    /// carried on, and the run failed three hundred steps later with a message about a missing
    /// leader. Recording the reports here lets the invariant runner fail the run on the rule that
    /// actually broke, at the step it broke.</para>
    ///
    /// <para>Filtered on endpoint, because <see cref="RaftInvariants.Violated"/> is process-wide and
    /// a unit test in another collection may violate a rule on purpose while this cluster runs.</para>
    /// </summary>
    public IReadOnlyList<RaftInvariantViolation> LibraryInvariantViolations
    {
        get
        {
            lock (violationGate)
                return [.. libraryViolations];
        }
    }

    private void RecordLibraryViolation(RaftInvariantViolation violation)
    {
        if (violation.LocalEndpoint is null)
            return;

        bool ours = false;
        foreach (SimulationNode node in nodes)
        {
            if (string.Equals(node.Endpoint, violation.LocalEndpoint, StringComparison.Ordinal))
            {
                ours = true;
                break;
            }
        }

        if (!ours)
            return;

        lock (violationGate)
            libraryViolations.Add(violation);
    }

    /// <summary>
    /// Every snapshot install a node accepted while its own log could still have been repaired by
    /// backfill, oldest first. Empty on a healthy run.
    ///
    /// <para><b>Why the harness judges this.</b> A snapshot is the repair for a follower that sits
    /// below what the leader's log can serve. Nothing in the protocol's own checks says anything
    /// about a snapshot sent to a follower that was NOT below the floor: every install succeeds,
    /// every frontier converges, and the run reports a healthy cluster. The Caraxes bank-leader-kill
    /// restart was exactly that — a restarted node whose log covered the leader's floor by millions
    /// of entries reported a frontier of 0 before its restore had run, and was re-seeded by a
    /// 38-second snapshot it did not need. Judged at import time, before the install changes the
    /// node's log, from the node's own contiguous resolved prefix against the most compacted log
    /// among the other running nodes: if even that log still begins at or below the node's
    /// prefix + 1, a backfill anchored there would have been served.</para>
    /// </summary>
    public IReadOnlyList<UnnecessarySnapshotImport> UnnecessarySnapshotImports
    {
        get
        {
            lock (violationGate)
                return [.. unnecessarySnapshotImports];
        }
    }

    private void RecordSnapshotImport(SimulationNode importer, int partitionId)
    {
        SimulatedWalPartitionSnapshot? own = importer.SimulatedWal?.Snapshot().Partition(partitionId);
        if (own is null)
            return;

        // The node's contiguous RESOLVED prefix above whatever it is no longer required to hold. A
        // Proposed tail above it is not evidence of position: it may be a deposed leader's orphan
        // that backfill overwrites, so the anchor a leader would use sits at the resolved prefix.
        long heldThrough = own.CoveredThrough;
        foreach (RaftLog log in importer.Wal.ReadLogsRange(partitionId, heldThrough + 1))
        {
            if (log.Id != heldThrough + 1)
                break;
            if (log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                break;
            heldThrough = log.Id;
        }

        long firstRetainedElsewhere = -1;
        string retainedBy = "";
        foreach (SimulationNode other in nodes)
        {
            if (other == importer || other.LifecycleStatus != SimulationNodeLifecycleStatus.Running)
                continue;

            SimulatedWalPartitionSnapshot? store = other.SimulatedWal?.Snapshot().Partition(partitionId);
            if (store is null || store.FirstLogId <= 0)
                continue;

            if (store.FirstLogId > firstRetainedElsewhere)
            {
                firstRetainedElsewhere = store.FirstLogId;
                retainedBy = other.Endpoint;
            }
        }

        if (firstRetainedElsewhere < 0 || heldThrough < firstRetainedElsewhere - 1)
            return;

        lock (violationGate)
            unnecessarySnapshotImports.Add(new UnnecessarySnapshotImport(
                StepNumber, importer.Endpoint, partitionId, heldThrough, firstRetainedElsewhere, retainedBy));
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

    /// <summary>
    /// Driving rounds run so far, and how many of those had to wait because the round moved
    /// nothing while work was still in flight. The ratio is the cost of a run: a waiting round
    /// costs a real millisecond or more, a working round costs nothing. Exposed because tuning
    /// this by guesswork does not work — the two obvious guesses both changed nothing.
    /// </summary>
    public long PumpRounds { get; private set; }

    /// <inheritdoc cref="PumpRounds"/>
    public long WaitingPumpRounds { get; private set; }

    /// <summary>Simulated time at which the next membership refresh is due.</summary>
    private long nextUpdateNodesAtMilliseconds;

    /// <summary>
    /// Membership refreshes started but not finished, in driven mode. They are started rather than
    /// awaited because each awaits partition operations that only the driver can complete, so the
    /// settle loop is what waits for them.
    /// </summary>
    private readonly List<Task> pendingUpdateNodes = [];

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

        // The transfer stub survives a restart with the node's store, so one installation covers
        // every life of the node.
        foreach (SimulationNode node in cluster.nodes)
            node.StateTransfer.ImportObserver = partitionId => cluster.RecordSnapshotImport(node, partitionId);

        // Subscribed before the first join, because a restore runs inside the join and a restore
        // is where the one violation this exists for was seen. Unsubscribed in DisposeAsync.
        RaftInvariants.Violated += cluster.onLibraryViolation;

        cluster.PublishRoutingTable();

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
    /// Publishes the endpoint-to-node routing table. Called again after a restart, because the
    /// restarted node is a different <see cref="RaftManager"/> behind the same endpoint and the
    /// table holds the object, not the address.
    /// </summary>
    private void PublishRoutingTable() =>
        Transport.SetNodes(nodes.ToDictionary(node => node.Endpoint, node => (IRaft)node.Manager));

    /// <summary>
    /// Kills a node without warning and settles the cluster around the loss.
    ///
    /// <para>The node's store survives and loses only what was inside its fsync window. Its
    /// endpoint is both frozen, so consensus traffic waits rather than vanishing, and partitioned,
    /// so a control-plane call fails instead of reaching a disposed manager.</para>
    /// </summary>
    public async Task CrashNodeAsync(SimulationNode node, CancellationToken cancellationToken)
    {
        node.Crash();
        Transport.PartitionNode(node.Endpoint);

        // The failure detector's verdict, delivered by the harness: the simulation drives no SWIM
        // probes, so without this a crashed node stays Alive to every peer for as long as it is
        // down, and every rule keyed on liveness — the leader's retention hold for silent peers
        // above all — is never exercised by a crash. Suspect is enough: the rules test for Alive.
        foreach (SimulationNode other in nodes)
        {
            if (other != node && other.HasLiveManager)
                other.Manager.Liveness.MarkSuspect(node.Endpoint);
        }

        await SettleAsync(deliverMessages: true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Starts a crashed node again over the same store and steps until it has rejoined.
    ///
    /// <para>Order matters, as it does at cluster start. The rebuilt manager goes into the routing
    /// table before it refreshes its membership, or it refreshes against a table that still names
    /// the dead one.</para>
    /// </summary>
    public async Task RestartNodeAsync(SimulationNode node, CancellationToken cancellationToken)
    {
        node.Rebuild();
        PublishRoutingTable();
        Transport.HealPartition(node.Endpoint);

        // The probe that would succeed now: the peers see the node alive again the moment it is
        // reachable, as a direct probe would report it.
        foreach (SimulationNode other in nodes)
        {
            if (other != node && other.HasLiveManager)
                other.Manager.Liveness.ClearSuspicion(node.Endpoint);
        }

        Task join = await node.BeginStartAsync(cancellationToken).ConfigureAwait(false);

        for (int step = 0; step < JoinStepBudget && !join.IsCompleted; step++)
            await StepAsync(JoinStepMilliseconds, cancellationToken).ConfigureAwait(false);

        if (!join.IsCompleted)
            throw new TimeoutException(
                $"{node.Endpoint} did not rejoin within {JoinStepBudget} simulated steps.");

        await join.ConfigureAwait(false);
    }

    /// <summary>
    /// Starts a crashed node again under the same endpoint with an empty store: its data directory
    /// is gone. See <see cref="WAL.SimulatedWAL.Wipe"/> for what that models and what Raft does not
    /// promise about it.
    /// </summary>
    public async Task RestartNodeBlankAsync(SimulationNode node, CancellationToken cancellationToken)
    {
        if (node.LifecycleStatus != SimulationNodeLifecycleStatus.Crashed)
            throw new InvalidOperationException($"{node.Endpoint} must be crashed before its store is wiped.");

        if (node.SimulatedWal is null)
            throw new InvalidOperationException($"{node.Endpoint} has no simulated store to wipe.");

        node.SimulatedWal.Wipe();

        // The application's own store lives on the same disk.
        node.StateTransfer.ClearApplied();

        await RestartNodeAsync(node, cancellationToken).ConfigureAwait(false);
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
            {
                if (Options.DrivenScheduling)
                    pendingUpdateNodes.Add(node.BeginTickUpdateNodes());
                else
                    await node.TickUpdateNodesAsync().ConfigureAwait(false);
            }
        }

        if (Options.ModelSwimLiveness)
            UpdateLivenessModel();

        foreach (SimulationNode node in nodes)
            node.TickCheckLeader();

        if (deliverMessages)
            await Transport.DeliverAll().ConfigureAwait(false);

        await SettleAsync(deliverMessages, cancellationToken).ConfigureAwait(false);
        StepNumber++;
    }

    /// <summary>
    /// Simulated time at which each ordered pair (observer, peer) last lost its probe path, for the
    /// pairs that have none now. See <see cref="UpdateLivenessModel"/>.
    /// </summary>
    private readonly Dictionary<(string Observer, string Peer), long> unreachableSince = [];

    /// <summary>
    /// Plays the SWIM failure detector for every live node. Runs once per step, before the leader
    /// check, when <see cref="SimulationClusterOptions.ModelSwimLiveness"/> is on.
    ///
    /// <para><b>The model.</b> A probe from an observer reaches a peer when the ping and its answer
    /// both get through, directly or through one relay that the observer and the peer can each reach
    /// in both directions: the SWIM ping-req path. A peer with no such path for the observer's
    /// <c>PingInterval + PingTimeout</c> of simulated time is marked <c>Suspect</c> in the
    /// observer's table. A path that comes back clears the suspicion at once, as a successful probe
    /// does.</para>
    ///
    /// <para><b>What it leaves out.</b> Suspicion never turns into <c>Dead</c>: that transition runs
    /// on the SWIM tick, which the harness does not drive, and no rule under test needs more than
    /// "not Alive". Gossip does not spread a verdict either: each node judges only its own
    /// probes.</para>
    /// </summary>
    private void UpdateLivenessModel()
    {
        long now = Clock.LogicalMilliseconds;
        List<SimulationNode> live = nodes.Where(node => node.HasLiveManager).ToList();

        bool TwoWay(string a, string b) => Transport.CanDeliver(a, b) && Transport.CanDeliver(b, a);

        foreach (SimulationNode observer in live)
        {
            TimeSpan window = observer.Manager.Configuration.PingInterval + observer.Manager.Configuration.PingTimeout;

            foreach (SimulationNode peer in nodes)
            {
                if (peer == observer)
                    continue;

                bool reachable = TwoWay(observer.Endpoint, peer.Endpoint)
                                 || live.Any(relay =>
                                     relay != observer
                                     && relay != peer
                                     && TwoWay(observer.Endpoint, relay.Endpoint)
                                     && TwoWay(relay.Endpoint, peer.Endpoint));

                (string, string) key = (observer.Endpoint, peer.Endpoint);

                if (reachable)
                {
                    if (unreachableSince.Remove(key))
                        observer.Manager.Liveness.ClearSuspicion(peer.Endpoint);

                    continue;
                }

                if (!unreachableSince.TryGetValue(key, out long since))
                {
                    unreachableSince[key] = now;
                    continue;
                }

                if (now - since >= (long)window.TotalMilliseconds)
                    observer.Manager.Liveness.MarkSuspect(peer.Endpoint);
            }
        }
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
    /// <summary>
    /// Executor drains started but not yet finished, one slot per node.
    ///
    /// <para>They are started rather than awaited because a drain can be waiting for another node.
    /// A driver that awaited one would be parked inside the very node it must leave in order to
    /// service the other, which is how the first attempt at this mode deadlocked.</para>
    /// </summary>
    private Task<int>?[]? inFlightDrains;

    private async Task SettleAsync(bool deliverMessages, CancellationToken cancellationToken)
    {
        if (Options.DrivenScheduling)
        {
            await SettleDrivenAsync(deliverMessages, cancellationToken).ConfigureAwait(false);
            return;
        }

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

    /// <summary>
    /// Settles a cluster whose nodes own no scheduling threads.
    ///
    /// <para><b>The shape that works.</b> Each node's executor drain is <em>started</em> and kept
    /// in flight. The driver then keeps doing the two things a parked drain is waiting for:
    /// flushing every node's outbound transport, and delivering the wire. A drain that finishes
    /// frees its slot and a fresh one starts. The round ends when nothing moved and no drain is
    /// still running.</para>
    ///
    /// <para>Flushing the transport of a node whose drain is in flight is not optional. The
    /// request that drain is waiting on may still be sitting in that node's own outbound queue,
    /// and a driver that flushed only idle nodes would never send it.</para>
    /// </summary>
    private async Task SettleDrivenAsync(bool deliverMessages, CancellationToken cancellationToken)
    {
        for (int round = 0; round < MaxSettleRounds; round++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (await PumpRoundAsync(deliverMessages, cancellationToken).ConfigureAwait(false))
                return;
        }
    }

    /// <summary>
    /// Runs one round of driven work and reports whether the cluster is now idle.
    ///
    /// <para>Each node's executor drain is <em>started</em> and kept in flight rather than awaited.
    /// A drain can be waiting for another node, and a driver that awaited one would be parked
    /// inside the very node it must leave in order to service the other. While the drains run,
    /// this keeps doing the things a parked drain is waiting for: writing the write-ahead log,
    /// flushing every node's outbound transport, and delivering the wire.</para>
    ///
    /// <para>Flushing a node whose drain is in flight is not optional. The request that drain is
    /// waiting on may still be sitting in that node's own outbound queue.</para>
    /// </summary>
    private async Task<bool> PumpRoundAsync(bool deliverMessages, CancellationToken cancellationToken)
    {
        inFlightDrains ??= new Task<int>?[nodes.Count];
        PumpRounds++;

        int work = 0;

        for (int index = 0; index < nodes.Count; index++)
        {
            Task<int>? drain = inFlightDrains[index];

            if (drain is not null && !drain.IsCompleted)
                continue;

            if (drain is not null)
                work += await drain.ConfigureAwait(false);

            inFlightDrains[index] = nodes[index].Manager.PumpExecutorsAsync().AsTask();
        }

        // Storage first: a completion posted here becomes executor work on the next round.
        foreach (SimulationNode node in nodes)
            work += node.Manager.PumpWriteAheadLog();

        foreach (SimulationNode node in nodes)
            work += await node.Manager.FlushTransportAsync().ConfigureAwait(false);

        if (deliverMessages)
            work += await Transport.DeliverAll().ConfigureAwait(false);

        pendingUpdateNodes.RemoveAll(task => task.IsCompleted);

        bool anyRunning = inFlightDrains.Any(drain => drain is not null && !drain.IsCompleted);
        bool coordinatorBusy = nodes.Any(node => node.HasPendingCoordinatorWork);

        if (work == 0 && !anyRunning && !coordinatorBusy && pendingUpdateNodes.Count == 0)
            return true;

        // A round that moved something has more to do right now, so go straight around again.
        // Only a round that moved nothing while something is still in flight has to wait, and then
        // only to let those in-flight tasks advance. Waiting after every round instead cost about
        // a second per simulated step and made this category too slow to run on a pull request.
        //
        // The wait measures nothing the cluster reads: every timeout reads the simulated clock,
        // which this method never advances.
        // A round that moved something has more to do right now, so go straight around again.
        // A round that moved nothing while work is still in flight has to wait, and a yield is not
        // enough: the in-flight drains run on the thread pool, and a yield does not reliably give
        // them a turn. Removing this wait makes the rounds spin without progress until the bound
        // trips — measured, not assumed.
        //
        // The wait measures nothing the cluster reads: every timeout reads the simulated clock,
        // which this method never advances. It costs real seconds and buys no determinism, which
        // is why the state sequence is reproducible while the wall-clock shape of a run is not.
        if (work == 0)
        {
            WaitingPumpRounds++;
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await Task.Yield();
        }

        return false;
    }

    /// <summary>
    /// Rounds a driven call may spend before it is declared stuck. Generous, because a client
    /// proposal legitimately needs a full replication round trip.
    /// </summary>
    private const int MaxDriveRounds = 5_000;

    /// <summary>
    /// Runs <paramref name="operation"/> while driving the cluster, and returns its result.
    ///
    /// <para><b>Why every call needs this in driven mode.</b> With the nodes' threads gone, no work
    /// happens unless the driver makes it happen — and that includes work the caller itself
    /// started. Reading a partition view posts a request to an executor. Proposing an entry waits
    /// for a replication round trip. Awaiting either without driving would wait forever, because
    /// the awaiting thread is the only one that could serve it.</para>
    /// </summary>
    public async Task<T> DriveAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        Task<T> task = operation();

        if (!Options.DrivenScheduling)
            return await task.ConfigureAwait(false);

        await DriveUntilAsync(() => task.IsCompleted, cancellationToken).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    /// <summary>
    /// Drives the cluster until <paramref name="condition"/> holds.
    ///
    /// <para>Shared by <see cref="DriveAsync{T}"/> and by the batched view read, so several
    /// operations started together are carried by one driving pass rather than one each.</para>
    /// </summary>
    private async Task DriveUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        if (!Options.DrivenScheduling || condition())
            return;

        for (int round = 0; round < MaxDriveRounds && !condition(); round++)
            await PumpRoundAsync(deliverMessages: true, cancellationToken).ConfigureAwait(false);

        if (!condition())
        {
            throw new InvalidOperationException(
                $"A driven operation did not finish within {MaxDriveRounds} rounds. The cluster is " +
                "stuck, or the operation is waiting on something the driver does not pump.");
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
        // A crashed node's manager is disposed and a stopped one never started. Asking either
        // throws rather than answering, and a run must survive its own faults.
        List<SimulationNode> live = nodes.Where(node => node.HasLiveManager).ToList();

        // Every read is started before any is driven. A view read posts a request to that node's
        // executor, and in driven mode nothing runs those executors unless this driver does — so
        // starting them together lets one driving pass answer all of them. Driving them one at a
        // time costs a separate pass per node and was the largest single cost in the driven tests.
        List<Task<RaftPartitionView?>> reads = live
            .Select(node => node.GetPartitionViewAsync(partitionId, cancellationToken))
            .ToList();

        await DriveUntilAsync(() => reads.All(read => read.IsCompleted), cancellationToken)
            .ConfigureAwait(false);

        List<RaftPartitionView> views = [];

        foreach (Task<RaftPartitionView?> read in reads)
        {
            RaftPartitionView? view = await read.ConfigureAwait(false);

            if (view is not null)
                views.Add(view);
        }

        return views;
    }

    /// <summary>
    /// Reads every node's write-ahead log, keyed by endpoint.
    ///
    /// <para>A partition view says how far a node believes it committed. Only the store says what
    /// is behind that belief, and the gap between the two is where several shipped defects lived.
    /// Nodes whose scenario asked for a plain in-memory log contribute nothing.</para>
    ///
    /// <para>This reads the store directly rather than through an executor. It is safe because the
    /// store is thread-safe by contract, but a caller that wants a settled reading should still
    /// take it at a step boundary.</para>
    /// </summary>
    public IReadOnlyDictionary<string, SimulatedWalSnapshot> GetWalSnapshots()
    {
        Dictionary<string, SimulatedWalSnapshot> snapshots = new();

        foreach (SimulationNode node in nodes)
        {
            if (node.SimulatedWal is not null)
                snapshots[node.Endpoint] = node.SimulatedWal.Snapshot();
        }

        return snapshots;
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
            RaftPartitionView? view = !node.HasLiveManager
                ? null
                : await DriveAsync(
                    () => node.GetPartitionViewAsync(partitionId, cancellationToken),
                    cancellationToken).ConfigureAwait(false);

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

        RaftInvariants.Violated -= onLibraryViolation;

        // Release any message still held, so a node blocked on a reply can finish its teardown.
        Transport.HoldMessages = false;
        await Transport.DeliverAll().ConfigureAwait(false);

        // The cluster is torn down outright, never by graceful leave. A graceful leave commits a
        // roster change, and a roster change needs a leader of the system partition. Nobody advances
        // simulated time during teardown, so when the leaving node was that leader no successor is
        // ever elected, and every later leave burns its full timeout: ten real seconds each, on a
        // cluster nobody is going to use again. This used to apply only to a cluster that ended
        // unhealthy. Measured on 2026-09-23, healthy clusters paid it too: 8 of 22 three-node read
        // runs spent 10 or 20 seconds here against under a second for the run itself, and a
        // five-node family paid it on almost every seed. Graceful leave has its own tests.
        foreach (SimulationNode node in nodes)
        {
            node.SkipGracefulLeave = true;
            await node.DisposeAsync().ConfigureAwait(false);
        }
    }
}
