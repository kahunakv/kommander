using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.History;
using Kommander.Tests.Simulation.Diagnostics;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Replay;
using Kommander.Tests.Simulation.WAL;

namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// Runs a generated plan against a real cluster and checks everything the harness knows how to
/// check.
///
/// <para><b>Why a runner and not a test per scenario.</b> The scripted scenarios each pin one
/// state a reader already suspected. This one goes where nobody aimed it. Both findings the harness
/// has produced so far came from states near a fault, which says the space around a fault is
/// productive and largely unexplored, and a scripted test can only visit the part of it somebody
/// thought to write down.</para>
///
/// <para><b>The run has three phases, and the third is not optional.</b> The plan phase injects
/// faults and issues client operations. The healing phase ends every fault the plan left active.
/// The recovery phase issues one live write and waits for the cluster to converge. The live write
/// is there because a leader does not push merely-restored state at a voter that reported no
/// committed prefix, so a cluster healed in silence can sit short of convergence for reasons that
/// are documented behaviour rather than a defect. Ending on a write makes the convergence check
/// mean what it says.</para>
///
/// <para>Every step of every phase runs the per-step invariants. The end of the run adds the
/// convergence check and the client-history check, so a plan passes only if all three agree.</para>
/// </summary>
public sealed class RandomScenarioRunner
{
    private readonly SimulationCluster cluster;
    private readonly RandomScenarioOptions options;
    private readonly SimulationRandom random;
    private readonly RandomScenarioGenerator generator;
    private readonly SimulationMetricsCollector metrics = new();
    private readonly ClusterInvariantRunner invariants = new();
    private readonly ClientHistory history = new();
    private readonly List<RandomScenarioAction> actions = [];

    public RandomScenarioRunner(
        SimulationCluster cluster,
        RandomScenarioOptions options,
        SimulationRandom random)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(random);

        this.cluster = cluster;
        this.options = options;
        this.random = random;

        generator = new RandomScenarioGenerator(random, options);

        // The checker records its own time here. A run that spends nearly all of itself checking is
        // exploring almost nothing, and no single total would show that.
        invariants.Metrics = metrics;
    }

    /// <summary>The plan, as far as it has been drawn. Populated even when the run fails.</summary>
    public IReadOnlyList<RandomScenarioAction> Actions => actions;

    /// <summary>What the clients were told, as far as the run got.</summary>
    public ClientHistory History => history;

    /// <summary>Per-step invariant checks performed so far.</summary>
    public int InvariantChecks => invariants.ChecksRun;

    /// <summary>What the run has cost so far. Populated whether or not the run finished.</summary>
    public SimulationMetrics Metrics =>
        metrics.Snapshot(cluster.StepNumber, actions.Count, invariants.ChecksRun);

    /// <summary>
    /// The report as far as the run got.
    ///
    /// <para>For a run that threw. The caller needs the plan to write beside the failure, and
    /// building one by hand at the catch site risks describing a run that did not happen.
    /// <see cref="RandomScenarioReport.FinalCommitIndex"/> is -1 here, because the run stopped
    /// before it could read one.</para>
    /// </summary>
    public RandomScenarioReport Partial() =>
        new()
        {
            Seed = random.Seed,
            Options = options,
            Actions = actions,
            StepsRun = cluster.StepNumber,
            History = history,
            FinalCommitIndex = -1,
            InvariantChecks = invariants.ChecksRun,
            EntriesCompacted = EntriesCompacted(),
            SnapshotExportsServed = cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed),
            SnapshotExportsHung = cluster.Nodes.Sum(node => node.StateTransfer.ExportsHung),
            Metrics = Metrics,
        };

    /// <summary>
    /// Draws a plan, runs it, and checks it.
    ///
    /// <para>Throws whatever the checks throw. The caller is expected to catch, write the plan
    /// beside the failure, and name the seed — a failing seed nobody can re-run is worth
    /// nothing.</para>
    /// </summary>
    public Task<RandomScenarioReport> RunAsync(CancellationToken cancellationToken) =>
        ExecuteAsync(plan: null, cancellationToken);

    /// <summary>
    /// Applies a recorded plan instead of drawing one.
    ///
    /// <para>This is how a failing run is re-run. The generator is out of the loop entirely, so the
    /// second run performs the same actions in the same order whatever the cluster does between
    /// them — which a re-draw from the same seed cannot promise, because a draw depends on what the
    /// generator observed. See <see cref="RandomScenarioPlan"/> for what replay does and does not
    /// pin.</para>
    /// </summary>
    public Task<RandomScenarioReport> ReplayAsync(
        IReadOnlyList<RandomScenarioAction> plan,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);

        return ExecuteAsync(plan, cancellationToken);
    }

    private async Task<RandomScenarioReport> ExecuteAsync(
        IReadOnlyList<RandomScenarioAction>? plan,
        CancellationToken cancellationToken)
    {
        RequireSimulatedStores();

        await ElectAsync(cancellationToken).ConfigureAwait(false);

        if (plan is null)
        {
            for (int index = 0; index < options.ActionCount; index++)
            {
                RandomScenarioObservation observation =
                    await ObserveAsync(cancellationToken).ConfigureAwait(false);

                random.SetContext(cluster.StepNumber, cluster.Clock.LogicalMilliseconds);

                RandomScenarioAction action = generator.Next(observation);

                // The plan records what ran, not what was asked for. An episode's actions carry no
                // target until they run, and a plan that kept the blank would replay somewhere else.
                actions.Add(await PerformAsync(action, cancellationToken).ConfigureAwait(false));
            }

            // A drawn plan may still hold faults the age bound never reached. A recorded plan
            // already carries the heals it emitted, and its generator has drawn nothing.
            foreach (RandomScenarioAction heal in generator.HealAll())
            {
                actions.Add(heal);
                await ApplyAsync(heal, cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            foreach (RandomScenarioAction action in plan)
                actions.Add(await PerformAsync(action, cancellationToken).ConfigureAwait(false));
        }

        await HealEverythingAsync(cancellationToken).ConfigureAwait(false);
        await RecoverAsync(cancellationToken).ConfigureAwait(false);

        long finalCommitIndex = await HighestCommitIndexAsync(cancellationToken).ConfigureAwait(false);

        IReadOnlyList<RaftPartitionView> finalViews = await cluster
            .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        return new RandomScenarioReport
        {
            Seed = random.Seed,
            Options = options,
            Actions = actions,
            StepsRun = cluster.StepNumber,
            History = history,
            FinalCommitIndex = finalCommitIndex,
            InvariantChecks = invariants.ChecksRun,
            EntriesCompacted = EntriesCompacted(),
            SnapshotExportsServed = cluster.Nodes.Sum(node => node.StateTransfer.ExportsServed),
            SnapshotExportsHung = cluster.Nodes.Sum(node => node.StateTransfer.ExportsHung),
            Metrics = Metrics,
            QuiescedOutagesReached = QuiescedOutagesReached,
            LateBroadcastsCommittedFirst = LateBroadcastsCommittedFirst,
            TransferAnswers = new Dictionary<RaftOperationStatus, int>(transferAnswers),
            CutLeaderReadsReached = CutLeaderReadsReached,
            CutLeaderReadsServed = CutLeaderReadsServed,
            FinalTerm = finalViews.Count == 0 ? -1 : finalViews.Max(view => view.Term),
        };
    }

    /// <summary>Entries compaction removed across every node's store.</summary>
    private long EntriesCompacted() =>
        cluster.GetWalSnapshots().Values.Sum(snapshot => snapshot.Counters.EntriesCompacted);

    /// <summary>
    /// Writes the entropy the plan consumed as a replay log, in the same format the model-layer
    /// runs use. The plan is reproducible from the seed alone; this file is what shows a reader
    /// which draw produced which action when the two disagree.
    /// </summary>
    public string WriteReplayLog(string directory, string name)
    {
        Directory.CreateDirectory(directory);

        string path = Path.Combine(directory, $"{name}-seed-{random.Seed}.replay.jsonl");

        RandomClusterScenario scenario = new(name, random.Seed, options.ToParameters());

        using ReplayLogWriter writer = new(path, scenario);

        foreach (SimulationRandomChoice choice in random.RecordedChoices)
            writer.WriteRandomChoice(choice);

        return path;
    }

    // ── Phases ────────────────────────────────────────────────────────────

    /// <summary>
    /// Ends every fault the plan could have left behind, including the ones the generator does not
    /// track.
    ///
    /// <para>Belt and braces on purpose. The generator's table is the record of what it started,
    /// and teardown must not depend on that record being complete: a node still refusing writes
    /// cannot commit the roster change a graceful leave waits for, so one missed fault turns every
    /// shutdown into a timeout.</para>
    /// </summary>
    private async Task HealEverythingAsync(CancellationToken cancellationToken)
    {
        cluster.Transport.ClearLinkFaults();

        foreach (SimulationNode node in cluster.Nodes)
        {
            node.SimulatedWal?.ClearFaults();

            if (node.SimulatedWal is not null)
                node.SimulatedWal.WriteLatencyMilliseconds = 0;

            if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Paused)
                node.Resume();

            // A hung export stays armed, deliberately. Ending the hang is the library's job — the
            // per-step transfer timeout — and a heal phase that ended it would repair the state the
            // run is looking for. A validation run once missed a defect for exactly that reason: the
            // runner wrote to the cluster before it checked, and the write repaired the state.
        }

        foreach (SimulationNode node in cluster.Nodes)
        {
            if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Crashed)
                await cluster.RestartNodeAsync(node, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Waits for a leader, writes once, and waits for every live node to hold the same committed
    /// frontier and the same entries behind it.
    /// </summary>
    private async Task RecoverAsync(CancellationToken cancellationToken)
    {
        await CheckIdleConvergenceAsync(cancellationToken).ConfigureAwait(false);

        SimulationNode? leader = await ElectAsync(cancellationToken).ConfigureAwait(false);

        if (leader is not null)
        {
            await history
                .AppendUniqueAsync(cluster, leader, options.PartitionId, "Greeting", cancellationToken)
                .ConfigureAwait(false);
        }

        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                    .ConfigureAwait(false);

                IReadOnlyList<RaftPartitionView> views = await cluster
                    .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
                    .ConfigureAwait(false);

                int live = cluster.Nodes.Count(node => node.HasLiveManager);

                return views.Count == live
                       && views.Count > 0
                       && views.Select(view => view.CommitIndex).Distinct().Count() == 1;
            },
            options.RecoveryStepBudget,
            options.AdvanceMillisecondsPerStep,
            cancellationToken).ConfigureAwait(false);

        if (!converged)
        {
            // The state, not just the step count. A convergence failure has several very different
            // causes — a node short of the leader, a node holding entries nobody else has, a node
            // stranded below a compaction floor with no snapshot — and the frontiers plus the
            // retained ranges separate them at a glance.
            IReadOnlyList<RaftPartitionView> finalViews = await cluster
                .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
                .ConfigureAwait(false);

            List<string> lines = [];

            foreach (RaftPartitionView view in finalViews)
            {
                SimulatedWalPartitionSnapshot? store = cluster.Nodes
                    .FirstOrDefault(node => node.Endpoint == view.Endpoint)?
                    .SimulatedWal?.Snapshot().Partition(options.PartitionId);

                lines.Add(
                    $"{view.Endpoint} role={view.Role} term={view.Term} commit={view.CommitIndex} " +
                    $"maxLog={store?.MaxLogId} first={store?.FirstLogId} " +
                    $"compactedThrough={store?.CompactedThrough} snapshotBoundary={store?.SnapshotBoundary} missing=[{string.Join(",", store?.MissingIds ?? [])}]");
            }

            Assert.Fail(
                $"The healed cluster did not converge within {options.RecoveryStepBudget} steps. " +
                $"Final state: {string.Join(" | ", lines)}{Environment.NewLine}" +
                $"Unobservable: {DescribeUnobservableNodes(finalViews)}{Environment.NewLine}" +
                $"Snapshot rescue: {DescribeSnapshotRescue()}");
        }

        await invariants.CheckConvergedAsync(cluster, options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        // The application's view, after the log's: a converged log with an application that never
        // received part of it is the install that imported nothing (DST-20).
        await AppliedStateRule.CheckAsync(cluster, options.PartitionId, options.AdvanceMillisecondsPerStep, cancellationToken)
            .ConfigureAwait(false);

        SimulationNode reader = cluster.Nodes.First(node => node.HasLiveManager);

        // The reader's own compaction floor, not the cluster's. What this node threw away, or holds
        // only as a snapshot, is what its log cannot answer for, and the history is read against
        // this node.
        long compactedThrough = reader.SimulatedWal?.Snapshot()
            .Partition(options.PartitionId)?.CoveredThrough ?? -1;

        ClientHistoryChecker.Check(
            history,
            reader.Wal.ReadLogsRange(options.PartitionId, 0),
            cluster.StepNumber,
            compactedThrough);
    }

    /// <summary>
    /// Steps a healed cluster is given to converge before anybody writes to it. Configurable,
    /// because it is the bound this check's verdict rests on — see
    /// <see cref="RandomScenarioOptions.IdleConvergenceStepBudget"/>.
    /// </summary>
    private int IdleConvergenceStepBudget => options.IdleConvergenceStepBudget;

    /// <summary>
    /// A healed cluster converges on its own, without a client writing to it first.
    ///
    /// <para><b>Why this runs before the recovery write, and why it exists at all.</b> The recovery
    /// write repairs a follower that is short of the leader, so a check placed only after it can
    /// never see a follower that would have stayed short forever. That is not a hypothetical: a
    /// validation run over a reintroduced defect (`32348e83`, a voter with a sub-threshold gap on an
    /// idle range) passed every seed, because this runner healed the very state it was looking
    /// for.</para>
    ///
    /// <para><b>Why nodes at frontier zero are exempt, and why that is not a loophole.</b> A leader
    /// deliberately does not push merely-restored state at a voter that has never reported holding
    /// any of this log — the confinement that protects the highest-write-ahead-log election
    /// preference. Such a voter waits for the next live write by design, so failing it here would
    /// report documented behaviour as a defect. A voter that <i>has</i> reported a committed prefix
    /// makes no such claim on the floor, and it must converge without help.</para>
    /// </summary>
    private async Task CheckIdleConvergenceAsync(CancellationToken cancellationToken)
    {
        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                    .ConfigureAwait(false);

                return await IdleFrontiersAgreeAsync(cancellationToken).ConfigureAwait(false);
            },
            IdleConvergenceStepBudget,
            options.AdvanceMillisecondsPerStep,
            cancellationToken).ConfigureAwait(false);

        if (converged)
            return;

        IReadOnlyList<RaftPartitionView> views = await cluster
            .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        string state = string.Join(
            ", ", views.Select(view => $"{view.Endpoint}={view.CommitIndex}"));

        // What the leader believes about each peer, beside what each peer actually holds. The
        // leader's repair decision is computed from these values and nothing else, so a follower
        // that is never caught up is explained by one of them — most often a frontier the leader
        // never recorded, which it treats as a gap of zero and therefore never repairs.
        string leaderBelief = string.Join(
            " | ",
            views
                .Where(view => view.Peers.Count > 0)
                .Select(view => $"{view.Endpoint} sees [{string.Join("; ", view.Peers)}]"));

        if (leaderBelief.Length == 0)
            leaderBelief = "no node reported peer state, so no node was leading at the end.";

        // Backfill refusals, from the library's own queryable diagnostic. A leader that decided to
        // ship a repair and then could not says so here; an empty list means the send was never
        // refused, which points the reader at the decision rather than at the log read.
        List<string> refusals = [];

        foreach (SimulationNode node in cluster.Nodes.Where(candidate => candidate.HasLiveManager))
        {
            foreach (RaftBackfillStatus status in node.Manager.GetBackfillStatuses(options.PartitionId))
            {
                refusals.Add(
                    $"{node.Endpoint}->{status.FollowerEndpoint} anchor={status.AnchorIndex} " +
                    $"firstAvailable={status.FirstAvailableIndex} " +
                    $"lastCheckpoint={status.LastCheckpoint} occurrences={status.Occurrences}");
            }
        }

        string refusalText = refusals.Count > 0
            ? string.Join(" | ", refusals)
            : "none, so no leader was refused a repair it decided to send.";

        // Snapshot rescue, from the same library diagnostic. A follower whose log the leader has
        // already compacted past cannot be repaired by backfill at all: the entries it needs are
        // gone, and a snapshot install is the only way back. That escalation either happened or it
        // did not, and until now the harness could not tell — which left "no snapshot install is
        // ever attempted" (the Caraxes anchor-1 wedge) and "the rescue was attempted and failed"
        // reading identically from a failure message.
        //
        // Read the empty list carefully. The library records an entry only while a transfer is in
        // flight or a transfer has failed, so a healthy partition is empty too. Empty is therefore
        // only meaningful beside the stores printed below: a follower stuck under the leader's
        // first available index with nothing here was never rescued.
        string rescueText = DescribeSnapshotRescue();

        // Each node's store beside its committed frontier, which separates the two remaining
        // causes. A follower whose log holds the entry but whose frontier is short received the
        // repair and did not commit it. A follower whose log stops at its frontier was never sent
        // anything. The failure message alone cannot tell those apart, and they are different
        // defects.
        string stores = string.Join(
            " | ",
            views.Select(view =>
            {
                SimulatedWalPartitionSnapshot? store = cluster.Nodes
                    .FirstOrDefault(node => node.Endpoint == view.Endpoint)?
                    .SimulatedWal?.Snapshot().Partition(options.PartitionId);

                // Role and quiescence beside the log. Quiescing stops the heartbeat, and the
                // heartbeat hosts the only catch-up path — so a partition that quiesced with a
                // follower still short can never repair it, and the leader's last recorded
                // decision stays frozen at whatever it believed when the rounds stopped.
                return $"{view.Endpoint} role={view.Role} quiesced={view.Quiesced} " +
                       $"commit={view.CommitIndex} maxLog={store?.MaxLogId} " +
                       $"first={store?.FirstLogId} missing=[{string.Join(",", store?.MissingIds ?? [])}]";
            }));

        // Two different failures wear this one signature, and conflating them cost two rounds of
        // investigating the wrong mechanism.
        //
        // A cluster with no leader cannot converge by the heartbeat path, because there is no
        // heartbeat path: nobody ships anything to anybody. Reporting that as "a node that reported
        // a committed prefix must be caught up" points the reader at the repair decision, which is
        // not running at all. It is a liveness failure, and it is named as one.
        //
        // A cluster that does have a leader, and still leaves a follower short, is the repair
        // failure the rule was written for. Only then is the leader's belief worth reading.
        // A live node that answered no view is neither a follower nor absent, and the two verdicts
        // below both assume the views are the whole cluster. A view read is a client-kind operation
        // the executor refuses until the partition's restore completes, so the node this hides is
        // typically a restarted one — and FINDING 5 was exactly that: the unobservable node was the
        // leader, and the run was reported as leaderless. Name the node and its restore state, and
        // stop; what the visible nodes look like is not the finding.
        string unobservable = DescribeUnobservableNodes(views);

        if (unobservable.Length > 0)
        {
            Assert.Fail(
                $"idle-convergence-unobservable: after {IdleConvergenceStepBudget} steps a live node " +
                $"answered no partition view, so the cluster's state cannot be judged from the views " +
                $"that did arrive. Unobservable: {unobservable}. Frontiers seen: {state}.{Environment.NewLine}" +
                $"Roles seen: {string.Join(", ", views.Select(view => $"{view.Endpoint}={view.Role}"))}{Environment.NewLine}" +
                $"Snapshot rescue: {rescueText}{Environment.NewLine}" +
                $"Stores: {stores}");
        }

        bool leaderless = views.All(view => view.Role != RaftNodeState.Leader);

        if (leaderless)
        {
            Assert.Fail(
                $"idle-convergence-leaderless: the healed cluster had no leader after " +
                $"{IdleConvergenceStepBudget} steps. Frontiers: {state}. With no leader there is no " +
                "heartbeat path, so nothing could have repaired anybody — this is a liveness " +
                $"failure, not a repair failure.{Environment.NewLine}" +
                $"Roles: {string.Join(", ", views.Select(view => $"{view.Endpoint}={view.Role}"))}{Environment.NewLine}" +
                $"Snapshot rescue: {rescueText}{Environment.NewLine}" +
                $"Stores: {stores}");
        }

        Assert.Fail(
            $"idle-convergence: the healed cluster did not converge in {IdleConvergenceStepBudget} " +
            $"steps without a client write. Frontiers: {state}. A node that reported a committed " +
            $"prefix must be caught up by the heartbeat path alone.{Environment.NewLine}" +
            $"Leader belief: {leaderBelief}{Environment.NewLine}" +
            $"Backfill refusals: {refusalText}{Environment.NewLine}" +
            $"Snapshot rescue: {rescueText}{Environment.NewLine}" +
            $"Stores: {stores}");
    }


    /// <summary>
    /// The live nodes that answered no view for this partition, each with the executor's own account
    /// of why, or an empty string when every live node answered.
    ///
    /// <para>The view read is refused while a partition's restore is incomplete, and a restore whose
    /// second phase failed is incomplete forever. Such a node still acks appends and still runs the
    /// election path, so it is the one node a convergence failure most needs to name — and the one
    /// the views cannot. Printed in every convergence failure for that reason.</para>
    /// </summary>
    private string DescribeUnobservableNodes(IReadOnlyList<RaftPartitionView> views)
    {
        List<string> missing = [];

        foreach (SimulationNode node in cluster.Nodes)
        {
            if (!node.HasLiveManager)
                continue;

            if (views.Any(view => string.Equals(view.Endpoint, node.Endpoint, StringComparison.Ordinal)))
                continue;

            missing.Add($"{node.Endpoint} ({node.DescribePartitionState(options.PartitionId)})");
        }

        return string.Join(", ", missing);
    }

    /// <summary>
    /// What the leaders record about snapshot rescue on this partition, for a failure message.
    ///
    /// <para>Shared by both convergence failures on purpose. The recovery check and the idle check
    /// end in the same place — a replica the cluster did not repair — and the reader's next question
    /// is the same in both: was a snapshot even tried? Printing it in one and not the other is how a
    /// run gets diagnosed twice.</para>
    ///
    /// <para><b>Read the empty case carefully.</b> The library records an entry only while a
    /// transfer is in flight or a transfer has failed, so a healthy partition reports empty too.
    /// Empty is meaningful only beside the stores: a follower under the leader's first available
    /// index with nothing here was never rescued, and that is a different defect from a rescue that
    /// was attempted and failed.</para>
    /// </summary>
    private string DescribeSnapshotRescue()
    {
        List<string> rescues = [];

        foreach (SimulationNode node in cluster.Nodes.Where(candidate => candidate.HasLiveManager))
        {
            foreach (RaftSnapshotStatus status in node.Manager.GetSnapshotStatuses(options.PartitionId))
            {
                rescues.Add(
                    $"{node.Endpoint}->{status.FollowerEndpoint} inFlight={status.InFlight} " +
                    $"failedAttempts={status.FailedAttempts} unproducible={status.Unproducible} " +
                    $"lastError={status.LastError ?? "none"}");
            }
        }

        return rescues.Count > 0
            ? string.Join(" | ", rescues)
            : "none recorded, so no leader had a transfer in flight or a failed one to report.";
    }

    /// <summary>
    /// True when every live node that reported holding part of this log agrees on how much of it is
    /// committed.
    /// </summary>
    private async Task<bool> IdleFrontiersAgreeAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views = await cluster
            .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        int live = cluster.Nodes.Count(node => node.HasLiveManager);

        if (views.Count != live || views.Count == 0)
            return false;

        List<long> frontiers = views
            .Select(view => view.CommitIndex)
            .Where(index => index >= 1)
            .ToList();

        // Nothing committed anywhere: a run whose every append was refused has nothing to converge
        // on, and waiting out the budget for it would only slow the run down.
        if (frontiers.Count == 0)
            return true;

        return frontiers.Distinct().Count() == 1;
    }

    // ── Applying one action ───────────────────────────────────────────────

    /// <summary>Steps spent letting the returned node settle before the next action is chosen.</summary>
    private const int OutageRecoverySteps = 4;

    /// <summary>
    /// How many action-lengths an outage waits for the replacement leader. The election timeout is a
    /// few steps of simulated time, so this is generous; an outage that never produced an election
    /// is left to end anyway rather than fail, because a run's verdict belongs to its checks.
    /// </summary>
    private const int OutageElectionBudgetFactor = 4;

    /// <summary>
    /// Performs one action and gives the cluster its steps to react. Returns the action as it
    /// actually ran, which is what the plan records.
    /// </summary>
    private async Task<RandomScenarioAction> PerformAsync(
        RandomScenarioAction action,
        CancellationToken cancellationToken)
    {
        RandomScenarioAction resolved = await ResolveAsync(action, cancellationToken).ConfigureAwait(false);

        if (resolved.Kind == RandomScenarioActionKind.LeaderOutage && resolved.Target is not null)
        {
            await RunLeaderOutageAsync(resolved.Target, cancellationToken).ConfigureAwait(false);
            return resolved;
        }

        if (resolved.Kind == RandomScenarioActionKind.AppendAcrossOutage && resolved.Target is not null)
        {
            await RunAppendAcrossOutageAsync(resolved.Target, cancellationToken).ConfigureAwait(false);
            return resolved;
        }

        if (resolved.Kind == RandomScenarioActionKind.AppendAcrossQuorumLoss && resolved.Target is not null)
        {
            await RunAppendAcrossQuorumLossAsync(resolved.Target, cancellationToken).ConfigureAwait(false);
            return resolved;
        }

        if (resolved.Kind == RandomScenarioActionKind.QuiescedLeaderOutage && resolved.Target is not null)
        {
            await RunQuiescedLeaderOutageAsync(resolved.Target, cancellationToken).ConfigureAwait(false);
            return resolved;
        }

        if (resolved.Kind == RandomScenarioActionKind.TransferLeadership
            && resolved.Target is not null
            && resolved.Secondary is not null)
        {
            await RunLeadershipTransferAsync(resolved.Target, resolved.Secondary, cancellationToken).ConfigureAwait(false);
            return resolved;
        }

        if (resolved.Kind == RandomScenarioActionKind.ReadAtCutLeader && resolved.Target is not null)
        {
            await RunReadAtCutLeaderAsync(resolved.Target, cancellationToken).ConfigureAwait(false);
            return resolved;
        }

        if (resolved.Kind == RandomScenarioActionKind.LateBroadcast && resolved.Target is not null)
        {
            await RunLateBroadcastAsync(resolved.Target, cancellationToken).ConfigureAwait(false);
            return resolved;
        }

        bool deliver = await ApplyAsync(resolved, cancellationToken).ConfigureAwait(false);

        await RunStepsAsync(options.StepsPerAction, deliver, cancellationToken).ConfigureAwait(false);

        return resolved;
    }

    /// <summary>
    /// Quiesced outages that found the leader's partition quiesced before the cut. A family whose
    /// runs never reach it tested only the plain outage, and says so in its output.
    /// </summary>
    public int QuiescedOutagesReached { get; private set; }

    /// <summary>
    /// Steps the cut stays in place after another node leads. Long enough to pass the check-quorum
    /// window of the quiescence family (a 300 to 600 ms election timeout), so the cut leader must
    /// have stepped down by the end if check-quorum works.
    /// </summary>
    private const int QuiescedOutageExtraSteps = 16;

    /// <summary>
    /// Idles until the leader's partition quiesces, cuts the leader off, waits until another node
    /// leads and the check-quorum window has passed, writes to the cut node, and heals. See
    /// <see cref="RandomScenarioActionKind.QuiescedLeaderOutage"/>.
    ///
    /// <para>When the partition does not quiesce inside the budget, the action goes on as a plain
    /// outage with a write. It is not skipped, because the plan already records it, and
    /// <see cref="QuiescedOutagesReached"/> says how often the quiesced shape really ran.</para>
    ///
    /// <para><b>Cost.</b> The write at the cut leader has no quorum. It resolves when the leader
    /// learns the new term after the heal, which is simulated time; in the worst case it waits the
    /// library's ten real seconds of quorum wait. That is the price of the one write that can be
    /// lost here.</para>
    /// </summary>
    private async Task RunQuiescedLeaderOutageAsync(string endpoint, CancellationToken cancellationToken)
    {
        int quiesceBudget = (int)(options.QuiesceAfterMs / Math.Max(1, options.AdvanceMillisecondsPerStep))
                            + options.StepsPerAction * 2;

        bool quiesced = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                    .ConfigureAwait(false);

                RaftPartitionView? view = await Node(endpoint)
                    .GetPartitionViewAsync(options.PartitionId, cancellationToken)
                    .ConfigureAwait(false);

                return view is { Role: RaftNodeState.Leader, Quiesced: true };
            },
            quiesceBudget,
            options.AdvanceMillisecondsPerStep,
            cancellationToken).ConfigureAwait(false);

        if (quiesced)
            QuiescedOutagesReached++;

        cluster.Transport.PartitionNode(endpoint);

        Task<ClientOperation>? append = null;

        try
        {
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    IReadOnlyList<RaftPartitionView> views = await cluster
                        .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    return views.Any(view =>
                        view.Role == RaftNodeState.Leader
                        && !string.Equals(view.Endpoint, endpoint, StringComparison.Ordinal));
                },
                options.StepsPerAction * OutageElectionBudgetFactor * 2,
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);

            await RunStepsAsync(QuiescedOutageExtraSteps, deliver: true, cancellationToken).ConfigureAwait(false);

            // The write comes at the end of the cut, not at its start. A proposal wakes a quiesced
            // leader, and an awake leader is judged by the ordinary check-quorum path, so a write at
            // the start hid the very defect this action is for: measured, the pre-14b564a shape
            // passed every seed with the write first. By now a correct leader has stepped down and
            // refuses the write; a stale one still takes it.
            if (Node(endpoint).LifecycleStatus == SimulationNodeLifecycleStatus.Running)
                append = history.AppendUniqueAsync(
                    cluster, Node(endpoint), options.PartitionId, "Greeting", cancellationToken);

            await RunStepsAsync(LateBroadcastHoldSteps, deliver: true, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            cluster.Transport.HealPartition(endpoint);
        }

        if (append is not null)
        {
            await cluster.RunUntilAsync(
                () => Task.FromResult(append.IsCompleted),
                options.StepsPerAction * OutageElectionBudgetFactor,
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);

            await append.ConfigureAwait(false);
        }

        await RunStepsAsync(OutageRecoverySteps, deliver: true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads at a cut leader that found the state they exist for: another node led, and it had
    /// acknowledged a write the cut leader never received. A family where this stays at zero never
    /// asked the question.
    /// </summary>
    public int CutLeaderReadsReached { get; private set; }

    /// <summary>
    /// Of <see cref="CutLeaderReadsReached"/>, the reads the cut leader served. On a correct build a
    /// served read here is legal only when the cut leader held the new write anyway, which it cannot
    /// while the cut holds; the history checker decides.
    /// </summary>
    public int CutLeaderReadsServed { get; private set; }

    /// <summary>
    /// Reads at the leader, cuts it off, waits until another node leads, writes through the new
    /// leader, and reads at the cut leader before the heal. See
    /// <see cref="RandomScenarioActionKind.ReadAtCutLeader"/>.
    ///
    /// <para><b>The read budget.</b> A correct cut leader cannot confirm, and it refuses the read when
    /// its confirmation times out: <c>LeadershipConfirmationTimeout</c>, two seconds by default,
    /// enforced from its own tick in simulated time. The cut holds for that long plus a margin, so the
    /// answer arrives while the cut still holds. A read that is still open at the heal is waited out
    /// after it, and the history records whatever answer it gets.</para>
    ///
    /// <para>The endpoint is healed in a <c>finally</c>, like every outage.</para>
    /// </summary>
    private async Task RunReadAtCutLeaderAsync(string endpoint, CancellationToken cancellationToken)
    {
        // One read at the leader before the cut, as a client that reads all the time would make.
        // It leaves a fresh confirmation behind, and a leader that reused that confirmation after
        // the cut without a time limit would serve the second read from it. Without this read the
        // action cannot find such a defect: measured, a confirmation that never expires passed the
        // scripted cut-leader scenario until it read first.
        if (Node(endpoint).LifecycleStatus == SimulationNodeLifecycleStatus.Running)
        {
            Task<ClientOperation> before = history.ReadAsync(
                cluster, Node(endpoint), options.PartitionId, cancellationToken);

            await cluster.RunUntilAsync(
                () => Task.FromResult(before.IsCompleted),
                ReadBudgetSteps(),
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);

            await before.ConfigureAwait(false);
        }

        cluster.Transport.PartitionNode(endpoint);

        Task<ClientOperation>? read = null;
        bool reached = false;

        try
        {
            string? successor = null;

            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    IReadOnlyList<RaftPartitionView> views = await cluster
                        .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    successor = views
                        .FirstOrDefault(view =>
                            view.Role == RaftNodeState.Leader
                            && !string.Equals(view.Endpoint, endpoint, StringComparison.Ordinal))?
                        .Endpoint;

                    return successor is not null;
                },
                options.StepsPerAction * OutageElectionBudgetFactor,
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);

            ClientOperation? write = null;

            if (successor is not null)
            {
                Task<ClientOperation> append = history.AppendUniqueAsync(
                    cluster, Node(successor), options.PartitionId, "Greeting", cancellationToken);

                await cluster.RunUntilAsync(
                    () => Task.FromResult(append.IsCompleted),
                    options.StepsPerAction * OutageElectionBudgetFactor,
                    options.AdvanceMillisecondsPerStep,
                    cancellationToken).ConfigureAwait(false);

                write = await append.ConfigureAwait(false);
            }

            if (Node(endpoint).LifecycleStatus == SimulationNodeLifecycleStatus.Running)
            {
                read = history.ReadAsync(cluster, Node(endpoint), options.PartitionId, cancellationToken);

                await cluster.RunUntilAsync(
                    () => Task.FromResult(read.IsCompleted),
                    ReadBudgetSteps(),
                    options.AdvanceMillisecondsPerStep,
                    cancellationToken).ConfigureAwait(false);

                reached = write is { Outcome: ClientOperationOutcome.Ok };
            }
        }
        finally
        {
            cluster.Transport.HealPartition(endpoint);
        }

        if (read is not null)
        {
            await cluster.RunUntilAsync(
                () => Task.FromResult(read.IsCompleted),
                ReadBudgetSteps(),
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);

            ClientOperation answer = await read.ConfigureAwait(false);

            if (reached)
            {
                CutLeaderReadsReached++;

                if (answer.Outcome == ClientOperationOutcome.Ok)
                    CutLeaderReadsServed++;
            }
        }

        await RunStepsAsync(OutageRecoverySteps, deliver: true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Steps a read may take: the library's default confirmation timeout in simulated time, plus two
    /// action-lengths of margin for the tick that enforces it.
    /// </summary>
    private int ReadBudgetSteps() =>
        (int)(new RaftConfiguration().LeadershipConfirmationTimeout.TotalMilliseconds
              / Math.Max(1, options.AdvanceMillisecondsPerStep))
        + options.StepsPerAction * 2;

    /// <summary>
    /// Answers the leadership transfers of this run received, by status. Printed by the transfer
    /// family, because a family whose transfers were all refused before they started tested little.
    /// </summary>
    public IReadOnlyDictionary<RaftOperationStatus, int> TransferAnswers => transferAnswers;

    private readonly Dictionary<RaftOperationStatus, int> transferAnswers = [];

    /// <summary>
    /// The answers a leadership transfer may give when the cluster had no fault at all. Each is a
    /// definite result or names its reason: the handover happened or was sent, the target could not
    /// catch up inside the bound, or leadership had already moved before the call arrived.
    /// </summary>
    private static readonly HashSet<RaftOperationStatus> DefiniteTransferAnswers =
    [
        RaftOperationStatus.Success,
        RaftOperationStatus.Pending,
        RaftOperationStatus.TargetNotCaughtUp,
        RaftOperationStatus.NodeIsNotLeader,
    ];

    /// <summary>
    /// True when nothing in the harness is impairing the cluster: every node runs, the wire is
    /// perfect, and no store refuses writes.
    ///
    /// <para>Read from the cluster, not from the generator's fault table, so that a replayed plan
    /// reaches the same verdict as the drawn one.</para>
    /// </summary>
    private bool ClusterIsHealthy() =>
        cluster.Nodes.All(node => node.LifecycleStatus == SimulationNodeLifecycleStatus.Running)
        && cluster.Transport.IsHealthy
        && cluster.Nodes.All(node => node.SimulatedWal is not { HasWriteFault: true });

    /// <summary>
    /// Starts a client write at the leader and, while it is in flight, asks the leader to hand over
    /// to <paramref name="target"/>. Both are driven to completion and recorded.
    ///
    /// <para>Rule <c>transfer-definite-when-healthy</c> (DST-20 item 2): when the cluster had no fault
    /// at the moment of the call, the answer must be one of <see cref="DefiniteTransferAnswers"/>.
    /// A transient failure such as <c>ReplicationFailed</c> breaks no other rule, so without this
    /// one the defect of <c>38a5e2b</c> — a target one entry behind refused at once, and the move
    /// silently dropped — passed every check.</para>
    ///
    /// <para>When the node is no longer the leader, or the target is not running, the action only
    /// lets time pass; the plan still records it.</para>
    /// </summary>
    private async Task RunLeadershipTransferAsync(string leader, string target, CancellationToken cancellationToken)
    {
        RandomScenarioObservation observation = await ObserveAsync(cancellationToken).ConfigureAwait(false);

        if (observation.Leader != leader || !observation.Running.Contains(target))
        {
            await RunStepsAsync(options.StepsPerAction, deliver: true, cancellationToken).ConfigureAwait(false);
            return;
        }

        bool healthy = ClusterIsHealthy();

        Task<ClientOperation> append = history.AppendUniqueAsync(
            cluster, Node(leader), options.PartitionId, "Greeting", cancellationToken);

        Task<RaftOperationStatus> transfer = Node(leader).Manager
            .TransferLeadershipAsync(options.PartitionId, target, cancellationToken);

        await cluster.RunUntilAsync(
            () => Task.FromResult(append.IsCompleted && transfer.IsCompleted),
            options.StepsPerAction * OutageElectionBudgetFactor * 2,
            options.AdvanceMillisecondsPerStep,
            cancellationToken).ConfigureAwait(false);

        await append.ConfigureAwait(false);
        RaftOperationStatus answer = await transfer.ConfigureAwait(false);

        transferAnswers[answer] = transferAnswers.GetValueOrDefault(answer) + 1;

        if (healthy && !DefiniteTransferAnswers.Contains(answer))
        {
            Assert.Fail(
                $"transfer-definite-when-healthy: {leader} was asked to hand leadership to {target} with no " +
                $"fault active, and answered {answer}. With no fault the answer must be one of " +
                $"[{string.Join(", ", DefiniteTransferAnswers)}]: a transient failure here is a move the caller " +
                "drops, and nothing else in the run would notice.");
        }

        await RunStepsAsync(OutageRecoverySteps, deliver: true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Steps a late broadcast holds the follower's traffic after the write completes.</summary>
    private const int LateBroadcastHoldSteps = 2;

    /// <summary>
    /// Holds one follower's traffic with a late wire copy, writes one entry through the leader, and
    /// then lets the traffic through. See <see cref="RandomScenarioActionKind.LateBroadcast"/>.
    ///
    /// <para>The write is awaited while the cluster is driven: the leader reaches its quorum through
    /// the other follower, so it commits the entry and retypes it while the held follower's copy is
    /// still waiting. When the target no longer follows, or no single leader is visible, the action
    /// only lets time pass. The plan still records it, because what it did depends on the state it
    /// found, and a replay finds the same state.</para>
    ///
    /// <para>The follower is released in a <c>finally</c>, so a failure inside the window does not
    /// leave a frozen node for the teardown to time out on.</para>
    /// </summary>
    private async Task RunLateBroadcastAsync(string target, CancellationToken cancellationToken)
    {
        RandomScenarioObservation observation = await ObserveAsync(cancellationToken).ConfigureAwait(false);

        if (observation.Leader is null
            || observation.Leader == target
            || !observation.Running.Contains(target))
        {
            await RunStepsAsync(options.StepsPerAction, deliver: true, cancellationToken).ConfigureAwait(false);
            return;
        }

        // The first type each new id carried when it reached the target. An id the target already
        // held is not a first sight, so only ids above its log before the hold count.
        long heldBefore = Store(target).GetMaxLog(options.PartitionId);
        Dictionary<long, RaftLogType> firstSight = [];

        void Observe(string from, string to, IReadOnlyList<RaftLog> logs)
        {
            if (to != target)
                return;

            lock (firstSight)
            {
                foreach (RaftLog log in logs)
                {
                    if (log.Id > heldBefore)
                        firstSight.TryAdd(log.Id, log.Type);
                }
            }
        }

        cluster.Transport.AppendLogsDelivered += Observe;
        cluster.Transport.SetLateSerialization(target, true);
        cluster.Transport.FreezeEndpoint(target);

        try
        {
            Task<ClientOperation> append = history.AppendUniqueAsync(
                cluster, Node(observation.Leader), options.PartitionId, "Greeting", cancellationToken);

            await cluster.RunUntilAsync(
                () => Task.FromResult(append.IsCompleted),
                options.StepsPerAction * OutageElectionBudgetFactor,
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);

            await append.ConfigureAwait(false);

            await RunStepsAsync(LateBroadcastHoldSteps, deliver: true, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            cluster.Transport.ThawEndpoint(target);
            cluster.Transport.SetLateSerialization(target, false);
        }

        await RunStepsAsync(options.StepsPerAction, deliver: true, cancellationToken).ConfigureAwait(false);

        cluster.Transport.AppendLogsDelivered -= Observe;

        lock (firstSight)
        {
            if (firstSight.Values.Any(type => type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                LateBroadcastsCommittedFirst++;
        }
    }

    /// <summary>
    /// Late broadcasts whose target first saw a new entry as a committed row. The state the action
    /// exists for; a family where this stays at zero never reached it.
    /// </summary>
    public int LateBroadcastsCommittedFirst { get; private set; }

    /// <summary>
    /// What the leader believed about a follower's durable log just before the follower crashed.
    /// </summary>
    /// <param name="Leader">The leader that holds the record.</param>
    /// <param name="Reported">The follower's last reported durable commit frontier.</param>
    /// <param name="LeaderCommit">The leader's commit index when the record was read.</param>
    private sealed record ReportedDurability(string Leader, long Reported, long LeaderCommit);

    /// <summary>
    /// Reads the leader's record of <paramref name="endpoint"/>'s durable frontier, or null when no
    /// single other node leads or the leader has no report from it.
    /// </summary>
    private async Task<ReportedDurability?> ReadReportedDurabilityAsync(
        string endpoint,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views = await cluster
            .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        List<RaftPartitionView> leaders = views
            .Where(view => view.Role == RaftNodeState.Leader && view.Endpoint != endpoint)
            .ToList();

        if (leaders.Count != 1)
            return null;

        RaftFollowerProgress? progress = Node(leaders[0].Endpoint).Manager
            .GetFollowerProgress(options.PartitionId, endpoint);

        if (progress is null || progress.DurableFrontier <= 0)
            return null;

        return new ReportedDurability(leaders[0].Endpoint, progress.DurableFrontier, leaders[0].CommitIndex);
    }

    /// <summary>
    /// Rule <c>reported-durable-survives-crash</c>: every id a follower reported to its leader as
    /// committed and durable is still there, resolved, after the follower crashes.
    ///
    /// <para><b>Why the report is a promise.</b> The leader holds WAL retention at the follower's
    /// reported durable frontier plus one, and compacts below it. An id the follower reported and then
    /// lost can therefore be gone from both logs, and the only repair left is a snapshot. The report
    /// also has to name a resolved row, not only a present one: a restarted follower holding the id as
    /// <c>Proposed</c> is above its own commit frontier, and it refuses the leader's backfill anchored
    /// on the compacted id (DST FINDING 7).</para>
    ///
    /// <para><b>Why the rule is sound across a stale record.</b> The leader keeps a follower's last
    /// report until it steps down, so the record can be older than the crash. A true report stays
    /// true: resolved durable rows are never truncated, and ids that compaction or a snapshot removed
    /// are skipped. The ids checked stop at the leader's commit index, so a report about entries the
    /// leader itself has not committed is never in scope.</para>
    /// </summary>
    private void CheckReportedDurableSurvivedCrash(string endpoint, ReportedDurability? before)
    {
        if (before is null)
            return;

        WAL.SimulatedWAL store = Store(endpoint);
        long covered = store.Snapshot().Partition(options.PartitionId)?.CoveredThrough ?? 0;
        long bound = Math.Min(before.Reported, before.LeaderCommit);

        Dictionary<long, RaftLogType> held = store
            .ReadLogsRange(options.PartitionId, covered + 1)
            .ToDictionary(log => log.Id, log => log.Type);

        List<string> broken = [];

        for (long id = Math.Max(covered + 1, 1); id <= bound; id++)
        {
            if (!held.TryGetValue(id, out RaftLogType type))
                broken.Add($"{id}:absent");
            else if (type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                broken.Add($"{id}:{type}");
        }

        if (broken.Count == 0)
            return;

        Assert.Fail(
            $"reported-durable-survives-crash: {endpoint} reported its durable commit frontier as " +
            $"{before.Reported} to leader {before.Leader} (leader commit {before.LeaderCommit}), and " +
            $"after the crash its store no longer holds these ids resolved: [{string.Join(", ", broken)}]. " +
            $"Covered by compaction or a snapshot through {covered}. The leader's WAL retention trusts " +
            "this report, so the entries can be gone from both logs.");
    }

    /// <summary>
    /// Fills in a target the generator left open.
    ///
    /// <para>An episode decides its steps before they happen, so "write at the leader" cannot name
    /// a node yet — the leader it means is the one in place when the step runs, which is usually not
    /// the one in place when the episode began. A leaderless cluster still takes the write: the
    /// answer is a refusal, and a refused append that must not reach the log is a check of its
    /// own.</para>
    /// </summary>
    private async Task<RandomScenarioAction> ResolveAsync(
        RandomScenarioAction action,
        CancellationToken cancellationToken)
    {
        if (action.Target is not null)
            return action;

        if (action.Kind is not (RandomScenarioActionKind.AppendAtLeader
            or RandomScenarioActionKind.AppendAtFollower
            or RandomScenarioActionKind.LeaderOutage
            or RandomScenarioActionKind.AppendAcrossOutage
            or RandomScenarioActionKind.AppendAcrossQuorumLoss
            or RandomScenarioActionKind.Checkpoint))
        {
            return action;
        }

        RandomScenarioObservation observation = await ObserveAsync(cancellationToken).ConfigureAwait(false);

        if (observation.Leader is not null)
            return action with { Target = observation.Leader };

        // An outage with nobody to cut off is not an outage. It becomes an idle action rather than
        // a silent skip, so the plan still accounts for the step.
        if (action.Kind is RandomScenarioActionKind.LeaderOutage
            or RandomScenarioActionKind.AppendAcrossOutage
            or RandomScenarioActionKind.AppendAcrossQuorumLoss
            or RandomScenarioActionKind.Checkpoint)
            return new RandomScenarioAction(action.Index, RandomScenarioActionKind.Idle);

        return observation.Running.Count > 0
            ? action with { Target = observation.Running[0] }
            : new RandomScenarioAction(action.Index, RandomScenarioActionKind.Idle);
    }

    /// <summary>
    /// Cuts one endpoint off in both directions until the rest of the cluster elects somebody else,
    /// then lets it back and gives the reunion a few steps to settle.
    ///
    /// <para><b>Why the leader alone and not the whole wire.</b> The first version held every link.
    /// It produced the election and an unbounded backlog with it: no call can complete while the
    /// wire is held, the senders keep sending, and releasing the pile turned a twenty-second run
    /// into a six-minute one. A partition drops instead of storing, so the cost is flat.</para>
    ///
    /// <para><b>Why skipping delivery was never enough.</b> The transport sends inline unless it is
    /// told otherwise, so a step that merely declines to flush the queue leaves the cluster talking
    /// normally: the leader keeps its heartbeats and no timeout expires. The action a plan recorded
    /// as an outage cost the run nothing, and a validation run over a reintroduced defect found the
    /// election it was supposed to cause missing.</para>
    ///
    /// <para>The endpoint is healed in a <c>finally</c>. A run that failed mid-outage must not also
    /// leave a partitioned node behind for the teardown to time out on.</para>
    /// </summary>
    private async Task RunLeaderOutageAsync(string endpoint, CancellationToken cancellationToken)
    {
        cluster.Transport.PartitionNode(endpoint);

        try
        {
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    IReadOnlyList<RaftPartitionView> views = await cluster
                        .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    return views.Any(view =>
                        view.Role == RaftNodeState.Leader
                        && !string.Equals(view.Endpoint, endpoint, StringComparison.Ordinal));
                },
                options.StepsPerAction * OutageElectionBudgetFactor,
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            cluster.Transport.HealPartition(endpoint);
        }

        await RunStepsAsync(OutageRecoverySteps, deliver: true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Performs one action. Returns whether the steps that follow it should deliver messages.
    ///
    /// <para>Lifecycle actions are guarded against the state they expect: a restart is applied only
    /// to a crashed node, a resume only to a paused one. The guard is not defensive clutter — the
    /// generator draws from an observation taken a step earlier, and applying a restart to a
    /// running node would tear down a healthy manager for no reason the plan records.</para>
    /// </summary>
    private async Task<bool> ApplyAsync(RandomScenarioAction action, CancellationToken cancellationToken)
    {
        switch (action.Kind)
        {
            case RandomScenarioActionKind.ReadAtNode:
                if (Node(action.Target!).LifecycleStatus == SimulationNodeLifecycleStatus.Running)
                {
                    Task<ClientOperation> read = history.ReadAsync(
                        cluster, Node(action.Target!), options.PartitionId, cancellationToken);

                    await cluster.RunUntilAsync(
                        () => Task.FromResult(read.IsCompleted),
                        ReadBudgetSteps(),
                        options.AdvanceMillisecondsPerStep,
                        cancellationToken).ConfigureAwait(false);

                    await read.ConfigureAwait(false);
                }

                return true;

            case RandomScenarioActionKind.AppendAtLeader:
            case RandomScenarioActionKind.AppendAtFollower:
                await history
                    .AppendUniqueAsync(
                        cluster, Node(action.Target!), options.PartitionId, "Greeting", cancellationToken)
                    .ConfigureAwait(false);
                return true;

            case RandomScenarioActionKind.CrashNode:
            {
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Running)
                {
                    ReportedDurability? before = await ReadReportedDurabilityAsync(node.Endpoint, cancellationToken)
                        .ConfigureAwait(false);

                    await cluster.CrashNodeAsync(node, cancellationToken).ConfigureAwait(false);

                    CheckReportedDurableSurvivedCrash(node.Endpoint, before);
                }

                return true;
            }

            case RandomScenarioActionKind.RestartNode:
            {
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Crashed)
                    await cluster.RestartNodeAsync(node, cancellationToken).ConfigureAwait(false);

                return true;
            }

            case RandomScenarioActionKind.RestartNodeBlank:
            {
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Crashed)
                    await cluster.RestartNodeBlankAsync(node, cancellationToken).ConfigureAwait(false);

                return true;
            }

            case RandomScenarioActionKind.PauseNode:
            {
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Running)
                    node.Pause();

                return true;
            }

            case RandomScenarioActionKind.ResumeNode:
            {
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Paused)
                    node.Resume();

                return true;
            }

            case RandomScenarioActionKind.BlockLink:
                cluster.Transport.BlockLink(action.Target!, action.Secondary!);
                return true;

            case RandomScenarioActionKind.UnblockLink:
                cluster.Transport.UnblockLink(action.Target!, action.Secondary!);
                return true;

            case RandomScenarioActionKind.DuplicateLink:
                cluster.Transport.SetLinkDuplication(action.Target!, action.Secondary!, (int)action.Value);
                return true;

            case RandomScenarioActionKind.StarveDisk:
                Store(action.Target!).SetOutOfSpace(true, options.PartitionId);
                return true;

            case RandomScenarioActionKind.FreeDisk:
                Store(action.Target!).SetOutOfSpace(false, options.PartitionId);
                return true;

            case RandomScenarioActionKind.FailWrites:
                Store(action.Target!).FailNextWrites((int)action.Value, options.PartitionId);
                return true;

            case RandomScenarioActionKind.SlowDisk:
                Store(action.Target!).WriteLatencyMilliseconds = action.Value;
                return true;

            case RandomScenarioActionKind.FastDisk:
                Store(action.Target!).WriteLatencyMilliseconds = 0;
                return true;

            case RandomScenarioActionKind.Checkpoint:
            {
                // Only a leader can write one, and leadership may have moved since the draw. A
                // checkpoint nobody can write is not an error: the run simply had no leader at that
                // moment, which is a state the plan is entitled to reach.
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Running)
                {
                    // Started, then driven, rather than simply awaited. A checkpoint needs a quorum
                    // like any other write, and a quorum needs the cluster to keep running — a
                    // plain await would stop the harness stepping and leave the call to time out on
                    // the wall clock instead of committing.
                    Task<RaftReplicationResult> checkpoint =
                        node.Manager.ReplicateCheckpoint(options.PartitionId, cancellationToken);

                    bool settled = await cluster.RunUntilAsync(
                        () => Task.FromResult(checkpoint.IsCompleted),
                        options.StepsPerAction * OutageElectionBudgetFactor,
                        options.AdvanceMillisecondsPerStep,
                        cancellationToken).ConfigureAwait(false);

                    // Never block on the call once the budget is spent. The library retries a
                    // checkpoint refused as "active proposal" on the wall clock and resolves it
                    // only through leader ticks, which this harness drives; a plain await here
                    // stopped the stepping and could not complete (a leader churning under
                    // check-quorum consumed the budget with elections on one seed). The write may
                    // still land while later actions drive the cluster; its failure is observed,
                    // not thrown, because a budget overrun is a fact about this run's timing.
                    if (settled)
                        await checkpoint.ConfigureAwait(false);
                    else
                        _ = checkpoint.ContinueWith(
                            static t => _ = t.Exception,
                            CancellationToken.None,
                            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                            TaskScheduler.Default);
                }

                return true;
            }

            case RandomScenarioActionKind.HoldRetention:
                // Index 1 pins the whole log. A hold further up would be a weaker version of the
                // same idea and would need the run to know where the frontier is.
                Store(action.Target!).SetRetentionHold(options.PartitionId, 1);
                return true;

            case RandomScenarioActionKind.ReleaseRetention:
                Store(action.Target!).ClearRetentionHold(options.PartitionId);
                return true;

            case RandomScenarioActionKind.HangSnapshotExport:
            {
                // A crashed node has no process to hang, and a crash releases every hang anyway.
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus is SimulationNodeLifecycleStatus.Running or SimulationNodeLifecycleStatus.Paused)
                    node.StateTransfer.HangNextExports((int)action.Value);

                return true;
            }

            default:
                return true;
        }
    }

    /// <summary>
    /// Starts a client append, cuts the leader off underneath it, and waits for whatever answer the
    /// client is finally given.
    ///
    /// <para><b>Why this is worth its complexity.</b> Every other action in the vocabulary happens
    /// between client operations, so the client is never mid-call when the cluster changes shape. A
    /// client that is never mid-call cannot be misinformed about its own write, and the answers
    /// that go wrong are decided exactly when a proposal outlives the leader that accepted it: the
    /// entry is already appended, the leader can no longer speak for it, and the next leader may
    /// still commit it. Whether the client is told "refused" or "unknown" there is the difference
    /// between a correct history and a phantom write.</para>
    ///
    /// <para>The append is started and not awaited while the cluster is driven, because the
    /// proposal runs on the library's own threads and the harness must keep stepping for the
    /// election to happen at all. It is awaited before the action ends, so only one client
    /// operation is ever in flight and the history's ordering stays single-threaded.</para>
    ///
    /// <para>The answer is recorded whatever it is. A refusal, an acknowledgement, and a timeout
    /// are all legal here; the history checker decides which of them the log then contradicts.</para>
    /// </summary>
    private async Task RunAppendAcrossOutageAsync(string endpoint, CancellationToken cancellationToken)
    {
        Task<ClientOperation> append = history.AppendUniqueAsync(
            cluster, Node(endpoint), options.PartitionId, "Greeting", cancellationToken);

        cluster.Transport.PartitionNode(endpoint);

        try
        {
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    IReadOnlyList<RaftPartitionView> views = await cluster
                        .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
                        .ConfigureAwait(false);

                    return views.Any(view =>
                        view.Role == RaftNodeState.Leader
                        && !string.Equals(view.Endpoint, endpoint, StringComparison.Ordinal));
                },
                options.StepsPerAction * OutageElectionBudgetFactor,
                options.AdvanceMillisecondsPerStep,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            cluster.Transport.HealPartition(endpoint);
        }

        // The cut leader learns of the new term once it is reachable again, and that is what
        // resolves a proposal it can no longer finish. Stepping is what delivers that news.
        await cluster.RunUntilAsync(
            () => Task.FromResult(append.IsCompleted),
            options.StepsPerAction * OutageElectionBudgetFactor,
            options.AdvanceMillisecondsPerStep,
            cancellationToken).ConfigureAwait(false);

        await append.ConfigureAwait(false);

        await RunStepsAsync(OutageRecoverySteps, deliver: true, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Steps a leader spends without a quorum. Short enough that it usually keeps its term.</summary>
    private const int QuorumLossSteps = 2;

    /// <summary>
    /// Starts a client append, takes the leader's quorum away underneath it, and gives it back
    /// before an election can replace the leader.
    ///
    /// <para><b>What this reaches that nothing else does.</b> The followers receive the entries and
    /// the leader hears nothing back, so it still owes the client an answer for entries that are, in
    /// fact, already replicated. Whether those entries resolve
    /// depends on the leader retrying replication of its own accord — Raft's own requirement, and
    /// the thing a partition wedges without. Every other action either leaves the quorum intact, so
    /// the proposal resolves at once, or removes the leader, so the next one inherits the
    /// problem.</para>
    ///
    /// <para><b>Why the window is measured in a couple of steps.</b> A leader with no quorum waits
    /// ten <b>real</b> seconds inside its quorum wait. The window has to be shorter than the
    /// election timeout to keep the leader, and far shorter than that wait to keep the run cheap;
    /// two steps of simulated time is both.</para>
    ///
    /// <para>The links are restored in a <c>finally</c>, so a failure inside the window cannot leave
    /// a partitioned cluster for the teardown to time out on.</para>
    /// </summary>
    private async Task RunAppendAcrossQuorumLossAsync(string endpoint, CancellationToken cancellationToken)
    {
        Task<ClientOperation> append = history.AppendUniqueAsync(
            cluster, Node(endpoint), options.PartitionId, "Greeting", cancellationToken);

        // The acknowledgement direction only. Cutting both directions would drop the entries too,
        // and ordinary replication then simply sends them again — the leader never has to remember
        // anything. Dropping only the replies leaves every follower holding the data and the leader
        // believing nobody took it, which is the state an unresolved proposal wedges in.
        List<string> peers = cluster.Nodes
            .Select(node => node.Endpoint)
            .Where(peer => !string.Equals(peer, endpoint, StringComparison.Ordinal))
            .ToList();

        foreach (string peer in peers)
            cluster.Transport.BlockLink(peer, endpoint);

        try
        {
            await RunStepsAsync(QuorumLossSteps, deliver: true, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            foreach (string peer in peers)
                cluster.Transport.UnblockLink(peer, endpoint);
        }

        await cluster.RunUntilAsync(
            () => Task.FromResult(append.IsCompleted),
            options.StepsPerAction * OutageElectionBudgetFactor,
            options.AdvanceMillisecondsPerStep,
            cancellationToken).ConfigureAwait(false);

        await append.ConfigureAwait(false);

        await RunStepsAsync(OutageRecoverySteps, deliver: true, cancellationToken).ConfigureAwait(false);
    }

    // ── Observing and stepping ────────────────────────────────────────────

    /// <summary>
    /// Reads the state the generator is allowed to see.
    ///
    /// <para>A running node that has not materialized the partition is left out of
    /// <see cref="RandomScenarioObservation.Running"/>. It cannot serve a client and it holds no
    /// opinion about the partition, so offering it as a target would produce actions that fail for
    /// reasons the plan does not describe.</para>
    ///
    /// <para>A leader is reported only when exactly one node claims the role. Two claimants means
    /// the cluster is mid-election as far as this reading is concerned, and the honest answer is
    /// that there is no leader to write to.</para>
    /// </summary>
    private async Task<RandomScenarioObservation> ObserveAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views = await cluster
            .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        HashSet<string> withView = views.Select(view => view.Endpoint).ToHashSet(StringComparer.Ordinal);

        List<string> running = [];
        List<string> crashed = [];
        List<string> paused = [];

        foreach (SimulationNode node in cluster.Nodes)
        {
            switch (node.LifecycleStatus)
            {
                case SimulationNodeLifecycleStatus.Running when withView.Contains(node.Endpoint):
                    running.Add(node.Endpoint);
                    break;

                case SimulationNodeLifecycleStatus.Crashed:
                    crashed.Add(node.Endpoint);
                    break;

                case SimulationNodeLifecycleStatus.Paused:
                    paused.Add(node.Endpoint);
                    break;
            }
        }

        List<RaftPartitionView> leaders = views
            .Where(view => view.Role == RaftNodeState.Leader && running.Contains(view.Endpoint))
            .ToList();

        return new RandomScenarioObservation
        {
            Running = running,
            Crashed = crashed,
            Paused = paused,
            Leader = leaders.Count == 1 ? leaders[0].Endpoint : null,
        };
    }

    /// <summary>Runs steps, checking every per-step invariant after each one.</summary>
    private async Task RunStepsAsync(int stepCount, bool deliver, CancellationToken cancellationToken)
    {
        for (int step = 0; step < stepCount; step++)
        {
            await cluster
                .StepAsync(options.AdvanceMillisecondsPerStep, cancellationToken, deliver)
                .ConfigureAwait(false);

            await invariants.CheckAsync(cluster, options.PartitionId, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Waits for exactly one leader among the running nodes and returns it, or null when none
    /// appeared inside the budget.
    ///
    /// <para>Null is returned rather than thrown. A plan is allowed to leave the cluster without a
    /// leader — that is what a fault does — and the run's verdict belongs to the checks at the end,
    /// not to a helper part-way through.</para>
    /// </summary>
    private async Task<SimulationNode?> ElectAsync(CancellationToken cancellationToken)
    {
        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                IReadOnlyList<RaftPartitionView> views = await cluster
                    .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
                    .ConfigureAwait(false);

                return views.Count(view => view.Role == RaftNodeState.Leader) == 1;
            },
            options.RecoveryStepBudget,
            options.AdvanceMillisecondsPerStep,
            cancellationToken).ConfigureAwait(false);

        if (!elected)
            return null;

        foreach (SimulationNode node in cluster.Nodes)
        {
            if (node.LifecycleStatus != SimulationNodeLifecycleStatus.Running)
                continue;

            RaftPartitionView? view = await node
                .GetPartitionViewAsync(options.PartitionId, cancellationToken)
                .ConfigureAwait(false);

            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        return null;
    }

    private async Task<long> HighestCommitIndexAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views = await cluster
            .GetPartitionViewsAsync(options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        return views.Count == 0 ? -1 : views.Max(view => view.CommitIndex);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private SimulationNode Node(string endpoint) =>
        cluster.Nodes.First(node => node.Endpoint == endpoint);

    private WAL.SimulatedWAL Store(string endpoint) =>
        Node(endpoint).SimulatedWal
        ?? throw new InvalidOperationException($"{endpoint} has no simulated store.");

    /// <summary>
    /// Refuses to run against plain in-memory stores.
    ///
    /// <para>Half the fault vocabulary is storage. A run that silently skipped those actions would
    /// report a passing random search that never touched a disk, which is worse than not running
    /// at all.</para>
    /// </summary>
    private void RequireSimulatedStores()
    {
        foreach (SimulationNode node in cluster.Nodes)
        {
            if (node.SimulatedWal is null)
            {
                throw new InvalidOperationException(
                    $"{node.Endpoint} has no simulated store. A random run needs " +
                    $"{nameof(SimulationClusterOptions.UseSimulatedWal)} on every node.");
            }
        }
    }
}
