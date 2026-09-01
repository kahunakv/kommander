using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.History;
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
    }

    /// <summary>The plan, as far as it has been drawn. Populated even when the run fails.</summary>
    public IReadOnlyList<RandomScenarioAction> Actions => actions;

    /// <summary>What the clients were told, as far as the run got.</summary>
    public ClientHistory History => history;

    /// <summary>Per-step invariant checks performed so far.</summary>
    public int InvariantChecks => invariants.ChecksRun;

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
                    $"compactedThrough={store?.CompactedThrough} missing=[{string.Join(",", store?.MissingIds ?? [])}]");
            }

            Assert.Fail(
                $"The healed cluster did not converge within {options.RecoveryStepBudget} steps. " +
                $"Final state: {string.Join(" | ", lines)}");
        }

        await invariants.CheckConvergedAsync(cluster, options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        SimulationNode reader = cluster.Nodes.First(node => node.HasLiveManager);

        // The reader's own compaction floor, not the cluster's. What this node threw away is what
        // its log cannot answer for, and the history is read against this node.
        long compactedThrough = reader.SimulatedWal?.Snapshot()
            .Partition(options.PartitionId)?.CompactedThrough ?? -1;

        ClientHistoryChecker.Check(
            history,
            reader.Wal.ReadLogsRange(options.PartitionId, 0),
            cluster.StepNumber,
            compactedThrough);
    }

    /// <summary>Steps a healed cluster is given to converge before anybody writes to it.</summary>
    private const int IdleConvergenceStepBudget = 300;

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

        Assert.Fail(
            $"idle-convergence: the healed cluster did not converge in {IdleConvergenceStepBudget} " +
            $"steps without a client write. Frontiers: {state}. A node that reported a committed " +
            "prefix must be caught up by the heartbeat path alone.");
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

        bool deliver = await ApplyAsync(resolved, cancellationToken).ConfigureAwait(false);

        await RunStepsAsync(options.StepsPerAction, deliver, cancellationToken).ConfigureAwait(false);

        return resolved;
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
                    await cluster.CrashNodeAsync(node, cancellationToken).ConfigureAwait(false);

                return true;
            }

            case RandomScenarioActionKind.RestartNode:
            {
                SimulationNode node = Node(action.Target!);

                if (node.LifecycleStatus == SimulationNodeLifecycleStatus.Crashed)
                    await cluster.RestartNodeAsync(node, cancellationToken).ConfigureAwait(false);

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

                    await cluster.RunUntilAsync(
                        () => Task.FromResult(checkpoint.IsCompleted),
                        options.StepsPerAction * OutageElectionBudgetFactor,
                        options.AdvanceMillisecondsPerStep,
                        cancellationToken).ConfigureAwait(false);

                    await checkpoint.ConfigureAwait(false);
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
