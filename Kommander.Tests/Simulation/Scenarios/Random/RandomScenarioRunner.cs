using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.History;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Replay;

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
                actions.Add(action);

                bool deliver = await ApplyAsync(action, cancellationToken).ConfigureAwait(false);

                await RunStepsAsync(options.StepsPerAction, deliver, cancellationToken)
                    .ConfigureAwait(false);
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
            {
                actions.Add(action);

                bool deliver = await ApplyAsync(action, cancellationToken).ConfigureAwait(false);

                await RunStepsAsync(options.StepsPerAction, deliver, cancellationToken)
                    .ConfigureAwait(false);
            }
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
        };
    }

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

        Assert.True(
            converged,
            $"The healed cluster did not converge within {options.RecoveryStepBudget} steps.");

        await invariants.CheckConvergedAsync(cluster, options.PartitionId, cancellationToken)
            .ConfigureAwait(false);

        SimulationNode reader = cluster.Nodes.First(node => node.HasLiveManager);

        ClientHistoryChecker.Check(
            history,
            reader.Wal.ReadLogsRange(options.PartitionId, 0),
            cluster.StepNumber);
    }

    // ── Applying one action ───────────────────────────────────────────────

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
            case RandomScenarioActionKind.Quiet:
                return false;

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

            default:
                return true;
        }
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
