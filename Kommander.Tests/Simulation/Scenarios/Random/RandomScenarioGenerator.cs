using Kommander.Tests.Simulation.Random;

namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// Draws the next action of a random run from the seed and the cluster's current state.
///
/// <para><b>Why the generator holds the fault bookkeeping.</b> Every fault it emits is paired with
/// the action that ends it, and the pairing lives here rather than in the runner so that one place
/// decides both what may break and what must be repaired. A runner that healed on its own would
/// produce a plan that does not describe the run.</para>
///
/// <para><b>Two rules keep a run productive rather than merely random.</b> A budget caps how many
/// faults that cost the cluster a node are active at once, so a proposal always has a quorum to
/// reach; and a fault is healed once it is older than the age bound whether or not the draw asks
/// for it. Without the first, the run pays ten real seconds per proposal into a cluster that cannot
/// commit. Without the second, faults accumulate and every later action lands on a wedged
/// cluster.</para>
///
/// <para>Every decision goes through <see cref="SimulationRandom"/> under a named choice, so the
/// whole plan is a function of the seed and can be compared between two runs of it.</para>
/// </summary>
public sealed class RandomScenarioGenerator
{
    /// <summary>
    /// A fault the run has started and not yet ended.
    /// </summary>
    /// <param name="Key">Identity, so the same fault is never started twice.</param>
    /// <param name="StartedAtAction">Action index it began at, for the age bound.</param>
    /// <param name="Heal">The action that ends it, or null when it ends on its own.</param>
    /// <param name="CostsQuorum">Whether it takes a node out of the cluster's working majority.</param>
    private sealed record ActiveFault(
        string Key,
        int StartedAtAction,
        RandomScenarioAction? Heal,
        bool CostsQuorum);

    private readonly SimulationRandom random;
    private readonly RandomScenarioOptions options;
    private readonly List<ActiveFault> active = [];

    /// <summary>
    /// Actions already decided, waiting their turn. See <see cref="StartEpisode"/>.
    /// </summary>
    private readonly Queue<(RandomScenarioAction Action, string? ClearsKey)> pending = new();

    private int actionIndex;

    public RandomScenarioGenerator(SimulationRandom random, RandomScenarioOptions options)
    {
        ArgumentNullException.ThrowIfNull(random);
        ArgumentNullException.ThrowIfNull(options);

        this.random = random;
        this.options = options;
    }

    /// <summary>Faults that are active right now. A test can assert the budget held.</summary>
    public int ActiveFaultCount => active.Count;

    /// <summary>Faults active right now that cost the cluster a node.</summary>
    public int ImpairedNodeCount => active.Count(fault => fault.CostsQuorum);

    /// <summary>
    /// Chooses the next action.
    ///
    /// <para>The age bound is applied before the draw, so an overdue heal always wins over a new
    /// fault. That ordering is what makes the bound a guarantee rather than a preference.</para>
    /// </summary>
    public RandomScenarioAction Next(RandomScenarioObservation observation)
    {
        ArgumentNullException.ThrowIfNull(observation);

        int index = actionIndex++;

        // An episode already decided what happens next. It runs before anything else, including the
        // age bound: the bound exists to stop a fault outliving its usefulness, and an episode is
        // the fault being used.
        if (pending.Count > 0)
        {
            (RandomScenarioAction queued, string? clearsKey) = pending.Dequeue();

            if (clearsKey is not null)
                active.RemoveAll(fault => fault.Key == clearsKey);

            return queued with { Index = index };
        }

        // A fault that ends on its own needs no action. It is dropped silently, because emitting a
        // heal for it would put a line in the plan that the run never performed.
        active.RemoveAll(fault => fault.Heal is null && IsOverdue(fault, index));

        ActiveFault? overdue = active.FirstOrDefault(fault => IsOverdue(fault, index));

        if (overdue is not null)
        {
            active.Remove(overdue);
            return overdue.Heal! with { Index = index };
        }

        return Draw(index, observation);
    }

    /// <summary>
    /// The actions that return the cluster to health, in the order the run broke it.
    ///
    /// <para>Called once the action budget is spent. A run's last phase must be a healthy cluster,
    /// because convergence is only a promise about a cluster that can still talk to itself, and a
    /// convergence check against a partitioned cluster would report a defect that is really the
    /// fault the run injected.</para>
    /// </summary>
    public IReadOnlyList<RandomScenarioAction> HealAll()
    {
        List<RandomScenarioAction> plan = [];

        foreach (ActiveFault fault in active)
        {
            if (fault.Heal is not null)
                plan.Add(fault.Heal with { Index = actionIndex++ });
        }

        active.Clear();
        return plan;
    }

    private bool IsOverdue(ActiveFault fault, int index) =>
        index - fault.StartedAtAction >= options.MaxFaultAgeInActions;

    private bool HasBudget => ImpairedNodeCount < options.MaxImpairedNodes;

    private bool HasStorageFaultOn(string endpoint) =>
        active.Any(fault => fault.Key.StartsWith("disk/", StringComparison.Ordinal)
                            && fault.Key.EndsWith("/" + endpoint, StringComparison.Ordinal));

    private RandomScenarioAction Draw(int index, RandomScenarioObservation observation)
    {
        // Categories are weighted, and only the ones with a legal action take part in the draw.
        // Legality is decided without consuming entropy: a category that drew and then discarded
        // would change every later choice depending on cluster state, and two runs of one seed
        // would still agree only by luck.
        List<(int Weight, int Category)> categories = [];

        void Offer(int weight, int category)
        {
            if (weight > 0)
                categories.Add((weight, category));
        }

        bool canClient = observation.Running.Count > 0;
        bool canNetwork = HasBudget && observation.Running.Count >= 2;
        bool canStorage = HasBudget && observation.Running.Count > 0;
        bool canLifecycle = HasBudget && observation.Running.Count > 0;
        bool canHeal = active.Count > 0;

        // A fault nobody writes through teaches nothing, so client operations weigh more while one
        // is active. Measured, not guessed: a validation run over a reintroduced defect showed the
        // search reaching a fault often and a write-during-that-fault rarely.
        int clientWeight = active.Count > 0
            ? options.ClientWeight * Math.Max(1, options.ClientWeightDuringFault)
            : options.ClientWeight;

        if (canClient) Offer(clientWeight, 0);
        Offer(options.IdleWeight, 1);
        if (canNetwork) Offer(options.NetworkFaultWeight, 2);
        if (canStorage) Offer(options.StorageFaultWeight, 3);
        if (canLifecycle) Offer(options.LifecycleFaultWeight, 4);
        if (canHeal) Offer(options.HealWeight, 5);
        if (observation.Running.Count > 0) Offer(options.MaintenanceWeight, 7);

        // An outage costs the cluster its leader for as long as it lasts, so it takes the same
        // budget a fault does. It needs a leader to cut off, and it heals inside its own action, so
        // it never joins the fault table.
        if (HasBudget && observation.Leader is not null) Offer(options.OutageWeight, 6);

        int total = categories.Sum(entry => entry.Weight);

        if (total <= 0)
            return new RandomScenarioAction(index, RandomScenarioActionKind.Idle);

        int draw = random.NextInt("action-category", 0, total);
        int category = categories[^1].Category;

        foreach ((int weight, int candidate) in categories)
        {
            if (draw < weight)
            {
                category = candidate;
                break;
            }

            draw -= weight;
        }

        return category switch
        {
            0 => DrawClient(index, observation),
            2 => DrawNetwork(index, observation),
            3 => DrawStorage(index, observation),
            4 => DrawLifecycle(index, observation),
            5 => DrawHeal(index),
            7 => DrawMaintenance(index, observation),
            // Two thirds of the outages carry a client write into the disruption. That overlap is
            // the only place a client can be told the wrong thing about its own operation, and the
            // two write variants disrupt different halves of it: one takes the leader away for
            // good, the other takes its quorum away and gives it back.
            6 => new RandomScenarioAction(
                index,
                random.NextInt("outage-kind", 0, 3) switch
                {
                    0 => RandomScenarioActionKind.AppendAcrossOutage,
                    1 => RandomScenarioActionKind.AppendAcrossQuorumLoss,
                    _ => RandomScenarioActionKind.LeaderOutage,
                },
                observation.Leader),
            _ => DrawIdle(index),
        };
    }

    private static RandomScenarioAction DrawIdle(int index) =>
        new(index, RandomScenarioActionKind.Idle);

    private RandomScenarioAction DrawClient(int index, RandomScenarioObservation observation)
    {
        // A leader whose own disk is refusing writes cannot commit its own entry, so a proposal to
        // it waits out the ten real seconds of the quorum timeout. The state is worth reaching and
        // the wait is not worth paying repeatedly, so the run reaches it through the fault and
        // writes elsewhere while it lasts.
        bool leaderUsable = observation.Leader is not null
                            && observation.Running.Contains(observation.Leader)
                            && !HasStorageFaultOn(observation.Leader);

        List<string> followers = observation.Running
            .Where(endpoint => endpoint != observation.Leader)
            .ToList();

        bool preferFollower = random.NextInt("client-target", 0, 4) == 0;

        if ((preferFollower || !leaderUsable) && followers.Count > 0)
        {
            int pick = random.NextInt("client-follower", 0, followers.Count);

            return new RandomScenarioAction(
                index, RandomScenarioActionKind.AppendAtFollower, followers[pick]);
        }

        if (!leaderUsable)
            return DrawIdle(index);

        return new RandomScenarioAction(
            index, RandomScenarioActionKind.AppendAtLeader, observation.Leader);
    }

    private RandomScenarioAction DrawNetwork(int index, RandomScenarioObservation observation)
    {
        (string from, string to) = PickPair(observation);

        if (random.NextInt("network-kind", 0, 2) == 0)
        {
            RandomScenarioAction action = new(
                index, RandomScenarioActionKind.BlockLink, from, to);

            return Start(
                action,
                key: $"link/{from}/{to}",
                heal: action with { Kind = RandomScenarioActionKind.UnblockLink },
                costsQuorum: true,
                index);
        }

        int copies = random.NextInt("duplicate-copies", 2, 4);

        RandomScenarioAction duplicate = new(
            index, RandomScenarioActionKind.DuplicateLink, from, to, copies);

        // Duplication costs the cluster nothing it is allowed to notice — the protocol claims its
        // remote calls are idempotent — so it does not count against the quorum budget. It is still
        // healed, because leaving it on for the whole run makes every later message threefold.
        return Start(
            duplicate,
            key: $"duplicate/{from}/{to}",
            heal: duplicate with { Kind = RandomScenarioActionKind.DuplicateLink, Value = 1 },
            costsQuorum: false,
            index);
    }

    private RandomScenarioAction DrawStorage(int index, RandomScenarioObservation observation)
    {
        string target = PickRunning(observation, "storage-target");
        int kind = random.NextInt("storage-kind", 0, 3);

        if (kind == 0)
        {
            RandomScenarioAction action = new(index, RandomScenarioActionKind.StarveDisk, target);

            return Start(
                action,
                key: $"disk/starve/{target}",
                heal: action with { Kind = RandomScenarioActionKind.FreeDisk },
                costsQuorum: true,
                index);
        }

        if (kind == 1)
        {
            int writes = random.NextInt("fail-writes", 1, 6);

            // A transient fault ends itself once the budget of refused writes is spent, so it
            // carries no heal. It still occupies the quorum budget while it lasts: a node refusing
            // writes is a node that cannot acknowledge.
            return Start(
                new RandomScenarioAction(index, RandomScenarioActionKind.FailWrites, target, null, writes),
                key: $"disk/fail/{target}",
                heal: null,
                costsQuorum: true,
                index);
        }

        int latency = random.NextInt("slow-disk-ms", 20, 200);

        RandomScenarioAction slow = new(
            index, RandomScenarioActionKind.SlowDisk, target, null, latency);

        // A slow disk still acknowledges, so it takes no node out of the majority. What it changes
        // is how much a crash takes, which is the interaction worth having.
        return Start(
            slow,
            key: $"disk/slow/{target}",
            heal: new RandomScenarioAction(index, RandomScenarioActionKind.FastDisk, target),
            costsQuorum: false,
            index);
    }

    private RandomScenarioAction DrawLifecycle(int index, RandomScenarioObservation observation)
    {
        string target = PickRunning(observation, "lifecycle-target");

        if (random.NextInt("lifecycle-kind", 0, 2) == 0)
        {
            return Start(
                new RandomScenarioAction(index, RandomScenarioActionKind.CrashNode, target),
                key: $"life/crash/{target}",
                heal: new RandomScenarioAction(index, RandomScenarioActionKind.RestartNode, target),
                costsQuorum: true,
                index);
        }

        return Start(
            new RandomScenarioAction(index, RandomScenarioActionKind.PauseNode, target),
            key: $"life/pause/{target}",
            heal: new RandomScenarioAction(index, RandomScenarioActionKind.ResumeNode, target),
            costsQuorum: true,
            index);
    }

    /// <summary>
    /// Draws a checkpoint or a retention hold.
    ///
    /// <para>The checkpoint carries no target: it is resolved to whoever leads when it runs, because
    /// only a leader can write one. The hold is a fault with a repair, but it takes no quorum budget
    /// — a node that keeps too much of its log still answers every request.</para>
    /// </summary>
    private RandomScenarioAction DrawMaintenance(int index, RandomScenarioObservation observation)
    {
        // A checkpoint is a full quorum write, so it is gated exactly like a client write: a leader
        // whose own disk refuses writes cannot commit one, and the call then waits out the ten real
        // seconds of the quorum timeout. Leaving this ungated made some runs take minutes.
        bool leaderUsable = observation.Leader is not null
                            && observation.Running.Contains(observation.Leader)
                            && !HasStorageFaultOn(observation.Leader);

        if (leaderUsable && random.NextInt("maintenance-kind", 0, 3) > 0)
            return new RandomScenarioAction(index, RandomScenarioActionKind.Checkpoint, observation.Leader);

        string target = PickRunning(observation, "retention-target");

        return Start(
            new RandomScenarioAction(index, RandomScenarioActionKind.HoldRetention, target),
            key: $"retention/{target}",
            heal: new RandomScenarioAction(index, RandomScenarioActionKind.ReleaseRetention, target),
            costsQuorum: false,
            index);
    }

    private RandomScenarioAction DrawHeal(int index)
    {
        int pick = random.NextInt("heal-pick", 0, active.Count);
        ActiveFault fault = active[pick];
        active.RemoveAt(pick);

        return fault.Heal is null
            ? new RandomScenarioAction(index, RandomScenarioActionKind.Idle)
            : fault.Heal with { Index = index };
    }

    /// <summary>
    /// Records a fault and returns the action that starts it. A fault whose key is already active
    /// is refused and the action becomes an idle one, because starting the same fault twice would
    /// leave a heal in the table that no longer matches what the cluster is doing.
    /// </summary>
    private RandomScenarioAction Start(
        RandomScenarioAction action,
        string key,
        RandomScenarioAction? heal,
        bool costsQuorum,
        int index)
    {
        if (active.Any(fault => fault.Key == key))
            return new RandomScenarioAction(index, RandomScenarioActionKind.Idle);

        active.Add(new ActiveFault(key, index, heal, costsQuorum));

        if (options.EnableFaultEpisodes && heal is not null && random.NextInt("episode", 0, 2) == 0)
            StartEpisode(key, heal);

        return action;
    }

    /// <summary>
    /// Queues the rest of a fault's life: use it, repair it, change leadership, use it again.
    ///
    /// <para><b>Why a template at all, in a random search.</b> The defects worth finding live in
    /// conjunctions, not in single faults. A uniform draw reaches "a disk refused a write" often and
    /// "a disk refused a write, a client wrote through it, the disk recovered, leadership moved, and
    /// a client wrote again" almost never — the five-step order is a small fraction of a
    /// twenty-four action plan, and a validation run over a reintroduced defect measured the
    /// consequence: thirty seeds found nothing a scripted scenario finds every time.</para>
    ///
    /// <para><b>Why this is not teaching to the test.</b> The shape is the life of any fault, not
    /// the recipe for one defect: what breaks, which node, whether an episode happens at all, and
    /// everything between the steps stay random. The template supplies the order; the seed supplies
    /// the content.</para>
    ///
    /// <para>The repair is the fault's own heal, and dequeuing it clears the fault from the table,
    /// so the age bound never repairs the same fault a second time.</para>
    /// </summary>
    private void StartEpisode(string key, RandomScenarioAction heal)
    {
        // Null targets are resolved when the action runs, and the plan records what was actually
        // used. An episode is decided before its steps happen, and the leader it wants is the one
        // in place by then rather than the one in place now.
        pending.Enqueue((new RandomScenarioAction(0, RandomScenarioActionKind.AppendAtLeader), null));
        pending.Enqueue((heal, key));
        pending.Enqueue((new RandomScenarioAction(0, RandomScenarioActionKind.LeaderOutage), null));
        pending.Enqueue((new RandomScenarioAction(0, RandomScenarioActionKind.AppendAtLeader), null));
    }

    private string PickRunning(RandomScenarioObservation observation, string choiceName)
    {
        int pick = random.NextInt(choiceName, 0, observation.Running.Count);
        return observation.Running[pick];
    }

    private (string From, string To) PickPair(RandomScenarioObservation observation)
    {
        int fromIndex = random.NextInt("link-from", 0, observation.Running.Count);
        int offset = random.NextInt("link-to", 1, observation.Running.Count);
        int toIndex = (fromIndex + offset) % observation.Running.Count;

        return (observation.Running[fromIndex], observation.Running[toIndex]);
    }
}
