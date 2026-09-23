using System.Globalization;

namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// Bounds and weights for a random run. Everything a run does is a function of these values plus
/// the seed, so both are recorded in the plan artifact.
/// </summary>
public sealed record RandomScenarioOptions
{
    /// <summary>Partition the run exercises. Never zero: partition 0 is the control plane.</summary>
    public int PartitionId { get; init; } = 1;

    /// <summary>How many actions the run draws before it starts healing.</summary>
    public int ActionCount { get; init; } = 24;

    /// <summary>Simulation steps run after each action, so the cluster can react to it.</summary>
    public int StepsPerAction { get; init; } = 6;

    /// <summary>Simulated milliseconds each step advances.</summary>
    public long AdvanceMillisecondsPerStep { get; init; } = 50;

    /// <summary>
    /// How many faults that cost the cluster a node may be active at once.
    ///
    /// <para><b>One, and the default is not timid.</b> A three-node cluster commits with two nodes,
    /// so one fault leaves every proposal fast and every commit reachable. Two faults remove the
    /// quorum, and a proposal with no quorum does not fail quickly: it waits ten <b>real</b>
    /// seconds inside <c>WaitForQuorum</c> before it reports a timeout. A run that loses quorum
    /// therefore stops exploring and starts paying, which is the opposite of what a random search
    /// is for. Raise this only for a scenario written to study a cluster with no quorum, and
    /// expect it to run in real seconds rather than simulated ones.</para>
    /// </summary>
    public int MaxImpairedNodes { get; init; } = 1;

    /// <summary>
    /// Actions a fault may stay active before the generator heals it whether or not the draw asks.
    ///
    /// <para>Without a bound the search converges on a permanently broken cluster: faults arrive
    /// faster than heals are drawn, and every later action is spent on a cluster that can no longer
    /// do anything. The bound is what keeps a run moving through states rather than sitting in
    /// one.</para>
    /// </summary>
    public int MaxFaultAgeInActions { get; init; } = 4;

    /// <summary>Steps allowed for the healed cluster to converge before the run reports a failure.</summary>
    public int RecoveryStepBudget { get; init; } = 900;

    /// <summary>
    /// Steps a healed cluster is given to converge <em>before</em> anybody writes to it.
    ///
    /// <para><b>Why this is separate from <see cref="RecoveryStepBudget"/>, and smaller.</b> That
    /// budget covers a recovery that includes a client write, and a write repairs a follower that is
    /// merely behind. This one deliberately withholds the write, so it measures only what the
    /// heartbeat path does on its own — a much narrower claim, and one a cluster should satisfy
    /// quickly or not at all.</para>
    ///
    /// <para><b>Why it is a knob rather than a constant.</b> It is the bound the check's verdict
    /// depends on, so it is the first thing to vary when the check fires. A failure that goes away at
    /// a larger budget is a bound set too low, not a defect — and a bound nobody can vary cannot be
    /// tested for that.</para>
    /// </summary>
    public int IdleConvergenceStepBudget { get; init; } = 300;

    /// <summary>Weight of a client operation in the draw.</summary>
    public int ClientWeight { get; init; } = 30;

    /// <summary>Weight of letting time pass with the network working.</summary>
    public int IdleWeight { get; init; } = 14;

    /// <summary>
    /// Weight of cutting the leader off until another is elected.
    ///
    /// <para>Its own category rather than a sub-draw of an idle action, because it is the only way
    /// a run elects a leader on purpose, and a leader elected after the damage is what several
    /// known defects need. A validation run over a reintroduced defect measured the cost of not
    /// having it: thirty seeds found nothing.</para>
    /// </summary>
    public int OutageWeight { get; init; } = 12;

    /// <summary>Weight of a network fault.</summary>
    public int NetworkFaultWeight { get; init; } = 12;

    /// <summary>Weight of a storage fault.</summary>
    public int StorageFaultWeight { get; init; } = 12;

    /// <summary>Weight of a crash or a pause.</summary>
    public int LifecycleFaultWeight { get; init; } = 12;

    /// <summary>
    /// Weight of a maintenance action: a checkpoint, or a retention hold.
    ///
    /// <para>Modest, because a checkpoint is not a fault. It is here so that compaction happens at
    /// all during a run, which nothing else in the vocabulary causes.</para>
    /// </summary>
    public int MaintenanceWeight { get; init; } = 10;

    /// <summary>
    /// Operations between compaction sweeps on each node. The production default.
    ///
    /// <para>A generated run is a few dozen entries long, so at this cadence a run never compacts.
    /// A scenario that wants compaction lowers it — see the checkpoint smoke test, which sets it to
    /// eight.</para>
    ///
    /// <para><b>Why the sweeping runs do not lower it.</b> At a cadence of eight, roughly one run in
    /// eight ends with a follower holding durable entries above a presence gap and a committed
    /// frontier of zero, unrepaired after forty-five seconds of simulated time. That is recorded as
    /// an open finding rather than absorbed here: leaving the aggressive cadence on would leave the
    /// standing test set red, and raising it hides the state. The finding carries the exact setting
    /// that reproduces it.</para>
    /// </summary>
    public int CompactEveryOperations { get; init; } = 10_000;

    /// <summary>
    /// Entries a leader retains below its checkpoint for a lagging follower it counts as alive.
    /// Null keeps the production default.
    ///
    /// <para><b>Why a run needs to lower it.</b> A crashed simulated node can stay alive in the
    /// leader's view for the whole run, and the production budget of 100,000 entries then holds
    /// compaction at that node's position for good. A run of a few dozen entries can never push a
    /// follower below the leader's compaction floor, and the snapshot rescue path is never
    /// reached. Production reaches the same state with a follower that lags beyond the budget; a
    /// small budget reaches it with a short log.</para>
    /// </summary>
    public long? CompactionLiveReplicaLagBudget { get; init; }

    /// <summary>
    /// Weight of arming a hang in the leader's application export. Zero by default.
    ///
    /// <para><b>Why zero.</b> A hung export changes nothing until a follower needs a snapshot, and at
    /// the production compaction cadence no generated run ever does. A non-zero default would only
    /// change the draws of every existing seed, and the corpus seeds are measured evidence about
    /// the draws they make today. The snapshot-rescue sweep turns it on together with the settings
    /// that make a rescue happen.</para>
    /// </summary>
    public int TransferFaultWeight { get; init; }

    /// <summary>
    /// Weight of <see cref="RandomScenarioActionKind.LateBroadcast"/>. Zero by default, so the
    /// existing sweeps draw exactly the plans they drew before; a category with no weight takes no
    /// entropy. The late-broadcast sweep turns it on.
    /// </summary>
    public int LateBroadcastWeight { get; init; }

    /// <summary>
    /// Turns quiescence on for every node, with <see cref="QuiesceAfterMs"/> as its idle window, and
    /// turns on the harness's SWIM model (<c>SimulationClusterOptions.ModelSwimLiveness</c>) with it.
    ///
    /// <para>Production ships quiescence on, and the Kahuna lost-write bug (<c>39bf62e2</c>) was a
    /// quiesced leader that never stepped down. The other families run with it off, so that whole
    /// class was outside the search. A quiesced follower relies on SWIM to notice a dead leader, and
    /// the harness drives no SWIM probes, so quiescence without the model would report the missing
    /// detector as a liveness defect.</para>
    /// </summary>
    public bool EnableQuiescence { get; init; }

    /// <summary>
    /// The idle window before a partition quiesces, in simulated milliseconds, when
    /// <see cref="EnableQuiescence"/> is on. The production default is 1,500 ms, which is 30 steps
    /// here and longer than most idle stretches a plan draws; the family shortens it so that
    /// quiescence really happens between actions.
    /// </summary>
    public int QuiesceAfterMs { get; init; } = 400;

    /// <summary>
    /// Weight of <see cref="RandomScenarioActionKind.QuiescedLeaderOutage"/>. Zero by default; the
    /// quiescence family turns it on.
    /// </summary>
    public int QuiescedOutageWeight { get; init; }

    /// <summary>
    /// Percentage of crash draws whose heal is <see cref="RandomScenarioActionKind.RestartNodeBlank"/>
    /// instead of an ordinary restart. Zero by default, and at zero no entropy is drawn for it, so
    /// the existing plans do not change.
    /// </summary>
    public int BlankRestartPercent { get; init; }

    /// <summary>
    /// Weight of <see cref="RandomScenarioActionKind.TransferLeadership"/>. Zero by default, so the
    /// existing plans do not change.
    /// </summary>
    public int TransferLeadershipWeight { get; init; }

    /// <summary>
    /// Weight of <see cref="RandomScenarioActionKind.ReadAtNode"/>. Zero by default, so the existing
    /// plans do not change; the read family turns it on.
    /// </summary>
    public int ReadWeight { get; init; }

    /// <summary>
    /// Weight of <see cref="RandomScenarioActionKind.ReadAtCutLeader"/>. Zero by default, so the
    /// existing plans do not change; the read family turns it on.
    /// </summary>
    public int CutLeaderReadWeight { get; init; }

    /// <summary>
    /// Applies the node configuration these options imply. One place, so the sweep, the shrinker
    /// and the regression replay cannot build a cluster that differs from the run they reproduce.
    /// </summary>
    /// <summary>
    /// The cluster a run with these options needs: three nodes, one data partition, the run's seed,
    /// and every cluster-level setting the options carry.
    ///
    /// <para><b>One factory for every caller.</b> The sweep, the regression replay, and the shrink
    /// oracle each built this by hand, and the two replay paths dropped the election timeouts. A
    /// check-quorum plan then replayed on the stock 100 ms timeouts, which is a different run. A
    /// setting added here reaches all three.</para>
    /// </summary>
    public Cluster.SimulationClusterOptions ToClusterOptions(ulong seed)
    {
        Cluster.SimulationClusterOptions cluster = new()
        {
            NodeCount = 3,
            PartitionCount = 1,
            Seed = seed,
            ConfigureNode = ApplyTo,
            ModelSwimLiveness = EnableQuiescence,
        };

        if (StartElectionTimeoutMs is { } startElection)
            cluster = cluster with { StartElectionTimeoutMs = startElection };
        if (EndElectionTimeoutMs is { } endElection)
            cluster = cluster with { EndElectionTimeoutMs = endElection };

        return cluster;
    }

    public void ApplyTo(RaftConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        configuration.CompactEveryOperations = CompactEveryOperations;

        if (CompactionLiveReplicaLagBudget is { } budget)
            configuration.CompactionLiveReplicaLagBudget = budget;

        if (EnableCheckQuorum)
            configuration.EnableCheckQuorum = true;

        if (EnableQuiescence)
        {
            configuration.EnableQuiescence = true;
            configuration.QuiesceAfter = TimeSpan.FromMilliseconds(QuiesceAfterMs);

            // The harness's SWIM model reads these for its suspicion window. Six steps: short enough
            // that a cut leader is suspected inside one outage, long enough that a single slow step
            // is not a verdict.
            configuration.PingInterval = TimeSpan.FromMilliseconds(200);
            configuration.PingTimeout = TimeSpan.FromMilliseconds(100);
        }
    }

    /// <summary>
    /// Runs the cluster with check-quorum on (the production default). Off here by default: the
    /// simulation's stock election timeout equals one heartbeat ack round trip, so the derived
    /// window would step every leader down on nearly every tick — see <c>SimulationNode</c>. A
    /// family that turns it on must also widen <see cref="StartElectionTimeoutMs"/> and
    /// <see cref="EndElectionTimeoutMs"/> so the window holds several round trips.
    /// </summary>
    public bool EnableCheckQuorum { get; init; }

    /// <summary>Overrides the cluster's start election timeout in milliseconds (null = the cluster default).</summary>
    public int? StartElectionTimeoutMs { get; init; }

    /// <summary>Overrides the cluster's end election timeout in milliseconds (null = the cluster default).</summary>
    public int? EndElectionTimeoutMs { get; init; }

    /// <summary>Weight of healing something that is currently broken.</summary>
    public int HealWeight { get; init; } = 14;

    /// <summary>
    /// How much the client weight rises while a fault is active.
    ///
    /// <para>A fault nobody writes through teaches nothing. The interesting states are the ones
    /// where a client operation and a broken node overlap, and uniform weights reach that overlap
    /// far less often than the fault rate alone suggests.</para>
    /// </summary>
    public int ClientWeightDuringFault { get; init; } = 2;

    /// <summary>
    /// Whether a fault may pull the rest of its life in behind it: use it, repair it, change
    /// leadership, use it again. One drawn fault in two does, on average.
    ///
    /// <para>On by default, and worth the loss of independence. See
    /// <c>RandomScenarioGenerator.StartEpisode</c> for the measurement that justifies it.</para>
    /// </summary>
    public bool EnableFaultEpisodes { get; init; } = true;

    /// <summary>
    /// Where a failing run writes its plan. Null puts it beside the test binary, which is what a
    /// continuous-integration job can collect.
    /// </summary>
    public string? ArtifactDirectory { get; init; }

    /// <summary>Parameters recorded in the plan artifact and the replay-log header.</summary>
    public IReadOnlyDictionary<string, string> ToParameters() =>
        new Dictionary<string, string>
        {
            ["partitionId"] = PartitionId.ToString(),
            ["actionCount"] = ActionCount.ToString(),
            ["stepsPerAction"] = StepsPerAction.ToString(),
            ["advanceMillisecondsPerStep"] = AdvanceMillisecondsPerStep.ToString(),
            ["maxImpairedNodes"] = MaxImpairedNodes.ToString(),
            ["maxFaultAgeInActions"] = MaxFaultAgeInActions.ToString(),
            ["recoveryStepBudget"] = RecoveryStepBudget.ToString(),
            ["idleConvergenceStepBudget"] = IdleConvergenceStepBudget.ToString(),
            ["clientWeight"] = ClientWeight.ToString(),
            ["idleWeight"] = IdleWeight.ToString(),
            ["outageWeight"] = OutageWeight.ToString(),
            ["networkFaultWeight"] = NetworkFaultWeight.ToString(),
            ["storageFaultWeight"] = StorageFaultWeight.ToString(),
            ["lifecycleFaultWeight"] = LifecycleFaultWeight.ToString(),
            ["healWeight"] = HealWeight.ToString(),
            ["maintenanceWeight"] = MaintenanceWeight.ToString(),
            ["compactEveryOperations"] = CompactEveryOperations.ToString(),
            ["clientWeightDuringFault"] = ClientWeightDuringFault.ToString(),
            ["enableFaultEpisodes"] = EnableFaultEpisodes.ToString(),
            ["compactionLiveReplicaLagBudget"] =
                CompactionLiveReplicaLagBudget?.ToString(CultureInfo.InvariantCulture) ?? "default",
            ["transferFaultWeight"] = TransferFaultWeight.ToString(),
            ["lateBroadcastWeight"] = LateBroadcastWeight.ToString(),
            ["enableQuiescence"] = EnableQuiescence.ToString(),
            ["quiesceAfterMs"] = QuiesceAfterMs.ToString(),
            ["quiescedOutageWeight"] = QuiescedOutageWeight.ToString(),
            ["blankRestartPercent"] = BlankRestartPercent.ToString(),
            ["transferLeadershipWeight"] = TransferLeadershipWeight.ToString(),
            ["readWeight"] = ReadWeight.ToString(),
            ["cutLeaderReadWeight"] = CutLeaderReadWeight.ToString(),
            ["enableCheckQuorum"] = EnableCheckQuorum.ToString(),
            ["startElectionTimeoutMs"] = StartElectionTimeoutMs?.ToString(CultureInfo.InvariantCulture) ?? "default",
            ["endElectionTimeoutMs"] = EndElectionTimeoutMs?.ToString(CultureInfo.InvariantCulture) ?? "default",
        };

    /// <summary>
    /// Rebuilds the bounds a recorded run was drawn under.
    ///
    /// <para><b>Why a plan cannot be replayed without this.</b> The actions say what happened; the
    /// bounds say what the run was allowed to do. Steps per action and the recovery budget are not
    /// decoration — a plan replayed at six steps per action and a plan replayed at three are two
    /// different experiments, and only one of them is the one that failed. A promoted regression
    /// that lost its bounds would be a test of something nobody recorded.</para>
    ///
    /// <para><b>Why a missing key is a default and not an error.</b> The artifact format grows: a
    /// plan written before <c>maintenanceWeight</c> existed has no line for it. Refusing such a file
    /// would make every older regression unloadable the day a knob is added, which is the opposite
    /// of what a regression corpus is for. An unreadable <em>value</em> is still an error, because
    /// that is corruption rather than age.</para>
    /// </summary>
    public static RandomScenarioOptions FromParameters(IReadOnlyDictionary<string, string> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        RandomScenarioOptions defaults = new();

        return new RandomScenarioOptions
        {
            PartitionId = Int(parameters, "partitionId", defaults.PartitionId),
            ActionCount = Int(parameters, "actionCount", defaults.ActionCount),
            StepsPerAction = Int(parameters, "stepsPerAction", defaults.StepsPerAction),
            AdvanceMillisecondsPerStep =
                Int(parameters, "advanceMillisecondsPerStep", (int)defaults.AdvanceMillisecondsPerStep),
            MaxImpairedNodes = Int(parameters, "maxImpairedNodes", defaults.MaxImpairedNodes),
            MaxFaultAgeInActions = Int(parameters, "maxFaultAgeInActions", defaults.MaxFaultAgeInActions),
            RecoveryStepBudget = Int(parameters, "recoveryStepBudget", defaults.RecoveryStepBudget),
            IdleConvergenceStepBudget =
                Int(parameters, "idleConvergenceStepBudget", defaults.IdleConvergenceStepBudget),
            ClientWeight = Int(parameters, "clientWeight", defaults.ClientWeight),
            IdleWeight = Int(parameters, "idleWeight", defaults.IdleWeight),
            OutageWeight = Int(parameters, "outageWeight", defaults.OutageWeight),
            NetworkFaultWeight = Int(parameters, "networkFaultWeight", defaults.NetworkFaultWeight),
            StorageFaultWeight = Int(parameters, "storageFaultWeight", defaults.StorageFaultWeight),
            LifecycleFaultWeight = Int(parameters, "lifecycleFaultWeight", defaults.LifecycleFaultWeight),
            HealWeight = Int(parameters, "healWeight", defaults.HealWeight),
            MaintenanceWeight = Int(parameters, "maintenanceWeight", defaults.MaintenanceWeight),
            CompactEveryOperations =
                Int(parameters, "compactEveryOperations", defaults.CompactEveryOperations),
            ClientWeightDuringFault =
                Int(parameters, "clientWeightDuringFault", defaults.ClientWeightDuringFault),
            EnableFaultEpisodes = Bool(parameters, "enableFaultEpisodes", defaults.EnableFaultEpisodes),
            CompactionLiveReplicaLagBudget = OptionalLong(parameters, "compactionLiveReplicaLagBudget"),
            TransferFaultWeight = Int(parameters, "transferFaultWeight", defaults.TransferFaultWeight),
            LateBroadcastWeight = Int(parameters, "lateBroadcastWeight", defaults.LateBroadcastWeight),
            EnableQuiescence = Bool(parameters, "enableQuiescence", defaults.EnableQuiescence),
            QuiesceAfterMs = Int(parameters, "quiesceAfterMs", defaults.QuiesceAfterMs),
            QuiescedOutageWeight = Int(parameters, "quiescedOutageWeight", defaults.QuiescedOutageWeight),
            BlankRestartPercent = Int(parameters, "blankRestartPercent", defaults.BlankRestartPercent),
            TransferLeadershipWeight = Int(parameters, "transferLeadershipWeight", defaults.TransferLeadershipWeight),
            ReadWeight = Int(parameters, "readWeight", defaults.ReadWeight),
            CutLeaderReadWeight = Int(parameters, "cutLeaderReadWeight", defaults.CutLeaderReadWeight),
            EnableCheckQuorum = Bool(parameters, "enableCheckQuorum", defaults.EnableCheckQuorum),
            StartElectionTimeoutMs = (int?)OptionalLong(parameters, "startElectionTimeoutMs"),
            EndElectionTimeoutMs = (int?)OptionalLong(parameters, "endElectionTimeoutMs"),
        };
    }

    private static int Int(IReadOnlyDictionary<string, string> parameters, string key, int fallback)
    {
        if (!parameters.TryGetValue(key, out string? text))
            return fallback;

        if (!int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
            throw new FormatException($"Parameter '{key}' is not a number: '{text}'.");

        return value;
    }

    /// <summary>
    /// Reads an optional number. A missing key and the word <c>default</c> both mean "not set", so a
    /// plan written before the key existed and a plan that left it at the production value read the
    /// same way.
    /// </summary>
    private static long? OptionalLong(IReadOnlyDictionary<string, string> parameters, string key)
    {
        if (!parameters.TryGetValue(key, out string? text) || text == "default")
            return null;

        if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value))
            throw new FormatException($"Parameter '{key}' is not a number: '{text}'.");

        return value;
    }

    private static bool Bool(IReadOnlyDictionary<string, string> parameters, string key, bool fallback)
    {
        if (!parameters.TryGetValue(key, out string? text))
            return fallback;

        if (!bool.TryParse(text, out bool value))
            throw new FormatException($"Parameter '{key}' is not a true or false value: '{text}'.");

        return value;
    }
}
