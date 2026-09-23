using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Diagnostics;
using Kommander.Tests.Simulation.Random;
using Kommander.Tests.Simulation.Scenarios.Random;
using Kommander.Tests.Simulation.Shrinking;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Generated runs against a real three-node cluster.
///
/// <para><b>Why random runs at all, when the scripted scenarios already find defects.</b> Both
/// findings so far came from states next to a fault, which says that region is productive. A
/// scripted scenario can only visit the part of it somebody thought to write down, and each one
/// costs a person an afternoon. A generated plan costs a seed.</para>
///
/// <para><b>What a passing run means.</b> Every step is checked against the per-step invariants;
/// the end of the run adds the convergence check and the client-history check. A plan passes only
/// if all three agree, so a pass is a statement about node state, about where the run ended, and
/// about every promise made to a client along the way.</para>
///
/// <para><b>What a failing run gives back.</b> Two files beside the test binary: the plan in the
/// order it happened, and the entropy it consumed in the same replay format the model-layer runs
/// use. The plan is the replay unit — <see cref="RandomScenarioRunner.ReplayAsync"/> applies it
/// again, action for action.</para>
///
/// <para><b>What the seed does and does not promise.</b> It fixes every draw the generator makes.
/// It does not fix the run, because a draw depends on what the generator observed and these
/// clusters run on their own threads: two runs of one seed can observe different leaders and
/// diverge from there. Exact reproduction needs the driven scheduling mode, which costs minutes per
/// run rather than seconds. Stated plainly because the opposite is easy to assume: a failing seed
/// is a strong lead, not a guaranteed repeat, and the plan beside it is what makes the failure
/// re-runnable.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestRandomScenarios
{
    /// <summary>
    /// Turns the frequent-compaction sweep on. Off by default — see
    /// <see cref="AGeneratedRunUnderFrequentCompaction_HoldsEveryCheck"/> for why a second sweep
    /// exists and why it is not part of the standing set.
    /// </summary>
    public const string CompactionSweepVariable = "KOMMANDER_DST_COMPACTION_SWEEP";

    private static bool CompactionSweepEnabled =>
        Environment.GetEnvironmentVariable(CompactionSweepVariable) == "1";

    private readonly ILogger<IRaft> logger;
    private readonly ITestOutputHelper output;

    public TestRandomScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
        output = outputHelper;
    }

    /// <summary>
    /// One short generated run, on every pull request.
    ///
    /// <para>Its job is not to search. It is to prove the machinery still works — that a plan is
    /// drawn, applied, healed and checked — so that the nightly search fails for reasons about
    /// Raft rather than reasons about the harness.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AShortGeneratedRun_HoldsEveryCheck()
    {
        RandomScenarioOptions options = new()
        {
            ActionCount = 12,
            StepsPerAction = 5,
        };

        RandomScenarioReport report = await RunSeedAsync(
            20260901, options, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");
        Assert.Equal(options.ActionCount, report.Actions.Count(action => action.Index < options.ActionCount));

        // The recovery write is unconditional, so a run always ends with at least one entry in the
        // log. A run that acknowledged nothing would have checked a history of nothing.
        Assert.True(report.History.Count > 0, "No client operation was issued.");
        Assert.True(report.FinalCommitIndex >= 0, "The cluster committed nothing at all.");

        // Printed on every run, passing or not. What a search costs is the number that settles an
        // argument about whether an unusual run was the change or the machine, and it is worth
        // nothing if it only appears once something has already gone wrong.
        Assert.NotNull(report.Metrics);
        output.WriteLine(report.Metrics.Describe());

        Assert.True(report.Metrics.StepsPerSecond > 0, "The run reported no throughput.");

        Assert.InRange(report.Metrics.InvariantShare, 0, 1);

        // The checker reads every node's view and every node's store on every settled state, so a
        // run that reported no time at all in it has a timer around the wrong code.
        Assert.True(
            report.Metrics.InvariantTime > TimeSpan.Zero,
            "The run reported no time spent checking invariants, which cannot be true.");
    }

    /// <summary>
    /// A run that checkpoints really compacts.
    ///
    /// <para>Two invariants describe compaction: one says compaction is never asked to remove an
    /// entry above the certified checkpoint, the other tolerates a compacted head when it looks for
    /// a hole. Until a generated run actually compacted, both described something that never
    /// happened, and a rule nothing exercises is decoration. This run weights the draw towards
    /// client writes and checkpoints and then insists that entries really left the log.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task ARunThatCheckpoints_ReallyCompacts()
    {
        RandomScenarioOptions options = new()
        {
            ActionCount = 16,
            StepsPerAction = 5,
            ClientWeight = 30,
            MaintenanceWeight = 30,
            IdleWeight = 0,
            OutageWeight = 0,
            NetworkFaultWeight = 0,
            StorageFaultWeight = 0,
            LifecycleFaultWeight = 0,
            HealWeight = 0,

            // Low enough that a run of this length really compacts. The sweeping runs leave it at
            // the production default; see RandomScenarioOptions.CompactEveryOperations for why.
            CompactEveryOperations = 8,
        };

        RandomScenarioReport report = await RunSeedAsync(
            20260907, options, TestContext.Current.CancellationToken);

        Assert.True(
            report.CountOf(RandomScenarioActionKind.Checkpoint) > 0,
            "No checkpoint was drawn, so nothing could compact.");

        Assert.True(
            report.EntriesCompacted > 0,
            $"The run wrote checkpoints and compacted nothing ({report.EntriesCompacted} entries). " +
            "The compaction rules are describing something that does not happen.");
    }

    /// <summary>
    /// The nightly search. The corpus seeds always run; the sweep beside them moves with
    /// <see cref="RandomSeedCorpus.SeedBaseVariable"/>, and its length with
    /// <see cref="RandomSeedCorpus.SeedCountVariable"/>.
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRun_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, new RandomScenarioOptions(), TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");
    }

    /// <summary>
    /// The same search, with compaction turned up until the leader really throws entries away.
    ///
    /// <para><b>Why a second sweep rather than a setting on the first.</b> Compaction opens a state
    /// nothing else reaches: a follower that falls below the leader's first available index cannot
    /// be repaired by backfill at all, because the entries it needs no longer exist. A snapshot
    /// install is the only way back, so the whole rescue path — escalation, transfer, install,
    /// convergence — is unreachable at the production cadence, where a run of this length compacts
    /// nothing.</para>
    ///
    /// <para><b>Why it is off by default.</b> At a cadence of eight, roughly one run in eight ends
    /// with a follower holding durable entries above a presence gap and a committed frontier of
    /// zero. That state is real and reproducible, and it is <em>not</em> established as a library
    /// defect: whether a cadence far below the working set is a configuration Kommander promises to
    /// support is an open question for whoever owns the compaction contract. Leaving the sweep on
    /// would keep the standing set red over that open question, and raising the cadence would hide
    /// the state. So it runs on demand, under
    /// <see cref="CompactionSweepVariable"/>, and it skips rather than passes when it is off —
    /// a test that quietly returns is a test nobody knows did not run.</para>
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRunUnderFrequentCompaction_HoldsEveryCheck(ulong seed)
    {
        if (!CompactionSweepEnabled)
            Assert.Skip(
                $"The frequent-compaction sweep is off. Set {CompactionSweepVariable}=1 to run it, " +
                "and expect roughly one failure in eight from an open question about the compaction " +
                "cadence rather than from the change under test.");

        RandomScenarioReport report = await RunSeedAsync(
            seed,
            new RandomScenarioOptions { CompactEveryOperations = 8 },
            TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");
    }

    /// <summary>
    /// The options of the snapshot-rescue sweep. Public so a plan replay or a probe can rebuild the
    /// same run.
    /// </summary>
    public static RandomScenarioOptions SnapshotRescueOptions => new()
    {
        CompactEveryOperations = 8,
        CompactionLiveReplicaLagBudget = 4,
        TransferFaultWeight = 8,
    };

    /// <summary>
    /// The same search, tuned so that followers really fall below the leader's compaction floor,
    /// with a hung application export in the vocabulary.
    ///
    /// <para><b>Why a third sweep.</b> The frequent-compaction sweep compacts, but it rarely strands a
    /// follower below the floor: the leader keeps up to 100,000 entries for a follower it counts as
    /// alive, and a crashed simulated node can stay alive in its view for the whole run. So the
    /// snapshot rescue — escalation, export, install — almost never runs, and the defects that live
    /// there cannot be found. This sweep lowers that retention budget to four entries, which is the
    /// state production reaches with a follower that lags beyond its budget.</para>
    ///
    /// <para><b>Why the hung export belongs here and nowhere else.</b> It changes nothing until a
    /// rescue runs. In this sweep rescues run, and a hang during one is the shape of the Caraxes
    /// anchor-1 wedge (vorpal <c>d11fd5f9</c>): a transfer that nothing ends, and an in-flight guard
    /// that silently vetoes every later rescue. The existing sweeps keep their draws unchanged,
    /// because the transfer weight is zero there.</para>
    ///
    /// <para><b>What its first full run found.</b> DST FINDING 6: the live-replica retention hold
    /// compacted the entry just below a lagging follower's backfill anchor, the leader shipped the
    /// batch with a previous term of -1, the follower rejected it as divergent, and the leader
    /// re-shipped it forever. Corpus seed 20260902 reached it about one replay in twenty-four. Fixed
    /// in <c>FollowerAppendHandler</c>, and pinned by
    /// <c>TestCompactionFloorBackfillScenarios</c>.</para>
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRunUnderSnapshotRescue_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, SnapshotRescueOptions, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");

        // Printed on every run. Whether the rescue path ran at all is the one number that says
        // whether this seed tested anything this sweep exists for.
        output.WriteLine(
            $"seed={seed} entriesCompacted={report.EntriesCompacted} " +
            $"snapshotExportsServed={report.SnapshotExportsServed} snapshotExportsHung={report.SnapshotExportsHung} " +
            $"hangsArmed={report.CountOf(RandomScenarioActionKind.HangSnapshotExport)}");
    }

    /// <summary>
    /// The check-quorum family. The stock cluster runs a 100 ms election timeout against a 100 ms
    /// heartbeat ack round trip, so check-quorum (the production default) is off there. This family
    /// widens the election timeouts to 300–600 ms — six heartbeats, the production ratio — and turns
    /// it on, so the isolated-leader step-down, the quiesce-free probe path and the term fence run
    /// under generated partitions, outages and disk faults instead of only under unit tests.
    /// </summary>
    public static RandomScenarioOptions CheckQuorumOptions => new()
    {
        EnableCheckQuorum = true,
        StartElectionTimeoutMs = 300,
        EndElectionTimeoutMs = 600,
    };

    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRunUnderCheckQuorum_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, CheckQuorumOptions, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");
    }

    /// <summary>
    /// The options of the production-safety sweep: check-quorum and quiescence both on, as they
    /// ship. Public so a plan replay or a probe can rebuild the same run.
    /// </summary>
    public static RandomScenarioOptions ProductionSafetyOptions => CheckQuorumOptions with
    {
        EnableQuiescence = true,
        QuiescedOutageWeight = 12,
    };

    /// <summary>
    /// The production-safety family: check-quorum and quiescence on, with an outage aimed at a
    /// quiesced leader.
    ///
    /// <para><b>Why this family exists.</b> Production ships both safety features on, and every
    /// other family turns quiescence off. The Kahuna lost-write bug (<c>39bf62e2</c>) was a quiesced
    /// leader that never stepped down, so the search could not reach that class at all. The family
    /// also turns on the harness's SWIM model, because a quiesced follower relies on SWIM to notice
    /// a dead leader and the harness drives no probes.</para>
    ///
    /// <para>Each run prints how many quiesced outages really found the leader quiesced. A run with
    /// none tested the plain outage only.</para>
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRunUnderProductionSafetyDefaults_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, ProductionSafetyOptions, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");

        output.WriteLine(
            $"seed={seed} quiescedOutages={report.CountOf(RandomScenarioActionKind.QuiescedLeaderOutage)} " +
            $"reachedQuiesced={report.QuiescedOutagesReached} finalTerm={report.FinalTerm}");
    }

    /// <summary>
    /// A healthy run with the production safety defaults keeps its leader.
    ///
    /// <para><b>Why this is measured.</b> Check-quorum steps a leader down when it has not heard a
    /// majority inside its window, and quiescence stops the heartbeats that the window is fed by. A
    /// window too tight for the transport makes a healthy leader step down over and over: the stock
    /// simulation timeouts did exactly that, which is why check-quorum was off here at all. The
    /// family above uses 300 to 600 ms election timeouts. This run measures the churn those timeouts
    /// produce with no fault at all, and fails if a healthy cluster elects more than once after its
    /// first leader.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTSmoke")]
    public async Task AHealthyRunUnderProductionSafetyDefaults_KeepsItsLeader()
    {
        RandomScenarioOptions options = ProductionSafetyOptions with
        {
            ActionCount = 20,
            OutageWeight = 0,
            NetworkFaultWeight = 0,
            StorageFaultWeight = 0,
            LifecycleFaultWeight = 0,
            HealWeight = 0,
            MaintenanceWeight = 0,
            QuiescedOutageWeight = 0,
        };

        RandomScenarioReport report = await RunSeedAsync(
            20260923, options, TestContext.Current.CancellationToken);

        output.WriteLine($"finalTerm={report.FinalTerm} steps={report.StepsRun}");

        Assert.InRange(report.FinalTerm, 1, 2);
    }

    /// <summary>
    /// The options of the blank-restart sweep. Public so a plan replay or a probe can rebuild the
    /// same run.
    /// </summary>
    public static RandomScenarioOptions BlankRestartOptions => new()
    {
        BlankRestartPercent = 50,
        LifecycleFaultWeight = 20,
    };

    /// <summary>
    /// The blank-restart family: half the crashes come back with an empty store under the same
    /// endpoint.
    ///
    /// <para><b>Why.</b> DST-19 item 1 and the Kahuna finding <c>e618064e</c>: the roster still names
    /// the node, so nothing resets what the leader recorded about its previous life, and the leader
    /// anchored every repair above what the blank node held (fixed in <c>3552ab9</c>). The other
    /// families always restart over the old store.</para>
    ///
    /// <para><b>Read a one-leader-per-term failure here with care.</b> A wiped store loses the stored
    /// vote, and Raft's safety argument assumes a vote survives a restart. See
    /// <c>SimulatedWAL.Wipe</c>.</para>
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRunUnderBlankRestarts_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, BlankRestartOptions, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");

        output.WriteLine(
            $"seed={seed} blankRestarts={report.CountOf(RandomScenarioActionKind.RestartNodeBlank)} " +
            $"restarts={report.CountOf(RandomScenarioActionKind.RestartNode)}");
    }

    /// <summary>
    /// The options of the leadership-transfer sweep. Public so a plan replay or a probe can rebuild
    /// the same run.
    /// </summary>
    public static RandomScenarioOptions TransferLeadershipOptions => new()
    {
        TransferLeadershipWeight = 14,
    };

    /// <summary>
    /// The leadership-transfer family: the leader is asked to hand over to a follower that is one
    /// write behind, and the answer is judged when the cluster had no fault.
    ///
    /// <para><b>Why.</b> DST-19 item 2 and DST-20 item 2, for <c>38a5e2b</c> (Kahuna/CamusDB,
    /// <c>31098233</c>): a transfer to a target one entry behind answered <c>ReplicationFailed</c> at
    /// once, and the caller dropped it. No action transferred leadership, and no rule judged the
    /// answer of a call made with no fault.</para>
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRunUnderLeadershipTransfers_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, TransferLeadershipOptions, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");

        output.WriteLine(
            $"seed={seed} transfers={report.CountOf(RandomScenarioActionKind.TransferLeadership)} " +
            $"answers=[{string.Join(", ", report.TransferAnswers.Select(pair => $"{pair.Key}:{pair.Value}"))}]");
    }

    /// <summary>
    /// The options of the late-broadcast sweep. Public so a plan replay or a probe can rebuild the
    /// same run.
    /// </summary>
    public static RandomScenarioOptions LateBroadcastOptions => new()
    {
        LateBroadcastWeight = 12,
    };

    /// <summary>
    /// The late-broadcast family: a follower's first sight of an entry is the committed row, and in
    /// half the draws the follower crashes right after it.
    ///
    /// <para><b>Why a family of its own.</b> The state needs three things at once: the follower's
    /// traffic held across the commit, a wire copy taken after the leader's in-place retype, and a
    /// crash before any later synced write. The other families reach the first often and the last
    /// two never. Before the transport gave every receiver its own copy, shared objects made the
    /// crash harmless, so this family could not have found <c>367eac9</c> or DST FINDING 7.</para>
    ///
    /// <para>The check it exists for is <c>reported-durable-survives-crash</c> in the runner. It runs
    /// on every crash in every family, and this family is where it has something to find.</para>
    /// </summary>
    [Theory]
    [Trait("Category", "DSTRandom")]
    [MemberData(nameof(Seeds))]
    public async Task AGeneratedRunUnderLateBroadcast_HoldsEveryCheck(ulong seed)
    {
        RandomScenarioReport report = await RunSeedAsync(
            seed, LateBroadcastOptions, TestContext.Current.CancellationToken);

        Assert.True(report.InvariantChecks > 0, "The run checked no invariants.");

        output.WriteLine(
            $"seed={seed} lateBroadcasts={report.CountOf(RandomScenarioActionKind.LateBroadcast)} " +
            $"committedFirst={report.LateBroadcastsCommittedFirst} " +
            $"crashes={report.CountOf(RandomScenarioActionKind.CrashNode)}");
    }

    /// <summary>
    /// A generated plan replays from the artifact a failure would leave behind.
    ///
    /// <para>The check the whole failure report depends on. A plan is written, read back, and
    /// applied to a fresh cluster; if the file could not carry the run, every failing plan the
    /// nightly search produces would be unusable, and nobody would find that out until the night it
    /// mattered.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTRandom")]
    public async Task AGeneratedPlan_ReplaysFromItsArtifact()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        RandomScenarioOptions options = new() { ActionCount = 10, StepsPerAction = 5 };

        RandomScenarioReport drawn = await RunSeedAsync(20260902, options, cancellationToken);

        string directory = Path.Combine(AppContext.BaseDirectory, "dst-artifacts");
        string path = drawn.WriteArtifact(directory, "replay-check");

        IReadOnlyList<RandomScenarioAction> plan = RandomScenarioPlan.ParseFile(path);

        Assert.Equal(drawn.Actions.Count, plan.Count);

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 20260902 },
            logger,
            cancellationToken);

        RandomScenarioRunner runner = new(cluster, options, new SimulationRandom(20260902));

        RandomScenarioReport replayed = await runner.ReplayAsync(plan, cancellationToken);

        // The plan is applied action for action. What the cluster does between the actions is its
        // own business, so the two runs are compared on the plan rather than on the outcome.
        Assert.Equal(
            plan.Select(action => action.Describe()),
            replayed.Actions.Select(action => action.Describe()));

        Assert.True(replayed.InvariantChecks > 0, "The replay checked no invariants.");
    }

    /// <summary>
    /// A failing run shrinks its own plan when the environment asks for it.
    ///
    /// <para><b>What this protects.</b> The nightly job turns the shrinker on with an environment
    /// variable and collects the file it leaves behind. Nothing else exercises that path, so a
    /// mistake in it — a variable read wrongly, an artifact written to the wrong folder, a shrink
    /// that throws over the finding — would stay invisible until the night a real seed failed, which
    /// is the worst possible moment to learn about it.</para>
    ///
    /// <para>The failure is forced by giving the run a recovery budget of one step, so the
    /// convergence check cannot pass. Every candidate the shrinker tries fails the same way, which
    /// is fine: what is under test is the wiring, not the reduction.</para>
    /// </summary>
    [Fact]
    [Trait("Category", "DSTRandom")]
    public async Task AFailingRun_ShrinksItsPlanWhenAsked()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        string directory = Path.Combine(
            AppContext.BaseDirectory, "dst-artifacts", "shrink-wiring-check");

        if (Directory.Exists(directory))
            Directory.Delete(directory, recursive: true);

        string? enabled = Environment.GetEnvironmentVariable(ShrinkPolicy.EnabledVariable);
        string? budget = Environment.GetEnvironmentVariable(ShrinkPolicy.BudgetVariable);

        try
        {
            Environment.SetEnvironmentVariable(ShrinkPolicy.EnabledVariable, "1");
            Environment.SetEnvironmentVariable(ShrinkPolicy.BudgetVariable, "3");

            RandomScenarioOptions options = new()
            {
                ActionCount = 4,
                StepsPerAction = 3,

                // Zero steps to converge, so the convergence check reports a failure without ever
                // looking. One step does not work: the check runs after each step and a healthy
                // cluster passes on the first, so the run would succeed and this test would prove
                // nothing. Zero is the only budget that fails whatever the cluster does, which is
                // what a test of the reporting path needs.
                RecoveryStepBudget = 0,
                ArtifactDirectory = directory,
            };

            InvalidOperationException error =
                await Assert.ThrowsAsync<InvalidOperationException>(
                    () => RunSeedAsync(20260912, options, cancellationToken));

            Assert.Contains("Shrunk:", error.Message, StringComparison.Ordinal);

            string[] shrunk = Directory.GetFiles(directory, "*.shrunk.plan.txt");

            Assert.NotEmpty(shrunk);

            // The file the nightly collects has to carry the bounds, or a reader who promotes it
            // gets a plan that replays under the defaults instead of under the run that failed.
            IReadOnlyDictionary<string, string> header =
                RandomScenarioPlan.ParseHeaderFile(shrunk[0]);

            Assert.Equal("20260912", header["seed"]);
            Assert.Equal("0", header["recoveryStepBudget"]);
            Assert.True(header.ContainsKey("signature"), "The shrunk plan does not name the failure.");
        }
        finally
        {
            Environment.SetEnvironmentVariable(ShrinkPolicy.EnabledVariable, enabled);
            Environment.SetEnvironmentVariable(ShrinkPolicy.BudgetVariable, budget);

            // The file this test writes is named exactly as a failing seed's file is, and the
            // nightly job collects those and reports them as failures. A test that left one behind
            // would make every green night look like a bad one.
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>
    /// Seeds this run will explore: the corpus first, then the sweep. Duplicates are dropped so a
    /// sweep that lands on a corpus seed does not produce two tests with one name.
    /// </summary>
    public static TheoryData<ulong> Seeds()
    {
        TheoryData<ulong> data = new();
        HashSet<ulong> seen = [];

        foreach (ulong seed in RandomSeedCorpus.Load())
        {
            if (seen.Add(seed))
                data.Add(seed);
        }

        IReadOnlyList<ulong> sweep = RandomSeedCorpus.Sweep(
            RandomSeedCorpus.ConfiguredSeedBase(), RandomSeedCorpus.ConfiguredSeedCount());

        foreach (ulong seed in sweep)
        {
            if (seen.Add(seed))
                data.Add(seed);
        }

        return data;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Fails a run that broke its cost budget, when a budget is in force.
    ///
    /// <para><b>Why a passing run is checked at all.</b> A run that stops making progress still
    /// passes: every check holds, it simply takes an order of magnitude longer, and nothing reports
    /// that. The loss is silent, which is the only kind worth building a detector for.</para>
    ///
    /// <para><b>Why it is off unless the environment asks.</b> A developer's machine is not a
    /// controlled environment. A suite that failed because a build was running beside it would teach
    /// people to ignore the failure, and the metrics would then be worth less than nothing. The
    /// continuous-integration jobs set the variable.</para>
    /// </summary>
    private static void RequireWithinBudget(ulong seed, RandomScenarioReport report)
    {
        if (report.Metrics is null)
            return;

        IReadOnlyList<string> breaches = SimulationBudgetPolicy.Current().Breaches(report.Metrics);

        Assert.True(
            breaches.Count == 0,
            $"Seed {seed} ran outside its budget.{Environment.NewLine}" +
            $"{string.Join(Environment.NewLine, breaches)}{Environment.NewLine}" +
            $"Measured: {report.Metrics.Describe()}");
    }


    /// <summary>
    /// Runs one seed and turns any failure into a report a reader can act on.
    ///
    /// <para>The catch is the point of this method. A generated failure with no plan beside it is
    /// a number and a stack trace, and the reader's first question — what did the run do? — has no
    /// answer. The plan and the replay log are written before the failure is rethrown, and the seed
    /// is named in the message so the run can be repeated on the spot.</para>
    /// </summary>
    private async Task<RandomScenarioReport> RunSeedAsync(
        ulong seed,
        RandomScenarioOptions options,
        CancellationToken cancellationToken)
    {
        // A generated run is a few dozen entries long. At the production compaction cadence no run
        // would ever compact, and every rule about compaction would go unexercised, so the options
        // carry their own node configuration.
        SimulationClusterOptions clusterOptions = options.ToClusterOptions(seed);

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            clusterOptions,
            logger,
            cancellationToken);

        SimulationRandom random = new(seed);
        RandomScenarioRunner runner = new(cluster, options, random);

        string directory = options.ArtifactDirectory
                           ?? Path.Combine(AppContext.BaseDirectory, "dst-artifacts");

        RandomScenarioReport partial;
        Exception failure;
        string planPath;
        string replayPath;
        string failurePath;

        try
        {
            RandomScenarioReport report = await runner.RunAsync(cancellationToken);

            RequireWithinBudget(seed, report);

            return report;
        }
        catch (Exception error)
        {
            partial = runner.Partial();

            planPath = partial.WriteArtifact(directory, "random");
            replayPath = runner.WriteReplayLog(directory, "random");
            failure = error;

            // The finding goes to disk before anything else runs. The shrink below costs tens of
            // cluster runs, and a host that dies during it — the hang detector, a cancelled job —
            // used to take the failure message with it: the nightly of 2026-09-12 left a plan that
            // stopped at action 18 of 24 and not one line saying why. The file beside the plan is
            // what survives, and the job's summary step prints it.
            failurePath = WriteFailureArtifact(directory, seed, error, partial);
        }

        // The failing cluster is disposed before a shrink begins. A shrink starts a fresh cluster
        // for every candidate, and this one is still holding its endpoints, its threads and
        // whatever fault the failure left behind.
        await cluster.DisposeAsync();

        string shrink = await ShrinkIfAskedAsync(
            seed, options, partial, failure, directory, cancellationToken);

        throw new InvalidOperationException(
            $"Random run failed on seed {seed}. Re-run this seed to reproduce it exactly." +
            $"{Environment.NewLine}Plan: {planPath}" +
            $"{Environment.NewLine}Replay: {replayPath}" +
            $"{Environment.NewLine}Failure: {failurePath}" +
            shrink +
            $"{Environment.NewLine}{partial.Describe()}",
            failure);
    }

    /// <summary>
    /// Writes the failure beside the plan, as <c>random-seed-N.failure.txt</c>, and returns the path.
    ///
    /// <para>The exception first, in full, then the partial report. The first line is the failure
    /// signature a reader would otherwise reconstruct from a stack trace, and the report under it
    /// is the same text the test message carries — so the file stands on its own when the test
    /// message was never printed.</para>
    /// </summary>
    private static string WriteFailureArtifact(
        string directory,
        ulong seed,
        Exception failure,
        RandomScenarioReport partial)
    {
        Directory.CreateDirectory(directory);

        string path = Path.Combine(directory, $"random-seed-{seed}.failure.txt");

        File.WriteAllText(
            path,
            $"signature={FailureSignature.Of(failure)}{Environment.NewLine}" +
            $"{Environment.NewLine}{failure}{Environment.NewLine}" +
            $"{Environment.NewLine}{partial.Describe()}");

        return path;
    }

    /// <summary>
    /// Reduces the failing plan to the actions the failure needs, when the environment asks for it.
    ///
    /// <para><b>Why this is opt-in.</b> A shrink costs one cluster run per candidate, tens of them
    /// for one failure. The nightly search is where a failure is the point and where a reader will
    /// read the answer in the morning; a pull request wants the failure reported and nothing more.
    /// See <see cref="ShrinkPolicy"/>.</para>
    ///
    /// <para><b>Why it cannot throw.</b> The original failure is the result. A shrink that broke —
    /// on its own budget, on a cluster that would not start — must not replace the finding with a
    /// report about the tool that was looking at it, so its own failure is written into the message
    /// as text and the original is rethrown regardless.</para>
    /// </summary>
    private async Task<string> ShrinkIfAskedAsync(
        ulong seed,
        RandomScenarioOptions options,
        RandomScenarioReport partial,
        Exception failure,
        string directory,
        CancellationToken cancellationToken)
    {
        if (!ShrinkPolicy.Enabled())
            return string.Empty;

        string signature = FailureSignature.Of(failure);

        if (signature == FailureSignature.None)
            return $"{Environment.NewLine}Shrink: skipped, the failure carried no signature.";

        try
        {
            PlanShrinker shrinker = new(
                new ClusterPlanOracle(seed, options, logger).AsOracle(),
                ShrinkPolicy.Options());

            ShrinkResult result = await shrinker.ShrinkAsync(
                partial.Actions, signature, cancellationToken);

            // The shrunk file carries the same header the plan artifact does, so it can be dropped
            // straight into the regression corpus without a reader reconstructing the bounds.
            result = result with { Header = PlanHeader.For(seed, options) };

            string path = result.WriteArtifact(directory, $"random-seed-{seed}");

            return $"{Environment.NewLine}Shrunk: {path} " +
                   $"({result.Original.Count} actions to {result.Shrunk.Count} " +
                   $"in {result.CandidatesRun} runs)";
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error)
        {
            return $"{Environment.NewLine}Shrink failed: {error.Message}";
        }
    }
}
