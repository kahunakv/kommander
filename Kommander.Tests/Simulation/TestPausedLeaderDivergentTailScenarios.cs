using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.History;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.Transport;
using Kommander.Tests.Simulation.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// A paused leader leaves two live voters whose logs disagree in the way Raft §5.4.1 is written
/// for: one holds an older-term tail that is longer, the other holds one entry of the newer term.
/// The shape of CamusDB fault soak fs11 (Kommander 1.8.2, 2026-09-25).
///
/// <para><b>The defect.</b> The voter with the longer, older tail is behind by (term, index) and
/// grants. The candidate re-checked the grant by index alone, saw the voter's higher index, and
/// discarded it — every round, for eight terms. Each grant also re-armed the voter's candidacy
/// cooldown, so the voter never campaigned either. The partition had no leader for the whole 30 s
/// pause, and the run failed its total-outage check.</para>
///
/// <para><b>How the scenario builds the logs.</b> The leader A is cut off and given three
/// proposals it can only write locally: an uncommitted term-T tail no one else has. B and C elect B
/// in T+1 and commit one write there, so C's log is one entry longer than the committed prefix and
/// ends in T+1. B is then paused <i>before</i> A is healed, so nothing repairs A's tail. A and C are
/// now the live majority: A's log ends at (T, prefix+3), C's at (T+1, prefix+1).</para>
///
/// <para><b>Why this scenario, when the random search has a pause action.</b> The search pauses
/// nodes, but the state needs a cut leader with unreplicated proposals, a new leader that reached
/// only one follower, and a pause of that new leader before the cut leader is repaired — three
/// draws in one order, with a window of a few steps for the last one. The per-step rule
/// <c>available-majority-leads</c> now makes such a state a failure wherever the search does reach
/// it; this scenario makes sure the state is reached on every run.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestPausedLeaderDivergentTailScenarios
{
    private const int PartitionId = 1;

    /// <summary>Proposals the cut leader writes alone. More than the one entry the new term commits elsewhere.</summary>
    private const int TailLength = 3;

    private readonly ILogger<IRaft> logger;
    private readonly ITestOutputHelper output;

    public TestPausedLeaderDivergentTailScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
        output = outputHelper;
    }

    /// <summary>
    /// With the new leader paused, the two remaining voters elect one of themselves within a few
    /// election timeouts, the winner is the node with the newer last term, and the old leader's
    /// orphaned tail is overwritten. The per-step invariants run throughout, so the
    /// <c>available-majority-leads</c> rule judges the same window: on 1.8.2 the rule fires (or, at a
    /// tighter budget, the election assertion does) and the leader appears only once B resumes.
    /// </summary>
    [Fact]
    public async Task TwoLiveVoters_OlderLongerTailVersusNewerTerm_ElectWhileTheLeaderIsPaused()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 20260925 },
            logger,
            cancellationToken);

        SimulatedTransport transport = cluster.Transport;
        // Eight election timeouts (2 s of simulated time) is plenty for a two-node failover and
        // short enough that the rule, not the step budget below, is what names the defect.
        ClusterInvariantRunner invariants = new() { LeaderlessBoundInElectionTimeouts = 8 };
        ClientHistory history = new();

        SimulationNode oldLeader = await ElectAsync(cluster, cancellationToken);
        await history.AppendUniqueAsync(cluster, oldLeader, PartitionId, "Greeting", cancellationToken);
        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);

        long oldTerm = (await oldLeader.GetPartitionViewAsync(PartitionId, cancellationToken))!.Term;
        long prefix = MaxLogId(cluster, oldLeader);
        List<SimulationNode> others = cluster.Nodes.Where(node => node != oldLeader).ToList();

        // 1. Cut the leader off and hand it proposals only it can write.
        transport.PartitionNode(oldLeader.Endpoint);

        List<Task<ClientOperation>> tailWrites = [];
        for (int index = 0; index < TailLength; index++)
            tailWrites.Add(history.AppendUniqueAsync(cluster, oldLeader, PartitionId, "OrphanedTail", cancellationToken));

        Assert.True(
            await cluster.RunUntilAsync(
                () => Task.FromResult(MaxLogId(cluster, oldLeader) >= prefix + TailLength),
                stepCount: 60,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The cut leader did not write its {TailLength} proposals locally within 60 steps " +
            $"(max log id {MaxLogId(cluster, oldLeader)}, prefix {prefix}).");

        SimulationNode newLeader;
        SimulationNode follower;

        try
        {
            // 2. The other two elect a new leader, and one write in the new term reaches the
            //    remaining follower — the entry that makes its log fresher by term.
            newLeader = await ElectOnAsync(cluster, others, oldTerm, cancellationToken);
            follower = others.First(node => node != newLeader);

            long newTerm = (await newLeader.GetPartitionViewAsync(PartitionId, cancellationToken))!.Term;

            ClientOperation newTermWrite = await history.AppendUniqueAsync(cluster, newLeader, PartitionId, "NewTerm", cancellationToken);
            Assert.Equal(ClientOperationOutcome.Ok, newTermWrite.Outcome);

            Assert.True(
                await cluster.RunUntilAsync(
                    async () =>
                    {
                        RaftPartitionView? view = await follower.GetPartitionViewAsync(PartitionId, cancellationToken);
                        return view is not null && view.Term == newTerm && view.CommitIndex > prefix;
                    },
                    stepCount: 100,
                    advanceMilliseconds: 50,
                    cancellationToken),
                $"The follower did not commit the new term's write within 100 steps. prefix={prefix} newTerm={newTerm}. {await DescribeAsync(cluster, cancellationToken)}");

            // 3. Pause the new leader first, then heal the old one: nothing may repair the tail.
            newLeader.Pause();
        }
        finally
        {
            transport.HealPartition(oldLeader.Endpoint);
        }

        long oldLeaderMax = MaxLogId(cluster, oldLeader);
        long followerMax = MaxLogId(cluster, follower);

        output.WriteLine(
            $"old leader {oldLeader.Endpoint} term {oldTerm} max log {oldLeaderMax}; new leader {newLeader.Endpoint} paused; " +
            $"follower {follower.Endpoint} max log {followerMax}; prefix {prefix}");

        // The state the defect needs: the old leader's log is longer, the follower's ends in a newer term.
        Assert.True(oldLeaderMax > followerMax,
            $"Precondition: the cut leader's tail ({oldLeaderMax}) must be longer than the follower's log ({followerMax}).");
        Assert.True(followerMax > prefix, "Precondition: the follower must hold the new term's write.");

        // 4. The live pair must elect. Sixty steps is three seconds of simulated time, twelve
        //    election timeouts at the widest draw; a correct pair needs one or two, and the
        //    available-majority-leads rule fires at eight if they do not.
        string? winner = null;

        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                foreach (SimulationNode node in new[] { oldLeader, follower })
                {
                    RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);

                    if (view is { Role: RaftNodeState.Leader })
                    {
                        winner = node.Endpoint;
                        return true;
                    }
                }

                return false;
            },
            stepCount: 60,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            elected,
            $"With {newLeader.Endpoint} paused, {oldLeader.Endpoint} (log ends in term {oldTerm}, {oldLeaderMax} entries) and " +
            $"{follower.Endpoint} (newer term, {followerMax} entries) elected no leader in 60 steps. Longest leaderless " +
            $"available-majority episode so far: {invariants.LongestLeaderlessAvailableMajorityMs} ms. " +
            $"{await DescribeAsync(cluster, cancellationToken)}");

        Assert.Equal(follower.Endpoint, winner);

        // 5. The winner repairs the old leader: it follows the winner and reaches its commit
        //    frontier, with the new term's write in place of the first orphaned proposal. Orphaned
        //    Proposed rows above the winner's tail may linger until a later write overwrites them;
        //    the committed-prefix rules judge what matters.
        Assert.True(
            await cluster.RunUntilAsync(
                async () =>
                {
                    await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                    RaftPartitionView? repaired = await oldLeader.GetPartitionViewAsync(PartitionId, cancellationToken);
                    RaftPartitionView? leading = await follower.GetPartitionViewAsync(PartitionId, cancellationToken);

                    return repaired is not null
                           && leading is not null
                           && repaired.Leader == follower.Endpoint
                           && repaired.CommitIndex == leading.CommitIndex
                           && repaired.CommitIndex > prefix;
                },
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            $"The old leader was not repaired by the winner. {await DescribeAsync(cluster, cancellationToken)}");

        // The orphaned proposals were never acknowledged.
        Assert.True(
            await cluster.RunUntilAsync(
                () => Task.FromResult(tailWrites.All(write => write.IsCompleted)),
                stepCount: 200,
                advanceMilliseconds: 50,
                cancellationToken),
            "The proposals at the cut leader never answered.");

        foreach (Task<ClientOperation> write in tailWrites)
        {
            ClientOperation operation = await write;
            output.WriteLine($"orphaned proposal: {operation}");
            Assert.NotEqual(ClientOperationOutcome.Ok, operation.Outcome);
        }

        // 6. Everybody back; the cluster converges and the history holds.
        newLeader.Resume();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        await history.AppendUniqueAsync(cluster, leader, PartitionId, "Greeting", cancellationToken);
        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);
        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);

        ClientHistoryChecker.Check(
            history,
            cluster.Nodes.First(node => node.HasLiveManager).Wal.ReadLogsRange(PartitionId, 0),
            cluster.StepNumber);
    }

    /// <summary>
    /// The liveness rule stays quiet where the library is entitled to have no leader: two of three
    /// nodes paused is no majority, however long it lasts. Then the leader alone is paused and the
    /// rule must see the failover, not fire on it.
    /// </summary>
    [Fact]
    public async Task AvailableMajorityRule_IsSilentWithoutAMajority_AndAcrossAnOrdinaryFailover()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = 20260926 },
            logger,
            cancellationToken);

        ClusterInvariantRunner invariants = new() { LeaderlessBoundInElectionTimeouts = 8 };
        ClientHistory history = new();

        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        await history.AppendUniqueAsync(cluster, leader, PartitionId, "Greeting", cancellationToken);
        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);

        List<SimulationNode> followers = cluster.Nodes.Where(node => node != leader).ToList();

        // No majority: the leader and one follower paused, for far longer than the bound.
        leader.Pause();
        followers[0].Pause();

        for (int step = 0; step < 120; step++)
        {
            await cluster.StepAsync(50, cancellationToken);
            await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
        }

        Assert.Equal(0, invariants.LongestLeaderlessAvailableMajorityMs);

        // Ordinary failover: only the leader stays paused. The rule times the leaderless window and
        // must stay under its bound because the two followers elect.
        followers[0].Resume();

        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);
                IReadOnlyList<RaftPartitionView> views = await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);
                return views.Any(view => view.Role == RaftNodeState.Leader && view.Endpoint != leader.Endpoint);
            },
            stepCount: 100,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "The two live followers did not elect a leader within 100 steps.");

        long bound = invariants.LeaderlessBoundInElectionTimeouts * cluster.Options.EndElectionTimeoutMs;
        Assert.True(
            invariants.LongestLeaderlessAvailableMajorityMs < bound,
            $"An ordinary failover took {invariants.LongestLeaderlessAvailableMajorityMs} ms, at or over the {bound} ms bound.");

        leader.Resume();
        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static long MaxLogId(SimulationCluster cluster, SimulationNode node) =>
        cluster.GetWalSnapshots().TryGetValue(node.Endpoint, out SimulatedWalSnapshot? snapshot)
            ? snapshot.Partition(PartitionId)?.MaxLogId ?? -1
            : -1;

    private static async Task<string> DescribeAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        List<string> lines = [];

        foreach (SimulationNode node in cluster.Nodes)
        {
            RaftPartitionView? view = node.HasLiveManager
                ? await node.GetPartitionViewAsync(PartitionId, cancellationToken)
                : null;

            string entries = string.Join(
                ",",
                node.Wal.ReadLogsRange(PartitionId, 0).TakeLast(6).Select(log => $"{log.Id}:t{log.Term}:{log.Type}"));

            lines.Add($"{node.Endpoint} {node.LifecycleStatus} role={view?.Role} term={view?.Term} leader={view?.Leader} " +
                      $"commit={view?.CommitIndex} tail=[{entries}]");
        }

        return string.Join(" | ", lines);
    }

    private static async Task<SimulationNode> ElectAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        SimulationNode? leader = null;

        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                IReadOnlyList<RaftPartitionView> views = await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);
                List<RaftPartitionView> leaders = views.Where(view => view.Role == RaftNodeState.Leader).ToList();

                leader = leaders.Count == 1
                    ? cluster.Nodes.First(node => node.Endpoint == leaders[0].Endpoint)
                    : null;

                return leader is not null;
            },
            stepCount: 400,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No single leader was elected within the step budget.");

        return leader!;
    }

    /// <summary>Waits until one of <paramref name="candidates"/> leads in a term above <paramref name="aboveTerm"/>.</summary>
    private static async Task<SimulationNode> ElectOnAsync(
        SimulationCluster cluster,
        IReadOnlyList<SimulationNode> candidates,
        long aboveTerm,
        CancellationToken cancellationToken)
    {
        SimulationNode? winner = null;

        bool elected = await cluster.RunUntilAsync(
            async () =>
            {
                foreach (SimulationNode node in candidates)
                {
                    RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);

                    if (view is { Role: RaftNodeState.Leader } && view.Term > aboveTerm)
                    {
                        winner = node;
                        return true;
                    }
                }

                return false;
            },
            stepCount: 400,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(
            elected,
            $"Neither {string.Join(" nor ", candidates.Select(node => node.Endpoint))} was elected within 400 steps.");

        return winner!;
    }

    private static async Task ConvergeAsync(
        SimulationCluster cluster,
        ClusterInvariantRunner invariants,
        long index,
        CancellationToken cancellationToken)
    {
        bool converged = await cluster.RunUntilAsync(
            async () =>
            {
                await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

                IReadOnlyList<RaftPartitionView> views = await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);
                int live = cluster.Nodes.Count(node => node.HasLiveManager);

                return views.Count == live
                       && views.All(view => view.CommitIndex >= index)
                       && views.Select(view => view.CommitIndex).Distinct().Count() == 1;
            },
            stepCount: 600,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(converged, $"The cluster did not converge at or above {index} on every live node. {await DescribeAsync(cluster, cancellationToken)}");
    }
}
