using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.History;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.Transport;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// A five-node ring partition in which one follower is in both majorities: the Jepsen shape of
/// <c>681cf397</c>.
///
/// <para><b>The defect.</b> A follower that granted a vote in term T+1 kept its in-memory term at T.
/// The append fence compares the follower's term with the leader's, so the follower went on
/// accepting the deposed term-T leader's appends. That leader kept a quorum it should not have had,
/// and it committed and acknowledged writes that the new leader then overwrote. The Jepsen run lost
/// acknowledged writes (Kahuna nightly, 2026-08-11).</para>
///
/// <para><b>Why five nodes.</b> The defect needs two majorities that overlap in one node. With five
/// nodes A to E, A reaches {A, C, E} and B reaches {B, D, E}, and E is in both. Three nodes cannot
/// build it: every majority of three is two nodes, and two majorities that share one node leave
/// no third node for either side.</para>
///
/// <para><b>Why the scenario fixes the order of the election.</b> The follower in both majorities,
/// the straddler, must vote for the new candidate and then hear the old leader and not the new one.
/// Timers alone reach that order rarely, and a pre-vote from the candidate is refused while the
/// straddler still hears the old leader. So the scenario uses the transport's traffic filter
/// (<see cref="SimulatedTransport.SetLinkTraffic"/>):</para>
/// <list type="number">
/// <item>The straddler cannot send vote requests, so it cannot win the election itself.</item>
/// <item>The straddler and the old leader are cut off from each other until the new leader is
/// elected, so the straddler's leader goes stale and it grants the candidate's pre-vote.</item>
/// <item>The new side's links to the straddler carry election traffic only, so the straddler votes
/// and then never receives an append from the new leader.</item>
/// </list>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestStraddlingVoterScenarios
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;
    private readonly ITestOutputHelper output;

    public TestStraddlingVoterScenarios(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
        output = outputHelper;
    }

    /// <summary>
    /// A deposed leader cannot commit through a follower that voted in a higher term.
    ///
    /// <para>On a correct build the straddler adopts the higher term when it votes, refuses the old
    /// leader's append, and the old leader's write is not acknowledged. On the <c>681cf397</c> build
    /// the straddler accepts it, and the old leader acknowledges the write through {old leader, its
    /// follower, straddler}. Three of five nodes then hold the write as committed, and the new leader
    /// holds nothing at its index. The per-step rule <c>leader-completeness</c> fails on that state,
    /// through its check of the current leader's whole log. Measured: before that check existed, the
    /// defect showed only as a cluster that never converged, because the new leader's commit index
    /// stayed below the write and the committed-window check never read it.</para>
    ///
    /// <para><b>How Control A rebuilds the defect.</b> Two changes in <c>ElectionCoordinator.VoteAsync</c>:
    /// the follower does not adopt the vote's term in memory, and the vote is written synchronously.
    /// The second is needed because the queued vote write sends its grant only when the node's term
    /// equals the vote's term, and the completion router discards a completion from another term, so
    /// the first change alone stops every election.</para>
    /// </summary>
    [Fact]
    public async Task ADeposedLeader_CannotCommitThroughAFollowerThatVotedInAHigherTerm()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 5, PartitionCount = 1, Seed = 20260926 },
            logger,
            cancellationToken);

        SimulatedTransport transport = cluster.Transport;
        ClusterInvariantRunner invariants = new();
        ClientHistory history = new();

        SimulationNode oldLeader = await ElectAsync(cluster, cancellationToken);
        await history.AppendUniqueAsync(cluster, oldLeader, PartitionId, "Greeting", cancellationToken);
        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);

        List<SimulationNode> others = cluster.Nodes.Where(node => node != oldLeader).ToList();
        SimulationNode straddler = others[0];
        SimulationNode oldSide = others[1];
        SimulationNode[] newSide = [others[2], others[3]];

        long oldTerm = (await oldLeader.GetPartitionViewAsync(PartitionId, cancellationToken))!.Term;

        // The ring: the old side and the new side cannot reach each other.
        foreach (SimulationNode near in new[] { oldLeader, oldSide })
        {
            foreach (SimulationNode far in newSide)
            {
                transport.BlockLink(near.Endpoint, far.Endpoint);
                transport.BlockLink(far.Endpoint, near.Endpoint);
            }
        }

        // The straddler cannot start an election, and it stops hearing the old leader.
        foreach (SimulationNode node in others.Skip(1))
            transport.SetLinkTraffic(straddler.Endpoint, node.Endpoint, LinkTraffic.NoVoteRequests);

        transport.BlockLink(oldLeader.Endpoint, straddler.Endpoint);
        transport.BlockLink(straddler.Endpoint, oldLeader.Endpoint);

        // The new side reaches the straddler with election traffic only.
        foreach (SimulationNode node in newSide)
            transport.SetLinkTraffic(node.Endpoint, straddler.Endpoint, LinkTraffic.ElectionOnly);

        ClientOperation? write = null;

        try
        {
            SimulationNode newLeader = await ElectOnAsync(cluster, newSide, oldTerm, cancellationToken);

            RaftPartitionView straddlerView = (await straddler.GetPartitionViewAsync(PartitionId, cancellationToken))!;
            RaftPartitionView newLeaderView = (await newLeader.GetPartitionViewAsync(PartitionId, cancellationToken))!;

            output.WriteLine(
                $"old leader {oldLeader.Endpoint} term {oldTerm}; new leader {newLeader.Endpoint} term " +
                $"{newLeaderView.Term}; straddler {straddler.Endpoint} term {straddlerView.Term}");

            // The straddler hears the old leader again, and still not the new one.
            transport.UnblockLink(oldLeader.Endpoint, straddler.Endpoint);
            transport.UnblockLink(straddler.Endpoint, oldLeader.Endpoint);

            Task<ClientOperation> append = history.AppendUniqueAsync(
                cluster, oldLeader, PartitionId, "Greeting", cancellationToken);

            Assert.True(
                await cluster.RunUntilAsync(
                    () => Task.FromResult(append.IsCompleted),
                    stepCount: 400,
                    advanceMilliseconds: 50,
                    cancellationToken),
                "The write at the old leader did not answer within 400 steps.");

            write = await append;

            output.WriteLine($"write at the old leader: {write}");
        }
        finally
        {
            transport.ClearLinkFaults();
        }

        await ConvergeAsync(cluster, invariants, history.AcknowledgedCount, cancellationToken);

        // One more write at the healed cluster, so every node's log ends on a live commit and the
        // convergence check compares complete logs.
        SimulationNode leader = await ElectAsync(cluster, cancellationToken);
        await history.AppendUniqueAsync(cluster, leader, PartitionId, "Greeting", cancellationToken);
        await ConvergeAsync(cluster, invariants, 0, cancellationToken);

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);

        ClientHistoryChecker.Check(
            history,
            cluster.Nodes.First(node => node.HasLiveManager).Wal.ReadLogsRange(PartitionId, 0),
            cluster.StepNumber);

        Assert.NotEqual(ClientOperationOutcome.Ok, write!.Outcome);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

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
            $"Neither {string.Join(" nor ", candidates.Select(node => node.Endpoint))} was elected " +
            "on the new side within 400 steps.");

        return winner!;
    }

    /// <summary>
    /// Steps until every live node's commit index is at least <paramref name="index"/> and all are
    /// equal, running the per-step invariants on every step.
    /// </summary>
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

        if (converged)
            return;

        // Each node's frontier and the entries it holds, with their terms and types. A cluster that
        // committed two different entries at one index and cannot repair either shows it here.
        List<string> lines = [];

        foreach (SimulationNode node in cluster.Nodes.Where(candidate => candidate.HasLiveManager))
        {
            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);

            string entries = string.Join(
                ",",
                node.Wal.ReadLogsRange(PartitionId, 0)
                    .Select(log => $"{log.Id}:t{log.Term}:{log.Type}:{log.LogType}"));

            lines.Add($"{node.Endpoint} role={view?.Role} term={view?.Term} commit={view?.CommitIndex} [{entries}]");
        }

        Assert.Fail(
            $"The cluster did not converge at or above {index} on every live node. " +
            string.Join(" | ", lines));
    }
}
