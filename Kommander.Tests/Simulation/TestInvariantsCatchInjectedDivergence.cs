using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;
using Kommander.Tests.Simulation.Invariants;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Proves the invariant checker catches a real divergence in a real cluster, not only in a
/// hand-built dictionary.
///
/// <para><b>Why this is separate from the unit tests.</b> `TestClusterInvariantSet` proves each
/// rule fires when handed a violation. It cannot prove the runner ever hands it one. Between the
/// rules and the cluster sit the partition views, the write-ahead-log reads, the window bounds,
/// and the committed-type filter — any of which could silently drop every violation on the floor
/// and leave a green suite that checks nothing.</para>
///
/// <para>So these tests corrupt a running cluster's storage directly and require the checker to
/// notice. That is the end-to-end claim the harness rests on: if a node's committed prefix
/// diverges, the run fails.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "DSTSmoke")]
public sealed class TestInvariantsCatchInjectedDivergence
{
    private const int PartitionId = 1;

    private readonly ILogger<IRaft> logger;

    public TestInvariantsCatchInjectedDivergence(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        logger = loggerFactory.CreateLogger<IRaft>();
    }

    /// <summary>
    /// Two different payloads at one committed index must fail the run.
    ///
    /// <para>This is the log-matching violation in its plainest form. The cluster commits an entry
    /// on every node, then one follower's storage is rewritten to hold a different value at that
    /// same index. Nothing in the protocol did this — the point is that the checker must catch it
    /// whatever caused it.</para>
    /// </summary>
    [Fact]
    public async Task DivergentPayloadAtACommittedIndex_FailsTheRun()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartConvergedClusterAsync(seed: 555, cancellationToken);
        ClusterInvariantRunner invariants = new();

        // A clean check first. Without this, a checker that always threw would pass this test.
        await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

        SimulationNode victim = await GetFollowerAsync(cluster, cancellationToken);
        OverwriteCommittedEntry(victim, index: 1, term: 1, payload: "a different value"u8.ToArray());

        InvariantViolationException error = await Assert.ThrowsAsync<InvariantViolationException>(
            () => invariants.CheckAsync(cluster, PartitionId, cancellationToken));

        Assert.Equal(ClusterInvariantSet.CommittedEntriesAgree, error.InvariantName);
        Assert.Contains(victim.Endpoint, error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A hole in the leader's committed prefix must fail the run.
    ///
    /// <para>This is the shape that matters most. A leader missing a committed entry cannot
    /// replicate it, so the entry is lost for the rest of that term. The cluster commits three
    /// entries, then the middle one is removed from the leader's storage while the followers keep
    /// it. Leader completeness is the rule that must object.</para>
    /// </summary>
    [Fact]
    public async Task HoleInTheLeadersCommittedPrefix_FailsTheRun()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartConvergedClusterAsync(
            seed: 777, cancellationToken, proposals: 3);

        ClusterInvariantRunner invariants = new();

        // Record the committed history from the healthy cluster first. Leader completeness
        // measures a leader against what the cluster is known to have committed, so without this
        // pass there is nothing for the missing entry to be missing from.
        await invariants.CheckAsync(cluster, PartitionId, cancellationToken);

        SimulationNode leader = await GetLeaderAsync(cluster, cancellationToken);
        RemoveEntry(leader, index: 2);

        InvariantViolationException error = await Assert.ThrowsAsync<InvariantViolationException>(
            () => invariants.CheckAsync(cluster, PartitionId, cancellationToken));

        Assert.Equal(ClusterInvariantSet.LeaderCompleteness, error.InvariantName);
        Assert.Contains("hole", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Two nodes whose frontiers agree while their entries differ must fail the convergence check.
    ///
    /// <para>The frontier comparison alone would pass here, which is exactly why the convergence
    /// check also compares the entries behind it.</para>
    /// </summary>
    [Fact]
    public async Task AgreeingFrontiersOverDifferentEntries_FailTheConvergenceCheck()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using SimulationCluster cluster = await StartConvergedClusterAsync(seed: 999, cancellationToken);
        ClusterInvariantRunner invariants = new();

        await invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken);

        SimulationNode victim = await GetFollowerAsync(cluster, cancellationToken);
        OverwriteCommittedEntry(victim, index: 1, term: 1, payload: "diverged"u8.ToArray());

        InvariantViolationException error = await Assert.ThrowsAsync<InvariantViolationException>(
            () => invariants.CheckConvergedAsync(cluster, PartitionId, cancellationToken));

        Assert.Equal(ClusterInvariantSet.QuiescentConvergence, error.InvariantName);
        Assert.Contains("differs", error.Message, StringComparison.Ordinal);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Starts a cluster, elects a leader, commits <paramref name="proposals"/> entries, and waits
    /// until every node holds them.
    /// </summary>
    private async Task<SimulationCluster> StartConvergedClusterAsync(
        ulong seed,
        CancellationToken cancellationToken,
        int proposals = 1)
    {
        SimulationCluster cluster = await SimulationCluster.StartAsync(
            new SimulationClusterOptions { NodeCount = 3, PartitionCount = 1, Seed = seed },
            logger,
            cancellationToken);

        bool elected = await cluster.RunUntilAsync(
            () => HasSingleLeaderAsync(cluster, cancellationToken),
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(elected, "No leader was elected within the step budget.");

        SimulationNode leader = await GetLeaderAsync(cluster, cancellationToken);

        for (int proposal = 0; proposal < proposals; proposal++)
        {
            RaftReplicationResult result = await leader.Manager.ReplicateLogs(
                PartitionId,
                "Greeting",
                global::System.Text.Encoding.UTF8.GetBytes($"value-{proposal}"),
                cancellationToken: cancellationToken);

            Assert.Equal(RaftOperationStatus.Success, result.Status);
        }

        bool converged = await cluster.RunUntilAsync(
            () => Task.FromResult(cluster.Nodes.All(node => node.Wal.GetMaxLog(PartitionId) >= proposals)),
            stepCount: 200,
            advanceMilliseconds: 50,
            cancellationToken);

        Assert.True(converged, "The cluster did not converge before the injection.");
        return cluster;
    }

    /// <summary>
    /// Rewrites one committed entry in a node's storage. The in-memory write-ahead log keys
    /// entries by id, so writing an existing id replaces it in place.
    /// </summary>
    private static void OverwriteCommittedEntry(SimulationNode node, long index, long term, byte[] payload) =>
        node.Wal.Write(
        [
            (PartitionId, new List<RaftLog>
            {
                new()
                {
                    Id = index,
                    Term = term,
                    Type = RaftLogType.Committed,
                    LogType = "Greeting",
                    LogData = payload,
                },
            }),
        ]);

    /// <summary>
    /// Removes one entry from a node's storage, leaving a hole. Marking it rolled back is how the
    /// hole is made: the committed-type filter then skips it, which is exactly what a genuinely
    /// absent entry looks like to the checker.
    /// </summary>
    private static void RemoveEntry(SimulationNode node, long index) =>
        node.Wal.Write(
        [
            (PartitionId, new List<RaftLog>
            {
                new()
                {
                    Id = index,
                    Term = 1,
                    Type = RaftLogType.RolledBack,
                    LogType = "Greeting",
                    LogData = [],
                },
            }),
        ]);

    private static async Task<bool> HasSingleLeaderAsync(SimulationCluster cluster, CancellationToken cancellationToken)
    {
        IReadOnlyList<RaftPartitionView> views =
            await cluster.GetPartitionViewsAsync(PartitionId, cancellationToken);

        return views.Count(view => view.Role == RaftNodeState.Leader) == 1;
    }

    private static async Task<SimulationNode> GetLeaderAsync(
        SimulationCluster cluster,
        CancellationToken cancellationToken)
    {
        foreach (SimulationNode node in cluster.Nodes)
        {
            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view?.Role == RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No leader is present.");
    }

    private static async Task<SimulationNode> GetFollowerAsync(
        SimulationCluster cluster,
        CancellationToken cancellationToken)
    {
        foreach (SimulationNode node in cluster.Nodes)
        {
            RaftPartitionView? view = await node.GetPartitionViewAsync(PartitionId, cancellationToken);
            if (view is not null && view.Role != RaftNodeState.Leader)
                return node;
        }

        throw new InvalidOperationException("No follower is present.");
    }
}
