
using Kommander;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Fixed scenario 7 — duplicate and reordered append/completion. Every <c>AppendLogs</c> from the leader is
/// delivered twice and every <c>CompleteAppendLogs</c> is delayed, so followers observe duplicated appends and
/// reordered/stale commit acknowledgements. This exercises append idempotency and the exactly-once apply gate
/// (the regression guard for the follower duplicate-apply bug surfaced by the hash-chain oracle): the state
/// machines must show no divergence and no duplicate apply, and the cluster must still converge.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public class Scenario07_DuplicateReordered
{
    [Fact]
    public async Task DuplicatedAppends_ReorderedCompletions_NoDuplicateApply_Converges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 3, userPartitionCount: 1, seed: 7007,
            options: new ChaosClusterOptions { Scenario = "duplicate-reordered", BasePort = 8760 }, ct: ct);

        int partition = harness.UserPartitions[0];

        RaftManager leader = await harness.FindLeaderAsync(partition, ct);
        foreach (RaftManager follower in harness.Nodes.Where(n => n != leader))
        {
            // Duplicate every replicated append and delay the matching completion to reorder acknowledgements.
            harness.Nemesis.Duplicate(leader.LocalEndpoint, follower.LocalEndpoint,
                verb: NemesisVerb.AppendLogs, partition: partition, name: "dup");
            harness.Nemesis.Delay(leader.LocalEndpoint, follower.LocalEndpoint, TimeSpan.FromMilliseconds(30),
                verb: NemesisVerb.CompleteAppendLogs, partition: partition, name: "dup");
        }

        long lastIndex = 0;
        int committed = 0;
        for (int i = 0; i < 25; i++)
        {
            long idx = await harness.WriteAsync(partition, ct);
            if (idx > 0) { lastIndex = idx; committed++; }
        }
        Assert.True(committed >= 20, $"writes should still commit despite duplication/reordering (committed {committed}/25)");

        harness.Nemesis.Heal("dup");
        await harness.WaitForConvergenceAsync(partition, lastIndex, ct);

        // The oracle's strict exactly-once check: no index applied twice on any node.
        HashChainAssert.NoDuplicateApply(
            harness.Nodes.Select(n => harness.ChainFor(n.LocalEndpoint, partition)), partition, 7007);

        await Task.Delay(300, ct);
        harness.Checker.ThrowIfViolated();
    }
}
