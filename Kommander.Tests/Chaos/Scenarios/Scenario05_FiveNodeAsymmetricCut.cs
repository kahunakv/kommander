
using Kommander;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Five-node asymmetric cut. One-way drop rules are installed explicitly: the two minority
/// nodes can still <i>receive</i> from the majority but cannot <i>send</i> to it (their outbound links are
/// dropped). With the leader in the majority, the three-node majority keeps quorum; the asymmetry stresses the
/// completion/ack path rather than a clean bidirectional partition. Safety must hold throughout and, after the
/// one-way rules are healed, all five nodes must converge.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public class Scenario05_FiveNodeAsymmetricCut
{
    [Fact]
    public async Task AsymmetricCut_MajorityKeepsCommitting_HealsAndConverges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 5, userPartitionCount: 1, seed: 5005,
            options: new ChaosClusterOptions { Scenario = "five-node-asymmetric-cut", BasePort = 8720 }, ct: ct);

        int partition = harness.UserPartitions[0];

        RaftManager leader = await harness.FindLeaderAsync(partition, ct);
        RaftManager[] others = harness.Nodes.Where(n => n != leader).ToArray();
        RaftManager[] majority = [leader, others[0], others[1]];
        RaftManager[] minority = [others[2], others[3]];

        // Asymmetric: minority -> majority is dropped one way; majority -> minority still flows. Scoped to the
        // user partition so membership/SWIM stay healthy.
        foreach (RaftManager m in minority)
        foreach (RaftManager j in majority)
            harness.Nemesis.Partition(m.LocalEndpoint, j.LocalEndpoint, "oneway", partition: partition);

        long lastIndex = 0;
        int committed = 0;
        for (int i = 0; i < 20; i++)
        {
            long idx = await harness.WriteAsync(partition, ct);
            if (idx > 0) { lastIndex = idx; committed++; }
        }
        Assert.True(committed >= 15, $"majority should keep committing under the asymmetric cut (committed {committed}/20)");
        harness.Checker.ThrowIfViolated();

        harness.Nemesis.Heal("oneway");
        await harness.WaitForConvergenceAsync(partition, lastIndex, ct);

        await Task.Delay(300, ct);
        harness.Checker.ThrowIfViolated();
    }
}
