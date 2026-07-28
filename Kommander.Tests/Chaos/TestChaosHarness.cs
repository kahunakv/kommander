
namespace Kommander.Tests.Chaos;

/// <summary>
/// Smoke test for <see cref="ChaosClusterHarness"/>: a fault-free 3-node cluster must build, accept a batch
/// of uniquely-identified writes, converge with no divergence, keep every continuous invariant satisfied,
/// and dispose cleanly with no leaked held messages.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestChaosHarness
{
    [Fact]
    public async Task FaultFree_Writes_Converge_WithNoInvariantViolations()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 3, userPartitionCount: 1, seed: 42,
            options: new ChaosClusterOptions { Scenario = "smoke", BasePort = 8620 }, ct: ct);

        int partition = harness.UserPartitions[0];

        long lastIndex = 0;
        for (int i = 0; i < 25; i++)
        {
            long idx = await harness.WriteAsync(partition, ct);
            Assert.True(idx > 0, "each write should commit");
            lastIndex = idx;
        }

        await harness.WaitForConvergenceAsync(partition, lastIndex, ct);

        // Let the background invariant checker sample the converged, healthy cluster.
        await Task.Delay(400, ct);
        harness.Checker.ThrowIfViolated();
    }
}
