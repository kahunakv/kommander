
using Kommander;
using Kommander.Data;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Fixed scenario 3 — leadership-transfer notification loss. The leader initiates a graceful transfer to a
/// specific follower, but the logical <c>TransferLeadership</c> batch item on that directed link is dropped.
/// The cluster must recover through a bounded re-election to exactly one stable leader, and writes must resume
/// and converge. This verifies the transfer path degrades safely when its notification is lost rather than
/// leaving the partition leaderless.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public class Scenario03_LeadershipTransferLoss
{
    [Fact]
    public async Task DroppedTransferNotification_RecoversToSingleLeader_AndConverges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 3, userPartitionCount: 1, seed: 3003,
            options: new ChaosClusterOptions { Scenario = "leadership-transfer-loss", BasePort = 8680 }, ct: ct);

        int partition = harness.UserPartitions[0];

        long lastIndex = 0;
        for (int i = 0; i < 5; i++)
        {
            long idx = await harness.WriteAsync(partition, ct);
            if (idx > 0) lastIndex = idx;
        }
        await harness.WaitForConvergenceAsync(partition, lastIndex, ct);

        RaftManager leader = await harness.FindLeaderAsync(partition, ct);
        RaftManager target = harness.Nodes.First(n => n != leader);

        // Drop the TransferLeadership notification on the leader -> target link.
        harness.Nemesis.Drop(leader.LocalEndpoint, target.LocalEndpoint,
            verb: NemesisVerb.TransferLeadership, partition: partition, name: "xfer-loss");

        // Initiate the transfer; its notification to the target is lost.
        _ = await leader.TransferLeadershipAsync(partition, target.LocalEndpoint, ct);

        // Heal, then require a bounded recovery to exactly one stable leader with progress.
        harness.Nemesis.Heal("xfer-loss");
        RaftManager recovered = await harness.WaitForSingleLeaderAsync(partition, ct);

        for (int i = 0; i < 10; i++)
        {
            long idx = await harness.WriteAsync(partition, ct);
            if (idx > 0) lastIndex = idx;
        }
        await harness.WaitForConvergenceAsync(partition, lastIndex, ct);

        await Task.Delay(300, ct);
        harness.Checker.ThrowIfViolated();
    }
}
