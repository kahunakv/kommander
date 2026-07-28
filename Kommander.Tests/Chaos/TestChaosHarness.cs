
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

    [Fact]
    public async Task CommitQuorum_LiveAcknowledgements_AreVoters_AndSatisfyQuorumDiscipline()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 3, userPartitionCount: 1, seed: 43,
            options: new ChaosClusterOptions { Scenario = "quorum-acks", BasePort = 8630 }, ct: ct);

        int partition = harness.UserPartitions[0];

        long last = 0;
        for (int i = 0; i < 15; i++)
        {
            long idx = await harness.WriteAsync(partition, ct);
            if (idx > 0) last = idx;
        }
        await harness.WaitForConvergenceAsync(partition, last, ct);

        ClusterView view = await harness.SampleAsync(ct);

        // The live commit path supplied real acknowledgements — not the old permanently-empty set that made
        // the quorum-discipline invariant vacuous.
        Assert.NotEmpty(view.CommitAcks);

        // Every recorded acknowledger is a voter: learners are never counted toward quorum, so a learner ack
        // could never inflate a commit here. (The invariant's failure path — a learner substituted for a voter
        // — is exercised synthetically in TestClusterInvariants.)
        Assert.All(view.CommitAcks, a => Assert.True(a.AckerIsVoter, $"acker {a.Acker} must be a voter"));

        // Each committed index carries a voter majority, and the live invariant agrees.
        foreach (IGrouping<(int, long), CommitAck> g in view.CommitAcks.GroupBy(a => (a.Partition, a.Index)))
        {
            int voters = g.Max(a => a.VotersTotal);
            int voterAcks = g.Where(a => a.AckerIsVoter).Select(a => a.Acker).Distinct().Count();
            Assert.True(voterAcks >= InvariantPredicates.VoterMajority(voters),
                $"index {g.Key.Item2} had {voterAcks} voter acks (need {InvariantPredicates.VoterMajority(voters)} of {voters})");
        }

        harness.Checker.ThrowIfViolated();
    }
}
