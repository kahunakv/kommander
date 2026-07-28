
using Kommander;
using Kommander.Data;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Fixed scenario 6 — minority writes. The current leader is isolated (with one companion) into a two-node
/// minority; the three-node majority elects a new leader. Writes submitted through the deposed leader must
/// <b>never</b> be reported committed — the safety-critical property that a minority cannot commit. After
/// healing, writes through the majority leader converge across all five nodes, and no committed entry is ever
/// rolled back (checked continuously).
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public class Scenario06_MinorityWrites
{
    // SKIPPED: the post-heal convergence assertion surfaces the same deferred non-contiguous-delivery bug as
    // scenario 4 — a fully-isolated node that rejoins ends with a contiguous WAL but a consumer that skipped the
    // backfilled prefix. The safety half of this scenario (minority writes never commit) is unaffected; it is
    // the convergence half that trips the oracle. Un-skip once the delivery fix lands. See memory
    // non-contiguous-delivery-bug.
    [Fact]
    public async Task IsolatedMinority_WritesNeverCommit_MajorityConvergesAfterHeal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 5, userPartitionCount: 1, seed: 6006,
            options: new ChaosClusterOptions { Scenario = "minority-writes", BasePort = 8740 }, ct: ct);

        int partition = harness.UserPartitions[0];

        // Commit a baseline so there is a real committed prefix to protect.
        long baseline = 0;
        for (int i = 0; i < 5; i++)
        {
            long idx = await harness.WriteAsync(partition, ct);
            if (idx > 0) baseline = idx;
        }
        await harness.WaitForConvergenceAsync(partition, baseline, ct);

        // Isolate the leader + one companion as the minority; the other three are the majority.
        RaftManager deposed = await harness.FindLeaderAsync(partition, ct);
        RaftManager[] others = harness.Nodes.Where(n => n != deposed).ToArray();
        RaftManager companion = others[0];
        RaftManager[] majority = [others[1], others[2], others[3]];

        // Scope the cut to the user partition only: system-partition consensus, SWIM ping, and gossip keep
        // flowing so cluster membership stays healthy and the deposed user-partition leader steps down promptly
        // once healed. A cut that also severed those would collapse membership rather than test minority writes.
        foreach (RaftManager island in new[] { deposed, companion })
        foreach (RaftManager j in majority)
            harness.Nemesis.PartitionSymmetric(island.LocalEndpoint, j.LocalEndpoint, "island", partition: partition);

        // Writes through the deposed leader must not commit.
        for (int i = 0; i < 6; i++)
        {
            RaftReplicationResult r = await harness.WriteViaAsync(deposed, partition, ct);
            Assert.NotEqual(RaftOperationStatus.Success, r.Status);
        }
        harness.Checker.ThrowIfViolated();

        // Heal; the deposed leader must step down and a single leader drive convergence across all five nodes.
        harness.Nemesis.Heal("island");
        try
        {
            await harness.WaitForSingleLeaderAsync(partition, ct);
        }
        catch (TimeoutException)
        {
            // Spec liveness oracle: on timeout, emit the standard failure report.
            Assert.Fail(await harness.BuildFailureReportAsync("no-single-leader-after-heal", ct));
        }
        long lastIndex = baseline;
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
