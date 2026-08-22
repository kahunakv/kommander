
using Kommander;
using Kommander.Data;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Paused-leader resume (the Caraxes run H stall, Kommander 1.2.12). A partition leader is frozen
/// (SIGSTOP shape: nothing in, nothing out, everything queued for later) long enough for the
/// survivors to elect a successor, then resumed so the whole queued backlog lands in one burst —
/// stale heartbeats at the old term, stale acks, stale vote traffic. The cluster must converge
/// back to exactly one leader and serve writes within a bounded time. Run H wedged here
/// permanently while every health surface stayed green.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSmoke")]
public class Scenario08_PausedLeaderResume
{
    [Theory]
    [InlineData(9001, 8760)]
    [InlineData(9002, 8770)]
    [InlineData(9003, 8780)]
    public async Task PausedLeaderResumes_ClusterServesAgain(int seed, int basePort)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 3, userPartitionCount: 1, seed: seed,
            options: new ChaosClusterOptions { Scenario = "paused-leader-resume", BasePort = basePort }, ct: ct);

        int partition = harness.UserPartitions[0];

        // Establish a leader and a serving baseline.
        RaftManager pausedLeader = await harness.FindLeaderAsync(partition, ct);
        for (int i = 0; i < 5; i++)
            Assert.True(await harness.WriteAsync(partition, ct) > 0, "baseline write must commit");

        RaftManager[] survivors = harness.Nodes.Where(n => n != pausedLeader).ToArray();

        // Closed-loop background writers, one per node, like the bank workload: each keeps a
        // single request in flight against its node and tolerates failures.
        using CancellationTokenSource writerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        List<Task> writers = harness.Nodes.Select(n => Task.Run(async () =>
        {
            while (!writerCts.IsCancellationRequested)
            {
                try { await harness.WriteViaAsync(n, partition, writerCts.Token); }
                catch (OperationCanceledException) { break; }
                catch { /* transport faults are expected during the freeze */ }
                try { await Task.Delay(10, writerCts.Token); } catch (OperationCanceledException) { break; }
            }
        }, ct)).ToList();

        // Freeze: hold every message to and from the leader, all partitions and verbs — the
        // SIGSTOP shape. The held messages are NOT dropped; they deliver in a burst on resume.
        foreach (RaftManager peer in survivors)
        {
            harness.Nemesis.Hold(pausedLeader.LocalEndpoint, peer.LocalEndpoint, name: "freeze");
            harness.Nemesis.Hold(peer.LocalEndpoint, pausedLeader.LocalEndpoint, name: "freeze");
        }

        // The survivors must elect a successor while the old leader is frozen.
        await WaitAsync(async () =>
        {
            foreach (RaftManager n in survivors)
                if (await n.AmILeaderQuick(partition)) return true;
            return false;
        }, 15_000, ct);

        // Let stale in-flight traffic accumulate at the new term before the burst.
        await Task.Delay(750, ct);

        // Resume: heal the freeze first so fresh traffic flows, then release the whole backlog at
        // once (the resumed process drains its queues in a burst).
        harness.Nemesis.Heal("freeze");
        await harness.Nemesis.ReleaseHeldAsync();

        // Recovery oracle: exactly one stable leader, and a write through it commits, within a
        // bounded time. Run H never got here — the cluster stayed idle for an hour.
        RaftManager recovered = await harness.WaitForSingleLeaderAsync(partition, ct);

        long lastIndex = -1;
        await WaitAsync(async () =>
        {
            RaftReplicationResult r = await harness.WriteViaAsync(recovered, partition, ct);
            if (r.Status == RaftOperationStatus.Success) { lastIndex = r.LogIndex; return true; }
            // Leadership may have moved once more while converging; follow it.
            foreach (RaftManager n in harness.Nodes)
            {
                if (await n.AmILeaderQuick(partition)) { recovered = n; break; }
            }
            return false;
        }, 20_000, ct);

        writerCts.Cancel();
        await Task.WhenAll(writers);

        Assert.True(lastIndex > 0, "post-resume write must commit");
        harness.Checker.ThrowIfViolated();

        // All three nodes (including the resumed one) must converge on the committed history.
        await harness.WaitForConvergenceAsync(partition, lastIndex, ct);
        harness.Checker.ThrowIfViolated();
    }

    private static async Task WaitAsync(Func<Task<bool>> cond, int timeoutMs, CancellationToken ct)
    {
        DateTime deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (await cond()) return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Condition not met within {timeoutMs} ms.");
    }
}
