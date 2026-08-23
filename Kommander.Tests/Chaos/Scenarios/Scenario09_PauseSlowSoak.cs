

using Kommander;
using Kommander.Data;

namespace Kommander.Tests.Chaos.Scenarios;

/// <summary>
/// Run-H-shaped soak (Caraxes <c>bank-optimistic-2h-h</c>, Kommander 1.2.12): 3 nodes, 3 user
/// partitions, closed-loop writers, and randomized <c>[pause, slow]</c> nemesis cycles against
/// random nodes. A pause is the SIGSTOP shape — hold everything in and out, then release the whole
/// backlog in one burst. After every heal, EVERY user partition must commit a fresh write within a
/// bounded window. Run H failed this oracle permanently after the first pause that hit the
/// partition-3 leader, while all health surfaces stayed green.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
[Trait("Category", "ChaosSoak")]
public class Scenario09_PauseSlowSoak
{
    public static IEnumerable<object[]> Seeds()
    {
        // (seed, basePort, quiescence): quiescence=true mirrors the production default.
        yield return [11001, 8800, false];
        yield return [11002, 8810, false];
        yield return [11003, 8820, true];
        yield return [11004, 8830, true];
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task PauseSlowCycles_EveryPartitionServesAfterEveryHeal(int seed, int basePort, bool quiescence)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            nodeCount: 3, userPartitionCount: 3, seed: seed,
            options: new ChaosClusterOptions
            {
                Scenario = "pause-slow-soak",
                BasePort = basePort,
                ConfigureNode = cfg =>
                {
                    if (quiescence)
                    {
                        cfg.EnableQuiescence = true;
                        cfg.PingInterval = TimeSpan.FromMilliseconds(50);
                        cfg.PingTimeout = TimeSpan.FromMilliseconds(100);
                        cfg.SuspicionTimeout = TimeSpan.FromMilliseconds(400);
                        cfg.QuiesceAfter = TimeSpan.FromMilliseconds(400);
                    }
                },
            }, ct: ct);

        Random rng = new(seed);

        // Warm-up: every partition serves.
        foreach (int p in harness.UserPartitions)
            Assert.True(await harness.WriteAsync(p, ct) > 0, $"warm-up write must commit on partition {p}");

        // Closed-loop background writers, one per (node, partition), bank-workload style.
        using CancellationTokenSource writerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        List<Task> writers = [];
        foreach (RaftManager n in harness.Nodes)
        foreach (int p in harness.UserPartitions)
        {
            writers.Add(Task.Run(async () =>
            {
                while (!writerCts.IsCancellationRequested)
                {
                    // Bounded per call: a write that neither succeeds, fails, nor times out within
                    // its own bound is exactly the orphaned-reply defect this scenario exists to
                    // catch (Caraxes run H) — surface it as a failure, never as a hung test.
                    try { await harness.WriteViaAsync(n, p, writerCts.Token).WaitAsync(TimeSpan.FromSeconds(30), writerCts.Token); }
                    catch (OperationCanceledException) { break; }
                    catch (TimeoutException)
                    {
                        throw new InvalidOperationException(
                            $"writer via {n.LocalEndpoint} p{p}: write got no response within 30 s — orphaned client reply");
                    }
                    catch { /* transport faults are expected while a fault is active */ }
                    try { await Task.Delay(15, writerCts.Token); } catch (OperationCanceledException) { break; }
                }
            }, ct));
        }

        const int cycles = 10;
        for (int cycle = 0; cycle < cycles; cycle++)
        {
            RaftManager target = harness.Nodes[rng.Next(harness.Nodes.Count)];
            RaftManager[] others = harness.Nodes.Where(n => n != target).ToArray();
            bool pause = rng.Next(2) == 0;
            string ruleName = $"cycle-{cycle}";

            if (pause)
            {
                // SIGSTOP shape: hold everything in and out of the target, then burst-release.
                foreach (RaftManager peer in others)
                {
                    harness.Nemesis.Hold(target.LocalEndpoint, peer.LocalEndpoint, name: ruleName);
                    harness.Nemesis.Hold(peer.LocalEndpoint, target.LocalEndpoint, name: ruleName);
                }

                await Task.Delay(rng.Next(400, 1400), ct);

                harness.Nemesis.Heal(ruleName);
                await harness.Nemesis.ReleaseHeldAsync();
            }
            else
            {
                // netem-slow shape: delay all traffic in and out of the target.
                TimeSpan delay = TimeSpan.FromMilliseconds(rng.Next(20, 70));
                foreach (RaftManager peer in others)
                {
                    harness.Nemesis.Delay(target.LocalEndpoint, peer.LocalEndpoint, delay, name: ruleName);
                    harness.Nemesis.Delay(peer.LocalEndpoint, target.LocalEndpoint, delay, name: ruleName);
                }

                await Task.Delay(rng.Next(800, 2000), ct);
                harness.Nemesis.Heal(ruleName);
            }

            // Progress oracle: after the heal, every user partition must commit a fresh write
            // within a bounded window, through whichever node currently leads it.
            foreach (int p in harness.UserPartitions)
            {
                long committedIndex = await WriteUntilCommittedAsync(harness, p, 20_000, ct);
                if (committedIndex < 0)
                {
                    string report = await harness.BuildFailureReportAsync(
                        $"cycle {cycle} ({(pause ? "pause" : "slow")} of {target.LocalEndpoint}): partition {p} " +
                        "did not commit any write within 20 s after heal", ct);
                    Assert.Fail(report);
                }
            }

            harness.Checker.ThrowIfViolated();
        }

        writerCts.Cancel();
        await Task.WhenAll(writers);
        harness.Checker.ThrowIfViolated();
    }

    /// <summary>Retries a write against every node until one commits, or returns -1 at the deadline.</summary>
    private static async Task<long> WriteUntilCommittedAsync(ChaosClusterHarness harness, int partition, int timeoutMs, CancellationToken ct)
    {
        DateTime deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            foreach (RaftManager n in harness.Nodes)
            {
                try
                {
                    // Bounded per call so an orphaned client reply (the Caraxes run-H defect)
                    // fails the oracle at its deadline instead of hanging the test forever.
                    RaftReplicationResult r = await harness.WriteViaAsync(n, partition, ct).WaitAsync(TimeSpan.FromSeconds(15), ct);
                    if (r.Status == RaftOperationStatus.Success)
                        return r.LogIndex;
                }
                catch (OperationCanceledException) { throw; }
                catch { /* keep trying */ }
            }

            await Task.Delay(100, ct);
        }

        return -1;
    }
}
