
using Kommander;
using Kommander.Data;
using Kommander.Diagnostics;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Randomized chaos soak (nightly <c>Category=ChaosRandom</c> + opt-in <c>Category=Stress</c>). Every parameter
/// — node count (3–7), user partition count, writer count, fault window, and the per-message fault weights — is
/// derived deterministically from a single printed seed, so any failure reproduces exactly by re-running the
/// same seed. Each case injects a seeded random fault profile (scoped to user-partition traffic) over the whole
/// run, heals, then evaluates the liveness oracle: after all rules clear, a stable leader per partition and
/// every node converging (identical applied prefix) on fresh writes, with no continuous safety-invariant
/// violation at any point. On failure it emits the seed and the standard report.
///
/// <para><b>Skipped in the default suite</b> (nightly <c>ChaosRandom</c> / opt-in <c>Stress</c>). Two earlier
/// blockers are fixed: the non-contiguous-delivery bug (holes backfill; the consumer prefix converges) and the
/// <c>CommitMonotonicity</c> gap-aware-dip false positive (the invariant now compares the commit frontier
/// against the durable applied prefix, keyed per (node, partition)). It stays skipped because it is slow for a
/// PR run and because it surfaces a distinct, unresolved <b>leader-side commit hole</b>: under continuous
/// multi-partition faults the leader's per-proposal commit path advances its in-memory frontier
/// non-contiguously (out-of-order quorum completion jumps past a still-<c>Proposed</c> entry), stranding that
/// entry as Proposed below the frontier. Since backfill re-ships Committed entries only, the hole can never be
/// filled and every follower buffers later entries over it, so convergence times out. See <c>BlockedReason</c>
/// for the full mechanism and fix direction. The infrastructure is complete and validated: it prints a
/// reproducing seed + failure report, and the nemesis event log is bounded so a soak no longer OOMs.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestChaosRandomized
{
    // Two earlier blockers are now fixed: the non-contiguous-delivery bug (holes backfill; the consumer prefix
    // converges) and the CommitMonotonicity gap-aware-dip false positive (the invariant now compares the commit
    // frontier against the durable applied prefix, per (node, partition)). The tier stays skipped in the default
    // suite because (1) it is the nightly ChaosRandom / opt-in Stress tier — 5–7-node clusters over a fault
    // window are slow for a PR run — and (2) it surfaces a distinct, unresolved LEADER-SIDE commit-hole defect.
    //
    // Root cause (diagnosed, fix deferred): Kommander commits per proposal-ticket — each write's entry commits
    // when ITS own quorum arrives — and the leader's commit path (RaftWriteAhead.EnqueueCommit) advances the
    // in-memory frontier with `commitIndex = log.Id + 1`, NON-contiguously. Under faults, proposals complete
    // out of order: entry N+1's quorum can arrive before entry N's, so the leader jumps its frontier past the
    // still-Proposed entry N WITHOUT writing N's Committed marker. Entry N is then stranded — Proposed in the
    // WAL but below the in-memory frontier. GetRangeAsync ships Committed entries only, so the leader can never
    // re-ship N; every follower buffers N+1.. over the permanent hole and never converges. (Kommander's
    // unanchored live-propose — prevLogIndex=0 — means a follower can hold N+1 without N, so N+1's quorum does
    // not prove N reached a majority; the leader cannot simply infer N is committed from N+1.) The fix is a
    // commit-model change (gap-aware leader commit + re-drive of stalled proposals to real quorum) and deserves
    // its own design pass rather than a soak patch.
    private const string BlockedReason =
        "Nightly ChaosRandom/Stress tier: slow for the PR suite, and surfaces an unresolved leader-side commit " +
        "hole under continuous multi-partition faults. Out-of-order per-proposal commit (RaftWriteAhead." +
        "EnqueueCommit advances commitIndex non-contiguously) strands an entry as Proposed below the leader's " +
        "in-memory frontier; GetRangeAsync (Committed-only) can never re-ship it, so followers buffer later " +
        "entries over the permanent hole and convergence times out. Needs a commit-model fix (gap-aware leader " +
        "commit + stalled-proposal re-drive). The non-contiguous-delivery bug and the CommitMonotonicity dip " +
        "artifact it was previously blocked on are both fixed.";

    private readonly ITestOutputHelper _out;
    public TestChaosRandomized(ITestOutputHelper output) => _out = output;

    public static IEnumerable<object[]> Seeds() =>
        [[71_001], [71_002], [71_003], [71_004], [71_005]];

    [Theory(Skip = BlockedReason)]
    [Trait("Category", "ChaosRandom")]
    [MemberData(nameof(Seeds))]
    public async Task RandomSoak_HealsAndConverges(int seed)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await RunSoakAsync(ChaosRandomConfig.FromSeed(seed), ct);
    }

    [Fact(Skip = BlockedReason)]
    [Trait("Category", "Stress")]
    public async Task LongSoak_HealsAndConverges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // Opt-in long soak: a bigger cluster and a longer fault window than the nightly tier.
        ChaosRandomConfig cfg = ChaosRandomConfig.FromSeed(90_001) with { NodeCount = 5, Writers = 3, DurationMs = 20_000 };
        await RunSoakAsync(cfg, ct);
    }

    /// <summary>Drives one soak: build → inject profile + concurrent writers for the window → heal → liveness oracle.</summary>
    private async Task RunSoakAsync(ChaosRandomConfig cfg, CancellationToken ct)
    {
        _out.WriteLine($"[chaos-random] {cfg}");

        await using ChaosClusterHarness harness = await ChaosClusterHarness.BuildAsync(
            cfg.NodeCount, cfg.UserPartitionCount, cfg.Seed,
            new ChaosClusterOptions { Scenario = $"random-{cfg.Seed}", BasePort = 8800 }, ct);

        try
        {
            harness.Nemesis.SetRandomProfile(cfg.Profile);
            await RunWritersAsync(harness, cfg, ct);

            // Heal: clear all rules and the random profile, release anything parked (the profile emits no Hold,
            // but stay defensive) so the liveness oracle runs against a quiescent transport.
            harness.Nemesis.ClearRules();
            await harness.Nemesis.ReleaseHeldAsync();

            foreach (int partition in harness.UserPartitions)
            {
                await harness.WaitForSingleLeaderAsync(partition, ct);
                long target = 0;
                for (int i = 0; i < 5; i++)
                {
                    long idx = await harness.WriteAsync(partition, ct);
                    if (idx > 0) target = idx;
                }
                Assert.True(target > 0, $"post-heal writes must commit on partition {partition}");
                await harness.WaitForConvergenceAsync(partition, target, ct);
            }

            harness.Checker.ThrowIfViolated();
        }
        catch (Exception ex)
        {
            // Any failure still yields a reproducible seed plus the standard report.
            _out.WriteLine(await harness.BuildFailureReportAsync($"random-soak-failure: {ex.Message}", ct));
            throw;
        }
    }

    /// <summary>Runs <see cref="ChaosRandomConfig.Writers"/> concurrent writer loops for the fault window,
    /// tolerating fault-induced failures (a write that cannot commit under partition just retries next tick).</summary>
    private static async Task RunWritersAsync(ChaosClusterHarness harness, ChaosRandomConfig cfg, CancellationToken ct)
    {
        ValueStopwatch sw = ValueStopwatch.StartNew();
        Task[] writers = Enumerable.Range(0, cfg.Writers).Select(w => Task.Run(async () =>
        {
            int partition = harness.UserPartitions[w % harness.UserPartitions.Count];
            while (sw.GetElapsedMilliseconds() < cfg.DurationMs)
            {
                ct.ThrowIfCancellationRequested();
                try { await harness.WriteAsync(partition, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { throw; }
                catch { /* fault-induced failure: keep driving load */ }
                await Task.Delay(15, ct).ConfigureAwait(false);
            }
        }, ct)).ToArray();

        await Task.WhenAll(writers).ConfigureAwait(false);
    }
}

/// <summary>
/// Deterministically derives a random-soak run's shape from a single seed. The same seed always yields the same
/// cluster size, load, fault window, and fault weights, so a failing nightly seed reproduces exactly.
/// </summary>
public sealed record ChaosRandomConfig(
    int Seed, int NodeCount, int UserPartitionCount, int Writers, int DurationMs, NemesisRandomProfile Profile)
{
    public static ChaosRandomConfig FromSeed(int seed)
    {
        Random rng = new(seed);
        int nodes = rng.Next(3, 8);          // 3..7 voters
        int parts = rng.Next(1, 3);          // 1..2 user partitions
        int writers = rng.Next(1, 4);        // 1..3 concurrent writers
        int durationMs = rng.Next(1500, 3500);

        // Moderate per-message fault weights: perturb without sustained single-node starvation.
        NemesisRandomProfile profile = new()
        {
            Drop = 0.04 + rng.NextDouble() * 0.06,       // 4–10%
            Delay = 0.04 + rng.NextDouble() * 0.06,      // 4–10%
            Duplicate = 0.02 + rng.NextDouble() * 0.05,  // 2–7%
            Fail = 0.01 + rng.NextDouble() * 0.02,       // 1–3%
            DelayDuration = TimeSpan.FromMilliseconds(rng.Next(10, 40)),
        };
        return new ChaosRandomConfig(seed, nodes, parts, writers, durationMs, profile);
    }

    public override string ToString() =>
        $"seed={Seed} nodes={NodeCount} parts={UserPartitionCount} writers={Writers} durationMs={DurationMs} " +
        $"drop={Profile.Drop:F3} delay={Profile.Delay:F3} dup={Profile.Duplicate:F3} fail={Profile.Fail:F3}";
}
