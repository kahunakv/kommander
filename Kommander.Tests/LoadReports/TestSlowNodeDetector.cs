using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.LoadReports;

/// <summary>
/// Pure unit tests for <see cref="SlowNodeDetector"/> covering: the unknown/healthy distinction,
/// the relative median test, hysteresis in both directions, the small-cluster guard, and the
/// minority safety valve.
///
/// <para>Every test drives passes explicitly rather than waiting on a timer, because the
/// hysteresis is counted in passes, not in seconds.</para>
/// </summary>
public sealed class TestSlowNodeDetector
{
    private static readonly HLCTimestamp T0 = new(0, 1_000_000, 0);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ClusterMember Voter(string endpoint) =>
        new() { Endpoint = endpoint, Role = ClusterMemberRole.Voter };

    private static NodeLoadReport Report(
        string endpoint,
        double commitWaitMs,
        long samples = 100,
        long ageMs = 0,
        params (int pid, double load, long sinceMs)[] leaderships) =>
        new()
        {
            Endpoint = endpoint,
            ReportVersion = 1,
            Time = T0,
            NodeCommitWaitMs = commitWaitMs,
            NodeCommitWaitSamples = samples,
            NodeCommitWaitAgeMs = ageMs,
            Leaderships = leaderships
                .Select(l => new PartitionLoad { PartitionId = l.pid, Load = l.load, LeaderSinceMs = l.sinceMs })
                .ToList(),
        };

    private static GlobalLeadershipView BuildView(params NodeLoadReport[] reports)
    {
        List<ClusterMember> members = reports.Select(r => Voter(r.Endpoint)).ToList();

        return GlobalLeadershipView.Build(
            reports,
            members,
            aliveEndpoints: new HashSet<string>(members.Select(m => m.Endpoint)),
            Ttl,
            T0);
    }

    private static RaftConfiguration Config(
        bool enabled = true,
        double multiplier = 3.0,
        double floorMs = 10.0,
        long minSamples = 20,
        int enterPasses = 3,
        int exitPasses = 6) => new()
        {
            Host = "localhost",
            Port = 9000,
            EnableSlowNodeAvoidance = enabled,
            SlowNodeMultiplier = multiplier,
            SlowNodeFloorMs = floorMs,
            SlowNodeMinSamples = minSamples,
            SlowNodeObservationTtl = TimeSpan.FromSeconds(30),
            SlowNodeEnterPasses = enterPasses,
            SlowNodeExitPasses = exitPasses,
        };

    private static SlowNodeDetector NewDetector(RaftConfiguration config) =>
        new(config, NullLoggerFactory.Instance.CreateLogger<IRaft>());

    private static IReadOnlySet<string> RunPasses(
        SlowNodeDetector detector,
        GlobalLeadershipView view,
        int passes)
    {
        IReadOnlySet<string> result = detector.SlowNodes;
        for (int i = 0; i < passes; i++)
            result = detector.Classify(view);
        return result;
    }

    // ── Feature switch ────────────────────────────────────────────────────────

    [Fact]
    public void Disabled_ClassifiesNothingHoweverSlowTheNodeIs()
    {
        SlowNodeDetector detector = NewDetector(Config(enabled: false));

        GlobalLeadershipView view = BuildView(
            Report("a:1", 1.0),
            Report("b:1", 1.0),
            Report("c:1", 5000.0));

        Assert.Empty(RunPasses(detector, view, 20));
    }

    // ── Unknown is never slow ─────────────────────────────────────────────────

    [Fact]
    public void TooFewSamples_IsUnknownAndNeverClassified()
    {
        SlowNodeDetector detector = NewDetector(Config(minSamples: 20));

        // c:1 is enormously slow but has only 5 observations behind the estimate.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 1.0),
            Report("b:1", 1.0),
            Report("c:1", 500.0, samples: 5),
            Report("d:1", 1.0));

        Assert.Empty(RunPasses(detector, view, 10));
    }

    [Fact]
    public void StaleObservation_IsUnknownAndNeverClassified()
    {
        SlowNodeDetector detector = NewDetector(Config());

        // c:1's figure is 90 s old, past the 30 s observation TTL. The EWMA does not decay on its
        // own, so acting on it would drain a node on evidence that stopped being true.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 1.0),
            Report("b:1", 1.0),
            Report("c:1", 500.0, ageMs: 90_000),
            Report("d:1", 1.0));

        Assert.Empty(RunPasses(detector, view, 10));
    }

    [Fact]
    public void ZeroCommitWaitWithNoSamples_IsNotTreatedAsHealthyEvidence()
    {
        SlowNodeDetector detector = NewDetector(Config());

        // A node reporting 0 ms with 0 samples (a restarted node, or a peer too old to carry the
        // field) must not drag the median down and drag its peers over the threshold.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 0.0, samples: 0),
            Report("b:1", 30.0),
            Report("c:1", 30.0),
            Report("d:1", 30.0));

        Assert.Empty(RunPasses(detector, view, 10));
    }

    // ── Small clusters ────────────────────────────────────────────────────────

    [Fact]
    public void FewerThanThreeJudgeableNodes_ClassifiesNobody()
    {
        SlowNodeDetector detector = NewDetector(Config());

        // With two nodes the median is their mean, so the slow node lifts the very median it is
        // compared against and the ratio test loses its meaning.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 1.0),
            Report("b:1", 500.0));

        Assert.Empty(RunPasses(detector, view, 10));
    }

    // ── Relative test ─────────────────────────────────────────────────────────

    [Fact]
    public void SustainedOutlier_IsClassifiedAfterTheEnterThreshold()
    {
        RaftConfiguration config = Config(enterPasses: 3);
        SlowNodeDetector detector = NewDetector(config);

        GlobalLeadershipView view = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 200.0),
            Report("d:1", 5.0));

        Assert.Empty(RunPasses(detector, view, 2));

        IReadOnlySet<string> slow = detector.Classify(view);
        Assert.Equal(["c:1"], slow.Order().ToArray());
    }

    [Fact]
    public void SingleSpike_NeverClassifies()
    {
        SlowNodeDetector detector = NewDetector(Config(enterPasses: 3));

        GlobalLeadershipView healthy = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 5.0),
            Report("d:1", 5.0));

        GlobalLeadershipView spike = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 200.0),
            Report("d:1", 5.0));

        // Two isolated spikes, separated by a clean pass, must not accumulate into a
        // classification: the enter threshold means consecutive passes.
        detector.Classify(spike);
        detector.Classify(healthy);
        detector.Classify(spike);
        detector.Classify(healthy);
        Assert.Empty(detector.Classify(spike));
    }

    [Fact]
    public void UniformlySlowCluster_ClassifiesNobody()
    {
        SlowNodeDetector detector = NewDetector(Config());

        // Every node is far above the absolute floor, but no node stands out against its peers.
        // This is a busy cluster, not a broken node.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 400.0),
            Report("b:1", 420.0),
            Report("c:1", 380.0),
            Report("d:1", 410.0));

        Assert.Empty(RunPasses(detector, view, 10));
    }

    [Fact]
    public void BelowAbsoluteFloor_ClassifiesNobodyDespiteLargeRatio()
    {
        SlowNodeDetector detector = NewDetector(Config(floorMs: 10.0));

        // 0.9 ms is 9× the 0.1 ms median, but no workload cares about the difference.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 0.1),
            Report("b:1", 0.1),
            Report("c:1", 0.9),
            Report("d:1", 0.1));

        Assert.Empty(RunPasses(detector, view, 10));
    }

    // ── Minority safety valve ─────────────────────────────────────────────────

    [Fact]
    public void HalfTheClusterSlow_ClassifiesNobody()
    {
        SlowNodeDetector detector = NewDetector(Config());

        // Two of four voters exceed the threshold. Draining half the cluster's leadership is a
        // self-inflicted outage, so the pass is treated as a cluster-wide slowdown instead.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 500.0),
            Report("d:1", 500.0));

        Assert.Empty(RunPasses(detector, view, 10));
    }

    [Fact]
    public void MinorityOfCluster_IsStillClassified()
    {
        SlowNodeDetector detector = NewDetector(Config());

        // One of five is a minority, so the valve does not fire.
        GlobalLeadershipView view = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 500.0),
            Report("d:1", 5.0),
            Report("e:1", 5.0));

        Assert.Equal(["c:1"], RunPasses(detector, view, 5).Order().ToArray());
    }

    // ── Exit hysteresis ───────────────────────────────────────────────────────

    [Fact]
    public void RecoveredNode_IsReleasedOnlyAfterTheExitThreshold()
    {
        SlowNodeDetector detector = NewDetector(Config(enterPasses: 2, exitPasses: 4));

        GlobalLeadershipView slowView = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 200.0),
            Report("d:1", 5.0));

        GlobalLeadershipView healthyView = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 5.0),
            Report("d:1", 5.0));

        Assert.Equal(["c:1"], RunPasses(detector, slowView, 2).Order().ToArray());

        // Three clean passes are one short of the exit threshold.
        Assert.Equal(["c:1"], RunPasses(detector, healthyView, 3).Order().ToArray());

        Assert.Empty(detector.Classify(healthyView));
    }

    [Fact]
    public void RelapseBeforeRelease_RestartsTheExitCount()
    {
        SlowNodeDetector detector = NewDetector(Config(enterPasses: 2, exitPasses: 4));

        GlobalLeadershipView slowView = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 200.0),
            Report("d:1", 5.0));

        GlobalLeadershipView healthyView = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 5.0),
            Report("d:1", 5.0));

        RunPasses(detector, slowView, 2);
        RunPasses(detector, healthyView, 3);
        detector.Classify(slowView);          // relapse
        RunPasses(detector, healthyView, 3);  // would have been enough without the relapse

        Assert.Equal(["c:1"], detector.SlowNodes.Order().ToArray());
    }

    [Fact]
    public void ClusterGoingQuiet_AgesOutAnExistingClassification()
    {
        SlowNodeDetector detector = NewDetector(Config(enterPasses: 2, exitPasses: 3));

        GlobalLeadershipView slowView = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 200.0),
            Report("d:1", 5.0));

        // Every node's observation is now stale, so nothing is judgeable. A classified node must
        // still age out rather than stay drained because the cluster stopped writing.
        GlobalLeadershipView quietView = BuildView(
            Report("a:1", 5.0, ageMs: 90_000),
            Report("b:1", 5.0, ageMs: 90_000),
            Report("c:1", 200.0, ageMs: 90_000),
            Report("d:1", 5.0, ageMs: 90_000));

        Assert.Equal(["c:1"], RunPasses(detector, slowView, 2).Order().ToArray());
        Assert.Empty(RunPasses(detector, quietView, 3));
    }

    // ── Reset ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Reset_DropsClassificationAndProgress()
    {
        SlowNodeDetector detector = NewDetector(Config(enterPasses: 2));

        GlobalLeadershipView view = BuildView(
            Report("a:1", 5.0),
            Report("b:1", 5.0),
            Report("c:1", 200.0),
            Report("d:1", 5.0));

        Assert.Equal(["c:1"], RunPasses(detector, view, 2).Order().ToArray());

        detector.Reset();
        Assert.Empty(detector.SlowNodes);

        // Progress is gone too: one pass after a reset is not enough to re-classify.
        Assert.Empty(detector.Classify(view));
    }
}
