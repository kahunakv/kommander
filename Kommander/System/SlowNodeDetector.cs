using Microsoft.Extensions.Logging;

namespace Kommander.System;

/// <summary>
/// Classifies nodes whose WAL commit wait stands far above the cluster's as <c>Slow</c>, so the
/// leader balancer can refuse them as transfer targets and drain the leaderships they hold.
///
/// <para><b>Why a classifier and not a term in the load score.</b> Commit wait is a property of a
/// node's device, and <see cref="GlobalLeadershipView.LoadByNode"/> is a <i>sum</i> over the node's
/// partitions — so a latency term folded into <c>Load</c> would be counted once per leadership, with
/// an amplification nobody chose. The balancer's load tier also emits count-neutral swaps, which
/// would leave a degraded node with exactly as many fsync streams as before. Avoidance needs a gate
/// and a drain; those need a boolean, and a boolean needs hysteresis.</para>
///
/// <para><b>Relative, not absolute.</b> A node is judged against the median of its peers, because a
/// legitimately heavy write load raises every node's wait together and leaves the ratio flat. An
/// absolute threshold cannot tell those apart and would drain a healthy cluster at its busiest
/// moment.</para>
///
/// <para><b>Threading.</b> Owned by <see cref="LeaderBalancer"/> and therefore driven only from the
/// coordinator's single-consumer loop, so the counter table needs no lock. <see cref="Reset"/> is
/// called on P0 leadership loss, exactly like the balancer's cooldown and outstanding-move tables:
/// a new P0 leader re-derives everything from the passes it observes itself.</para>
/// </summary>
internal sealed class SlowNodeDetector
{
    /// <summary>
    /// Minimum number of judgeable nodes before anything is classified. With two, the median is
    /// their mean, so a single slow node lifts the very median it is compared against and the ratio
    /// test loses its meaning. Three is the smallest set where one outlier leaves the median alone.
    /// </summary>
    private const int MinJudgeableNodes = 3;

    // Endpoint → consecutive passes seen as a slow candidate (never negative).
    private readonly Dictionary<string, int> _candidatePasses = new(StringComparer.Ordinal);

    // Endpoint → consecutive clean passes since it was last a candidate (only for classified nodes).
    private readonly Dictionary<string, int> _cleanPasses = new(StringComparer.Ordinal);

    // Currently classified nodes. Rebuilt in place each pass; never handed out unwrapped.
    private readonly HashSet<string> _slow = new(StringComparer.Ordinal);

    private readonly RaftConfiguration configuration;
    private readonly ILogger<IRaft> logger;

    internal SlowNodeDetector(RaftConfiguration configuration, ILogger<IRaft> logger)
    {
        this.configuration = configuration;
        this.logger = logger;
    }

    /// <summary>
    /// Drops all classification state. Called when this node stops being the P0 leader, so a later
    /// P0 term never acts on evidence it did not observe.
    /// </summary>
    internal void Reset()
    {
        _candidatePasses.Clear();
        _cleanPasses.Clear();
        _slow.Clear();

        // Published here as well as at the end of a pass: on P0 leadership loss this node stops
        // running passes entirely, so without this the gauge would keep reporting the last
        // classification made under a term that has ended.
        PublishGauge();
    }

    /// <summary>Current classification. Empty when the feature is off or nothing qualifies.</summary>
    internal IReadOnlySet<string> SlowNodes => _slow;

    /// <summary>
    /// Runs one classification pass over the view's node-health samples and returns the updated
    /// slow set.
    ///
    /// <para>Call this exactly once per balancer pass, and only for a pass that actually ran: the
    /// hysteresis counts passes, so calling it twice for one pass halves the effective enter delay,
    /// and calling it for a skipped pass credits evidence that was never gathered.</para>
    /// </summary>
    internal IReadOnlySet<string> Classify(GlobalLeadershipView view)
    {
        if (!configuration.EnableSlowNodeAvoidance)
        {
            if (_slow.Count > 0 || _candidatePasses.Count > 0 || _cleanPasses.Count > 0)
                Reset();

            Diagnostics.KommanderMetrics.BalancerSlowNodes = 0;
            return _slow;
        }

        // Only live voters are judged. A node that is not a transfer target either way is not worth
        // classifying, and a dead node's lingering sample must not skew the median.
        List<(string Endpoint, double WaitMs)> judgeable = [];
        foreach (string endpoint in view.LiveVoters)
        {
            if (!view.NodeHealth.TryGetValue(endpoint, out NodeHealthSample sample))
                continue;

            if (sample.Samples < configuration.SlowNodeMinSamples)
                continue;

            if (sample.AgeMs > configuration.SlowNodeObservationTtl.TotalMilliseconds)
                continue;

            judgeable.Add((endpoint, sample.CommitWaitMs));
        }

        if (judgeable.Count < MinJudgeableNodes)
        {
            // Not enough evidence to compare against. The two directions are treated differently on
            // purpose. A classified node is aged out, because it must not stay drained just because
            // the cluster went quiet. A node part-way toward classification keeps its progress,
            // because an evidence gap is not evidence of health — resetting there would let a
            // genuinely failing disk postpone detection indefinitely by going quiet between passes.
            // "Consecutive" therefore means consecutive *judged* passes on the way in.
            AdvanceCleanPasses(_slow.ToList());
            PublishGauge();
            return _slow;
        }

        double median = Median(judgeable);
        double threshold = global::System.Math.Max(
            median * configuration.SlowNodeMultiplier,
            configuration.SlowNodeFloorMs);

        List<string> candidates = [];
        List<string> clean = [];
        foreach ((string endpoint, double waitMs) in judgeable)
        {
            if (waitMs >= threshold)
                candidates.Add(endpoint);
            else
                clean.Add(endpoint);
        }

        // A majority slowdown is a cluster condition, not a per-node fault, and draining it would be
        // a self-inflicted outage. Treat the whole pass as clean instead.
        if (candidates.Count * 2 >= view.LiveVoters.Count)
        {
            logger.LogWarning(
                "[SlowNodeDetector] {CandidateCount} of {VoterCount} live voters exceed the slow threshold " +
                "({ThresholdMs:F2} ms, median {MedianMs:F2} ms); treating this as a cluster-wide slowdown and " +
                "classifying none",
                candidates.Count, view.LiveVoters.Count, threshold, median);

            clean.AddRange(candidates);
            candidates.Clear();
        }

        foreach (string endpoint in candidates)
        {
            _cleanPasses.Remove(endpoint);

            _candidatePasses.TryGetValue(endpoint, out int passes);
            passes++;
            _candidatePasses[endpoint] = passes;

            if (passes >= configuration.SlowNodeEnterPasses && _slow.Add(endpoint))
            {
                logger.LogWarning(
                    "[SlowNodeDetector] {Endpoint} classified slow after {Passes} consecutive passes " +
                    "(threshold {ThresholdMs:F2} ms, median {MedianMs:F2} ms); it will receive no leadership " +
                    "and its current leaderships will drain",
                    endpoint, passes, threshold, median);
            }
        }

        AdvanceCleanPasses(clean);
        PublishGauge();
        return _slow;
    }

    /// <summary>
    /// Credits a clean pass to each listed endpoint, releasing a classified node once it reaches
    /// <see cref="RaftConfiguration.SlowNodeExitPasses"/>. Candidate progress is dropped on the
    /// first clean pass, so the enter threshold means <i>consecutive</i> passes and a node cannot
    /// accumulate its way to a classification one isolated spike at a time.
    /// </summary>
    private void AdvanceCleanPasses(List<string> endpoints)
    {
        foreach (string endpoint in endpoints)
        {
            _candidatePasses.Remove(endpoint);

            if (!_slow.Contains(endpoint))
            {
                _cleanPasses.Remove(endpoint);
                continue;
            }

            _cleanPasses.TryGetValue(endpoint, out int passes);
            passes++;

            if (passes >= configuration.SlowNodeExitPasses)
            {
                _slow.Remove(endpoint);
                _cleanPasses.Remove(endpoint);

                logger.LogWarning(
                    "[SlowNodeDetector] {Endpoint} released after {Passes} consecutive clean passes; " +
                    "it is eligible for leadership again",
                    endpoint, passes);
            }
            else
            {
                _cleanPasses[endpoint] = passes;
            }
        }
    }

    private void PublishGauge() =>
        Diagnostics.KommanderMetrics.BalancerSlowNodes = _slow.Count;

    /// <summary>
    /// Median commit wait across the judgeable nodes. The list is small (one entry per live voter),
    /// so a sort is cheaper than a selection algorithm and keeps the tie behaviour obvious.
    /// </summary>
    private static double Median(List<(string Endpoint, double WaitMs)> judgeable)
    {
        List<double> waits = new(judgeable.Count);
        foreach ((string _, double waitMs) in judgeable)
            waits.Add(waitMs);

        waits.Sort();

        int mid = waits.Count / 2;
        return waits.Count % 2 == 1 ? waits[mid] : (waits[mid - 1] + waits[mid]) / 2.0;
    }
}
