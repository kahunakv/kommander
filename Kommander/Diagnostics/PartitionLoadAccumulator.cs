
using System.Diagnostics;

namespace Kommander.Diagnostics;

/// <summary>
/// Per-partition EWMA (exponentially weighted moving average) accumulator that
/// tracks executor throughput as an ops/sec rate and exposes a composite load score
/// for advisory leader-balancing decisions.
///
/// <para>Uses continuous-time exponential decay so an idle partition cools naturally
/// without periodic ticks. The mathematical invariant is: when ops arrive at a
/// constant rate R, the steady-state level is R/λ, and the recovered rate is
/// level × λ. The default decay constant λ = <see cref="DefaultDecayConstant"/> gives
/// a half-life of ≈ 6.9 s, so a partition that goes completely idle drops to &lt;12.5 %
/// of its peak load within two 10-second report intervals.</para>
///
/// <para><b>Advisory, never authoritative.</b> A stale or imprecise value only delays
/// a balancing decision — it never violates Raft safety because every leadership
/// transfer is still validated by <c>TransferLeadershipAsync</c> at execution time.</para>
/// </summary>
public sealed class PartitionLoadAccumulator
{
    /// <summary>
    /// Default decay constant λ (1/s). Half-life = ln(2)/λ ≈ 6.9 s. Tuned so an idle
    /// partition cools well within a few <c>LeaderBalancerReportInterval</c> periods.
    /// </summary>
    public const double DefaultDecayConstant = 0.1;

    private static readonly double TicksToSeconds = 1.0 / Stopwatch.Frequency;

    private readonly double _decayConstant;
    private readonly Func<long> _getTicks;
    private readonly object _lock = new();

    // Decayed accumulator level. Steady-state for constant rate R is R/λ.
    // Multiplied by λ to recover the rate estimate.
    private double _level;
    private long _lastTick;

    /// <param name="decayConstant">
    /// λ in 1/s. Smaller values produce slower decay (longer memory).
    /// Defaults to <see cref="DefaultDecayConstant"/>.
    /// </param>
    /// <param name="getTicks">
    /// Clock source (ticks). Injected for unit tests; leave null to use
    /// <see cref="Stopwatch.GetTimestamp"/>.
    /// </param>
    public PartitionLoadAccumulator(double decayConstant = DefaultDecayConstant, Func<long>? getTicks = null)
    {
        _decayConstant = decayConstant;
        _getTicks = getTicks ?? Stopwatch.GetTimestamp;
        _lastTick = _getTicks();
    }

    /// <summary>
    /// Records <paramref name="count"/> operations at the current moment.
    /// Thread-safe; intended to be called inline from the partition executor on
    /// every dispatched operation, co-located with <c>ExecutorOperationsTotal.Add</c>.
    /// </summary>
    public void RecordOps(long count)
    {
        if (count <= 0)
            return;

        long now = _getTicks();
        lock (_lock)
        {
            double elapsed = (now - _lastTick) * TicksToSeconds;
            _level = _level * Math.Exp(-_decayConstant * elapsed) + count;
            _lastTick = now;
        }
    }

    /// <summary>
    /// Returns the current EWMA ops/sec estimate, decaying from the last
    /// <see cref="RecordOps"/> call to now. Cheap to call; no allocation.
    /// </summary>
    public double CurrentOpsPerSecond()
    {
        long now = _getTicks();
        double level;
        long lastTick;
        lock (_lock)
        {
            level = _level;
            lastTick = _lastTick;
        }

        double elapsed = (now - lastTick) * TicksToSeconds;
        return level * Math.Exp(-_decayConstant * elapsed) * _decayConstant;
    }

    /// <summary>
    /// Advisory composite load score for this partition:
    /// <c>wOps × OpsPerSecond + wQueue × (clientQueueDepth + walQueueDepth)</c>.
    /// <para>
    /// The queue terms capture instantaneous backlog pressure independently of throughput,
    /// so a partition with a large pending queue registers higher load even at a low commit
    /// rate. Both weights and the resulting score are advisory and non-authoritative.
    /// </para>
    /// </summary>
    public double CurrentLoad(double wOps, double wQueue, int clientQueueDepth, int walQueueDepth) =>
        wOps * CurrentOpsPerSecond() + wQueue * (clientQueueDepth + walQueueDepth);
}
