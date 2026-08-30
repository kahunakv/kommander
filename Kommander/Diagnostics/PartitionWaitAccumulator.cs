
using System.Diagnostics;

namespace Kommander.Diagnostics;

/// <summary>
/// Per-partition EWMA accumulator that tracks the average commit-wait latency
/// (enqueue → durable write completion) in milliseconds.
///
/// <para>Uses a simple geometric per-sample decay: each new observation is blended
/// with the running estimate at weight <see cref="DefaultAlpha"/>. Unlike
/// <see cref="PartitionLoadAccumulator"/>, this accumulator tracks a duration, not a
/// rate, so no continuous-time decay between samples is needed — the estimate ages
/// naturally because write batches arrive frequently under load and sparsely when
/// idle.</para>
///
/// <para><b>Advisory, never authoritative.</b> A stale or imprecise value only delays
/// a consumer decision — it never violates Raft safety.</para>
///
/// <para><b>The estimate does not age on its own.</b> Because the decay is per sample, an
/// accumulator that stops receiving observations holds its last value forever, and one that
/// never received any returns <c>0</c> — which reads as "perfectly fast" rather than "unknown".
/// Both are acceptable for a diagnostic read, and both are wrong for a placement decision, so
/// <see cref="SampleCount"/> and <see cref="AgeMs"/> exist to let a caller tell *unknown* and
/// *stale* apart from *healthy*. A caller that acts on the value must consult them.</para>
/// </summary>
public sealed class PartitionWaitAccumulator
{
    /// <summary>
    /// Default per-sample smoothing weight α. Higher values make the estimate
    /// more responsive to recent samples; lower values smooth more aggressively.
    /// 0.3 gives ≈4–5 samples to converge to 90 % of a sustained new level.
    /// </summary>
    public const double DefaultAlpha = 0.3;

    private readonly double _alpha;
    private readonly object _lock = new();

    private double _ewmaMs;
    private bool _hasObservation;
    private long _sampleCount;
    private long _lastObservationTicks;

    /// <param name="alpha">
    /// Per-sample smoothing weight ∈ (0, 1].  Defaults to <see cref="DefaultAlpha"/>.
    /// </param>
    /// <summary>
    /// Creates an accumulator.
    /// <paramref name="tickSource"/> supplies the observation stamps used to age a frozen value;
    /// it defaults to the process clock so existing callers are unchanged. The WAL scheduler
    /// passes the node's <see cref="RaftConfiguration.TickSource"/>, which keeps age computation
    /// deterministic in a simulation run.
    /// </summary>
    public PartitionWaitAccumulator(double alpha = DefaultAlpha, Kommander.Time.IMonotonicTickSource? tickSource = null)
    {
        _alpha = alpha;
        _tickSource = tickSource ?? Kommander.Time.SystemMonotonicTickSource.Instance;
    }

    private readonly Kommander.Time.IMonotonicTickSource _tickSource;

    /// <summary>
    /// Records a single batch's average enqueue-to-durable wait in milliseconds.
    /// Thread-safe; called by the WAL scheduler worker after each Write completes.
    /// </summary>
    public void RecordWaitMs(double avgWaitMs)
    {
        if (avgWaitMs < 0) return;
        lock (_lock)
        {
            if (!_hasObservation)
            {
                _ewmaMs = avgWaitMs;
                _hasObservation = true;
            }
            else
            {
                _ewmaMs = _alpha * avgWaitMs + (1.0 - _alpha) * _ewmaMs;
            }

            _sampleCount++;
            _lastObservationTicks = _tickSource.GetTimestamp();
        }
    }

    /// <summary>
    /// Number of observations recorded so far. <c>0</c> means <b>unknown</b>, not <b>fast</b>:
    /// a caller must not read a zero <see cref="CurrentWaitMs"/> from an unobserved accumulator
    /// as evidence of a healthy device. Saturates in practice only after ~2.9 × 10^11 batches.
    /// </summary>
    public long SampleCount
    {
        get { lock (_lock) return _sampleCount; }
    }

    /// <summary>
    /// Milliseconds elapsed since the last observation, or <c>0</c> when there is none
    /// (pair with <see cref="SampleCount"/> to tell those apart). Because the EWMA decays per
    /// sample rather than per second, this is the only way to detect that
    /// <see cref="CurrentWaitMs"/> describes the past rather than the present.
    /// </summary>
    public double AgeMs()
    {
        long lastTicks;
        lock (_lock)
        {
            if (_sampleCount == 0)
                return 0.0;
            lastTicks = _lastObservationTicks;
        }

        return (_tickSource.GetTimestamp() - lastTicks) * (1000.0 / _tickSource.Frequency);
    }

    /// <summary>
    /// Returns the current EWMA commit-wait estimate in milliseconds, or <c>0</c>
    /// if no batch has been recorded yet. Cheap to call; no allocation.
    /// </summary>
    public double CurrentWaitMs()
    {
        lock (_lock)
            return _ewmaMs;
    }
}
