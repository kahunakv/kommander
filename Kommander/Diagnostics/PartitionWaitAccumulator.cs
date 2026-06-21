
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

    /// <param name="alpha">
    /// Per-sample smoothing weight ∈ (0, 1].  Defaults to <see cref="DefaultAlpha"/>.
    /// </param>
    public PartitionWaitAccumulator(double alpha = DefaultAlpha)
    {
        _alpha = alpha;
    }

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
        }
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
