using Kommander.WAL.Data;

namespace Kommander.Diagnostics;

/// <summary>
/// Process-wide, opt-in instrumentation for the WAL write path, used to turn the
/// double-fsync spec's code-level diagnosis into measured numbers (see
/// <c>specs/wal-double-fsync-groupcommit-spec.md</c>).
///
/// <para>It decomposes a committed write into its two durable phases — leader
/// <see cref="WALWriteOperationType.LeaderPropose"/> and leader
/// <see cref="WALWriteOperationType.LeaderCommit"/> — plus the follower
/// <see cref="WALWriteOperationType.FollowerAppend"/> phase, and records, per phase:</para>
/// <list type="bullet">
///   <item><b>Enqueued count</b> — how many operations of that phase entered the WAL
///     scheduler. Recorded by <see cref="RaftWriteAhead"/> at enqueue time. Two
///     enqueues (propose + commit) per single-round committed write confirms the
///     spec's "two serial fsyncs" structure from the producer side, independent of
///     how the scheduler coalesces them.</item>
///   <item><b>Durable latency</b> — the enqueue→durable wait (ms) observed by
///     <see cref="Kommander.WAL.IO.FairWalScheduler"/> when the phase's write batch
///     completes. On a controlled single-writer harness with no scheduler queueing
///     this approximates the per-phase fsync cost, which is the per-phase latency
///     split the spec asks to confirm rather than assume.</item>
/// </list>
///
/// <para><b>Inert when off.</b> <see cref="Enabled"/> defaults to <c>false</c>; every
/// record path begins with a single volatile read and returns immediately, so the
/// production write path pays no measurable cost. This is a diagnostic, never a
/// correctness or scheduling input — a missed or stale sample only blurs a
/// measurement, it never affects durability or commit ordering.</para>
///
/// <para><b>Not concurrent-run safe.</b> The accumulators are process-global. Tests
/// that toggle <see cref="Enabled"/> or read <see cref="Snapshot"/> must be serialized
/// (e.g. via a non-parallel collection) so one harness's writes do not pollute
/// another's measurement window.</para>
/// </summary>
public static class WalPhaseInstrumentation
{
    /// <summary>
    /// Master switch. When <c>false</c> (the default) all <c>Record*</c> calls are a
    /// single volatile read and a return — no counting, no allocation, no locking.
    /// Flip to <c>true</c> around a measurement window, then back to <c>false</c>.
    /// </summary>
    public static volatile bool Enabled;

    private static readonly PhaseAccumulator Propose = new();
    private static readonly PhaseAccumulator Commit = new();
    private static readonly PhaseAccumulator FollowerAppend = new();

    /// <summary>
    /// Upper bound on retained durable-latency samples per phase. A measurement run
    /// drives a fixed, modest write count, so this only guards against an accidental
    /// long-lived enabled window; once reached, further samples are counted but not
    /// retained for percentile computation.
    /// </summary>
    private const int MaxSamplesPerPhase = 200_000;

    /// <summary>
    /// Records that an operation of <paramref name="type"/> was successfully enqueued
    /// onto the WAL scheduler. Called from the leader propose/commit and follower
    /// append paths. No-op unless <see cref="Enabled"/>.
    /// </summary>
    public static void RecordEnqueued(WALWriteOperationType type)
    {
        if (!Enabled)
            return;

        PhaseAccumulator? acc = AccumulatorFor(type);
        acc?.RecordEnqueued();
    }

    /// <summary>
    /// Records the enqueue→durable latency (milliseconds) for a completed phase write.
    /// Called by the WAL scheduler worker once the batch containing the operation has
    /// been durably written. No-op unless <see cref="Enabled"/>.
    /// </summary>
    public static void RecordDurable(WALWriteOperationType type, double latencyMs)
    {
        if (!Enabled)
            return;

        PhaseAccumulator? acc = AccumulatorFor(type);
        acc?.RecordDurable(latencyMs);
    }

    /// <summary>Clears all accumulated counts and samples. Call before a measurement window.</summary>
    public static void Reset()
    {
        Propose.Reset();
        Commit.Reset();
        FollowerAppend.Reset();
    }

    /// <summary>
    /// Returns an immutable snapshot of the current per-phase counts and latency
    /// percentiles. Safe to call while disabled (returns whatever was last recorded).
    /// </summary>
    public static InstrumentationSnapshot Snapshot() => new(
        Propose.Snapshot(),
        Commit.Snapshot(),
        FollowerAppend.Snapshot()
    );

    private static PhaseAccumulator? AccumulatorFor(WALWriteOperationType type) => type switch
    {
        WALWriteOperationType.LeaderPropose => Propose,
        WALWriteOperationType.LeaderCommit => Commit,
        WALWriteOperationType.FollowerAppend => FollowerAppend,
        _ => null,
    };

    /// <summary>
    /// Thread-safe per-phase accumulator: an enqueue/durable counter pair plus a
    /// bounded sample buffer for latency percentiles. All access is under
    /// <see cref="_lock"/>; only touched on the (debug-only) enabled path.
    /// </summary>
    private sealed class PhaseAccumulator
    {
        private readonly object _lock = new();
        private long _enqueued;
        private long _durable;
        private double _sumMs;
        private readonly List<double> _samples = new();

        public void RecordEnqueued()
        {
            lock (_lock)
                _enqueued++;
        }

        public void RecordDurable(double latencyMs)
        {
            lock (_lock)
            {
                _durable++;
                _sumMs += latencyMs;
                if (_samples.Count < MaxSamplesPerPhase)
                    _samples.Add(latencyMs);
            }
        }

        public void Reset()
        {
            lock (_lock)
            {
                _enqueued = 0;
                _durable = 0;
                _sumMs = 0;
                _samples.Clear();
            }
        }

        public PhaseSnapshot Snapshot()
        {
            lock (_lock)
            {
                double mean = _durable > 0 ? _sumMs / _durable : 0.0;
                double[] sorted = _samples.ToArray();
                Array.Sort(sorted);
                return new PhaseSnapshot(
                    Enqueued: _enqueued,
                    Durable: _durable,
                    MeanMs: mean,
                    P50Ms: Percentile(sorted, 0.50),
                    P99Ms: Percentile(sorted, 0.99)
                );
            }
        }

        private static double Percentile(double[] sorted, double q)
        {
            if (sorted.Length == 0)
                return 0.0;

            // Nearest-rank percentile: index = ceil(q * N) - 1, clamped into range.
            int rank = (int)Math.Ceiling(q * sorted.Length) - 1;
            if (rank < 0)
                rank = 0;
            if (rank >= sorted.Length)
                rank = sorted.Length - 1;
            return sorted[rank];
        }
    }
}

/// <summary>Per-phase counts and latency percentiles captured at one instant.</summary>
public readonly record struct PhaseSnapshot(long Enqueued, long Durable, double MeanMs, double P50Ms, double P99Ms);

/// <summary>Snapshot of all three WAL phases. The input to the Task 3 go/no-go decision.</summary>
public readonly record struct InstrumentationSnapshot(PhaseSnapshot Propose, PhaseSnapshot Commit, PhaseSnapshot FollowerAppend);
