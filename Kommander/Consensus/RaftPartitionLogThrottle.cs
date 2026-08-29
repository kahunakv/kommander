using System.Diagnostics;
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// Rate-limited diagnostic logging for the partition state machine's three storm-prone conditions:
/// WAL saturation (follower side), failed append acknowledgements (leader side), and the
/// backfill-decision probe.
///
/// <para><b>Why these are throttled at all.</b> Each condition is self-repeating by construction —
/// a saturated WAL rejects every subsequent append, a follower that cannot accept a batch cannot
/// accept the next one, and the backfill probe fires once per peer per heartbeat round. Logging
/// per occurrence turned one slow disk into a second source of disk pressure (a run that logged
/// each rejection produced 238k entries and a 251 MB log on one node) and buried the signal it was
/// meant to raise. Each method collapses to at most one line per second carrying the count
/// suppressed since the last, so the condition stays visible while the volume does not feed back
/// into the problem.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread and holds no locks by
/// design — the suppression counters are plain fields for that reason. Do not call from the
/// snapshot-send or any other background path.</para>
/// </summary>
internal sealed class RaftPartitionLogThrottle
{
    private readonly IRaftPartitionHost host;
    private readonly RaftPartitionCoreState coreState;
    private readonly ILogger<IRaft> logger;

    // WAL-saturation log throttle. A saturated partition rejects every inbound append, so the
    // condition is worth one line a second carrying a count, not one line per rejection: the log
    // is I/O contending with the very WAL writes whose slowness caused the saturation, so logging
    // each occurrence makes the condition it reports worse. 0 means "never logged" (mirrors the
    // Stopwatch-tick convention used elsewhere in the partition machine). Only touched on the
    // executor thread (single-threaded per partition), so neither field needs synchronization.
    private long lastWalSaturatedLogTicks;
    private int suppressedWalSaturatedLogs;

    // Same throttle on the leader's side of the same conversation. A saturated follower rejects
    // every batch it is sent, and the leader logged one warning per rejection: 15,484 in a run,
    // 2,365 within a single second. Keyed on the status so a *different* failure appearing during
    // a saturation storm is still reported at once rather than swallowed by the window. Only
    // touched on the executor thread, as above.
    private RaftOperationStatus? lastLoggedAckStatus;
    private long lastFailedAckLogTicks;
    private int suppressedFailedAckLogs;

    // Diagnostic throttle for the backfill-decision probe. It fires on a hot path (once per peer
    // per heartbeat round), so it collapses to one line a second. Executor thread only, as with
    // the throttles above.
    private long lastBackfillTraceTicks;
    private int suppressedBackfillTraces;

    // Diagnostic throttle for the served no-progress pause. The sender probes the pause on every
    // entry-carrying trigger, and the ack fast-path funnels every inbound ack into it — during
    // the wedge the pause exists for, that is once per network round-trip. Executor thread only.
    private long lastNoProgressPauseTraceTicks;
    private int suppressedNoProgressPauseTraces;

    public RaftPartitionLogThrottle(IRaftPartitionHost host, RaftPartitionCoreState coreState, ILogger<IRaft> logger)
    {
        this.host = host;
        this.coreState = coreState;
        this.logger = logger;
    }

    /// <summary>
    /// Reports that this follower rejected a replicated batch because its WAL queue is full,
    /// at most once per second per partition and carrying the count suppressed since the last
    /// line.
    /// </summary>
    /// <remarks>
    /// Throttled deliberately. Saturation rejects on every inbound append, so a per-occurrence
    /// log turns one slow disk into a second, larger source of disk pressure — the amplification
    /// is not hypothetical: a run that logged each rejection with a stack trace produced 238k
    /// entries and a 251 MB log on a single node. Aggregating loses nothing that matters here,
    /// because the useful facts are that the partition is saturated and roughly how hard, not
    /// the identity of any individual rejected batch.
    /// </remarks>
    public void LogWalSaturated(string endpoint, int depth, long localMaxLog)
    {
        long now = Stopwatch.GetTimestamp();

        if (lastWalSaturatedLogTicks != 0 && (now - lastWalSaturatedLogTicks) < Stopwatch.Frequency)
        {
            suppressedWalSaturatedLogs++;
            return;
        }

        lastWalSaturatedLogTicks = now;

        logger.LogWarning(
            "[{LocalEndpoint}/{PartitionId}/{State}] WAL saturated, rejecting append from {Endpoint}: depth={Depth} localMaxLog={LocalMaxLog} suppressedSinceLastLine={Suppressed}",
            host.LocalEndpoint,
            host.PartitionId,
            coreState.NodeState,
            endpoint,
            depth,
            localMaxLog,
            suppressedWalSaturatedLogs
        );

        suppressedWalSaturatedLogs = 0;
    }

    /// <summary>
    /// Reports a failed AppendLogs acknowledgement, collapsing consecutive acks carrying the
    /// same status into one line per second with the count suppressed since the last.
    /// </summary>
    /// <remarks>
    /// The leader mirror of <see cref="LogWalSaturated"/>, and it exists for the same reason: a
    /// follower that cannot accept a batch cannot accept the next one either, so the failure
    /// arrives once per attempt and the attempts are frequent. Keyed on the status so a new
    /// kind of failure during a storm is still surfaced immediately.
    /// </remarks>
    public void LogFailedAppendAck(RaftOperationStatus status, string endpoint, HLCTimestamp timestamp, long committedIndex)
    {
        long now = Stopwatch.GetTimestamp();

        if (lastLoggedAckStatus == status && (now - lastFailedAckLogTicks) < Stopwatch.Frequency)
        {
            suppressedFailedAckLogs++;
            return;
        }

        logger.LogWarning(
            "[{LocalEndpoint}/{PartitionId}/{State}] Got {Status} from {Endpoint} Timestamp={Timestamp} CommittedIndex={CommittedIndex} suppressedSinceLastLine={Suppressed}",
            host.LocalEndpoint,
            host.PartitionId,
            coreState.NodeState,
            status,
            endpoint,
            timestamp,
            committedIndex,
            suppressedFailedAckLogs
        );

        lastLoggedAckStatus   = status;
        lastFailedAckLogTicks = now;
        suppressedFailedAckLogs = 0;
    }

    /// <summary>
    /// DIAGNOSTIC. Records the inputs to one peer's backfill decision in a heartbeat round.
    /// </summary>
    /// <remarks>
    /// A permanent Debug-level probe, not a temporary one. A leader that sends nothing looks
    /// identical in the logs to a leader with nothing to send, and telling those apart is the whole
    /// question when replicas stop advancing. Every trigger here derives from
    /// <paramref name="followerMaxLog"/> — the leader's belief about the peer — so that value is
    /// what the trace exists to expose.
    ///
    /// <para><b>Two suppression stages, and why one is not enough.</b> The probe fires once per peer
    /// per heartbeat round, and the throttle state is per partition. The per-second cap alone
    /// therefore still emitted one line per partition per second, without end, on a healthy cluster.
    /// The interest gate below drops the rounds that carry no information: no batch to send, no gap,
    /// and backfill switched off by configuration. Such a round reports a decision that no input
    /// could have changed. A dropped round does not raise <c>suppressedBackfillTraces</c>, so that
    /// count stays a count of suppressed <i>interesting</i> traces.</para>
    /// </remarks>
    public void LogBackfillDecision(string endpoint, bool willBackfill, long followerMaxLog,
                                    long followerGap, bool idleTailGap, bool regressed, bool liveQuiet)
    {
        // Interest gate, ahead of the per-second throttle. See the remarks above: an idle partition
        // with backfill disabled reports the same all-zero decision every round forever.
        if (!willBackfill && followerGap == 0 && !host.Configuration.BackfillEnabled)
            return;

        long now = Stopwatch.GetTimestamp();

        if (lastBackfillTraceTicks != 0 && (now - lastBackfillTraceTicks) < Stopwatch.Frequency)
        {
            suppressedBackfillTraces++;
            return;
        }

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "[{LocalEndpoint}/{PartitionId}/{State}] DIAG backfill-decision peer={Endpoint} send={Send} enabled={Enabled} followerMaxLog={FollowerMaxLog} localCommitted={LocalCommitted} gap={Gap} threshold={Threshold} idleTailGap={IdleTailGap} regressed={Regressed} liveQuiet={LiveQuiet} liveCommitFloor={LiveCommitFloor} suppressedSinceLastLine={Suppressed}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, willBackfill,
                host.Configuration.BackfillEnabled,
                followerMaxLog, coreState.LocalCommittedIndex, followerGap, host.Configuration.BackfillThreshold,
                idleTailGap, regressed, liveQuiet, coreState.LiveCommitFloor, suppressedBackfillTraces);
        }

        lastBackfillTraceTicks   = now;
        suppressedBackfillTraces = 0;
    }

    /// <summary>
    /// DIAGNOSTIC. Records that the no-progress pause skipped one backfill batch to a peer.
    /// </summary>
    /// <remarks>
    /// Before this line existed, a partition could pace for an entire run with nothing in the log
    /// below the 4-ship episode Warning — the Jepsen analysis of the 1.3.4 meta-partition repair
    /// starvation could not tell whether partition 0 paced at all. One Debug line per second per
    /// partition, carrying the suppressed count, keeps the pacing visible without letting the
    /// wedge's per-ack probe rate flood the log.
    /// </remarks>
    public void LogBackfillNoProgressPaused(string endpoint, int fruitlessShips, TimeSpan pause, long reportedFrontier)
    {
        long now = Stopwatch.GetTimestamp();

        if (lastNoProgressPauseTraceTicks != 0 && (now - lastNoProgressPauseTraceTicks) < Stopwatch.Frequency)
        {
            suppressedNoProgressPauseTraces++;
            return;
        }

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "[{LocalEndpoint}/{PartitionId}/{State}] DIAG backfill no-progress pause peer={Endpoint} fruitlessShips={FruitlessShips} pauseMs={PauseMs} reportedFrontier={ReportedFrontier} suppressedSinceLastLine={Suppressed}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint,
                fruitlessShips, pause.TotalMilliseconds, reportedFrontier, suppressedNoProgressPauseTraces);
        }

        lastNoProgressPauseTraceTicks   = now;
        suppressedNoProgressPauseTraces = 0;
    }
}
