using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kommander.Time;

namespace Kommander.Diagnostics;

/// <summary>
/// One interval of a proposal round. Each value is recorded into
/// <see cref="RoundStageInstrumentation"/>'s <c>raft.round.stage_ms</c> histogram under the
/// <c>stage</c> tag named by <see cref="RoundStageInstrumentation.StageName"/>.
///
/// <para>The leader stages <see cref="LeaderQueue"/> … <see cref="LeaderResume"/> run in series
/// and add up to <see cref="LeaderRound"/>, apart from the gateway's own work before the first
/// executor hop. With <c>RaftConfiguration.FanOutBeforeLocalWrite</c> on (the default),
/// <see cref="LeaderFanout"/> follows <see cref="LeaderPropose"/> in the same executor turn, and
/// <see cref="LeaderWal"/> and <see cref="LeaderWalCompletion"/> run beside
/// <see cref="LeaderReplication"/> instead of before it: the chain is queue, propose, fanout,
/// replication, ack, resume. <see cref="LeaderReplication"/> is the leader-side view of the follower round
/// trip; the follower stages and the leader's ack stages split it, and what is left over is
/// transport (dispatch, serialization, sockets, TLS).</para>
/// </summary>
public enum RoundStage
{
    /// <summary>Leader: a client <c>ReplicateLogs</c> request waits in the partition executor's queue.</summary>
    LeaderQueue,

    /// <summary>Leader: the executor starts the proposal until the Proposed write is enqueued on the WAL scheduler.</summary>
    LeaderPropose,

    /// <summary>Leader: the Proposed write waits in the WAL scheduler and is written (enqueue → durable).</summary>
    LeaderWal,

    /// <summary>Leader: the Proposed write is durable until the executor handles its completion (callback, executor queue).</summary>
    LeaderWalCompletion,

    /// <summary>
    /// Leader: the executor registers the quorum and hands one <c>AppendLogs</c> per follower to the transport
    /// (when the Proposed write is queued, or after it is durable with <c>FanOutBeforeLocalWrite</c> off).
    /// </summary>
    LeaderFanout,

    /// <summary>
    /// Leader: fan-out is done until the step that makes the quorum releases the ticket — a follower's ack, or
    /// the leader's own propose completion when the followers answered before its write was durable.
    /// </summary>
    LeaderReplication,

    /// <summary>Leader: a follower's <c>CompleteAppendLogs</c> ack waits in the partition executor's queue (every ack).</summary>
    LeaderAckQueue,

    /// <summary>Leader: the executor processes the ack that makes the quorum, until the ticket is released.</summary>
    LeaderAck,

    /// <summary>Leader: the ticket is released until the caller's <c>ReplicateLogs</c> continuation runs.</summary>
    LeaderResume,

    /// <summary>Leader: the whole successful <c>ReplicateLogs</c> call, from the gateway entry to the caller's resume.</summary>
    LeaderRound,

    /// <summary>Leader: the commit-marker write in the WAL scheduler. Off the caller's path when the single-fsync commit is on.</summary>
    LeaderCommitWal,

    /// <summary>Follower: an entry-carrying <c>AppendLogs</c> waits in the partition executor's queue.</summary>
    FollowerQueue,

    /// <summary>Follower: the executor starts the append until the write is enqueued on the WAL scheduler.</summary>
    FollowerAppend,

    /// <summary>Follower: the append waits in the WAL scheduler and is written (enqueue → durable).</summary>
    FollowerWal,

    /// <summary>Follower: the append is durable until the executor handles its completion.</summary>
    FollowerWalCompletion,

    /// <summary>
    /// Follower: the executor handles the completion until the ack is handed to the transport. With
    /// <c>RaftConfiguration.FollowerApplyInOwnTurn</c> off, this includes delivering newly committed
    /// entries to the application (<c>OnReplicationReceived</c>), which the follower then does before
    /// it acks; with it on (the default) that delivery is <see cref="FollowerApply"/>.
    /// </summary>
    FollowerAck,

    /// <summary>
    /// Follower: one apply turn that delivered committed entries to the application, when the follower
    /// applies after its ack (<c>RaftConfiguration.FollowerApplyInOwnTurn</c>). Off the round's path; an
    /// append that arrives during a turn waits for it in <see cref="FollowerQueue"/>.
    /// </summary>
    FollowerApply,

    /// <summary>Either side: an <c>AppendLogs</c> or <c>CompleteAppendLogs</c> waits in the transport dispatcher until its send starts.</summary>
    TransportDispatch,
}

/// <summary>
/// Opt-in per-stage timing of a proposal round, recorded as the <c>raft.round.stage_ms</c>
/// histogram (tag <c>stage</c>) on the <c>Kommander</c> meter. It exists to split the round cost
/// that CPU sampling cannot place (a leader trace under load found ~1.0 ms of a 2.3 ms round
/// as waiting with no stack): sampling does not time sub-millisecond waits, and a per-stage stamp
/// does.
///
/// <para><b>One clock.</b> Every stamp is <see cref="Stopwatch.GetTimestamp"/>, never the
/// configured <see cref="IMonotonicTickSource"/>: the stages subtract stamps taken in different
/// components, and a simulated tick source (DST) would mix clocks. The one exception is the WAL
/// write stages, which reuse the scheduler's own enqueue → durable measurement on its tick source;
/// in production that source is the stopwatch. On Linux the stopwatch is
/// <c>CLOCK_MONOTONIC</c>, which every container on one host shares, so the leader and follower
/// histograms of one host describe the same time line.</para>
///
/// <para><b>Inert when off.</b> The switch is <see cref="Enabled"/> (default from the
/// <c>KOMMANDER_ROUND_STAGES</c> environment variable, <c>1</c> or <c>true</c>) AND a listener on
/// the histogram. When either is missing, <see cref="Stamp"/> returns 0 after one volatile read
/// and one field read, and every <c>Record</c> call returns at once on a 0 start stamp. A stamp is
/// never a correctness or scheduling input: a lost or stale stamp only blurs a measurement.</para>
///
/// <para><b>Stages are intervals, not a trace.</b> Each stage is recorded where its interval ends,
/// with a start stamp carried in an object that already travels between the two points (the
/// executor's queue envelope, the WAL operation, the WAL completion, the proposal quorum). No
/// stage needs a per-proposal lookup, except <see cref="RoundStage.LeaderResume"/>, whose start
/// (the ticket release, on the executor) and end (the caller's continuation) share nothing but the
/// ticket; it goes through a small bounded side table keyed by (partition, ticket).</para>
///
/// <para><b>Means add up; percentiles do not.</b> The stage means of the leader chain sum to
/// the round mean. The follower stages are averaged over both followers, while the round waits
/// only for the faster one, so the follower means slightly overstate the quorum follower's
/// share.</para>
/// </summary>
public static class RoundStageInstrumentation
{
    /// <summary>The environment variable that sets the initial value of <see cref="Enabled"/>.</summary>
    public const string EnvironmentVariable = "KOMMANDER_ROUND_STAGES";

    /// <summary>The histogram name on the <c>Kommander</c> meter.</summary>
    public const string HistogramName = "raft.round.stage_ms";

    /// <summary>The tag that carries the stage name.</summary>
    public const string StageTag = "stage";

    /// <summary>
    /// Master switch. The histogram also needs a listener; <see cref="IsActive"/> is the
    /// combination. Flip at any time; stages already in flight when it flips on are dropped
    /// because their start stamp is 0.
    /// </summary>
    public static volatile bool Enabled = ReadEnvironmentSwitch();

    private static readonly string[] StageNames =
    [
        "leader.queue",
        "leader.propose",
        "leader.wal",
        "leader.wal_completion",
        "leader.fanout",
        "leader.replication",
        "leader.ack_queue",
        "leader.ack",
        "leader.resume",
        "leader.round",
        "leader.commit_wal",
        "follower.queue",
        "follower.append",
        "follower.wal",
        "follower.wal_completion",
        "follower.ack",
        "follower.apply",
        "transport.dispatch",
    ];

    private static readonly KeyValuePair<string, object?>[] StageTags =
        [.. StageNames.Select(name => new KeyValuePair<string, object?>(StageTag, name))];

    private static readonly Histogram<double> StageHistogram =
        KommanderMetrics.Meter.CreateHistogram<double>(
            HistogramName,
            unit: "ms",
            description: "Per-stage duration of a proposal round (opt-in: KOMMANDER_ROUND_STAGES=1), by stage.");

    /// <summary>
    /// Ticket release stamps waiting for the caller's continuation. Bounded: a caller that never
    /// resumes (timeout, cancellation, a waiter found already complete) leaves its entry, and the
    /// table is cleared when it reaches <see cref="MaxPendingReleases"/> rather than grow.
    /// </summary>
    private static readonly ConcurrentDictionary<(int PartitionId, HLCTimestamp Ticket), long> PendingReleases = new();

    private const int MaxPendingReleases = 65_536;

    private static readonly double TicksToMs = 1000.0 / Stopwatch.Frequency;

    /// <summary>True when stages are both switched on and listened to.</summary>
    public static bool IsActive => Enabled && StageHistogram.Enabled;

    /// <summary>Returns the tag value recorded for <paramref name="stage"/>.</summary>
    public static string StageName(RoundStage stage) => StageNames[(int)stage];

    /// <summary>All stage tag values, in <see cref="RoundStage"/> order.</summary>
    public static IReadOnlyList<string> AllStageNames => StageNames;

    /// <summary>
    /// A start stamp for a later <see cref="Record(RoundStage, long)"/>, or 0 when inactive. A 0
    /// stamp makes the matching <c>Record</c> a no-op, so an interval that starts while inactive is
    /// never recorded with a bogus length.
    /// </summary>
    internal static long Stamp() => IsActive ? Stopwatch.GetTimestamp() : 0;

    /// <summary>Records the interval from <paramref name="startTicks"/> to now. No-op on a 0 start.</summary>
    internal static void Record(RoundStage stage, long startTicks)
    {
        if (startTicks == 0 || !IsActive)
            return;

        Record(stage, startTicks, Stopwatch.GetTimestamp());
    }

    /// <summary>Records the interval between two stopwatch stamps. No-op on a 0 start.</summary>
    internal static void Record(RoundStage stage, long startTicks, long endTicks)
    {
        if (startTicks == 0 || endTicks < startTicks)
            return;

        StageHistogram.Record((endTicks - startTicks) * TicksToMs, StageTags[(int)stage]);
    }

    /// <summary>Records an interval already measured in milliseconds.</summary>
    internal static void RecordMs(RoundStage stage, double milliseconds)
    {
        if (!IsActive)
            return;

        StageHistogram.Record(milliseconds, StageTags[(int)stage]);
    }

    /// <summary>
    /// Notes the moment the ticket of a proposal is released to its waiter. Called on the leader's
    /// executor; the caller's continuation takes it with <see cref="RecordResume"/>.
    /// </summary>
    internal static void MarkReleased(int partitionId, HLCTimestamp ticket)
    {
        if (!IsActive)
            return;

        if (PendingReleases.Count >= MaxPendingReleases)
            PendingReleases.Clear();

        PendingReleases[(partitionId, ticket)] = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Records <see cref="RoundStage.LeaderResume"/> for the ticket, if its release was marked,
    /// and <see cref="RoundStage.LeaderRound"/> from <paramref name="roundStartTicks"/>.
    /// </summary>
    internal static void RecordResume(int partitionId, HLCTimestamp ticket, long roundStartTicks)
    {
        if (!IsActive)
            return;

        long now = Stopwatch.GetTimestamp();

        if (PendingReleases.TryRemove((partitionId, ticket), out long releasedTicks))
            Record(RoundStage.LeaderResume, releasedTicks, now);

        Record(RoundStage.LeaderRound, roundStartTicks, now);
    }

    private static bool ReadEnvironmentSwitch()
    {
        string? value = Environment.GetEnvironmentVariable(EnvironmentVariable);
        return value is not null && (value == "1" || value.Equals("true", StringComparison.OrdinalIgnoreCase));
    }
}
