
namespace Kommander.Data;

/// <summary>
/// Leader-side diagnostic view of one follower's snapshot-transfer condition on a partition,
/// returned by <see cref="IRaft.GetSnapshotStatuses"/>. An entry exists only while the leader has
/// an in-flight transfer to the follower or has recorded transfer failures for it — a healthy
/// partition reports an empty list. This is the queryable form of "follower X requires a snapshot
/// and the last attempt failed with …", which previously existed only as a log line per heartbeat.
/// <para>
/// Diagnostic only: values are point-in-time reads of state mutated concurrently by background
/// transfer tasks, and the entry disappears as soon as a transfer to the follower succeeds. Never
/// gate correctness on it.
/// </para>
/// </summary>
public sealed class RaftSnapshotStatus
{
    /// <summary>The follower endpoint the leader is trying to seed.</summary>
    public required string FollowerEndpoint { get; init; }

    /// <summary>Consecutive failed transfer attempts in the current episode (0 while a first attempt is in flight).</summary>
    public int FailedAttempts { get; init; }

    /// <summary>Human-readable message of the most recent failure, or null when none has been recorded.</summary>
    public string? LastError { get; init; }

    /// <summary>
    /// True when the failure cause is permanent for this configuration — no snapshot transfer is
    /// registered, or the application rejected the export as unsupported
    /// (<see cref="NotSupportedException"/>). The follower cannot catch up until the operator
    /// registers a working transfer (see <see cref="IRaft.RegisterPartitionStateTransfer"/>);
    /// retries are paced at the maximum backoff rather than stopped, so registration is picked up.
    /// </summary>
    public bool Unproducible { get; init; }

    /// <summary>True while a transfer to this follower is currently in flight.</summary>
    public bool InFlight { get; init; }

    /// <summary>
    /// How long the in-flight transfer has been running, or null when none is in flight. A value
    /// far above the configured <c>SnapshotTransferStepTimeout</c> should be impossible; a value
    /// near it points at a slow or hung step about to be abandoned and retried.
    /// </summary>
    public TimeSpan? InFlightFor { get; init; }

    /// <summary>When the current failure episode started (UTC), or null if nothing has failed.</summary>
    public DateTimeOffset? FirstFailureAt { get; init; }

    /// <summary>When the most recent failure was recorded (UTC), or null if nothing has failed.</summary>
    public DateTimeOffset? LastFailureAt { get; init; }

    /// <summary>Time remaining before the next transfer attempt is allowed; zero when not backing off.</summary>
    public TimeSpan RetryBackoffRemaining { get; init; }

    /// <summary>
    /// True when the rescue convergence breaker is tripped for this follower: every recent
    /// snapshot install reported success, yet the follower returned below the WAL compaction floor
    /// each time, so the leader has stopped escalating (probes excepted — see
    /// <see cref="RaftConfiguration.SnapshotRescueProbeInterval"/>). A follower in this state
    /// cannot be repaired by re-sending snapshots and needs operator attention; note that
    /// <see cref="FailedAttempts"/> stays 0 here because nothing in the loop ever fails.
    /// </summary>
    public bool RescueNotConverging { get; init; }

    /// <summary>
    /// Consecutive non-converging rescue cycles observed for this follower (a successful install
    /// followed by another below-floor escalation). Resets when the follower's refusals stop.
    /// </summary>
    public int ConsecutiveRescueCycles { get; init; }
}
